"""Bounded, isolated Olist acceptance scenario shared by both shell wrappers."""

import argparse
import base64
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def request_json(url, *, method="GET", payload=None, auth=False):
    headers = {"Content-Type": "application/json"}
    if auth:
        headers["Authorization"] = "Basic " + base64.b64encode(b"admin:admin").decode()
    request = Request(url, data=json.dumps(payload).encode() if payload is not None else None,
                      headers=headers, method=method)
    with urlopen(request, timeout=10) as response:
        return json.load(response)


def wait_for(label, probe, timeout):
    """Retry only an explicit pending result; errors propagate immediately."""
    deadline = time.monotonic() + timeout
    while True:
        result = probe()
        if result is not None:
            return result
        if time.monotonic() >= deadline:
            raise TimeoutError(f"{label}: timeout after {timeout}s")
        time.sleep(min(2, max(0, deadline - time.monotonic())))


def run_state(document, run_id):
    if document.get("dag_run_id") != run_id:
        raise RuntimeError(f"Airflow returned a different run for {run_id}")
    state = document.get("state")
    if state == "success":
        return document
    if state == "failed":
        raise RuntimeError(f"DAG failed: {run_id}")
    if state not in {"queued", "running"}:
        raise RuntimeError(f"Unexpected DAG state for {run_id}: {state!r}")
    return None


class Demo:
    def __init__(self, args):
        self.args = args
        self.project = f"reliability-demo-{uuid4().hex[:10]}"
        self.env = dict(os.environ, COMPOSE_PROJECT_NAME=self.project)
        for name in ("POSTGRES_PORT", "KAFKA_PORT", "S3_PORT", "S3_CONSOLE_PORT",
                     "BACKEND_PORT", "AIRFLOW_PORT", "KAFKA_UI_PORT", "ADMINER_PORT"):
            self.env[name] = "0"
        self.output = ROOT / "test-results" / self.project
        self.output.mkdir(parents=True)
        self.stage = "preflight"

    def compose(self, *arguments, timeout=60, check=True):
        result = subprocess.run(["docker", "compose", *arguments], cwd=ROOT, env=self.env,
                                capture_output=True, text=True, encoding="utf-8", errors="replace",
                                timeout=timeout)
        with (self.output / "commands.log").open("a", encoding="utf-8") as log:
            log.write(f"\n[{self.stage}] compose {' '.join(arguments)}\n" + result.stdout + result.stderr)
        if check and result.returncode:
            raise RuntimeError(f"compose {' '.join(arguments)}: {result.stderr[-4000:]} {result.stdout[-2000:]}")
        return result

    def address(self, service, port):
        binding = self.compose("port", service, str(port)).stdout.strip().splitlines()[0]
        return f"http://127.0.0.1:{binding.rsplit(':', 1)[1]}"

    def diagnostics(self):
        for name, args in (
            ("containers", ("ps", "--all")),
            ("logs", ("logs", "--no-color", "--tail", "300")),
            ("dag-import-errors", ("exec", "-T", "airflow", "airflow", "dags", "list-import-errors", "-o", "json")),
        ):
            try:
                result = self.compose(*args, check=False, timeout=30)
                (self.output / f"{name}.txt").write_text(result.stdout + result.stderr, encoding="utf-8")
                if name != "logs":
                    print(result.stdout[-4000:], flush=True)
            except Exception as error:
                (self.output / f"{name}.txt").write_text(str(error), encoding="utf-8")

    def database_outage(self, backend):
        """Queue a real event while this disposable project's database is stopped."""
        self.stage = "PostgreSQL outage and queued-event recovery"
        self.compose("stop", "--timeout", "3", "postgres")
        try:
            try:
                request_json(backend + "/ready")
            except HTTPError as error:
                if error.code != 503:
                    raise
            else:
                raise RuntimeError("Backend reported ready while PostgreSQL was stopped")
            result = self.compose("exec", "-T", "-e", "KAFKA_BOOTSTRAP_SERVERS=kafka:19092", "backend",
                                  "python", "-m", "scripts.outage_event")
            event = json.loads(result.stdout)
        finally:
            self.compose("up", "-d", "--wait", "--wait-timeout", "60", "postgres", timeout=90)

        def recovered():
            try:
                path = backend + "/api/v1/datasets/" + quote(event["dataset_id"], safe="")
                data = request_json(path + "/evaluations?scope_id=" + quote(event["batch_id"], safe=""))
            except HTTPError as error:
                if error.code in {404, 503}:
                    return None
                raise
            if not data["items"] or data["items"][0]["state"] in {"PENDING", "RUNNING"}:
                return None
            if len(data["items"]) != 1 or data["items"][0]["state"] != "COMPLETED":
                raise RuntimeError("Queued event was duplicated or did not recover")
            detail = request_json(path + "/evaluations/" + quote(data["items"][0]["evaluation_id"], safe=""))
            return detail if detail["evaluation"]["published"] else None

        evidence = wait_for(self.stage, recovered, self.args.quality_timeout)
        (self.output / "postgres-outage.json").write_text(json.dumps(evidence, indent=2), encoding="utf-8")

    def execute(self):
        from services.ingestion.olist_sample import write_sample
        from services.ingestion.sources import OlistSource

        windows = self.args.windows or ([0, 1, 2, 3] if self.args.mode == "synthetic" else [16, 17, 18, 19])
        if len(windows) != 4 or windows != sorted(set(windows)) or min(windows) < 0:
            raise ValueError("Exactly four distinct increasing nonnegative windows are required")
        data = ROOT / "data" / self.project if self.args.mode == "synthetic" else Path(self.args.olist_dir).resolve()
        if self.args.mode == "synthetic":
            if max(windows) >= 12:
                raise ValueError("Synthetic generator supports windows 0..11")
            write_sample(data, months=max(windows) + 1, orders_per_month=300, seed=0)
        expected = []
        for index, window in enumerate(windows):
            batches = {}
            for dataset in OlistSource.FILES:
                factor = 0.4 if index == 3 and dataset == "order_payments" else 1.0
                batch = OlistSource(dataset, data, window, factor).fetch()
                if not batch.records:
                    raise ValueError(f"Empty period: {dataset}, window {window}")
                batches[dataset] = len(batch.records)
            expected.append(batches)
        self.env["OLIST_HOST_DIR"] = str(data)
        manifest = {"project": self.project, "mode": self.args.mode, "windows": windows, "runs": []}
        self.stage = "build"
        print(f"Project: {self.project}; artifacts: {self.output}", flush=True)
        self.compose("build", "backend", "airflow", timeout=self.args.build_timeout)
        self.stage = "pull infrastructure images"
        self.compose("pull", "kafka", "postgres", "rustfs", timeout=self.args.build_timeout)
        self.stage = "service readiness"
        self.compose("up", "-d", "--wait", "--wait-timeout", str(self.args.start_timeout),
                     "backend", "stream-processor", "airflow", "airflow-scheduler",
                     timeout=self.args.start_timeout + 30)
        airflow = self.address("airflow", 8080)
        backend = self.address("backend", 8000)
        manifest.update(airflow=airflow, backend=backend)
        self.stage = "scheduler heartbeat"

        def scheduler():
            health = request_json(airflow + "/health")
            return health if health.get("scheduler", {}).get("status") == "healthy" else None

        wait_for(self.stage, scheduler, self.args.start_timeout)
        self.stage = "DAG discovery"
        dag = airflow + "/api/v1/dags/olist_ingestion"

        def discover():
            try:
                document = request_json(dag, auth=True)
                if document.get("has_import_errors"):
                    raise RuntimeError("olist_ingestion has import errors")
                return document
            except HTTPError as error:
                if error.code == 404:
                    return None
                raise

        wait_for(self.stage, discover, self.args.start_timeout)
        request_json(dag, method="PATCH", payload={"is_paused": False}, auth=True)
        for index, window in enumerate(windows):
            run_id = f"{self.project}-{index + 1}"
            self.stage = f"DAG {run_id}"
            conf = {"window": window, "volume_factors": {"order_payments": 0.4} if index == 3 else {}}
            request_json(dag + "/dagRuns", method="POST", payload={"dag_run_id": run_id, "conf": conf}, auth=True)

            def completed():
                try:
                    return run_state(request_json(dag + "/dagRuns/" + quote(run_id, safe=""), auth=True), run_id)
                except HTTPError as error:
                    if error.code == 404:
                        raise RuntimeError(f"Triggered run disappeared: {run_id}") from error
                    raise

            wait_for(self.stage, completed, self.args.run_timeout)
            self.stage = f"quality and API for {run_id}"

            def quality():
                result = self.compose("exec", "-T", "backend", "python", "-m", "scripts.demo_probe",
                                      run_id, json.dumps(expected[index]), str(index == 3).lower())
                document = json.loads(result.stdout)
                return document if document["ready"] else None

            checked = wait_for(self.stage, quality, self.args.quality_timeout)
            manifest["runs"].append(checked)
            (self.output / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
            print(f"Verified {run_id}: 4 batches, completed evaluations and API", flush=True)
        if self.args.recovery:
            self.stage = "real-service recovery tests"
            result = self.compose(
                "exec", "-T", "-e", "TEST_DATABASE_URL=postgresql://reliability:reliability@postgres:5432/reliability_test",
                "-e", "KAFKA_BOOTSTRAP_SERVERS=kafka:19092", "-e", "CI_REQUIRE_NO_SKIPS=1",
                "backend", "python", "-m", "pytest", "tests/integration/test_recovery.py", "-q", "--tb=short",
                "-p", "no:cacheprovider", timeout=300, check=False,
            )
            (self.output / "recovery.txt").write_text(result.stdout + result.stderr, encoding="utf-8")
            if result.returncode:
                raise RuntimeError(result.stdout[-5000:] + result.stderr[-2000:])
            print(result.stdout, flush=True)
            self.database_outage(backend)
        print(f"PASS: 4 DAGs, 16 batches/evaluations; payment volume FAIL, policy WARN. Dashboard: {backend}")
        print(f"Stop this demo: docker compose -p {self.project} down")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=["synthetic", "olist"], default="synthetic")
    parser.add_argument("--olist-dir", default="data/olist")
    parser.add_argument("--windows", nargs=4, type=int)
    parser.add_argument("--build-timeout", type=int, default=1200)
    parser.add_argument("--start-timeout", type=int, default=300)
    parser.add_argument("--run-timeout", type=int, default=int(os.environ.get("RUN_TIMEOUT", "600")))
    parser.add_argument("--quality-timeout", type=int, default=120)
    parser.add_argument("--cleanup", action="store_true", help="Remove only this newly generated Compose project and its volumes")
    parser.add_argument("--recovery", action="store_true", help="Run fault injection against this project's disposable test database")
    args = parser.parse_args()
    if min(args.build_timeout, args.start_timeout, args.run_timeout, args.quality_timeout) <= 0:
        parser.error("timeouts must be positive")
    demo = Demo(args)
    try:
        demo.execute()
        return 0
    except (Exception, KeyboardInterrupt) as error:
        print(f"FAIL [{demo.stage}]: {type(error).__name__}: {error}", file=sys.stderr, flush=True)
        return 1
    finally:
        demo.diagnostics()
        if args.cleanup:
            demo.compose("down", "--volumes", "--remove-orphans", timeout=120, check=False)


if __name__ == "__main__":
    sys.exit(main())
