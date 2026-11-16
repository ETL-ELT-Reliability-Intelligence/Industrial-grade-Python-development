"""Verify only the exact committed batches of one current Airflow run."""

import json
import os
import sys
from urllib.parse import quote

from contracts.events import DatasetIngestedEvent, parse_event
from contracts.state import PipelineRunStatus
from services.ingestion.storage import store_from_env
from storage.postgres import connect, PostgresQualityRepository
from scripts.demo import request_json


def probe(run_id, expected, final):
    store = store_from_env()
    checked = []
    with connect(os.environ["DATABASE_URL"]) as connection:
        pipeline = connection.execute("SELECT status FROM pipeline_runs WHERE pipeline_id = %s AND run_id = %s",
                                      ("olist_ingestion", run_id)).fetchone()
        if not pipeline or pipeline["status"] == PipelineRunStatus.RUNNING.value:
            return {"ready": False}
        if pipeline["status"] != PipelineRunStatus.SUCCESS.value:
            raise RuntimeError(f"Pipeline {run_id}: {pipeline['status']}")
        for dataset, count in expected.items():
            committed = store.load_run(dataset, run_id)
            if committed is None:
                raise RuntimeError(f"Successful DAG has no manifest: {run_id}/{dataset}")
            event = next(e for e in map(parse_event, committed) if isinstance(e, DatasetIngestedEvent))
            if event.dataset_id != dataset or event.records != count:
                raise RuntimeError(f"Unexpected manifest count/ownership for {dataset}")
            batch = connection.execute("SELECT records FROM batches WHERE dataset_id = %s AND batch_id = %s",
                                       (dataset, event.batch_id)).fetchone()
            if batch is None:
                return {"ready": False}
            if batch["records"] != count:
                raise RuntimeError(f"Wrong stored row count for {dataset}/{event.batch_id}")
            evaluations = PostgresQualityRepository(connection).list_evaluations(dataset, event.batch_id)
            if not evaluations:
                return {"ready": False}
            evaluation = evaluations[0]
            if evaluation.state.value in {"PENDING", "RUNNING"} or not evaluation.published:
                return {"ready": False}
            if evaluation.state.value != "COMPLETED":
                raise RuntimeError(f"Evaluation failed: {evaluation.evaluation_id}")
            if final and dataset == "order_payments":
                volume = [c for c in evaluation.checks if c.spec.kind.value == "row_count"]
                if not volume or any(c.result is None or c.result.status.value != "FAIL" for c in volume):
                    raise RuntimeError("Expected payment row_count FAIL")
                if evaluation.decision.status.value != "WARN":
                    raise RuntimeError("Expected payment policy WARN")
            path = f"http://localhost:8000/api/v1/datasets/{quote(dataset, safe='')}"
            api = request_json(path + "/evaluations/" + quote(evaluation.evaluation_id, safe=""))
            if api["scope_id"] != event.batch_id or api["evaluation"] != evaluation.model_dump(mode="json"):
                raise RuntimeError("API evidence differs from persisted evaluation")
            batches = request_json(path + "/batches?limit=200")["items"]
            if not any(b["batch_id"] == event.batch_id and b["records"] == count for b in batches):
                raise RuntimeError("Current batch is missing from API")
            checked.append({"dataset_id": dataset, "batch_id": event.batch_id,
                            "evaluation_id": evaluation.evaluation_id, "records": count})
        runs = request_json("http://localhost:8000/api/v1/pipeline-runs?pipeline_id=olist_ingestion&limit=200")["items"]
        if not any(run["run_id"] == run_id and run["status"] == PipelineRunStatus.SUCCESS.value for run in runs):
            raise RuntimeError("Current successful pipeline is missing from API")
    return {"ready": True, "run_id": run_id, "batches": checked}


if __name__ == "__main__":
    print(json.dumps(probe(sys.argv[1], json.loads(sys.argv[2]), sys.argv[3] == "true")))
