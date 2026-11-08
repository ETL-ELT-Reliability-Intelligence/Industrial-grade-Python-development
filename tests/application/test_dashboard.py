"""Browser → real ASGI app; PostgreSQL scenario additionally uses actual storage."""

import os
import sys
from contextlib import contextmanager
from urllib.parse import urlsplit

import pytest
from fastapi.testclient import TestClient

from contracts.quality import DecisionReason, QualityDecision, QualityStatus
from services.backend.main import create_app
from services.backend.models import Dataset
from services.backend.postgres import open_repository
from tests.application.support import NOW, MemoryRepository
from tests.application.evaluation_support import evaluation_record

playwright_api = pytest.importorskip("playwright.sync_api")
expect = playwright_api.expect


@pytest.fixture(scope="module")
def browser():
    with playwright_api.sync_playwright() as playwright:
        options = {"headless": True}
        if executable := os.environ.get("PLAYWRIGHT_EXECUTABLE_PATH"):
            options["executable_path"] = executable
        elif sys.platform == "win32":
            options["channel"] = "msedge"
        browser = playwright.chromium.launch(**options)
        yield browser
        browser.close()


@contextmanager
def browser_page(browser_context, repository_factory, theme=None):
    with browser_context(locale="ru-RU", timezone_id="Europe/Moscow") as context:
        if theme is not None:
            context.add_init_script(
                "try { if (localStorage.getItem('ri.theme') === null) "
                f"localStorage.setItem('ri.theme', '{theme}'); }} catch {{}}"
            )
        page = context.new_page()
        page.set_default_timeout(5000)
        with TestClient(create_app(repository_factory=repository_factory)) as client:

            def serve(route):
                url = urlsplit(route.request.url)
                response = client.get(url.path + (f"?{url.query}" if url.query else ""))
                route.fulfill(
                    status=response.status_code,
                    content_type=response.headers.get("content-type", "text/plain"),
                    body=response.content,
                )

            page.route("http://application.test/**", serve)
            yield page


@pytest.fixture(params=("classic", "cs", "wizard"))
def theme(request):
    return request.param


@pytest.fixture
def page(browser_context, theme):
    with browser_page(browser_context, MemoryRepository().factory, theme) as page:
        yield page


def test_select_dataset_scope_and_evidence(page):
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.goto("http://application.test/")
    page.get_by_role("button", name="orders", exact=True).click()
    expect(page.locator("#dataset h2")).to_have_text("orders")
    expect(page.locator("#batches")).to_contain_text("batch-1")
    expect(page.locator("#quality")).to_contain_text("Выберите область")
    page.get_by_role("button", name="window-1", exact=True).click()
    expect(page.locator("#quality")).to_contain_text("BLOCK")
    expect(page.locator("#quality .status-unknown")).to_have_text("UNKNOWN")
    expect(page.locator("#quality table")).to_contain_text("Нет значения")
    expect(page.locator("#quality table")).to_contain_text("0")
    expect(page.locator("#quality")).to_contain_text("below_minimum")
    assert "dataset=orders" in page.url and "scope=window-1" in page.url
    assert not errors


def test_batch_source_is_displayed_as_untrusted_text(browser_context, theme):
    repository = MemoryRepository()
    source = 'file:///raw/<img src=x onerror="window.injected=true">/batch.jsonl'
    repository.batches[0] = repository.batches[0].model_copy(
        update={"object_key": source}
    )
    with browser_page(browser_context, repository.factory, theme) as page:
        page.goto("http://application.test/?dataset=orders")
        expect(page.locator("#batches")).to_contain_text(source)
        assert page.locator("#batches img, #batches a").count() == 0
        assert page.evaluate("window.injected") is None


def test_missing_batch_source_is_explicit(page):
    page.goto("http://application.test/?dataset=orders")
    expect(page.locator("#batches")).to_contain_text("Не указан")


def test_deep_link_back_and_empty_states(page):
    page.goto("http://application.test/?dataset=orders&scope=window-1")
    expect(page.locator("#quality .status-block")).to_have_text("BLOCK")
    page.get_by_role("button", name="empty", exact=True).click()
    expect(page.locator("#batches")).to_contain_text("нет сохранённых загрузок")
    expect(page.locator("#scopes")).to_contain_text("нет областей")
    expect(page.locator("#quality")).to_contain_text("Выберите область")
    page.go_back()
    expect(page.locator("#quality .status-block")).to_have_text("BLOCK")
    page.goto("http://application.test/?dataset=orders&scope=absent")
    expect(page.locator("#quality")).to_contain_text("Решение ещё не сохранено")
    expect(page.locator("#quality")).to_contain_text(
        "Проверки для этой области не сохранены"
    )
    page.goto("http://application.test/?dataset=missing")
    expect(page.locator("#dataset [role=alert]")).to_contain_text("Dataset не найден")


def test_error_retry_and_refresh(page):
    path = "http://application.test/api/v1/datasets?*"

    def fail(route):
        route.fulfill(
            status=503, content_type="application/json", body='{"detail":"Unavailable"}'
        )

    page.route(path, fail)
    page.goto("http://application.test/")
    expect(page.locator("#catalog [role=alert]")).to_contain_text(
        "Хранилище недоступно"
    )
    page.unroute(path, fail)
    page.locator("#catalog").get_by_role("button", name="Повторить").click()
    expect(page.get_by_role("button", name="orders", exact=True)).to_be_visible()
    page.get_by_role("button", name="Обновить данные").click()
    expect(page.locator("#refreshed-at")).to_contain_text("не свежесть исходных данных")


def test_catalog_pagination(browser_context, theme):
    repository = MemoryRepository()
    repository.datasets = [
        Dataset(dataset_id=f"dataset-{i:02}", created_at=NOW) for i in range(51)
    ]
    with browser_page(browser_context, repository.factory, theme) as page:
        page.goto("http://application.test/")
        expect(page.locator(".catalog-item")).to_have_count(50)
        page.locator("#catalog").get_by_role("button", name="Далее").click()
        expect(page.locator(".catalog-item")).to_have_count(1)
        expect(page.locator(".catalog-item .incident-title")).to_have_text("dataset-50")
        page.get_by_label("Оформление", exact=True).select_option(
            "wizard" if theme != "wizard" else "cs"
        )
        expect(page.locator(".catalog-item")).to_have_count(1)
        expect(page.locator(".catalog-item .incident-title")).to_have_text("dataset-50")
        expect(page.locator("#catalog .pager")).to_contain_text("Страница 2")
        expect(
            page.locator("#catalog").get_by_role("button", name="Далее")
        ).to_be_disabled()
        page.locator("#catalog").get_by_role("button", name="Назад").click()
        expect(page.locator(".catalog-item")).to_have_count(50)


def test_batches_and_scopes_pagination(browser_context, theme):
    repository = MemoryRepository()
    repository.batches = [
        repository.batches[0].model_copy(update={"batch_id": f"batch-{i:02}"})
        for i in range(51)
    ]
    quality = repository.qualities[("orders", "window-1")]
    repository.qualities = {
        ("orders", f"window-{i:02}"): quality.model_copy(
            update={"scope_id": f"window-{i:02}"}
        )
        for i in range(51)
    }
    with browser_page(browser_context, repository.factory, theme) as page:
        page.goto("http://application.test/?dataset=orders")
        for panel in ("batches", "scopes"):
            root = page.locator(f"#{panel}")
            expect(root.locator("tbody tr")).to_have_count(50)
            root.get_by_role("button", name="Далее").click()
            expect(root.locator("tbody tr")).to_have_count(1)
            expect(root.get_by_role("button", name="Далее")).to_be_disabled()
            root.get_by_role("button", name="Назад").click()
            expect(root.locator("tbody tr")).to_have_count(50)


def test_late_response_does_not_replace_new_selection(page):
    held = []
    page.route(
        "http://application.test/api/v1/datasets/orders/quality?*",
        lambda route: held.append(route),
    )
    page.goto("http://application.test/?dataset=orders&scope=window-1")
    expect(page.locator("#quality")).to_contain_text("Загрузка")
    page.get_by_role("button", name="empty", exact=True).click()
    expect(page.locator("#dataset h2")).to_have_text("empty")
    assert held
    held[0].fulfill(
        status=200,
        content_type="application/json",
        body='{"scope_id":"STALE","checks":[],"decision":null,"decided_at":null}',
    )
    expect(page.locator("#quality")).to_contain_text("Выберите область")
    expect(page.locator("#quality")).not_to_contain_text("STALE")


def test_mobile_keyboard_and_untrusted_text(browser_context, theme):
    repository = MemoryRepository()
    malicious_id = '<img src=x onerror="window.injected=true">'
    repository.datasets.append(Dataset(dataset_id=malicious_id, created_at=NOW))
    with browser_page(browser_context, repository.factory, theme) as page:
        page.set_viewport_size({"width": 390, "height": 844})
        page.goto("http://application.test/?dataset=orders&scope=window-1")
        expect(page.locator("#quality .status-block")).to_be_visible()
        expect(
            page.get_by_role("button", name=malicious_id, exact=True)
        ).to_be_visible()
        assert page.locator("#catalog img").count() == 0
        assert page.evaluate("window.injected") is None
        assert page.evaluate(
            "document.documentElement.scrollWidth <= window.innerWidth"
        )
        choice = page.get_by_role("button", name="empty", exact=True)
        choice.focus()
        page.keyboard.press("Enter")
        expect(page.locator("#dataset h2")).to_have_text("empty")


def test_postgres_change_appears_after_refresh(browser_context, theme):
    from tests.storage.support import TEST_DATABASE_URL, migrate

    if not TEST_DATABASE_URL:
        pytest.skip("Set TEST_DATABASE_URL to an expendable test database")
    from uuid import uuid4

    from storage.postgres import (
        PostgresDatasetRepository,
        PostgresQualityRepository,
        connect,
    )
    from tests.application.support import evidence

    migrate(TEST_DATABASE_URL)
    dataset_id = f"browser-{uuid4().hex}"
    with connect(TEST_DATABASE_URL) as connection:
        datasets = PostgresDatasetRepository(connection)
        quality = PostgresQualityRepository(connection)
        datasets.ensure_dataset(dataset_id)
        try:
            for result in evidence(dataset_id):
                quality.save_check_result(result)
            with browser_page(
                browser_context, lambda: open_repository(TEST_DATABASE_URL), theme
            ) as page:
                page.goto(
                    f"http://application.test/?dataset={dataset_id}&scope=window-1"
                )
                expect(page.locator("#quality")).to_contain_text(
                    "Решение ещё не сохранено"
                )
                quality.save_quality_decision(
                    dataset_id,
                    "window-1",
                    QualityDecision(
                        status=QualityStatus.WARN,
                        reasons=(
                            DecisionReason(
                                code="upstream_review",
                                message="Updated through storage repository",
                            ),
                        ),
                    ),
                    NOW,
                )
                page.get_by_role("button", name="Обновить данные").click()
                expect(page.locator("#quality .decision .status-warn")).to_have_text(
                    "WARN"
                )
                expect(page.locator("#quality")).to_contain_text(
                    "Updated through storage repository"
                )
        finally:
            for table in ("quality_check_results", "quality_decisions", "datasets"):
                connection.execute(
                    f"DELETE FROM {table} WHERE dataset_id = %s", (dataset_id,)
                )


def test_theme_switch_preserves_selection_without_data_requests(page):
    page.goto("http://application.test/?dataset=orders&scope=window-1&keep=yes#details")
    expect(page.locator("#quality .status-block")).to_be_visible()
    expect(page.locator('[aria-busy="true"]')).to_have_count(0)
    requests = []
    page.on(
        "request",
        lambda request: (
            requests.append(request.url) if "/api/v1/" in request.url else None
        ),
    )
    for theme in ("cs", "wizard", "classic"):
        page.get_by_label("Оформление", exact=True).select_option(theme)
        expect(page.locator("html")).to_have_attribute("data-theme", theme)
        expect(page.locator("#quality .status-block")).to_be_visible()
        assert f"theme={theme}" in page.url
        assert "dataset=orders" in page.url and "scope=window-1" in page.url
        assert "keep=yes" in page.url and page.url.endswith("#details")
    assert requests == []
    page.get_by_role("button", name="empty", exact=True).click()
    assert "theme=classic" in page.url and "keep=yes" in page.url
    assert "scope=" not in page.url
    page.go_back()
    expect(page.locator("#quality .status-block")).to_be_visible()
    expect(page.locator("html")).to_have_attribute("data-theme", "classic")


def test_theme_preferences_url_precedence_and_invalid_value(page):
    page.goto("http://application.test/")
    page.get_by_label("Оформление", exact=True).select_option("wizard")
    page.goto("http://application.test/?dataset=orders")
    expect(page.locator("html")).to_have_attribute("data-theme", "wizard")
    page.goto("http://application.test/?theme=cs&dataset=orders")
    expect(page.locator("html")).to_have_attribute("data-theme", "cs")
    page.reload()
    expect(page.locator("html")).to_have_attribute("data-theme", "cs")
    page.goto("http://application.test/?theme=removed-theme")
    expect(page.locator("html")).to_have_attribute("data-theme", "classic")
    expect(page.get_by_label("Оформление", exact=True)).to_have_value("classic")


def test_wizard_assistant_keyboard_and_persistence(page):
    page.goto("http://application.test/?theme=wizard&dataset=orders&scope=window-1")
    expect(page.locator("#quality .status-block")).to_be_visible()
    toggle = page.get_by_role("button", name="Помощник", exact=True)
    expect(page.locator("#wizard-assistant")).to_be_visible()
    toggle.focus()
    page.keyboard.press("Space")
    expect(toggle).to_have_attribute("aria-pressed", "false")
    expect(page.locator("#wizard-assistant")).to_be_hidden()
    page.reload()
    expect(page.locator("#wizard-assistant")).to_be_hidden()
    page.get_by_label("Оформление", exact=True).select_option("classic")
    expect(toggle).to_be_hidden()
    page.get_by_label("Оформление", exact=True).select_option("wizard")
    expect(page.locator("#wizard-assistant")).to_be_hidden()
    toggle.click()
    expect(page.locator("#wizard-assistant")).to_be_visible()
    page.wait_for_function(
        "document.querySelector('#wizard-assistant img').naturalWidth > 0"
    )
    expect(page.locator("#quality .status-block")).to_be_visible()


def test_default_and_unavailable_local_storage(browser_context):
    with browser_page(browser_context, MemoryRepository().factory) as page:
        page.goto("http://application.test/")
        expect(page.locator("html")).to_have_attribute("data-theme", "classic")
        page.context.add_init_script(
            "Object.defineProperty(window, 'localStorage', {get() {throw new Error('blocked');}});"
        )
        errors = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.goto("http://application.test/?theme=cs&dataset=orders&scope=window-1")
        expect(page.locator("#quality .status-block")).to_be_visible()
        page.get_by_label("Оформление", exact=True).select_option("wizard")
        expect(page.locator("html")).to_have_attribute("data-theme", "wizard")
        page.reload()
        expect(page.locator("html")).to_have_attribute("data-theme", "wizard")
        assert not errors


def history_repository():
    repository = MemoryRepository()
    repository.evaluations = [
        evaluation_record("new"),
        evaluation_record("old", completed=True),
    ]
    return repository


def test_evaluation_selection_url_history_refresh_and_theme(browser_context, theme):
    repository = history_repository()
    with browser_page(browser_context, repository.factory, theme) as page:
        page.goto(
            "http://application.test/?dataset=orders&scope=window-1&keep=yes#details"
        )
        expect(page.locator("#quality h3").first).to_have_text("Расчёт: new")
        assert "evaluation=new" in page.url
        expect(page.locator("#quality .status-pending")).to_have_count(2)
        expect(page.locator("#quality .decision")).to_contain_text(
            "Решение ещё не сохранено"
        )
        choice = page.locator("#evaluations").get_by_role(
            "button", name="old", exact=True
        )
        choice.focus()
        page.keyboard.press("Enter")
        expect(page.locator("#quality .decision .status-block")).to_have_text("BLOCK")
        expect(page.locator("#quality .status-completed")).to_have_count(2)
        assert (
            "evaluation=old" in page.url
            and "keep=yes" in page.url
            and page.url.endswith("#details")
        )
        page.go_back()
        expect(page.locator("#quality h3").first).to_have_text("Расчёт: new")
        page.go_forward()
        expect(page.locator("#quality h3").first).to_have_text("Расчёт: old")
        page.get_by_role("button", name="Обновить данные").click()
        expect(page.locator("#quality h3").first).to_have_text("Расчёт: old")
        expect(page.locator('[aria-busy="true"]')).to_have_count(0)
        requests = []
        page.on(
            "request",
            lambda request: (
                requests.append(request.url) if "/api/v1/" in request.url else None
            ),
        )
        page.get_by_label("Оформление", exact=True).select_option(
            "cs" if theme != "cs" else "wizard"
        )
        expect(page.locator("#quality h3").first).to_have_text("Расчёт: old")
        assert requests == []
        page.reload()
        expect(page.locator("#quality h3").first).to_have_text("Расчёт: old")
        page.get_by_role("button", name="empty", exact=True).click()
        assert "evaluation=" not in page.url
        expect(page.locator("#quality")).to_contain_text("Выберите область")


@pytest.mark.parametrize("evaluation_id", ["missing", "", "wrong-scope"])
def test_evaluation_invalid_link_never_falls_back(browser_context, evaluation_id):
    repository = history_repository()
    repository.evaluations.append(
        evaluation_record("wrong-scope", scope_id="another-scope")
    )
    with browser_page(browser_context, repository.factory) as page:
        requests = []
        page.on("request", lambda request: requests.append(request.url))
        page.goto(
            f"http://application.test/?dataset=orders&scope=window-1&evaluation={evaluation_id}"
        )
        expect(page.locator("#quality [role=alert]")).to_be_visible()
        expect(page.locator("#quality .decision")).to_have_count(0)
        assert not any("/quality?" in url for url in requests)


def test_evaluation_history_failure_retry_and_legacy(browser_context, theme):
    repository = history_repository()
    with browser_page(browser_context, repository.factory, theme) as page:
        pattern = "**/api/v1/datasets/orders/evaluations?*"
        page.route(
            pattern,
            lambda route: route.fulfill(status=503, json={"detail": "Unavailable"}),
        )
        requests = []
        page.on("request", lambda request: requests.append(request.url))
        page.goto("http://application.test/?dataset=orders&scope=window-1")
        expect(page.locator("#evaluations [role=alert]")).to_contain_text(
            "Хранилище недоступно"
        )
        expect(page.locator("#quality .decision")).to_have_count(0)
        assert not any("/quality?" in url for url in requests)
        page.unroute(pattern)
        page.locator("#evaluations").get_by_role("button", name="Повторить").click()
        expect(page.locator("#quality h3").first).to_have_text("Расчёт: new")
        page.goto("http://application.test/?dataset=orders&scope=absent")
        expect(page.locator("#quality")).to_contain_text("Истории расчётов нет")


def test_evaluation_pagination_preserves_selected_detail(browser_context, theme):
    repository = history_repository()
    repository.evaluations = [
        evaluation_record(f"evaluation-{i:02}") for i in range(51)
    ]
    with browser_page(browser_context, repository.factory, theme) as page:
        page.goto("http://application.test/?dataset=orders&scope=window-1")
        expect(page.locator("#evaluations tbody tr")).to_have_count(50)
        page.locator("#evaluations").get_by_role("button", name="Далее").click()
        expect(page.locator("#evaluations tbody tr")).to_have_count(1)
        expect(page.locator("#quality h3").first).to_have_text("Расчёт: evaluation-00")
        expect(
            page.locator("#evaluations").get_by_role("button", name="Далее")
        ).to_be_disabled()
        page.locator("#evaluations").get_by_role("button", name="evaluation-50").click()
        expect(page.locator("#quality h3").first).to_have_text("Расчёт: evaluation-50")
        page.locator("#evaluations").get_by_role("button", name="Назад").click()
        expect(page.locator("#evaluations tbody tr")).to_have_count(50)
        expect(page.locator("#quality h3").first).to_have_text("Расчёт: evaluation-50")


def test_evaluation_late_detail_does_not_replace_dataset(browser_context, theme):
    repository = history_repository()
    with browser_page(browser_context, repository.factory, theme) as page:
        held = []
        page.route(
            "**/datasets/orders/evaluations/new", lambda route: held.append(route)
        )
        with page.expect_request("**/evaluations/new"):
            page.goto("http://application.test/?dataset=orders&scope=window-1")
        expect(page.locator("#evaluations tbody tr")).to_have_count(2)
        page.get_by_role("button", name="empty", exact=True).click()
        expect(page.locator("#dataset h2")).to_have_text("empty")
        assert held
        held[0].fulfill(
            status=200, json=repository.evaluations[0].model_dump(mode="json")
        )
        expect(page.locator("#quality")).to_contain_text("Выберите область")
        expect(page.locator("#quality")).not_to_contain_text("Расчёт: new")


def test_evaluation_diagnostics_mobile_and_untrusted_text(browser_context, theme):
    from contracts.evaluation import Attempt, ExecutionStatus
    from contracts.quality_config import CheckSpec
    from contracts.quality import CheckKind

    repository = history_repository()
    record = repository.evaluations[0]
    injection = '<img src=x onerror="window.injected=true">'
    skipped_spec = CheckSpec(key="nulls", kind=CheckKind.NULL_RATE, column="email")
    from contracts.evaluation import CheckExecution

    skipped = CheckExecution(
        check_id="orders.nulls",
        spec=skipped_spec,
        state=ExecutionStatus.SKIPPED,
        code="insufficient_history",
        message="Недостаточно истории",
    )
    failed = record.evaluation.checks[0].model_copy(
        update={
            "state": ExecutionStatus.ERROR,
            "code": "source_timeout",
            "message": injection,
            "attempts": (
                Attempt(
                    number=1,
                    started_at=NOW,
                    finished_at=NOW,
                    code="source_timeout",
                    message=injection,
                ),
            ),
        }
    )
    suite = record.evaluation.suite.model_copy(
        update={"checks": (failed.spec, skipped_spec)}
    )
    evaluation = record.evaluation.model_copy(
        update={
            "checks": (failed, skipped),
            "suite": suite,
            "config_hash": suite.fingerprint,
        }
    )
    repository.evaluations[0] = record.model_copy(update={"evaluation": evaluation})
    with browser_page(browser_context, repository.factory, theme) as page:
        page.set_viewport_size({"width": 390, "height": 844})
        page.goto("http://application.test/?dataset=orders&scope=window-1")
        expect(page.locator("#quality .status-error")).to_contain_text("ERROR")
        expect(page.locator("#quality .status-skipped")).to_contain_text("SKIPPED")
        expect(page.locator("#quality")).to_contain_text("Результата нет")
        expect(page.locator("#quality")).to_contain_text("Недостаточно истории")
        expect(page.locator("#quality")).to_contain_text(injection)
        assert page.locator("#quality img").count() == 0
        assert page.evaluate("window.injected") is None
        assert page.locator("#evaluations > .table-wrap").evaluate(
            "element => element.scrollWidth > element.clientWidth"
        )
        summary = (
            page.locator("#quality summary").filter(has_text="Эталон сравнения").first
        )
        summary.focus()
        page.keyboard.press("Enter")
        expect(page.locator("#quality")).to_contain_text("historical-batch")
        assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")


def test_postgres_evaluation_execution_appears_after_refresh(browser_context, theme):
    from tests.storage.support import TEST_DATABASE_URL, migrate

    if not TEST_DATABASE_URL:
        pytest.skip("Set TEST_DATABASE_URL to an expendable test database")
    from uuid import uuid4
    from contracts.evaluation import ExecutionStatus
    from storage.postgres import connect, PostgresQualityRepository

    migrate(TEST_DATABASE_URL)
    dataset_id = f"evaluation-browser-{uuid4().hex}"
    record = evaluation_record(dataset_id=dataset_id)
    with connect(TEST_DATABASE_URL) as connection:
        quality = PostgresQualityRepository(connection)
        quality.save_evaluation(record.evaluation)
        try:
            with browser_page(
                browser_context, lambda: open_repository(TEST_DATABASE_URL), theme
            ) as page:
                page.goto(
                    f"http://application.test/?dataset={dataset_id}&scope=window-1"
                )
                expect(page.locator("#quality h3").first).to_have_text(
                    "Расчёт: evaluation-1"
                )
                expect(page.locator("#quality .status-pending")).to_have_count(2)
                execution = record.evaluation.checks[0].model_copy(
                    update={
                        "state": ExecutionStatus.ERROR,
                        "code": "read_error",
                        "message": "Изменение через storage",
                    }
                )
                quality.save_execution("evaluation-1", execution)
                page.get_by_role("button", name="Обновить данные").click()
                expect(page.locator("#quality .status-error")).to_contain_text("ERROR")
                expect(page.locator("#quality")).to_contain_text(
                    "Изменение через storage"
                )
                assert "evaluation=evaluation-1" in page.url
        finally:
            connection.execute(
                "DELETE FROM quality_evaluations WHERE dataset_id = %s", (dataset_id,)
            )
            connection.execute(
                "DELETE FROM datasets WHERE dataset_id = %s", (dataset_id,)
            )


def pipeline_repository():
    from datetime import timedelta

    from contracts.state import PipelineRun, PipelineRunStatus

    repository = MemoryRepository()
    repository.pipeline_runs = [
        PipelineRun(
            pipeline_id="orders/daily",
            run_id="active-run",
            status=PipelineRunStatus.RUNNING,
            started_at=NOW + timedelta(hours=2),
        ),
        PipelineRun(
            pipeline_id="orders/daily",
            run_id="failed-run",
            status=PipelineRunStatus.FAILED,
            started_at=NOW + timedelta(hours=1),
            finished_at=NOW + timedelta(hours=1, minutes=5),
            error="Источник недоступен",
        ),
        PipelineRun(
            pipeline_id="other",
            run_id="successful-run",
            status=PipelineRunStatus.SUCCESS,
            started_at=NOW,
            finished_at=NOW + timedelta(minutes=1),
        ),
    ]
    return repository


def test_pipeline_navigation_history_theme_and_refresh(browser_context, theme):
    repository = pipeline_repository()
    with browser_page(browser_context, repository.factory, theme) as page:
        errors = []
        requests = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.goto("http://application.test/?dataset=orders")
        page.get_by_role("link", name="Запуски pipelines", exact=True).click()
        expect(page.locator("html")).to_have_attribute("data-theme", theme)
        expect(page.locator("#pipeline-runs")).to_contain_text("Выберите pipeline")
        page.get_by_role("button", name="orders/daily", exact=True).focus()
        page.keyboard.press("Enter")
        expect(page.locator("#pipeline-runs tbody tr")).to_have_count(2)
        expect(page.locator("#pipeline-runs")).to_contain_text("RUNNING")
        expect(page.locator("#pipeline-runs")).to_contain_text("Ещё выполняется")
        expect(page.locator("#pipeline-runs")).to_contain_text("Источник недоступен")
        page.get_by_role("button", name="other", exact=True).click()
        expect(page.locator("#pipeline-runs")).to_contain_text("SUCCESS")
        page.go_back()
        expect(page.locator("#pipeline-heading")).to_have_text("Pipeline: orders/daily")
        page.go_forward()
        expect(page.locator("#pipeline-heading")).to_have_text("Pipeline: other")
        page.on("request", lambda request: requests.append(request.url))
        next_theme = "cs" if theme != "cs" else "wizard"
        page.get_by_label("Оформление", exact=True).select_option(next_theme)
        expect(page.locator("html")).to_have_attribute("data-theme", next_theme)
        assert requests == []
        page.get_by_role("button", name="Обновить данные").click()
        expect(page.locator("#pipeline-runs")).to_contain_text("successful-run")
        expect(page.locator('[aria-busy="true"]')).to_have_count(0)
        page.reload()
        expect(page.locator("#pipeline-heading")).to_have_text("Pipeline: other")
        page.get_by_role("link", name="Datasets и качество", exact=True).click()
        expect(page.locator("html")).to_have_attribute("data-theme", next_theme)
        expect(page.locator("#catalog")).to_contain_text("orders")
        assert not errors


def test_pipeline_pagination_keeps_selection(browser_context, theme):
    repository = pipeline_repository()
    prototype = repository.pipeline_runs[0]
    repository.pipeline_runs = [
        prototype.model_copy(update={"run_id": f"run-{index:02}"})
        for index in range(51)
    ] + [
        prototype.model_copy(update={"pipeline_id": f"pipeline-{index:02}"})
        for index in range(51)
    ]
    with browser_page(browser_context, repository.factory, theme) as page:
        page.goto(
            "http://application.test/pipelines?pipeline=orders%2Fdaily&keep=yes#runs"
        )
        expect(page.locator("#pipelines [data-pipeline]")).to_have_count(50)
        expect(page.locator("#pipeline-runs tbody tr")).to_have_count(50)
        page.locator("#pipeline-runs").get_by_role("button", name="Далее").click()
        expect(page.locator("#pipeline-runs tbody tr")).to_have_count(1)
        expect(page.locator("#pipeline-runs")).to_contain_text("run-00")
        page.locator("#pipelines").get_by_role("button", name="Далее").click()
        expect(page.locator("#pipelines [data-pipeline]")).to_have_count(2)
        expect(page.locator("#pipeline-runs tbody tr")).to_have_count(1)
        page.get_by_role("button", name="Обновить данные").click()
        expect(page.locator("#pipeline-runs")).to_contain_text("run-00")
        expect(page.locator("#pipelines [data-pipeline]")).to_have_count(2)
        page.get_by_role("button", name="pipeline-50", exact=True).click()
        expect(page.locator("#pipeline-heading")).to_have_text("Pipeline: pipeline-50")
        expect(
            page.locator("#pipeline-runs").get_by_role("button", name="Назад")
        ).to_be_disabled()
        assert "keep=yes" in page.url and page.url.endswith("#runs")


def test_pipeline_errors_retry_empty_and_late_response(browser_context, theme):
    repository = pipeline_repository()
    with browser_page(browser_context, repository.factory, theme) as page:
        pattern = "**/api/v1/pipeline-runs?*"
        page.route(pattern, lambda route: route.fulfill(status=503, json={}))
        page.goto("http://application.test/pipelines?pipeline=orders%2Fdaily")
        expect(page.locator("#pipeline-runs [role=alert]")).to_contain_text(
            "Хранилище недоступно"
        )
        expect(page.locator("#pipeline-runs tbody tr")).to_have_count(0)
        page.unroute(pattern)
        page.locator("#pipeline-runs").get_by_role("button", name="Повторить").click()
        expect(page.locator("#pipeline-runs tbody tr")).to_have_count(2)
        held = []
        page.route(
            "**/api/v1/pipeline-runs?pipeline_id=orders%2Fdaily&*",
            lambda route: held.append(route),
        )
        with page.expect_request("**/pipeline-runs?pipeline_id=orders%2Fdaily&*"):
            page.get_by_role("button", name="Обновить данные").click()
        page.get_by_role("button", name="other", exact=True).click()
        expect(page.locator("#pipeline-runs")).to_contain_text("successful-run")
        assert held
        held[0].fulfill(
            status=200, json={"items": [], "limit": 50, "offset": 0, "has_more": False}
        )
        expect(page.locator("#pipeline-runs")).to_contain_text("successful-run")
        page.goto("http://application.test/pipelines?pipeline=missing")
        expect(page.locator("#pipeline-runs")).to_contain_text(
            "нет сохранённых запусков"
        )
        page.goto("http://application.test/pipelines?pipeline=")
        expect(page.locator("#pipeline-runs")).to_contain_text("пустой идентификатор")
        repository.pipeline_runs = []
        page.goto("http://application.test/pipelines")
        expect(page.locator("#pipelines")).to_contain_text(
            "нет pipelines с сохранёнными запусками"
        )


def test_pipeline_untrusted_text_mobile_and_direct_link(browser_context, theme):
    from urllib.parse import urlencode

    repository = pipeline_repository()
    injection = '<img src=x onerror="window.injected=true">'
    pipeline_id = "источник / ? & # '"
    repository.pipeline_runs = [
        repository.pipeline_runs[1].model_copy(
            update={"pipeline_id": pipeline_id, "run_id": injection, "error": injection}
        )
    ]
    with browser_page(browser_context, repository.factory, theme) as page:
        page.set_viewport_size({"width": 390, "height": 844})
        page.goto(
            f"http://application.test/pipelines?{urlencode({'pipeline': pipeline_id})}"
        )
        expect(page.locator("#pipeline-heading")).to_have_text(
            f"Pipeline: {pipeline_id}"
        )
        expect(page.locator("#pipeline-runs")).to_contain_text(injection)
        expect(page.locator("#pipeline-runs .status-error")).to_have_text("FAILED")
        assert page.locator("#pipeline-runs img").count() == 0
        assert page.evaluate("window.injected") is None
        assert page.locator("#pipeline-runs .table-wrap").evaluate(
            "element => element.scrollWidth > element.clientWidth"
        )
        assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")


def test_postgres_pipeline_run_changes_after_refresh(browser_context, theme):
    from datetime import timedelta
    from uuid import uuid4

    from contracts.state import PipelineRun, PipelineRunStatus
    from storage.postgres import PostgresPipelineRunRepository, connect
    from tests.storage.support import TEST_DATABASE_URL, migrate

    if not TEST_DATABASE_URL:
        pytest.skip("Set TEST_DATABASE_URL to an expendable test database")
    migrate(TEST_DATABASE_URL)
    pipeline_id = f"browser-pipeline-{uuid4().hex}"
    run = PipelineRun(
        pipeline_id=pipeline_id,
        run_id="run-1",
        status=PipelineRunStatus.RUNNING,
        started_at=NOW,
    )
    with connect(TEST_DATABASE_URL) as connection:
        storage = PostgresPipelineRunRepository(connection)
        storage.upsert_pipeline_run(run)
        try:
            with browser_page(
                browser_context, lambda: open_repository(TEST_DATABASE_URL), theme
            ) as page:
                page.goto(f"http://application.test/pipelines?pipeline={pipeline_id}")
                expect(page.locator("#pipeline-runs .status-running")).to_have_text(
                    "RUNNING"
                )
                storage.upsert_pipeline_run(
                    run.model_copy(
                        update={
                            "status": PipelineRunStatus.FAILED,
                            "finished_at": NOW + timedelta(minutes=1),
                            "error": "Сбой, сохранённый через storage",
                        }
                    )
                )
                page.get_by_role("button", name="Обновить данные").click()
                expect(page.locator("#pipeline-runs .status-error")).to_have_text(
                    "FAILED"
                )
                expect(page.locator("#pipeline-runs")).to_contain_text(
                    "Сбой, сохранённый через storage"
                )
                assert f"pipeline={pipeline_id}" in page.url
        finally:
            connection.execute(
                "DELETE FROM pipeline_runs WHERE pipeline_id = %s", (pipeline_id,)
            )


def test_batch_identifiers_and_counts_stay_readable(browser_context, theme):
    repository = MemoryRepository()
    source = "s3://raw-data/order_payments/2026-11-08-004.jsonl"
    repository.batches[0] = repository.batches[0].model_copy(
        update={"batch_id": "2026-11-08-004", "records": 2854, "object_key": source}
    )
    with browser_page(browser_context, repository.factory, theme) as page:
        page.goto("http://application.test/?dataset=orders")
        expect(page.locator("#batches tbody tr")).to_have_count(1)
        for width in (1440, 1280, 1024, 900, 768, 390):
            page.set_viewport_size({"width": width, "height": 1000})
            facts = page.locator("#batches").evaluate(
                """root => {
                    const textLines = element => {
                        const range = document.createRange();
                        range.selectNodeContents(element);
                        return [...range.getClientRects()].filter(rect => rect.width > 0).length;
                    };
                    const cells = root.querySelectorAll('tbody td');
                    const wrap = root.querySelector('.table-wrap');
                    return {
                        idLines: textLines(cells[0].firstChild),
                        countLines: textLines(cells[1].firstChild),
                        countHeaderLines: textLines(root.querySelectorAll('th')[1]),
                        tableWidth: root.querySelector('table').getBoundingClientRect().width,
                        availableWidth: wrap.clientWidth,
                        sectionWidth: root.parentElement.getBoundingClientRect().width,
                        panelWidth: document.querySelector('#quality-section').getBoundingClientRect().width,
                        schemaWidth: cells[3].getBoundingClientRect().width,
                        sourceWidth: cells[4].getBoundingClientRect().width,
                        pageFits: document.documentElement.scrollWidth <= innerWidth,
                    };
                }"""
            )
            assert facts["idLines"] == 1, (width, facts)
            assert facts["countLines"] == 1, (width, facts)
            assert facts["countHeaderLines"] == 1, (width, facts)
            assert facts["pageFits"], (width, facts)
            assert facts["schemaWidth"] < facts["sourceWidth"], (width, facts)
            if theme == "wizard":
                assert abs(facts["sectionWidth"] - facts["panelWidth"]) < 2
                if width >= 768:
                    assert facts["tableWidth"] <= facts["availableWidth"] + 1
            expect(page.locator("#batches tbody td").nth(4)).to_have_text(source)
        page.set_viewport_size({"width": 1440, "height": 1000})
        # Opaque IDs and large measurements must stay accessible without overlapping cells.
        long_id = "batch-" + "long-identifier-" * 12
        repository.batches[0] = repository.batches[0].model_copy(
            update={"batch_id": long_id, "records": 123456789012}
        )
        page.get_by_role("button", name="Обновить данные").click()
        expect(page.locator("#batches tbody td").first).to_have_text(long_id)
        assert page.locator("#batches tbody td").first.evaluate(
            "cell => cell.scrollWidth <= cell.clientWidth + 1"
        )
        assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")


def configuration_repository():
    from contracts.evaluation import CheckExecution
    from contracts.quality import CheckKind
    from contracts.quality_config import CheckSpec, CheckSuite, RetrySettings

    injection = '<img src=x onerror="window.injected=true">'
    specs = (
        CheckSpec(
            key="volume",
            kind=CheckKind.ROW_COUNT,
            required=False,
            params={"mode": "fixed", "min_count": 0},
        ),
        CheckSpec(
            key="distribution",
            kind=CheckKind.DISTRIBUTION,
            column="payment_installments",
            params={
                "data_type": "numeric",
                "bins": [0, 0.00001, 1, 5, 10, 25, 50, 100],
            },
        ),
        CheckSpec(
            key="nulls",
            kind=CheckKind.NULL_RATE,
            column="email",
            params={"where_column": "status", "where_value": injection},
        ),
        CheckSpec(
            key="schema",
            kind=CheckKind.SCHEMA,
            params={
                "fields": [
                    {"name": "order_id", "data_type": "string"},
                    {"name": injection, "data_type": "string"},
                ],
                "allow_extra_fields": False,
            },
        ),
        CheckSpec(
            key="payment_value.non_negative",
            kind=CheckKind.VALUE_RULE,
            column="amount",
            params={
                "rule": "non_negative",
                "allow_null": False,
                "max_invalid_rate": 0.000001,
            },
        ),
        CheckSpec(key="distinct", kind=CheckKind.CARDINALITY, column="customer_id"),
    )
    suite = CheckSuite(
        dataset_id="orders",
        version=7,
        retry=RetrySettings(max_attempts=3, delays_seconds=(0, 2)),
        checks=specs,
    )
    record = evaluation_record()
    evaluation = record.evaluation.model_copy(
        update={
            "suite": suite,
            "config_hash": suite.fingerprint,
            "checks": tuple(
                CheckExecution(check_id=f"orders.{spec.key}", spec=spec)
                for spec in specs
            ),
        }
    )
    repository = MemoryRepository()
    repository.evaluations = [record.model_copy(update={"evaluation": evaluation})]
    return repository, injection


def test_configuration_is_compact_keyboard_accessible_and_preserves_values(
    browser_context, theme
):
    repository, injection = configuration_repository()
    with browser_page(browser_context, repository.factory, theme) as page:
        errors = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.goto("http://application.test/?dataset=orders&scope=window-1")
        expect(page.locator("#quality h3").first).to_contain_text("Расчёт:")
        outer = page.locator("#quality > details").filter(
            has=page.get_by_text("Конфигурация расчёта", exact=True)
        )
        outer.locator(":scope > summary").focus()
        page.keyboard.press("Enter")
        configuration = page.get_by_role(
            "region", name="Сохранённая конфигурация расчёта", exact=True
        )
        expect(configuration).to_be_visible()
        expect(configuration.locator("dd[data-config-key=version]")).to_have_text("7")
        expect(configuration.locator("dd[data-config-key=max_attempts]")).to_have_text(
            "3"
        )
        expect(
            configuration.locator("dd[data-config-key=delays_seconds]")
        ).to_have_text("0, 2")
        region = configuration.get_by_role(
            "region", name="Проверки в конфигурации", exact=True
        )
        checks = region.locator(
            "table.configuration-check-table > tbody > tr.configuration-check"
        )
        expect(checks).to_have_count(6)
        expect(
            region.get_by_role("columnheader", name="Параметры", exact=True)
        ).to_be_visible()
        expect(
            configuration.locator("details.configuration-check-properties[open]")
        ).to_have_count(0)
        assert configuration.bounding_box()["height"] < 1000
        distribution = checks.filter(has=page.get_by_text("distribution", exact=True))
        expect(distribution.locator("dd[data-config-key=bins]")).to_have_text(
            "0, 0.00001, 1, 5, 10, 25, 50, 100"
        )
        expect(distribution.locator(".configuration-fail")).to_have_text("BLOCK")
        properties = distribution.locator("details.configuration-check-properties")
        properties.locator(":scope > summary").focus()
        page.keyboard.press("Enter")
        expect(
            properties.locator("dd[data-config-key=on_insufficient_history]")
        ).to_have_text("WARN")
        # Sparse saved parameters must not acquire defaults while being displayed.
        expect(distribution.locator("dd[data-config-key=min_samples]")).to_have_count(0)
        volume = checks.filter(has=page.get_by_text("volume", exact=True))
        volume.get_by_text("Свойства", exact=True).click()
        expect(volume.locator("dd[data-config-key=min_count]")).to_have_text("0")
        expect(volume.locator("dd[data-config-key=required]")).to_have_text("Нет")
        nulls = checks.filter(has=page.get_by_text("nulls", exact=True))
        expect(nulls.locator("dd[data-config-key=where_value]")).to_have_text(injection)
        schema = checks.filter(has=page.get_by_text("schema", exact=True))
        schema.locator(".configuration-fields-disclosure > summary").click()
        expect(schema.get_by_role("table")).to_contain_text(injection)
        for width in (1440, 900, 390):
            page.set_viewport_size({"width": width, "height": 1000})
            assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
            assert configuration.evaluate(
                "element => element.scrollWidth <= element.clientWidth + 1"
            )
            expect(distribution.locator("dd[data-config-key=bins]")).to_be_visible()
            assert region.get_by_role("columnheader", name="При UNKNOWN").evaluate(
                """header => {
                    const text = header.firstChild;
                    const start = text.textContent.indexOf('UNKNOWN');
                    const word = document.createRange();
                    word.setStart(text, start);
                    word.setEnd(text, start + 'UNKNOWN'.length);
                    return word.getClientRects().length === 1;
                }"""
            )
            assert (
                checks.filter(
                    has=page.get_by_text("payment_value.non_negative", exact=True)
                )
                .locator(".configuration-check-key")
                .evaluate(
                    """element => {
                    const lines = new Map();
                    const walker = document.createTreeWalker(element, NodeFilter.SHOW_TEXT);
                    while (walker.nextNode()) {
                        const text = walker.currentNode;
                        for (let index = 0; index < text.length; index++) {
                            const letter = document.createRange();
                            letter.setStart(text, index); letter.setEnd(text, index + 1);
                            const top = Math.round(letter.getBoundingClientRect().top);
                            lines.set(top, (lines.get(top) ?? '') + text.textContent[index]);
                        }
                    }
                    return [...lines.values()].every(line => line.trim().length > 1);
                }"""
                )
            )
            if width == 390:
                assert region.evaluate(
                    "element => element.scrollWidth > element.clientWidth"
                )
        assert configuration.locator("img").count() == 0
        assert page.evaluate("window.injected") is None
        assert not errors
