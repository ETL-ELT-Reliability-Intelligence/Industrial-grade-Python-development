"""Browser → real ASGI app; PostgreSQL scenario additionally uses actual storage."""

from contextlib import contextmanager
import os
import sys
from urllib.parse import urlsplit

from fastapi.testclient import TestClient
import pytest

from contracts.quality import DecisionReason, QualityDecision, QualityStatus
from services.backend.main import create_app
from services.backend.postgres import open_repository
from services.backend.models import Dataset
from tests.application.support import MemoryRepository, NOW

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
def browser_page(browser_context, repository_factory):
    with browser_context(locale="ru-RU", timezone_id="Europe/Moscow") as context:
        page = context.new_page()
        page.set_default_timeout(5000)
        with TestClient(create_app(repository_factory=repository_factory)) as client:
            def serve(route):
                url = urlsplit(route.request.url)
                response = client.get(url.path + (f"?{url.query}" if url.query else ""))
                route.fulfill(status=response.status_code, content_type=response.headers.get("content-type", "text/plain"), body=response.content)

            page.route("http://application.test/**", serve)
            yield page


@pytest.fixture
def page(browser_context):
    with browser_page(browser_context, MemoryRepository().factory) as page:
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
    expect(page.locator("#quality")).to_contain_text("Проверки для этой области не сохранены")
    page.goto("http://application.test/?dataset=missing")
    expect(page.locator("#dataset [role=alert]")).to_contain_text("Dataset не найден")


def test_error_retry_and_refresh(page):
    path = "http://application.test/api/v1/datasets?*"

    def fail(route):
        route.fulfill(status=503, content_type="application/json", body='{"detail":"Unavailable"}')

    page.route(path, fail)
    page.goto("http://application.test/")
    expect(page.locator("#catalog [role=alert]")).to_contain_text("Хранилище недоступно")
    page.unroute(path, fail)
    page.locator("#catalog").get_by_role("button", name="Повторить").click()
    expect(page.get_by_role("button", name="orders", exact=True)).to_be_visible()
    page.get_by_role("button", name="Обновить данные").click()
    expect(page.locator("#refreshed-at")).to_contain_text("не свежесть исходных данных")


def test_catalog_pagination(browser_context):
    repository = MemoryRepository()
    repository.datasets = [Dataset(dataset_id=f"dataset-{i:02}", created_at=NOW) for i in range(51)]
    with browser_page(browser_context, repository.factory) as page:
        page.goto("http://application.test/")
        expect(page.locator(".catalog-item")).to_have_count(50)
        page.locator("#catalog").get_by_role("button", name="Далее").click()
        expect(page.locator(".catalog-item")).to_have_count(1)
        expect(page.locator(".catalog-item .incident-title")).to_have_text("dataset-50")
        expect(page.locator("#catalog").get_by_role("button", name="Далее")).to_be_disabled()
        page.locator("#catalog").get_by_role("button", name="Назад").click()
        expect(page.locator(".catalog-item")).to_have_count(50)


def test_batches_and_scopes_pagination(browser_context):
    repository = MemoryRepository()
    repository.batches = [repository.batches[0].model_copy(update={"batch_id": f"batch-{i:02}"}) for i in range(51)]
    quality = repository.qualities[("orders", "window-1")]
    repository.qualities = {("orders", f"window-{i:02}"): quality.model_copy(update={"scope_id": f"window-{i:02}"}) for i in range(51)}
    with browser_page(browser_context, repository.factory) as page:
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
    page.route("http://application.test/api/v1/datasets/orders/quality?*", lambda route: held.append(route))
    page.goto("http://application.test/?dataset=orders&scope=window-1")
    expect(page.locator("#quality")).to_contain_text("Загрузка")
    page.get_by_role("button", name="empty", exact=True).click()
    expect(page.locator("#dataset h2")).to_have_text("empty")
    assert held
    held[0].fulfill(status=200, content_type="application/json", body='{"scope_id":"STALE","checks":[],"decision":null,"decided_at":null}')
    expect(page.locator("#quality")).to_contain_text("Выберите область")
    expect(page.locator("#quality")).not_to_contain_text("STALE")


def test_mobile_keyboard_and_untrusted_text(browser_context):
    repository = MemoryRepository()
    malicious_id = '<img src=x onerror="window.injected=true">'
    repository.datasets.append(Dataset(dataset_id=malicious_id, created_at=NOW))
    with browser_page(browser_context, repository.factory) as page:
        page.set_viewport_size({"width": 390, "height": 844})
        page.goto("http://application.test/?dataset=orders&scope=window-1")
        expect(page.locator("#quality .status-block")).to_be_visible()
        expect(page.get_by_role("button", name=malicious_id, exact=True)).to_be_visible()
        assert page.locator("#catalog img").count() == 0
        assert page.evaluate("window.injected") is None
        assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
        choice = page.get_by_role("button", name="empty", exact=True)
        choice.focus()
        page.keyboard.press("Enter")
        expect(page.locator("#dataset h2")).to_have_text("empty")


def test_postgres_change_appears_after_refresh(browser_context):
    from tests.storage.support import TEST_DATABASE_URL, migrate
    if not TEST_DATABASE_URL:
        pytest.skip("Set TEST_DATABASE_URL to an expendable test database")
    from storage.postgres import PostgresDatasetRepository, PostgresQualityRepository, connect
    from tests.application.support import evidence
    from uuid import uuid4

    migrate(TEST_DATABASE_URL)
    dataset_id = f"browser-{uuid4().hex}"
    with connect(TEST_DATABASE_URL) as connection:
        datasets = PostgresDatasetRepository(connection)
        quality = PostgresQualityRepository(connection)
        datasets.ensure_dataset(dataset_id)
        try:
            for result in evidence(dataset_id):
                quality.save_check_result(result)
            with browser_page(browser_context, lambda: open_repository(TEST_DATABASE_URL)) as page:
                page.goto(f"http://application.test/?dataset={dataset_id}&scope=window-1")
                expect(page.locator("#quality")).to_contain_text("Решение ещё не сохранено")
                quality.save_quality_decision(dataset_id, "window-1", QualityDecision(
                    status=QualityStatus.WARN,
                    reasons=(DecisionReason(code="upstream_review", message="Updated through storage repository"),),
                ), NOW)
                page.get_by_role("button", name="Обновить данные").click()
                expect(page.locator("#quality .decision .status-warn")).to_have_text("WARN")
                expect(page.locator("#quality")).to_contain_text("Updated through storage repository")
        finally:
            for table in ("quality_check_results", "quality_decisions", "datasets"):
                connection.execute(f"DELETE FROM {table} WHERE dataset_id = %s", (dataset_id,))
