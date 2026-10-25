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
        page.get_by_label("Оформление", exact=True).select_option("wizard" if theme != "wizard" else "cs")
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
    page.on("request", lambda request: requests.append(request.url) if "/api/v1/" in request.url else None)
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
    page.wait_for_function("document.querySelector('#wizard-assistant img').naturalWidth > 0")
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
