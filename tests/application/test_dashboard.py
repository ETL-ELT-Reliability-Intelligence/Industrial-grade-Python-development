"""Real browser checks, using ASGI transport so no listening server is needed."""

import os
import sys
from urllib.parse import urlsplit

from fastapi.testclient import TestClient
import pytest

from services.backend.main import create_app

playwright_api = pytest.importorskip("playwright.sync_api", reason="Install requirements-browser.txt for UI checks")
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


@pytest.fixture
def page(browser):
    context = browser.new_context(locale="ru-RU", timezone_id="Europe/Moscow")
    page = context.new_page()
    page.set_default_timeout(5000)
    with TestClient(create_app()) as client:
        def serve(route):
            url = urlsplit(route.request.url)
            response = client.get(url.path + (f"?{url.query}" if url.query else ""))
            route.fulfill(status=response.status_code, content_type=response.headers.get("content-type", "text/plain"), body=response.content)

        page.route("http://application.test/**", serve)
        yield page
    context.close()


def test_incident_scenario_and_filter(page):
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.goto("http://application.test/")
    expect(page.locator(".stat")).to_have_count(4)
    expect(page.locator(".incident-row")).to_have_count(3)
    page.locator('[data-id="inc-042"]').click()
    expect(page.locator("#incident-detail h2")).to_contain_text("Снижение объёма заказов")
    expect(page.locator("#incident-detail")).to_contain_text("1 800 000")
    expect(page.locator("#incident-detail")).to_contain_text("customer_ltv")
    expect(page.locator("#incident-detail")).to_contain_text("не является вероятностью")
    page.get_by_label("Статус", exact=True).select_option("resolved")
    expect(page.locator(".incident-row")).to_have_count(1)
    page.locator('[data-id="inc-040"]').click()
    expect(page.locator("#incident-detail h2")).to_contain_text("Восстановлена загрузка")
    assert not errors


def test_api_error_retry_and_empty_state(page):
    path = "http://application.test/api/v1/incidents*"
    def fail(route):
        route.fulfill(status=503, content_type="application/json", body='{"detail":"Unavailable"}')

    page.route(path, fail)
    page.goto("http://application.test/")
    expect(page.locator("#incident-list [role=alert]")).to_contain_text("503")
    page.unroute(path, fail)
    page.locator("#incident-list").get_by_role("button", name="Повторить").click()
    expect(page.locator(".incident-row")).to_have_count(3)

    page.route(path, lambda route: route.fulfill(status=200, content_type="application/json", body='{"items":[],"total":0}'))
    page.get_by_label("Статус", exact=True).select_option("open")
    expect(page.locator("#incident-list")).to_contain_text("Инцидентов с этим статусом нет")


def test_detail_deep_link_and_not_found(page):
    page.goto("http://application.test/#inc-041")
    expect(page.locator("#incident-detail h2")).to_contain_text("Данные клиентов")
    expect(page.locator("#incident-detail")).to_contain_text("Кандидаты причин пока не определены")
    page.goto("http://application.test/#missing")
    expect(page.locator("#incident-detail [role=alert]")).to_contain_text("Инцидент не найден")


def test_mobile_layout(page):
    page.set_viewport_size({"width": 390, "height": 844})
    page.goto("http://application.test/#inc-042")
    expect(page.locator("#incident-detail h2")).to_be_visible()
    assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
