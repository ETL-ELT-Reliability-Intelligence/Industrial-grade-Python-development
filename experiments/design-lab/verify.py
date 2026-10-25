"""Check the current frontend via the explicit design-preview API; save screenshots."""

import os
from pathlib import Path
import sys

from playwright.sync_api import expect, sync_playwright

BASE = "http://127.0.0.1:8010"
THEMES = ("classic", "cs", "wizard")
SCREENSHOTS = Path(__file__).resolve().parent / "screenshots"


def verify():
    with sync_playwright() as playwright:
        options = {"headless": True}
        if executable := os.environ.get("PLAYWRIGHT_EXECUTABLE_PATH"):
            options["executable_path"] = executable
        elif sys.platform == "win32":
            options["channel"] = "msedge"
        browser = playwright.chromium.launch(**options)
        context = browser.new_context(locale="ru-RU", timezone_id="Europe/Moscow")
        page = context.new_page()
        errors = []
        page.on("pageerror", lambda error: errors.append(str(error)))
        SCREENSHOTS.mkdir(exist_ok=True)
        for theme in THEMES:
            page.set_viewport_size({"width": 1440, "height": 900})
            page.goto(f"{BASE}/app?theme={theme}&dataset=sample-app-orders&scope=sample-window-1", wait_until="networkidle")
            expect(page.locator("html")).to_have_attribute("data-theme", theme)
            expect(page.locator("#preview-notice")).to_be_visible()
            expect(page.locator("#quality .status-block")).to_have_text("BLOCK")
            expect(page.locator("#quality .status-unknown")).to_have_text("UNKNOWN")
            expect(page.locator("#batches tbody tr")).to_have_count(2)
            expect(page.locator("#quality table")).to_contain_text("Нет значения")
            expect(page.locator("#quality table")).to_contain_text("0")
            if theme == "wizard":
                expect(page.locator("#wizard-assistant")).to_be_visible()
                page.locator("#assistant-toggle").focus()
                page.keyboard.press("Enter")
                expect(page.locator("#wizard-assistant")).to_be_hidden()
                page.keyboard.press("Enter")
                expect(page.locator("#wizard-assistant")).to_be_visible()
                page.wait_for_function("document.querySelector('#wizard-assistant img').naturalWidth > 0")
            page.get_by_role("button", name="sample-app-empty", exact=True).click()
            expect(page.locator("#batches")).to_contain_text("нет сохранённых загрузок")
            assert f"theme={theme}" in page.url
            page.get_by_role("button", name="sample-app-orders", exact=True).click()
            page.get_by_role("button", name="sample-window-1", exact=True).click()
            expect(page.locator("#quality .status-block")).to_be_visible()
            page.get_by_role("button", name="Обновить данные").click()
            expect(page.locator("#quality .status-block")).to_be_visible()
            page.evaluate("document.activeElement.blur(); window.scrollTo(0,0)")
            assert page.evaluate("document.documentElement.scrollWidth <= innerWidth"), theme
            page.screenshot(path=str(SCREENSHOTS / f"{theme}-desktop.png"))
            page.set_viewport_size({"width": 390, "height": 844})
            assert page.evaluate("document.documentElement.scrollWidth <= innerWidth"), f"mobile {theme}"
            page.screenshot(path=str(SCREENSHOTS / f"{theme}-mobile.png"), full_page=True)
            page.route("**/api/v1/datasets?*", lambda route: route.fulfill(status=503, json={"detail": "Unavailable"}))
            page.get_by_role("button", name="Обновить данные").click()
            expect(page.locator("#catalog [role=alert]")).to_contain_text("Хранилище недоступно")
            page.unroute("**/api/v1/datasets?*")
            page.locator("#catalog").get_by_role("button", name="Повторить").click()
            expect(page.get_by_role("button", name="sample-app-orders", exact=True)).to_be_visible()
            print(f"PASS {theme}: API selection, refresh, retry, desktop, mobile")
        page.goto(BASE, wait_until="networkidle")
        expect(page.locator(".concept-card")).to_have_count(3)
        assert page.locator(".concept-card img").evaluate_all("images => images.every(img => img.complete && img.naturalWidth > 0)")
        assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
        page.screenshot(path=str(SCREENSHOTS / "gallery-mobile.png"), full_page=True)
        page.set_viewport_size({"width": 1440, "height": 900})
        page.screenshot(path=str(SCREENSHOTS / "gallery-desktop.png"))
        assert not errors, errors
        context.close()
        browser.close()
        print("PASS gallery; screenshots updated; no browser errors")


if __name__ == "__main__":
    verify()
