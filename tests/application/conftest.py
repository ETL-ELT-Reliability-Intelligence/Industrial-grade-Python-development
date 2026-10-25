"""Capture diagnostics before browser contexts close, including inline contexts."""

import re
from contextlib import contextmanager
from pathlib import Path

import pytest


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    if report.failed:
        item.browser_test_failed = True


@pytest.fixture
def browser_context(request, browser):
    counter = 0

    @contextmanager
    def create_context(**options):
        nonlocal counter
        counter += 1
        context = browser.new_context(**options)
        context.tracing.start(screenshots=True, snapshots=True, sources=True)
        failed = False
        try:
            yield context
        except BaseException:
            failed = True
            raise
        finally:
            try:
                if failed or getattr(request.node, "browser_test_failed", False):
                    name = re.sub(r"[^a-zA-Z0-9_.-]", "_", request.node.nodeid)
                    directory = Path("test-results/browser") / f"{name}-{counter}"
                    directory.mkdir(parents=True, exist_ok=True)
                    try:
                        for index, page in enumerate(context.pages):
                            page.screenshot(
                                path=str(directory / f"page-{index}.png"), timeout=5000
                            )
                    finally:
                        context.tracing.stop(path=str(directory / "trace.zip"))
                else:
                    context.tracing.stop()
            finally:
                context.close()

    return create_context
