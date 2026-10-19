"""CI must execute every selected test, including optional integrations."""

import os

import pytest


def pytest_sessionfinish(session, exitstatus):
    if os.environ.get("CI_REQUIRE_NO_SKIPS") != "1":
        return
    reporter = session.config.pluginmanager.get_plugin("terminalreporter")
    if reporter and reporter.stats.get("skipped"):
        reporter.write_sep("=", "CI forbids skipped tests in the selected suite", red=True)
        session.exitstatus = pytest.ExitCode.TESTS_FAILED
