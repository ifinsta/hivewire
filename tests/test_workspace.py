"""Assignment startup against real boards, without opening test desktop windows."""

from __future__ import annotations

import io
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from unittest import mock

from test_hardening import HardeningCase, core

import swarm_workspace as workspace


class Workspace(HardeningCase):
    def setUp(self):
        super().setUp()
        self.join("ann")

    @contextmanager
    def desktop(self):
        # Simulate a desktop with a terminal, independent of the CI host.
        plan = workspace.terminal_plan
        def simulated_plan(*args, **kwargs):
            kwargs["has"] = lambda name: name in {"xterm", "wt"}
            return plan(*args, **kwargs)

        with mock.patch.object(workspace, "desktop_available", return_value=True), \
             mock.patch.object(workspace, "terminal_plan", side_effect=simulated_plan):
            yield

    def dashboard(self):
        return mock.patch.object(workspace, "ensure_dashboard", return_value=({"id": "testboard"}, {"port": 8787, "token": "private-capability"}))

    def test_repeated_startup_reuses_viewers_and_does_not_start_agents(self):
        with self.board() as sw, self.desktop(), self.dashboard(), \
             mock.patch.object(workspace.webbrowser, "open", return_value=True) as browser, \
             mock.patch.object(workspace, "launch_terminal") as terminal:
            terminal.return_value.poll.return_value = None
            first = workspace.open_workspace(sw)
            second = workspace.open_workspace(sw)
            self.assertEqual(first["terminal"], "opening")
            self.assertEqual(second["terminal"], "already running")
            self.assertEqual(second["browser"], "already opened")
            self.assertEqual(browser.call_count, 1)
            self.assertEqual(terminal.call_count, 1)
            self.assertEqual(sw.db.execute("SELECT COUNT(*) FROM executions").fetchone()[0], 0)
            self.assertNotIn("private-capability", json.dumps(first))
            self.assertNotIn("private-capability", sw.meta("workspace_browser"))
            self.assertNotIn("private-capability", sw.db.execute("SELECT detail FROM events WHERE kind='workspace_opened' LIMIT 1").fetchone()[0])

    def test_closed_viewer_can_reopen_without_duplicating_browser(self):
        with self.board() as sw, self.desktop(), self.dashboard(), \
             mock.patch.object(workspace.webbrowser, "open", return_value=True) as browser, \
             mock.patch.object(workspace, "launch_terminal") as terminal:
            terminal.return_value.poll.return_value = None
            workspace.open_workspace(sw)
            token = workspace.state(sw, "workspace_viewer")["token"]
            self.assertTrue(workspace.touch_viewer(sw, token))
            self.assertTrue(workspace.touch_viewer(sw, token, close=True))
            workspace.open_workspace(sw)
            self.assertEqual(terminal.call_count, 2)
            self.assertEqual(browser.call_count, 1)
            workspace.open_workspace(sw, reopen=True)
            self.assertEqual(browser.call_count, 2)
            self.assertEqual(terminal.call_count, 2)

    def test_pending_concurrent_startup_does_not_open_more_views(self):
        with self.board() as sw:
            with sw.tx():
                sw.set_meta("workspace_opening", json.dumps({"token": "first", "at": core.iso()}))
            with self.dashboard() as dashboard, mock.patch.object(workspace, "launch_terminal") as terminal:
                result = workspace.open_workspace(sw)
            self.assertEqual(result["terminal"], "starting")
            dashboard.assert_not_called()
            terminal.assert_not_called()

    def test_actual_concurrent_call_reuses_the_inflight_request(self):
        entered, release = threading.Event(), threading.Event()

        def slow_dashboard(sw):
            entered.set()
            if not release.wait(15):
                raise OSError("test startup timed out")
            return {"id": "board"}, {"port": 8787, "token": "test-token"}

        def start():
            with self.board() as sw:
                return workspace.open_workspace(sw)

        with self.desktop(), mock.patch.object(workspace, "ensure_dashboard", side_effect=slow_dashboard) as dashboard, \
             mock.patch.object(workspace.webbrowser, "open", return_value=True), \
             mock.patch.object(workspace, "launch_terminal") as terminal, ThreadPoolExecutor(max_workers=2) as pool:
            terminal.return_value.poll.return_value = None
            first = pool.submit(start)
            try:
                self.assertTrue(entered.wait(10))
                second = pool.submit(start).result(timeout=10)
                self.assertEqual(second["terminal"], "starting")
            finally:
                release.set()
            self.assertEqual(first.result(timeout=10)["terminal"], "opening")
            self.assertEqual(dashboard.call_count, 1)
            self.assertEqual(terminal.call_count, 1)

    def test_stale_leases_and_dashboard_failure_do_not_block_terminal_recovery(self):
        with self.board() as sw:
            with sw.tx():
                for key in ("workspace_opening", "workspace_viewer"):
                    sw.set_meta(key, json.dumps({"token": "old", "at": "2000-01-01T00:00:00+00:00"}))
            with self.desktop(), mock.patch.object(workspace, "ensure_dashboard", side_effect=OSError("dashboard unavailable")), \
                 mock.patch.object(workspace, "launch_terminal") as terminal:
                terminal.return_value.poll.return_value = None
                result = workspace.open_workspace(sw)
            self.assertEqual(result["terminal"], "opening")
            self.assertEqual(result["dashboard"], "unavailable")
            self.assertTrue(result["warnings"])
            self.assertIsNone(sw.meta("workspace_opening"))

    def test_failed_terminal_and_browser_remain_retryable(self):
        with self.board() as sw, self.desktop(), self.dashboard(), \
             mock.patch.object(workspace.webbrowser, "open", return_value=False) as browser, \
             mock.patch.object(workspace, "launch_terminal", side_effect=OSError("terminal unavailable")) as terminal:
            result = workspace.open_workspace(sw)
            self.assertTrue(result["warnings"])
            self.assertFalse(sw.meta("workspace_viewer"))
            self.assertFalse(sw.meta("workspace_opening"))
            workspace.open_workspace(sw)
            self.assertEqual(terminal.call_count, 2)
            self.assertEqual(browser.call_count, 2)

    def test_headless_and_explicit_no_open_start_dashboard_without_desktop_side_effects(self):
        with self.board() as sw, self.dashboard() as dashboard, \
             mock.patch.object(workspace.webbrowser, "open") as browser, \
             mock.patch.object(workspace, "launch_terminal") as terminal:
            with mock.patch.object(workspace, "desktop_available", return_value=False):
                self.assertEqual(workspace.open_workspace(sw)["dashboard"], "ready")
            with self.desktop():
                self.assertEqual(workspace.open_workspace(sw, no_open=True)["browser"], "disabled")
            self.assertEqual(dashboard.call_count, 2)
            browser.assert_not_called()
            terminal.assert_not_called()

    def test_dry_run_json_is_finite_and_worker_cannot_open_human_views(self):
        result = self.json_command("workspace", "--as", "ann", "--dry-run", "--json")
        self.assertTrue(result["result"]["dry_run"])
        with self.board() as sw:
            self.assertIsNone(sw.meta("workspace_viewer"))
            self.assertIsNone(sw.meta("workspace_opening"))
        self.join("bob")
        denied = self.json_command("workspace", "--as", "bob", "--dry-run", "--json", ok=False)
        self.assertIn("coordinator", denied["error"]["message"])
        denied = self.json_command("sessions", "--once", "--workspace-console", "--json", ok=False)
        self.assertIn("private workspace console", denied["error"]["message"])

    def test_platform_plans_keep_spaces_literal_and_use_one_viewer(self):
        token = "a" * 32
        root = Path("C:/a project/repo")
        plan = workspace.terminal_plan(root, "board", token, "windows", has=lambda _: True)
        self.assertEqual(plan["argv"].count("new-tab"), 1)
        self.assertIn(str(root), plan["argv"])
        self.assertNotIn("cmd", plan["argv"])
        fallback = workspace.terminal_plan(root, "board", token, "windows", has=lambda _: False)
        self.assertTrue(fallback["new_console"])
        self.assertIn("--workspace-console", fallback["argv"])
        linux = workspace.terminal_plan(root, "board", token, "linux", has=lambda t: t == "xterm")
        self.assertEqual(linux["argv"][0], "xterm")
        mac = workspace.terminal_plan(root, "board", token, "macos")
        self.assertEqual(mac["argv"][0], "osascript")
        self.assertIsNone(workspace.terminal_plan(root, "board", token, "linux", display=False))

    def test_dashboard_background_start_is_hidden_and_has_no_open_switch(self):
        import swarm_web

        with self.board() as sw, mock.patch.object(swarm_web, "register", return_value={"id": "board"}), \
             mock.patch.object(swarm_web, "running", side_effect=[None, {"port": 1, "token": "secret"}]), \
             mock.patch.object(workspace.subprocess, "Popen") as launch:
            workspace.ensure_dashboard(sw)
        argv = launch.call_args.args[0]
        self.assertEqual(argv[:2], [sys.executable, str(core.LAUNCHER)])
        self.assertIn("--observe", argv)
        self.assertIn("--no-open", argv)
        self.assertEqual(launch.call_args.kwargs["stdout"], subprocess.DEVNULL)
        self.assertEqual(bool(launch.call_args.kwargs.get("creationflags", 0) & 0x08000000), os.name == "nt")

    def test_viewer_detach_releases_only_its_lease(self):
        import argparse
        from contextlib import redirect_stdout
        import swarm_sessions

        token = "a" * 32
        args = argparse.Namespace(once=False, agents="", unattended=False, limit=200,
                                  as_=None, json=False, workspace_id=token)
        with self.board() as sw:
            with sw.tx():
                sw.set_meta("workspace_viewer", json.dumps({"token": token, "at": core.iso()}))
            output = io.StringIO()
            with redirect_stdout(output), mock.patch.object(output, "isatty", return_value=True), \
                 mock.patch.object(sys.stdin, "isatty", return_value=True), \
                 mock.patch.object(swarm_sessions, "TerminalKeys") as keys:
                keys.return_value.__enter__.return_value.read.return_value = "q"
                core.cmd_sessions(sw, args)
            self.assertIsNone(sw.meta("workspace_viewer"))
            self.assertIn("Workers continue", output.getvalue())
            with sw.tx():
                sw.set_meta("workspace_viewer", json.dumps({"token": "b" * 32, "at": core.iso()}))
            self.assertFalse(workspace.touch_viewer(sw, token, close=True))
            self.assertIsNotNone(sw.meta("workspace_viewer"))


class HiddenDashboard(HardeningCase):
    def test_real_background_server_starts_authenticates_and_is_reused(self):
        import swarm_web

        child = None
        original = subprocess.Popen

        def spawn(*args, **kwargs):
            nonlocal child
            child = original(*args, **kwargs)
            return child

        try:
            with self.board() as sw, mock.patch.dict(os.environ, {"SWARM_WEB_HOME": str(self.root / "web-state")}), \
                 mock.patch.object(workspace.subprocess, "Popen", side_effect=spawn) as launch:
                first_entry, first = workspace.ensure_dashboard(sw)
                self.assertTrue(swarm_web.ping(first["port"], first["token"]))
                second_entry, second = workspace.ensure_dashboard(sw)
                self.assertEqual(first, second)
                self.assertEqual(first_entry["id"], second_entry["id"])
                self.assertEqual(launch.call_count, 1)
        finally:
            # Only stop the exact subprocess created by this test, never a PID
            # read from a pre-existing user dashboard's registry.
            if child is not None:
                child.terminate()
                child.wait(timeout=10)
