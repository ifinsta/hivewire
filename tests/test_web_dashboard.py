"""The browser dashboard (`swarm dashboard --web`): read-only, loopback-only, token-guarded, no markup from board text."""

from __future__ import annotations

import http.client
from contextlib import closing
import json
import os
import queue
import sqlite3
import subprocess
import sys
from pathlib import Path
import tempfile
import threading
import unittest
from unittest import mock
from datetime import timedelta
from urllib.parse import parse_qs, urlparse

from test_hardening import HardeningCase, core, fixtures

import swarm_web  # noqa: E402  (the fixtures put the skill directory on sys.path)


class WebCase(HardeningCase):
    def setUp(self):
        super().setUp()
        self.home = tempfile.TemporaryDirectory()
        self.saved_home = os.environ.get("SWARM_WEB_HOME")
        os.environ["SWARM_WEB_HOME"] = self.home.name
        self.join("ann")
        self.join("bob")
        with self.board() as sw:
            self.server = swarm_web.make_server(sw, port=0, describe=core.describe_event, command="python hivewire.py")
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        if self.saved_home is None:
            os.environ.pop("SWARM_WEB_HOME", None)
        else:
            os.environ["SWARM_WEB_HOME"] = self.saved_home
        self.home.cleanup()
        super().tearDown()

    def request(self, path, *, method="GET", token=True, headers=None, host=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        sent = {"Host": host or f"127.0.0.1:{self.port}", **(headers or {})}
        if token:
            sent["X-Swarm-Token"] = self.server.token
        try:
            conn.request(method, path, headers=sent)
            response = conn.getresponse()
            return response.status, dict(response.getheaders()), response.read()
        finally:
            conn.close()

    def api(self, path):
        status, _, body = self.request(path)
        self.assertEqual(status, 200, body)
        return json.loads(body)


class Bind(unittest.TestCase):
    def test_loopback_binding_does_not_query_reverse_dns(self):
        with mock.patch.object(swarm_web.socket, "getfqdn", side_effect=AssertionError("reverse DNS")):
            with swarm_web.Server(("127.0.0.1", 0), swarm_web.Handler) as server:
                self.assertEqual(server.server_name, "127.0.0.1")
                self.assertEqual(server.server_port, server.server_address[1])


class Guards(WebCase):
    def test_a_request_without_the_token_or_with_a_foreign_host_is_refused(self):
        self.assertEqual(self.request("/api/overview", token=False)[0], 403)
        self.assertEqual(self.request("/api/overview", token=False, headers={"X-Swarm-Token": "wrong"})[0], 403)
        self.assertEqual(self.request("/api/overview", host="evil.example")[0], 403)
        self.assertEqual(self.request("/api/overview", host=f"localhost.evil.example:{self.port}")[0], 403)
        status, headers, body = self.request("/", token=False)
        self.assertEqual(status, 200, "the page loads without a token so it can explain what is missing")
        self.assertNotIn(self.server.token.encode(), body, "and neither the token nor any board data")
        self.assertNotIn(b'"ann"', body)
        self.assertEqual(headers["Content-Type"], "text/html; charset=utf-8")
        self.assertEqual(self.request("/api/ping", token=False)[0], 403)
        self.assertEqual(self.request("/api/overview", headers={"Sec-Fetch-Site": "cross-site"})[0], 403)
        self.assertEqual(self.request("/api/overview", host=f"localhost:{self.port}")[0], 200)

    def test_only_get_is_served_and_unknown_paths_are_not_found(self):
        for method in ("PUT", "DELETE", "PATCH", "OPTIONS"):
            self.assertEqual(self.request("/api/overview", method=method)[0], 405, method)
        self.assertEqual(self.request("/api/overview", method="POST")[0], 405, "only /api/act takes a POST")
        self.assertEqual(self.request("/api/nothing")[0], 404)
        self.assertEqual(self.request("/api/message/99999")[0], 404)
        self.assertEqual(self.request("/api/task/NOPE")[0], 404)
        self.assertEqual(self.request("/elsewhere")[0], 404)

    def test_the_server_listens_on_loopback_only(self):
        self.assertEqual(self.server.server_address[0], "127.0.0.1")

    def test_the_page_is_locked_down_and_never_builds_markup_from_data(self):
        status, headers, body = self.request(f"/?t={self.server.token}", token=False)
        self.assertEqual(status, 200)
        policy = headers["Content-Security-Policy"]
        nonce = policy.split("'nonce-")[1].split("'")[0]
        self.assertIn("default-src 'none'", policy)
        self.assertIn("font-src data:", policy, "bundled fonts need no external font hosts")
        self.assertNotIn("unsafe-inline", policy)
        self.assertNotIn("unsafe-eval", policy)
        self.assertEqual(body.decode().count(f'nonce="{nonce}"'), 2, "one style and one script, both nonced")
        self.assertNotIn(b"/*__FONT_FACES__*/", body)
        self.assertEqual(body.count(b"@font-face{"), 4, "Archivo and three IBM Plex Mono weights are bundled")
        self.assertIn(b"data:font/woff2;base64,", body)
        self.assertEqual(headers["Cache-Control"], "no-store")
        for sink in ("innerHTML", "outerHTML", "insertAdjacentHTML", "document.write", "eval(", "new Function", "srcdoc"):
            self.assertNotIn(sink, swarm_web.PAGE, sink)
        self.assertNotIn("http://", swarm_web.PAGE.replace("http://www.w3.org/2000/svg", ""))
        self.assertNotIn("https://", swarm_web.PAGE, "the page loads nothing from the network")


class Responsive(unittest.TestCase):
    """The page adapts from a phone to a wide desktop. Layout itself is checked in a browser; these keep its rules from being dropped."""

    def test_the_page_declares_a_viewport_and_breakpoints_for_phone_and_tablet(self):
        page = swarm_web.PAGE
        self.assertIn('name="viewport" content="width=device-width,initial-scale=1"', page)
        for rule in ("@media (max-width:760px)", "@media (max-width:860px)", "@media (max-width:1000px)"):
            self.assertIn(rule, page, rule)

    def test_tables_stack_with_labels_and_the_team_is_one_canvas_everywhere(self):
        page = swarm_web.PAGE
        self.assertIn("content:attr(data-label)", page, "stacked rows keep their column names")
        self.assertNotIn("List view", page, "the team is shown as a canvas only")
        self.assertIn("class: 'toolrail'", page)
        self.assertNotIn(".rail{position:absolute", page, "the tool rail must not reuse the plan timeline's class")
        self.assertIn("min-height:44px", page, "touch targets on small screens")


class Payloads(WebCase):
    def test_overview_reports_agents_tasks_questions_and_events(self):
        self.sw("--as", "ann", "claim", "T1")
        self.sw("--as", "bob", "claim", "T2")
        self.sw("--as", "bob", "ask", "Should /ready fail while the cache is cold?", "--to", "human", "--critical")
        data = self.api("/api/overview")
        names = {a["name"]: a for a in data["agents"]}
        self.assertEqual(set(names), {"ann", "bob"})
        self.assertEqual(names["ann"]["task"]["id"], "T1")
        self.assertIsNotNone(names["ann"]["task"]["clock"]["active_min"], "a claim carries the configured contract")
        self.assertTrue(names["ann"]["coordinator"])
        self.assertIn("T1", [t["id"] for t in data["board"]["working"]])
        self.assertIn("T2", [t["id"] for t in data["board"]["working"]])
        self.assertIn("T4", [t["id"] for t in data["board"]["queued"]])
        self.assertEqual([t["waits"] for t in data["board"]["queued"] if t["id"] == "T3"], [["T1"]])
        question = [n for n in data["needs"] if n["type"] == "question"][0]
        self.assertTrue(question["critical"])
        self.assertEqual(question["from"], "bob")
        self.assertTrue(data["events"] and all({"text", "color", "at"} <= set(e) for e in data["events"]))
        self.assertEqual(data["pool"]["max_agents"], 5)
        self.assertGreaterEqual(data["counts"]["questions"], 1)

    def test_inbox_lists_messages_with_filters_and_a_message_shows_its_delivery(self):
        self.sw("--as", "ann", "send", "bob", "Please review T1", "--kind", "review_request")
        self.sw("--as", "bob", "send", "ann", "Careful: <img src=x onerror=alert(1)>", "--kind", "warning", "--priority", "critical")
        inbox = self.api("/api/inbox")
        self.assertEqual(inbox["tabs"]["warning"], 1)
        self.assertEqual(inbox["tabs"]["review"], 1)
        self.assertEqual([m["kind"] for m in self.api("/api/inbox?tab=warning")["messages"]], ["warning"])
        self.assertEqual(self.api("/api/inbox?tab=dead_letter")["messages"], [])
        warning = self.api("/api/inbox?tab=warning")["messages"][0]
        self.assertEqual(warning["priority"], "critical")
        detail = self.api(f"/api/message/{warning['id']}")
        self.assertIn("<img src=x", detail["message"]["body"], "the API returns the text; the page only ever sets textContent")
        self.assertEqual([r["agent"] for r in detail["recipients"]], ["ann"])
        self.assertIsNone(detail["recipients"][0]["acked_at"])
        self.assertEqual(detail["thread"][0]["id"], warning["id"])

    def test_inbox_reports_read_and_unread_state_per_recipient(self):
        self.sw("--as", "ann", "send", "bob", "Please review T1", "--kind", "review_request")
        self.sw("--as", "bob", "send", "ann", "Which port should the probe use?", "--kind", "decision")
        self.sw("--as", "bob", "inbox")
        inbox = self.api("/api/inbox")
        review = [m for m in inbox["messages"] if m["kind"] == "review_request"][0]
        decision = [m for m in inbox["messages"] if m["kind"] == "decision"][0]
        self.assertEqual(review["recipients"], {"total": 1, "read": 1, "acked": 0, "unread": 0})
        self.assertEqual(decision["recipients"], {"total": 1, "read": 0, "acked": 0, "unread": 1})
        self.assertEqual(inbox["tabs"]["unread"], 1)
        self.assertEqual([m["id"] for m in self.api("/api/inbox?tab=unread")["messages"]], [decision["id"]])
        self.assertIsNotNone(self.api(f"/api/message/{review['id']}")["recipients"][0]["read_at"])
        unread = self.api(f"/api/message/{decision['id']}")["recipients"][0]
        self.assertIsNone(unread["read_at"])
        self.assertIsNone(unread["acked_at"], "reading is an observation, not an acknowledgement")

    def test_overview_carries_attention_ownership_and_the_live_deputy_lease(self):
        self.join("cat")
        self.sw("--as", "ann", "send", "bob", "The release decision needs an owner", "--priority", "critical", "--kind", "warning")
        con = sqlite3.connect(self.root / ".git" / "swarm" / "swarm.db")
        con.execute(
            "UPDATE messages SET created_at=? WHERE id=(SELECT MAX(id) FROM messages)",
            (core.iso(core.utcnow() - timedelta(hours=1)),),
        )
        con.commit()
        con.close()
        self.sw("deputy", "scan", "--once")
        data = self.api("/api/overview")
        signal = next(i for i in data["attention"]["items"] if i["kind"] == "message")
        self.assertEqual(signal["severity"], "P1")
        self.assertEqual(signal["status"], "new")
        self.assertIsNone(signal["owner"])
        self.assertEqual(data["counts"]["attention"], data["attention"]["open"])
        self.sw("deputy", "activate", "cat", "--scope", "message:*", "--as", "ann")
        self.sw("attention", "claim", str(signal["id"]), "--as", "cat")
        data = self.api("/api/overview")
        lease = data["deputy"]["active"]
        self.assertEqual(lease["deputy"], "cat")
        self.assertEqual(lease["scope"], ["message:*"])
        self.assertIn("attention.report", lease["capabilities"])
        self.assertTrue(lease["live"])
        claimed = next(i for i in data["attention"]["items"] if i["id"] == signal["id"])
        self.assertEqual(claimed["owner"], "cat")
        self.assertTrue(any(h["item"] == signal["id"] and h["to"] == "claimed" for h in data["attention"]["history"]))

    def test_decisions_show_open_and_settled_questions(self):
        self.sw("--as", "bob", "claim", "T2")
        self.sw("--as", "bob", "ask", "Which port should the probe use?", "--to", "human")
        qid = self.api("/api/decisions")["questions"][0]["id"]
        self.assertTrue(self.api("/api/decisions")["questions"][0]["open"])
        self.sw("answer", str(qid), "--from-human", "4100")
        settled = self.api("/api/decisions")["questions"][0]
        self.assertFalse(settled["open"])
        self.assertEqual(settled["answer"], "4100")

    def test_task_detail_and_a_goal_with_an_unapproved_brief(self):
        self.sw("--as", "ann", "claim", "T1")
        detail = self.api("/api/task/T1")
        self.assertEqual(detail["task"]["state"], "claimed")
        self.assertEqual(detail["agent"]["name"], "ann")
        self.assertEqual(detail["task"]["clock"]["state"], "active")
        self.sw("goal", "new", "health", "Ship a health endpoint")
        self.assertEqual(self.api("/api/overview")["goals"][0]["id"], "health")
        self.architecture("health", approve=False)
        approvals = self.api("/api/decisions")["approvals"]
        self.assertEqual([a["goal"] for a in approvals], ["health"])
        self.assertEqual(self.api("/api/overview")["counts"]["decisions"], 1)


class Team(WebCase):
    def test_roles_are_derived_from_what_the_board_says_each_agent_is_doing(self):
        self.sw("--as", "bob", "claim", "T2")
        directory = self.api("/api/overview")["directory"]
        self.assertEqual(directory["ann"]["role"], "Orchestrator")
        self.assertEqual(directory["bob"]["role"], "Implementer")
        self.assertEqual(directory["human"]["display"], "You")
        self.assertEqual(directory["ann"]["display"], "Ann")
        agents = {a["name"]: a for a in self.api("/api/overview")["agents"]}
        self.assertEqual(agents["ann"]["status"]["label"], "Coordinating")
        self.assertEqual(agents["bob"]["status"]["tone"], "good")

    def test_team_graph_has_lanes_and_only_lines_the_board_supports(self):
        self.join("cy")
        self.sw("--as", "bob", "claim", "T2")
        graph = self.api("/api/team")
        lanes = {n["id"]: n["lane"] for n in graph["nodes"]}
        self.assertEqual((lanes["human"], lanes["ann"], lanes["bob"], lanes["gate"]), (0, 1, 2, 4))
        kinds = {(e["kind"], e["from"], e["to"]) for e in graph["edges"]}
        self.assertIn(("approves", "human", "ann"), kinds)
        self.assertIn(("assigns", "ann", "bob"), kinds)
        self.assertFalse([k for k in kinds if k[0] in ("submits", "review", "collab")], "nothing submitted, reviewed or exchanged yet")
        with closing(sqlite3.connect(self.root / ".git" / "swarm" / "swarm.db")) as con:
            con.execute("UPDATE tasks SET state='ready_to_merge' WHERE id='T2'")
            con.execute(
                "INSERT INTO incidents(task,subject,opened_by,signal,state,revision,created_at) VALUES('T2','bob','ann','a signal','open',1,'2026-01-01T00:00:00+00:00')"
            )
            con.execute("UPDATE agents SET reviewing=1 WHERE name='cy'")
            con.commit()
        self.sw("--as", "bob", "send", "cy", "Please look at my last step", "--kind", "review_request")
        graph = self.api("/api/team")
        nodes = {n["id"]: n for n in graph["nodes"]}
        self.assertEqual(nodes["cy"]["role"], "Independent reviewer")
        self.assertEqual(nodes["cy"]["lane"], 3)
        kinds = {(e["kind"], e["from"], e["to"]) for e in graph["edges"]}
        self.assertIn(("submits", "bob", "gate"), kinds)
        self.assertIn(("review", "cy", "bob"), kinds)
        self.assertNotIn(("assigns", "ann", "cy"), kinds, "a reviewer is not assigned work")
        self.assertIn(("collab", "bob", "cy"), kinds)
        self.assertEqual(nodes["bob"]["detail"]["relations"][-1]["who"], "cy")

    def test_events_read_as_sentences_with_display_names(self):
        texts = [e["text"] for e in self.api("/api/overview")["events"]]
        self.assertTrue(any(t.startswith("Ann ") for t in texts), texts)
        self.assertFalse([t for t in texts if "_" in t.split(" ")[0]], "no raw event ids")


class Shared(WebCase):
    """One dashboard per user: a stable link, every board listed in it, a second run joining the first."""

    def other_board(self, name="other"):
        root = Path(self.home.name) / name
        root.mkdir()
        for args in (("init", "-q", "-b", "main"), ("commit", "--allow-empty", "-qm", "init")):
            subprocess.run(["git", *args], cwd=root, check=True, capture_output=True)
        sw = core.Swarm(root)
        try:
            return swarm_web.register(sw, f"python {name}.py")
        finally:
            sw.db.close()

    def test_the_link_stays_the_same_across_restarts_and_is_new_only_on_request(self):
        first_token, first_port = self.server.token, self.server.server_address[1]
        self.server.shutdown()
        self.server.server_close()
        with self.board() as sw:
            self.server = swarm_web.make_server(sw, describe=core.describe_event, command="python hivewire.py")
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.assertEqual(self.server.token, first_token)
        self.assertEqual(self.server.server_address[1], first_port, "the saved port is tried first")
        self.server.shutdown()
        self.server.server_close()
        with self.board() as sw:
            self.server = swarm_web.make_server(sw, port=0, describe=core.describe_event, command="python hivewire.py", reset=True)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.assertNotEqual(self.server.token, first_token)
        saved = json.loads((Path(self.home.name) / "server.json").read_text(encoding="utf-8"))
        self.assertEqual(saved["token"], self.server.token)

    def test_running_finds_the_live_dashboard_and_nothing_else(self):
        found = swarm_web.running()
        self.assertEqual(found, {"port": self.server.server_address[1], "token": self.server.token})
        self.assertFalse(swarm_web.ping(self.server.server_address[1], "not-the-token"))
        self.server.shutdown()
        self.assertIsNone(swarm_web.running(), "nothing answers once it has stopped")
        self.server.server_close()
        self.assertIsNone(swarm_web.running(), "and a state file alone proves nothing")

    def test_every_registered_board_is_listed_and_selected_with_b(self):
        other = self.other_board()
        mine = self.api("/api/overview")
        self.assertEqual(mine["active"]["name"], self.root.name, "the board that started the dashboard is the default")
        self.assertEqual({b["id"] for b in mine["boards"]}, {mine["active"]["id"], other["id"]})
        theirs = self.api(f"/api/overview?b={other['id']}")
        self.assertEqual(theirs["active"]["name"], "other")
        self.assertEqual(theirs["agents"], [], "a different board shows its own agents, not ours")
        self.assertEqual(theirs["pool"]["command"], "python other.py")
        listed = self.api("/api/boards")
        self.assertEqual(listed["default"], mine["active"]["id"])
        self.assertEqual(self.request("/api/overview?b=deadbeef")[0], 404)
        self.assertEqual(self.request("/api/overview?b=../../etc")[0], 404)
        self.assertEqual(self.request("/api/overview?b=" + other["id"] + "x")[0], 404)

    def test_the_registry_never_names_a_file_that_is_not_a_swarm_board(self):
        registry = Path(self.home.name) / "boards"
        registry.mkdir(exist_ok=True)
        (registry / "evil.json").write_text(json.dumps({"id": "abcdef01", "path": str(Path(self.home.name) / "secret.db"), "name": "evil"}), encoding="utf-8")
        (registry / "badid.json").write_text(json.dumps({"id": "../x", "path": str(self.root / ".git" / "swarm" / "swarm.db"), "name": "bad"}), encoding="utf-8")
        (registry / "junk.json").write_text("not json", encoding="utf-8")
        ids = {b["id"] for b in self.api("/api/boards")["boards"]}
        self.assertNotIn("abcdef01", ids)
        self.assertEqual(len(ids), 1)

    def test_a_board_whose_database_is_gone_is_reported_not_crashed_on(self):
        other = self.other_board("gone")
        for f in (Path(self.home.name) / "gone" / ".git" / "swarm").glob("swarm.db*"):
            f.unlink()
        self.assertEqual(self.request(f"/api/overview?b={other['id']}")[0], 404)
        self.assertEqual(self.api("/api/overview")["active"]["name"], self.root.name)


class SessionPayloads(WebCase):
    def test_ec346_session_tabs_retain_failures_logs_and_are_isolated_by_board(self):
        execution = "b" * 32
        with self.board() as sw, sw.tx():
            sw.db.execute(
                "INSERT INTO executions(id,agent,task,epoch,state,started_at,exit_code,policy) VALUES(?,?,?,?,?,?,?,?)",
                (execution, "ann", "T1", 1, "failed", core.iso(), 9, '{}'),
            )
            path = sw.dir / "logs" / (execution + ".log")
        path.write_text("retained <script> text\n\x1b[2Jnot a control channel", encoding="utf-8")
        before = self.api("/api/overview")["events"]
        listed = self.api("/api/sessions")
        self.assertEqual(listed["sessions"][0]["id"], execution)
        details = self.api("/api/sessions/" + execution)
        self.assertEqual(details["session"]["exit_code"], 9)
        self.assertIn("<script>", details["worker"]["text"])
        self.assertNotIn("\x1b", details["worker"]["text"])
        self.assertEqual(details["task"]["id"], "T1")
        self.assertEqual(self.api("/api/overview")["events"], before)
        self.assertEqual(self.request("/api/sessions", token=False)[0], 403)
        self.assertIn(self.request("/api/sessions", method="POST")[0], (403, 404, 405))
        self.assertIn(b"VIEWS.sessions", self.request("/")[2], "the served dashboard includes the session view")
        other = Shared.other_board(self)
        self.assertEqual(self.api(f"/api/sessions?b={other['id']}")["sessions"], [])
        self.assertEqual(self.request(f"/api/sessions/{execution}?b={other['id']}")[0], 404)
        self.assertEqual(self.request("/api/sessions/does-not-exist")[0], 404)

    def test_ec346_foreground_sessions_never_invent_a_retained_transcript(self):
        execution = "c" * 32
        with self.board() as sw, sw.tx():
            sw.db.execute(
                "INSERT INTO executions(id,agent,task,epoch,state,policy) VALUES(?,?,?,?,?,?)",
                (execution, "ann", "T1", 1, "exited", json.dumps({"transport": "terminal"})),
            )
        details = self.api("/api/sessions/" + execution)
        self.assertFalse(details["worker"]["available"])
        self.assertIn("without a retained transcript", details["worker"]["reason"])
        self.assertEqual(details["worker"]["text"], "")
        self.assertEqual(self.request("/api/sessions/../../secret")[0], 404)


class ReadOnly(WebCase):
    def test_requests_never_write_to_the_board(self):
        self.sw("--as", "ann", "claim", "T1")
        db = self.root / ".git" / "swarm" / "swarm.db"

        def fingerprint():
            with closing(sqlite3.connect(db)) as con:
                return [tuple(con.execute(f"SELECT COUNT(*), COALESCE(MAX(id),0) FROM {t}").fetchone()) for t in ("events", "messages", "checkpoints")]

        before = fingerprint()
        for path in ("/api/overview", "/api/inbox", "/api/decisions", "/api/task/T1"):
            self.api(path)
        self.assertEqual(fingerprint(), before)

    def test_the_dashboards_connection_cannot_write(self):
        con = swarm_web.open_readonly(self.root / ".git" / "swarm" / "swarm.db")
        try:
            with self.assertRaises(sqlite3.OperationalError):
                con.execute("INSERT INTO events(at,kind) VALUES('now','x')")
            with self.assertRaises(sqlite3.OperationalError):
                con.execute("DELETE FROM tasks")
        finally:
            con.close()


class Command(HardeningCase):
    def test_bad_arguments_are_refused_before_a_server_starts(self):
        self.assertIn("--once", self.sw("dashboard", "--web", "--once", ok=False))
        self.assertIn("--port", self.sw("dashboard", "--web", "--port", "70000", ok=False))

    def test_the_command_prints_a_working_link_and_a_second_run_joins_it(self):
        self.join("ann")
        home = tempfile.TemporaryDirectory()
        env = {**os.environ, "SWARM_WEB_HOME": home.name}
        proc = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import faulthandler, runpy, sys; "
                "from pathlib import Path; "
                "faulthandler.dump_traceback_later(20); "
                "sys.argv.pop(0); "
                "sys.path.insert(0, str(Path(sys.argv[0]).parent)); "
                "runpy.run_path(sys.argv[0], run_name='__main__')",
                str(fixtures.SWARM),
                "dashboard", "--web", "--no-open", "--observe", "--port", "0",
            ],
            cwd=self.root,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            env=env,
        )
        lines: queue.Queue = queue.Queue()
        threading.Thread(target=lambda: lines.put(proc.stdout.readline()), daemon=True).start()
        try:
            try:
                first = lines.get(timeout=30)
            except queue.Empty:
                proc.kill()
                proc.wait(timeout=10)
                self.fail(f"Dashboard never printed its startup link. Child stderr:\n{proc.stderr.read()}")
            self.assertIn("Hivewire dashboard: http://127.0.0.1:", first, first + (proc.stderr.read() if proc.poll() else ""))
            url = urlparse(first.split(": ", 1)[1].strip())
            conn = http.client.HTTPConnection(url.hostname, url.port, timeout=10)
            conn.request("GET", f"{url.path}?{url.query}", headers={"Host": f"{url.hostname}:{url.port}"})
            response = conn.getresponse()
            self.assertEqual(response.status, 200)
            self.assertIn(b"<title>Hivewire</title>", response.read())
            conn.close()
            second = subprocess.run(
                [sys.executable, str(fixtures.SWARM), "dashboard", "--web", "--no-open", "--observe"],
                cwd=self.root,
                capture_output=True,
                text=True,
                encoding="utf-8",
                env=env,
                timeout=60,
                check=False,
            )
            self.assertEqual(second.returncode, 0, second.stdout + second.stderr)
            self.assertIn("already running", second.stdout)
            self.assertIn(f"{url.hostname}:{url.port}/?t=", second.stdout)
            self.assertEqual(parse_qs(urlparse(second.stdout.split("\n  ", 1)[1].split("\n")[0].strip()).query)["t"], parse_qs(url.query)["t"], "the same link, not a new one")
            refused = subprocess.run(
                [sys.executable, str(fixtures.SWARM), "dashboard", "--web", "--no-open", "--observe", "--reset-link"],
                cwd=self.root,
                capture_output=True,
                text=True,
                encoding="utf-8",
                env=env,
                timeout=60,
                check=False,
            )
            self.assertNotEqual(refused.returncode, 0)
            self.assertIn("already running", refused.stdout + refused.stderr)
        finally:
            proc.terminate()
            try:
                proc.wait(timeout=15)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
            proc.stdout.close()
            proc.stderr.close()
            home.cleanup()
