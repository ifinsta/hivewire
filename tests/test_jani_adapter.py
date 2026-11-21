"""Experimental Jani adapter: coordination is not execution authority."""

from __future__ import annotations

import argparse
import contextlib
import io
from unittest import mock

from test_hardening import HardeningCase, core


class JaniAdapter(HardeningCase):
    @staticmethod
    def args(**changes):
        args = dict(kind="jani", name="jani-observer", task="T1", model="",
                    caps="", unattended=False, dry_run=True, force=False, as_=None)
        args.update(changes)
        return argparse.Namespace(**args)

    def test_default_adapter_is_read_only_and_unverified(self):
        with self.board() as sw:
            ad = sw.config["adapters"]["jani"]
            self.assertFalse(ad["verified"])
            self.assertEqual(ad["auth"], [])
            self.assertEqual(ad["unattended"], [])
            self.assertIsNone(core.interactive_argv(sw, "jani"))
            argv = core.render(ad["run"], {"prompt": "read only", "cwd": "path with spaces"})
            self.assertEqual(argv, ["jani", "--print", "read only", "--repo", "path with spaces", "--max-steps", "8"])
            self.assertEqual(core.render(ad["model"], {"model": "example-model"}), ["--model", "example-model"])

    def test_unverified_launch_requires_explicit_force(self):
        with self.board() as sw, mock.patch.object(core.shutil, "which", return_value="jani"):
            with self.assertRaisesRegex(core.SwarmError, "unverified"):
                core.cmd_spawn(sw, self.args())
            self.assertIsNone(sw.agent("jani-observer"))

    def test_unattended_refused_even_when_globally_enabled(self):
        with self.board() as sw, mock.patch.object(core.shutil, "which", return_value="jani"):
            sw.config["spawn"]["allow_unattended"] = True
            with self.assertRaisesRegex(core.SwarmError, "does not support unattended"):
                core.cmd_spawn(sw, self.args(force=True, unattended=True))
            self.assertIsNone(sw.agent("jani-observer"))

    def test_missing_binary_refuses_before_claim(self):
        with self.board() as sw, mock.patch.object(core.shutil, "which", return_value=None):
            with self.assertRaisesRegex(core.SwarmError, "not installed"):
                core.cmd_spawn(sw, self.args(force=True))
            self.assertIsNone(sw.agent("jani-observer"))

    def test_forced_dry_run_does_not_join_claim_or_launch(self):
        with self.board() as sw, mock.patch.object(core.shutil, "which", return_value="jani"), \
             mock.patch.object(core, "require_options") as check, \
             mock.patch.object(core, "prepare_agent") as prepare, \
             contextlib.redirect_stdout(io.StringIO()) as output:
            core.cmd_spawn(sw, self.args(force=True, model="example-model"))
            check.assert_called_once()
            prepare.assert_not_called()
            self.assertIn("--print", output.getvalue())
            self.assertIn("--max-steps 8", output.getvalue())
            self.assertIn("--model example-model", output.getvalue())
            self.assertIn("nothing was changed", output.getvalue())
            self.assertNotIn("--allow-write", output.getvalue())
            self.assertNotIn("--allow-exec", output.getvalue())
            self.assertIsNone(sw.agent("jani-observer"))

    def test_option_drift_refuses_before_claim(self):
        with self.board() as sw, mock.patch.object(core.shutil, "which", return_value="jani"), \
             mock.patch.object(core, "require_options", side_effect=core.SwarmError("option drift")), \
             mock.patch.object(core, "prepare_agent") as prepare:
            with self.assertRaisesRegex(core.SwarmError, "option drift"):
                core.cmd_spawn(sw, self.args(force=True))
            prepare.assert_not_called()
            self.assertIsNone(sw.agent("jani-observer"))
