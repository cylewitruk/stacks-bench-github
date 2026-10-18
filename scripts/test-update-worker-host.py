#!/usr/bin/env python3
"""Check the worker updater's operator interface and fail-closed ordering."""

from pathlib import Path
import json
import os
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent.parent
SCRIPT = ROOT / "scripts/update-worker-host.sh"


class WorkerHostUpdaterTest(unittest.TestCase):
    def test_help_documents_scope_and_defaults(self) -> None:
        result = subprocess.run(
            [SCRIPT, "--help"],
            check=True,
            capture_output=True,
            text=True,
        )
        self.assertIn("default: combined", result.stdout)
        self.assertIn("wire-compatible worker-only", result.stdout)
        self.assertIn("authenticated sbgh-cli operator access", result.stdout)
        self.assertNotIn("backup", result.stdout.lower())

    def test_mutating_steps_keep_fail_closed_order(self) -> None:
        source = SCRIPT.read_text(encoding="utf-8")
        milestones = (
            'cordon=$(operator fleet cordon --worker-id "$worker_id"',
            'quiescent=$(operator fleet wait-worker --worker-id "$worker_id"',
            'systemctl stop "$worker_unit"',
            '"$repo_root/scripts/install-worker.sh"',
            '"$worker" --config "$worker_config" --preflight-only',
            "post_registration=$(worker_command fleet check)",
            'wait_for_worker "$worker_id" true',
            'safe_to_stop_worker=0',
            'operator fleet uncordon --worker-id "$worker_id"',
            'wait_for_worker "$worker_id" false',
        )
        positions = [source.rindex(milestone) for milestone in milestones]
        self.assertEqual(positions, sorted(positions))
        self.assertIn("trap recover_on_failure EXIT", source)
        self.assertIn("UPDATE FAILED — attempting fail-closed worker recovery", source)
        self.assertNotIn("install-daemon.sh", source)

    def run_fixture(self, *, legacy=False, failure=""):
        """Run the actual shell workflow against disposable, unprivileged commands."""
        with tempfile.TemporaryDirectory(prefix="sbgh-worker-update-") as directory:
            root = Path(directory)
            scripts = root / "scripts"
            commands = root / "commands"
            scripts.mkdir()
            commands.mkdir()
            (root / "combined.toml").write_text("# fixture\n")
            state_path = root / "state.json"
            log_path = root / "calls.jsonl"
            state_path.write_text(json.dumps({
                "generation": 0, "draining": False, "live": True, "active": True,
                "session": "00000000-0000-0000-0000-000000000002",
                "installed": False, "uncordoned": False,
            }))
            mock = commands / "mock"
            mock.write_text(MOCK)
            mock.chmod(0o755)
            for name in ["runuser", "systemctl", "systemd-analyze", "sbgh-cli", "sbgh-worker"]:
                (commands / name).symlink_to(mock)
            (scripts / "install-worker.sh").symlink_to(mock)
            source = SCRIPT.read_text()
            # Only the disposable script gets test paths and an unprivileged guard.
            # Production has no environment-controlled root/command bypass.
            source = source.replace('[[ ${EUID:-$(id -u)} -eq 0 ]]', '[[ 0 -eq 0 ]]')
            source = source.replace("cli=/usr/local/bin/sbgh-cli", f"cli={commands}/sbgh-cli")
            source = source.replace("worker=/usr/local/bin/sbgh-worker", f"worker={commands}/sbgh-worker")
            source = source.replace('worker_config="/etc/sbgh/worker/$profile.toml"', f'worker_config="{root}/combined.toml"')
            script = scripts / SCRIPT.name
            script.write_text(source)
            script.chmod(0o755)
            result = subprocess.run([script, "--no-build"], capture_output=True, text=True,
                timeout=15, env={**os.environ, "PATH": f"{commands}:{os.environ['PATH']}",
                    "FIXTURE_STATE": str(state_path), "FIXTURE_LOG": str(log_path),
                    "FIXTURE_LEGACY": str(int(legacy)), "FIXTURE_FAILURE": failure})
            calls = [json.loads(line) for line in log_path.read_text().splitlines()]
            return result, json.loads(state_path.read_text()), calls

    def test_resident_update_starts_cordoned_and_preserves_other_workers(self):
        """A resident successor is verified before its admission opens."""
        result, state, calls = self.run_fixture()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(state["draining"])
        shutdown = next(i for i, call in enumerate(calls) if call[:3] == ["sbgh-cli", "fleet", "shutdown-after-idle"])
        install = next(i for i, call in enumerate(calls) if call[0] == "install-worker.sh")
        start = next(i for i, call in enumerate(calls) if call[:2] == ["systemctl", "start"])
        uncordon = next(i for i, call in enumerate(calls) if call[:3] == ["sbgh-cli", "fleet", "uncordon"])
        self.assertLess(shutdown, install)
        self.assertLess(install, start)
        self.assertLess(start, uncordon)
        # Fixture reports debt on another worker throughout, and it must not block.
        for call in calls:
            if "--worker-id" in call:
                self.assertEqual(call[call.index("--worker-id") + 1], WORKER_ID)
            if call[0] == "systemctl" and call[1] in ("start", "stop", "restart"):
                self.assertEqual(call[2], "sbgh-worker@combined.service")

    def test_previous_worker_is_started_after_uncordon(self):
        """Keep the previous worker's exit-on-cordon startup ordering."""
        result, _, calls = self.run_fixture(legacy=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(any(call[:3] == ["sbgh-cli", "fleet", "shutdown-after-idle"] for call in calls))
        uncordon = next(i for i, call in enumerate(calls) if call[:3] == ["sbgh-cli", "fleet", "uncordon"])
        start = next(i for i, call in enumerate(calls) if call[:2] == ["systemctl", "start"])
        self.assertLess(uncordon, start)

    def test_missing_maintenance_api_refuses_before_any_mutation(self):
        """An older operator surface cannot trigger a partial upgrade."""
        result, state, calls = self.run_fixture(failure="unsupported")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(state["draining"])
        self.assertFalse(state["installed"])
        self.assertFalse(any(call[:3] == ["sbgh-cli", "fleet", "cordon"] for call in calls))

    def test_failed_wait_or_preflight_keeps_target_cordoned(self):
        """Failed checks never open admission or stop unquiesced work."""
        for failure in ["wait", "preflight"]:
            with self.subTest(failure=failure):
                result, state, calls = self.run_fixture(failure=failure)
                self.assertNotEqual(result.returncode, 0)
                self.assertTrue(state["draining"])
                self.assertFalse(any(call[:3] == ["sbgh-cli", "fleet", "uncordon"] for call in calls))
                if failure == "wait":
                    self.assertFalse(any(call[:2] == ["systemctl", "stop"] for call in calls))
                    self.assertFalse(any(call[0] == "install-worker.sh" for call in calls))

    def test_failure_after_uncordon_does_not_stop_new_execution(self):
        """A late diagnostic failure must not kill newly accepted work."""
        result, state, calls = self.run_fixture(failure="reactivation")
        self.assertNotEqual(result.returncode, 0)
        self.assertTrue(state["draining"])
        uncordon = next(i for i, call in enumerate(calls) if call[:3] == ["sbgh-cli", "fleet", "uncordon"])
        self.assertFalse(any(call[:2] == ["systemctl", "stop"] for call in calls[uncordon:]))


WORKER_ID = "00000000-0000-0000-0000-000000000001"

# Command fixtures implement only the interfaces exercised by the production script.
MOCK = r'''#!/usr/bin/env python3
import json, os, sys
from pathlib import Path
name = Path(sys.argv[0]).name
args = sys.argv[1:]
if name == "runuser":
    command = args[args.index("--") + 1:]
    os.execvp(command[0], command)
state_path = Path(os.environ["FIXTURE_STATE"])
state = json.loads(state_path.read_text())
with open(os.environ["FIXTURE_LOG"], "a") as log:
    log.write(json.dumps([name, *args]) + "\n")
legacy = os.environ["FIXTURE_LEGACY"] == "1"
failure = os.environ["FIXTURE_FAILURE"]
worker = "00000000-0000-0000-0000-000000000001"
def save():
    state_path.write_text(json.dumps(state))
def snapshot():
    return {"worker_id":worker, "generation":state["generation"], "enabled":True,
        "cordoned":state["draining"],"session_status":"draining" if state["draining"] else "idle",
        "session_id":state["session"], "session_live":state["live"],
        "protocol_features":[] if legacy else ["resident_cordon", "shutdown_after_idle"]}
if name == "systemctl":
    if args[0] == "is-active":
        sys.exit(0 if state["active"] else 3)
    if args[0] == "stop":
        state.update(active=False, live=False)
    if args[0] == "start":
        state.update(active=True, live=True, session="00000000-0000-0000-0000-000000000003")
    save()
elif name == "systemd-analyze":
    pass
elif name == "install-worker.sh":
    state["installed"] = True
    save()
elif name == "sbgh-worker":
    if "--preflight-only" in args:
        sys.exit(1 if failure == "preflight" else 0)
    semantic = "" if legacy else " cordon_semantic=resident"
    print(f"connectivity=ok registration=authorized worker_id={worker} protocol_version=2 draining={str(state['draining']).lower()} clock_skew_ms=1{semantic}")
elif name == "sbgh-cli":
    if args == ["status"]:
        print("api: ok scope: admin")
    elif args[:2] == ["fleet", "status"]:
        print("workers=2 online=2 draining=0 attempts=1 cleanup=1 gaps=1 staged_bytes=100")
    elif args[:2] == ["fleet", "show-worker"]:
        print(json.dumps({"worker":{"enabled":True,"draining":state["draining"],
            "session_status":"draining" if state["draining"] else "idle","attempt_id":None}}))
    elif args[:2] == ["fleet", "worker-status"]:
        if failure == "unsupported":
            sys.exit(1)
        if failure == "reactivation" and state["uncordoned"]:
            sys.exit(1)
        print(json.dumps(snapshot()))
    elif args[:2] == ["fleet", "wait-worker"]:
        if "--help" in args:
            sys.exit(0)
        if failure == "wait":
            sys.exit(1)
        assert state["draining"]
        assert int(args[args.index("--generation") + 1]) == state["generation"]
        print(json.dumps(snapshot()))
    elif args[1] in ("cordon", "uncordon", "shutdown-after-idle"):
        state["generation"] += 1
        if args[1] == "cordon":
            state["draining"] = True
        elif args[1] == "uncordon":
            state.update(draining=False, uncordoned=True)
        else:
            state.update(active=False, live=False)
        save()
        print(json.dumps({"generation":state["generation"],"target_session_id":state["session"]}))
    else:
        raise AssertionError(args)
else:
    raise AssertionError(name)
'''


if __name__ == "__main__":
    unittest.main()
