"""The nightly user units: safety directives present, every flag in ExecStart
known to the CLI (a unit must never call a flag the installed kb lacks)."""

import shlex
from pathlib import Path

from kb import cli

UNITS = Path(__file__).resolve().parents[1] / "units"


def directives(path: Path) -> list[tuple[str, str, str]]:
    out, section = [], None
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith(("#", ";")):
            continue
        if line.startswith("["):
            section = line.strip("[]")
            continue
        key, _, value = line.partition("=")
        out.append((section, key.strip(), value.strip()))
    return out


def values(path, key):
    return [v for _, k, v in directives(path) if k == key]


def test_service_is_offline_private_and_bounded():
    svc = UNITS / "kb-update.service"
    one = lambda k: values(svc, k)[-1]
    assert one("Type") == "oneshot"
    # systemd 249, user unit: PrivateNetwork alone is silently ignored ("network
    # namespace setup failed, ignoring") — it only works together with PrivateUsers.
    assert one("PrivateUsers") == "yes" and one("PrivateNetwork") == "yes"
    assert one("UMask") == "0077"
    assert one("MemoryMax") == "24G" and one("MemorySwapMax") == "0"
    assert "MemoryHigh" not in [k for _, k, _ in directives(svc)], "throttling creates the PSI it guards against"
    env = values(svc, "Environment")
    for var in ("HF_HUB_OFFLINE=1", "TRANSFORMERS_OFFLINE=1", "HF_HUB_DISABLE_TELEMETRY=1"):
        assert var in env, var


def test_execstart_flags_are_known_to_the_cli():
    svc = UNITS / "kb-update.service"
    argv = shlex.split(values(svc, "ExecStart")[-1])
    assert argv[0].endswith("/kb") and argv[1] == "update"
    args = cli.build_parser().parse_args(argv[1:])
    assert args.lock_timeout == 0, "the timer must not queue behind a manual writer"
    assert args.max_psi is not None
    assert args.embed and args.budget_minutes == 180, "drains the vector backlog in bounded nights"
    assert args.require_offline and not args.allow_network


def test_timer_runs_nightly_and_catches_up():
    timer = UNITS / "kb-update.timer"
    assert values(timer, "OnCalendar") == ["*-*-* 05:20"]
    assert values(timer, "Persistent") == ["true"]
    assert values(timer, "WantedBy") == ["timers.target"]


def test_a_failed_night_pages_the_operator():
    """Every non-zero exit is a page: 3 (missing root, mass-delete guard, parse
    failure, free-space gate, a skipped --embed pass), 75 (deferred by memory
    pressure — the index did not move that night), 1/137/203 (crash, MemoryMax,
    no venv). Same hook as the other user units of this machine."""
    svc = UNITS / "kb-update.service"
    unit = [(k, v) for s, k, v in directives(svc) if s == "Unit"]
    assert ("OnFailure", "signal-alert@%n.service") in unit
    assert not values(svc, "SuccessExitStatus"), "no exit code is waved through"


def test_a_crash_traceback_is_masked_on_its_way_to_the_journal(tmp_path):
    """OnFailure ships the journal tail to Telegram; an exception message can
    carry indexed text (a heading in a ValueError). `kb` installs a masking
    excepthook in main()."""
    import os
    import subprocess
    import sys
    key = "0x" + "7a" * 32
    root = Path(__file__).resolve().parents[1]
    code = (f"import sys; sys.path.insert(0, {str(root)!r})\n"
            "import kb.cli\n"
            "try:\n"
            "    kb.cli.main(['--help'])\n"
            "except SystemExit:\n"
            "    pass\n"
            f"raise RuntimeError('duplicate anchor # Ключ {key}')\n")
    r = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=60,
                       cwd=tmp_path, env=dict(os.environ))
    assert r.returncode == 1 and "Traceback" in r.stderr and "RuntimeError" in r.stderr
    assert key not in r.stderr and "redacted" in r.stderr
