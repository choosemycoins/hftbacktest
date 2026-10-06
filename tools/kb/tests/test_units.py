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


def test_timer_runs_nightly_and_catches_up():
    timer = UNITS / "kb-update.timer"
    assert values(timer, "OnCalendar") == ["*-*-* 05:20"]
    assert values(timer, "Persistent") == ["true"]
    assert values(timer, "WantedBy") == ["timers.target"]
