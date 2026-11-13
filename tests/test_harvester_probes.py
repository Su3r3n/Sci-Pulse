"""
Пробы harvester-go (правило курсора §5.3/§5.4, ошибки источников, --offline) на реальных
источниках OpenAlex/arXiv против httptest-серверов.

Go-код проб лежит в tests/go_probes/*.go.tmpl и подставляется в пакет
services/harvester-go/internal/runner через `go test -overlay`: в зону coder-1 ничего не пишется,
а пробы получают доступ к internal-пакетам.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
HARVESTER = REPO_ROOT / "services" / "harvester-go"
PROBE_SRC = Path(__file__).resolve().parent / "go_probes" / "runner_probe_test.go.tmpl"
PROBE_DST = HARVESTER / "internal" / "runner" / "zz_tester_probe_test.go"

pytestmark = pytest.mark.skipif(shutil.which("go") is None, reason="Go toolchain not installed")


def _run_probe(name: str, tmp_path: Path) -> subprocess.CompletedProcess:
    overlay = tmp_path / "overlay.json"
    overlay.write_text(json.dumps({"Replace": {str(PROBE_DST): str(PROBE_SRC)}}), encoding="utf-8")
    return subprocess.run(
        ["go", "test", "-count=1", f"-overlay={overlay}", "-run", f"^{name}$", "-v", "./internal/runner/"],
        cwd=HARVESTER,
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )


@pytest.mark.parametrize(
    "probe",
    [
        "TestProbeOpenAlex5xxKeepsCursorAtAckedPrefix",
        "TestProbeArxivFailingSetDoesNotMoveOtherScopes",
        "TestProbeOfflineArxivIsNotMultipliedBySets",
    ],
)
def test_harvester_probe(probe: str, tmp_path: Path) -> None:
    assert not PROBE_DST.exists(), f"{PROBE_DST} must not exist on disk; it is provided via -overlay"
    res = _run_probe(probe, tmp_path)
    out = res.stdout + res.stderr
    assert f"--- PASS: {probe}" in out, f"probe {probe} failed:\n{out[-3000:]}"
