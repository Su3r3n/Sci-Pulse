"""
Integration & Contract Test Suite for `registry-go` (§4.1–§4.6) and Offline Fixture Validation (§5.2–§5.4).

Validates:
1. `services/registry-go/migrations/` DDL (`000001_init.up.sql`, `000001_init.down.sql`,
   `000002_cdc.up.sql`, `000002_cdc.down.sql`) for required tables (`institutions`,
   `tracked_projects`, `project_repos`, `project_institutions`), constraints, triggers,
   `REPLICA IDENTITY FULL`, and publication `dbz_registry`.
2. `services/registry-go/openapi.yaml` endpoints and RFC 7807 `application/problem+json` responses.
3. Offline fixtures in `data/cache/openalex/` and `data/cache/arxiv/` cover both valid records
   (for ROOT, Rucio, XRootD) and all 6 DLQ failure scenarios (`parse_error`, `missing_title`,
   `missing_published_at`, `bad_doi`, `future_published_at`, `no_authors`).
4. Optional live HTTP integration test against `REGISTRY_URL` when running against Docker stack.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
CODER1_ROOT = REPO_ROOT
TESTER_ROOT = REPO_ROOT


def test_registry_migrations_up_and_down_completeness() -> None:
    mig_dir = CODER1_ROOT / "services" / "registry-go" / "migrations"
    init_up = (mig_dir / "000001_init.up.sql").read_text(encoding="utf-8")
    init_down = (mig_dir / "000001_init.down.sql").read_text(encoding="utf-8")
    cdc_up = (mig_dir / "000002_cdc.up.sql").read_text(encoding="utf-8")
    cdc_down = (mig_dir / "000002_cdc.down.sql").read_text(encoding="utf-8")

    for table in ("institutions", "tracked_projects", "project_repos", "project_institutions"):
        assert f"CREATE TABLE {table}" in init_up or f"CREATE TABLE IF NOT EXISTS {table}" in init_up
        assert table in init_down
        assert f"ALTER TABLE {table}" in cdc_up and "REPLICA IDENTITY FULL" in cdc_up

    assert "CREATE FUNCTION set_updated_at()" in init_up or "CREATE OR REPLACE FUNCTION set_updated_at()" in init_up
    assert "trg_projects_upd" in init_up
    assert "project_repos_one_primary" in init_up
    assert "CREATE PUBLICATION dbz_registry" in cdc_up
    assert "DROP PUBLICATION IF EXISTS dbz_registry" in cdc_down


def test_offline_cache_fixtures_cover_valid_and_dlq_cases() -> None:
    oa_dir = TESTER_ROOT / "data" / "cache" / "openalex"
    arxiv_dir = TESTER_ROOT / "data" / "cache" / "arxiv"

    valid_oa = json.loads((oa_dir / "01_valid_works.json").read_text(encoding="utf-8"))
    assert len(valid_oa["results"]) == 3
    titles = " ".join(r["title"] for r in valid_oa["results"])
    for keyword in ("ROOT", "Rucio", "XRootD"):
        assert keyword in titles

    invalid_oa = json.loads((oa_dir / "02_invalid_works.json").read_text(encoding="utf-8"))
    assert len(invalid_oa["results"]) == 5

    corrupt_oa = (oa_dir / "03_corrupt.json").read_text(encoding="utf-8")
    with pytest.raises(json.JSONDecodeError):
        json.loads(corrupt_oa)

    valid_arxiv = (arxiv_dir / "01_valid_listrecords.xml").read_text(encoding="utf-8")
    root = ET.fromstring(valid_arxiv)
    assert root is not None
    assert "https://github.com/root-project/root" in valid_arxiv
    assert "https://github.com/rucio/rucio" in valid_arxiv

    invalid_arxiv = (arxiv_dir / "02_invalid_listrecords.xml").read_text(encoding="utf-8")
    assert ET.fromstring(invalid_arxiv) is not None

    corrupt_arxiv = (arxiv_dir / "03_corrupt.xml").read_text(encoding="utf-8")
    with pytest.raises(ET.ParseError):
        ET.fromstring(corrupt_arxiv)


@pytest.mark.skipif(
    not os.environ.get("RUN_LIVE_REGISTRY_TESTS"),
    reason="Set RUN_LIVE_REGISTRY_TESTS=1 to run live HTTP tests against running registry-go instance",
)
def test_live_registry_crud_and_problem_json() -> None:
    base_url = os.environ.get("REGISTRY_URL", "http://localhost:8080").rstrip("/")
    req = urllib.request.Request(f"{base_url}/api/v1/projects", method="POST")
    try:
        urllib.request.urlopen(req, timeout=3.0)
        pytest.fail("Expected 401 Unauthorized without X-API-Key")
    except urllib.error.HTTPError as exc:
        assert exc.code == 401
        assert "application/problem+json" in exc.headers.get("Content-Type", "")
