#!/usr/bin/env python3
"""
SciPulse E2E CDC Latency Benchmark & Verification Script (§4.3–§4.6, §10.5 S2-8).

Executes INSERT, UPDATE, and DELETE operations on `tracked_projects` via `registry-go`
REST API (which commits to PostgreSQL -> WAL -> Debezium `dbz_registry` -> Kafka
`cdc.registry.public.tracked_projects` -> ClickHouse `registry_projects_queue` ->
`mv_registry_projects` -> `scipulse.v_projects_current`), and verifies:
  1. INSERT propagates to `scipulse.v_projects_current`.
  2. UPDATE reflects updated fields (`name`, `priority`) in `scipulse.v_projects_current`.
  3. DELETE removes the record from `scipulse.v_projects_current` (`__deleted` filtered out).
  4. Latency p50 and p95 are recorded, asserting p95 < 2.0s (SLO §4.6).
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import Any, Dict, List, Optional


def http_json(
    method: str,
    url: str,
    payload: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: float = 5.0,
) -> tuple[int, Any]:
    req_headers = {"Accept": "application/json"}
    if headers:
        req_headers.update(headers)
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        req_headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=req_headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8")
            return resp.status, (json.loads(raw) if raw.strip() else None)
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            body = json.loads(raw)
        except Exception:
            body = raw
        return exc.code, body


def query_clickhouse_project(
    ch_url: str,
    project_id: int | str,
    ch_user: str = "default",
    ch_password: str = "",
) -> Optional[Dict[str, Any]]:
    sql = (
        "SELECT id, slug, name, priority, status "  # v_projects_current has no updated_at (03_views.sql)
        "FROM scipulse.v_projects_current "
        f"WHERE id = {int(project_id)} "
        "FORMAT JSONEachRow"
    )
    params = urllib.parse.urlencode({"query": sql})
    headers: Dict[str, str] = {}
    if ch_user:
        headers["X-ClickHouse-User"] = ch_user
    if ch_password:
        headers["X-ClickHouse-Key"] = ch_password

    req = urllib.request.Request(f"{ch_url.rstrip('/')}/?{params}", headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=5.0) as resp:
        text = resp.read().decode("utf-8").strip()
        if not text:
            return None
        lines = [line for line in text.splitlines() if line.strip()]
        return json.loads(lines[-1]) if lines else None


def wait_until(
    predicate,
    timeout_s: float = 10.0,
    poll_interval_s: float = 0.05,
) -> float:
    t0 = time.perf_counter()
    deadline = t0 + timeout_s
    while time.perf_counter() < deadline:
        if predicate():
            return time.perf_counter() - t0
        time.sleep(poll_interval_s)
    raise TimeoutError(f"CDC condition not satisfied within {timeout_s:.2f}s")


def run_single_cycle(
    registry_url: str,
    api_key: str,
    ch_url: str,
    ch_user: str,
    ch_password: str,
    timeout_s: float,
) -> Dict[str, float]:
    slug = f"cdc-probe-{uuid.uuid4().hex[:10]}"
    initial_name = f"CDC Probe {slug}"
    updated_name = f"CDC Probe Updated {slug}"
    auth_headers = {"X-API-Key": api_key}

    # 1. INSERT via registry-go POST /api/v1/projects (§4.2)
    status, created = http_json(
        "POST",
        f"{registry_url.rstrip('/')}/api/v1/projects",
        payload={
            "slug": slug,
            "name": initial_name,
            "description": "E2E CDC latency probe project",
            "keywords": ["hep", "cdc-probe"],
            "topics": ["Particle Physics"],
            "priority": 2,
            "status": "active",
        },
        headers=auth_headers,
    )
    if status != 201 or not isinstance(created, dict) or "id" not in created:
        raise RuntimeError(f"Failed to create project via registry-go: HTTP {status} {created}")

    project_id = created["id"]

    insert_latency = wait_until(
        lambda: (
            (row := query_clickhouse_project(ch_url, project_id, ch_user, ch_password)) is not None
            and row.get("name") == initial_name
        ),
        timeout_s=timeout_s,
    )

    # 2. UPDATE via registry-go PATCH /api/v1/projects/{id} (§4.2)
    status, updated = http_json(
        "PATCH",
        f"{registry_url.rstrip('/')}/api/v1/projects/{project_id}",
        payload={"name": updated_name, "priority": 1},
        headers=auth_headers,
    )
    if status != 200:
        raise RuntimeError(f"Failed to update project {project_id}: HTTP {status} {updated}")

    update_latency = wait_until(
        lambda: (
            (row := query_clickhouse_project(ch_url, project_id, ch_user, ch_password)) is not None
            and row.get("name") == updated_name
            and int(row.get("priority", 0)) == 1
        ),
        timeout_s=timeout_s,
    )

    # 3. DELETE via registry-go DELETE /api/v1/projects/{id} (§4.2)
    status, deleted_body = http_json(
        "DELETE",
        f"{registry_url.rstrip('/')}/api/v1/projects/{project_id}",
        headers=auth_headers,
    )
    if status != 204:
        raise RuntimeError(f"Failed to delete project {project_id}: HTTP {status} {deleted_body}")

    delete_latency = wait_until(
        lambda: query_clickhouse_project(ch_url, project_id, ch_user, ch_password) is None,
        timeout_s=timeout_s,
    )

    return {
        "insert_s": insert_latency,
        "update_s": update_latency,
        "delete_s": delete_latency,
    }


def percentile(values: List[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, int(round((pct / 100.0) * (len(ordered) - 1)))))
    return ordered[idx]


def main() -> int:
    parser = argparse.ArgumentParser(description="Measure Postgres -> Debezium -> Kafka -> ClickHouse CDC latency")
    parser.add_argument(
        "--registry-url",
        default=os.environ.get("REGISTRY_URL", "http://localhost:8080"),
        help="Base URL of registry-go API",
    )
    parser.add_argument(
        "--api-key",
        default=os.environ.get("REGISTRY_API_KEY", "scipulse-registry-dev-key-change-me"),  # = .env.example
        help="X-API-Key for registry-go mutations",
    )
    parser.add_argument(
        "--clickhouse-url",
        default=os.environ.get("CLICKHOUSE_HTTP_URL", "http://localhost:8123"),
        help="Base HTTP URL of ClickHouse",
    )
    parser.add_argument(
        "--clickhouse-user",
        default=os.environ.get("CLICKHOUSE_USER", "default"),
    )
    parser.add_argument(
        "--clickhouse-password",
        default=os.environ.get("CLICKHOUSE_PASSWORD", ""),
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=int(os.environ.get("CDC_ITERATIONS", "5")),
        help="Number of full INSERT/UPDATE/DELETE cycles to execute",
    )
    parser.add_argument(
        "--slo-p95-seconds",
        type=float,
        default=2.0,
        help="SLO threshold for p95 CDC latency in seconds (§4.6 default: 2.0s)",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=10.0,
        help="Per-operation poll timeout in seconds",
    )
    args = parser.parse_args()

    all_latencies: List[float] = []
    cycles: List[Dict[str, float]] = []

    for i in range(1, args.iterations + 1):
        res = run_single_cycle(
            registry_url=args.registry_url,
            api_key=args.api_key,
            ch_url=args.clickhouse_url,
            ch_user=args.clickhouse_user,
            ch_password=args.clickhouse_password,
            timeout_s=args.timeout_seconds,
        )
        cycles.append(res)
        all_latencies.extend([res["insert_s"], res["update_s"], res["delete_s"]])
        print(
            f"[cycle {i}/{args.iterations}] "
            f"insert={res['insert_s']*1000:.1f}ms "
            f"update={res['update_s']*1000:.1f}ms "
            f"delete={res['delete_s']*1000:.1f}ms"
        )

    p50 = statistics.median(all_latencies)
    p95 = percentile(all_latencies, 95.0)
    summary = {
        "iterations": args.iterations,
        "samples": len(all_latencies),
        "p50_seconds": round(p50, 4),
        "p95_seconds": round(p95, 4),
        "slo_p95_seconds": args.slo_p95_seconds,
        "passed": p95 < args.slo_p95_seconds,
    }
    print(json.dumps(summary, indent=2))

    if p95 >= args.slo_p95_seconds:
        print(
            f"ERROR: CDC p95 latency {p95:.3f}s exceeded SLO threshold {args.slo_p95_seconds:.3f}s",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
