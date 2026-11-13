#!/usr/bin/env python3
"""Seed реестра (`make seed`), ТЗ §4.3: ROOT, Rucio, XRootD с их primary-репозиториями.

Идемпотентен: проект, который уже есть (409), находится через GET /api/v1/projects;
связка репозитория, которая уже есть (409), пропускается.

Переменные окружения:
  SEED_URL          базовый URL registry (по умолчанию http://127.0.0.1:8080)
  REGISTRY_API_KEY  ключ X-API-Key для записи
"""

import json
import os
import sys
import urllib.error
import urllib.request

BASE = os.environ.get("SEED_URL", "http://127.0.0.1:8080").rstrip("/")
API_KEY = os.environ.get("REGISTRY_API_KEY", "scipulse-registry-dev-key-change-me")

PROJECTS = [
    {
        "slug": "root",
        "name": "ROOT",
        "keywords": ["ROOT", "C++", "HEP"],
        "topics": ["hep-ex", "physics.data-an"],
        "priority": 1,
        "repos": [("root-project/root", True)],
    },
    {
        "slug": "rucio",
        "name": "Rucio",
        "keywords": ["Rucio", "data management"],
        "topics": ["cs.DC", "hep-ex"],
        "priority": 1,
        "repos": [("rucio/rucio", True)],
    },
    {
        "slug": "xrootd",
        "name": "XRootD",
        "keywords": ["XRootD", "storage"],
        "topics": ["cs.DC", "hep-ex"],
        "priority": 2,
        "repos": [("xrootd/xrootd", True)],
    },
]

PROJECT_FIELDS = ("slug", "name", "keywords", "topics", "priority")


def call(method, path, body=None):
    """Выполняет запрос к registry и возвращает (status, разобранное тело или текст)."""
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        BASE + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json", "X-API-Key": API_KEY},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as err:
        return err.code, err.read().decode("utf-8", errors="replace")


def find_project_id(slug):
    """Ищет id проекта по slug, листая GET /api/v1/projects курсором по id."""
    cursor = None
    while True:
        path = "/api/v1/projects?limit=100"
        if cursor is not None:
            path += f"&cursor={cursor}"
        status, body = call("GET", path)
        if status != 200 or not isinstance(body, dict):
            raise RuntimeError(f"list projects failed: HTTP {status} {body}")
        for item in body.get("items", []):
            if item.get("slug") == slug:
                return item["id"]
        cursor = body.get("next_cursor")
        if not cursor:
            return None


def main():
    failed = False
    for proj in PROJECTS:
        payload = {k: proj[k] for k in PROJECT_FIELDS}
        status, body = call("POST", "/api/v1/projects", payload)
        if status == 201:
            project_id = body["id"]
            print(f"project {proj['slug']}: created (id={project_id})")
        elif status == 409:
            project_id = find_project_id(proj["slug"])
            if project_id is None:
                print(f"project {proj['slug']}: conflict, but not found in list", file=sys.stderr)
                failed = True
                continue
            print(f"project {proj['slug']}: exists (id={project_id})")
        else:
            print(f"project {proj['slug']}: HTTP {status} {body}", file=sys.stderr)
            failed = True
            continue

        for repo, is_primary in proj["repos"]:
            status, body = call(
                "POST",
                f"/api/v1/projects/{project_id}/repos",
                {"github_full_name": repo, "is_primary": is_primary},
            )
            if status == 201:
                print(f"  repo {repo}: attached (primary={is_primary})")
            elif status == 409:
                print(f"  repo {repo}: exists")
            else:
                print(f"  repo {repo}: HTTP {status} {body}", file=sys.stderr)
                failed = True

    return 1 if failed else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (urllib.error.URLError, OSError) as err:
        print(f"seed failed: registry unreachable at {BASE}: {err}", file=sys.stderr)
        sys.exit(1)
