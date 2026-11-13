"""Нормализация имён GitHub-репозиториев. Без зависимостей от pyspark (тестируется отдельно)."""

import re

# owner/name из ссылок и текста. Суффиксы (.git, /, точка в конце предложения) отрезаются ниже.
GH_REPO_PATTERN = re.compile(r"github\.com/([A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+)")


def normalize_repo(full_name: str) -> str:
    """Возвращает owner/name без хвостов '/', '.' и суффикса '.git'.

    M5: прежде применялся str.rstrip(".git/"), который срезает набор символов,
    а не суффикс: 'root-project/root' превращался в 'root-project/roo'.
    """
    name = full_name.strip().rstrip("/").rstrip(".")
    if name.lower().endswith(".git"):
        name = name[: -len(".git")]
    return name.rstrip("/")


def extract_repos(text: str) -> list:
    """Все owner/name, найденные в тексте по ссылкам github.com/..., нормализованные."""
    if not text:
        return []
    return [normalize_repo(m) for m in GH_REPO_PATTERN.findall(text)]
