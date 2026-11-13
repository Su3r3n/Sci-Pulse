"""Юнит-тесты нормализации репозиториев (M5). Запуск: python3 -m pytest -q platform/spark"""

import pytest

from repo_names import extract_repos, normalize_repo


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("root-project/root", "root-project/root"),  # главный кейс M5: не 'root-project/roo'
        ("foo/bit", "foo/bit"),                       # не 'foo/b'
        ("root-project/root.git", "root-project/root"),
        ("root-project/root.GIT", "root-project/root"),
        ("root-project/root/", "root-project/root"),
        ("root-project/root.", "root-project/root"),  # точка в конце предложения
        ("root-project/root.git.", "root-project/root"),
        ("rucio/rucio", "rucio/rucio"),
    ],
)
def test_normalize_repo_strips_suffix_not_characters(raw, expected):
    assert normalize_repo(raw) == expected


def test_extract_repos_from_links_and_text():
    text = "Code at https://github.com/root-project/root.git and github.com/rucio/rucio."
    assert extract_repos(text) == ["root-project/root", "rucio/rucio"]


def test_extract_repos_empty():
    assert extract_repos(None) == []
    assert extract_repos("") == []
