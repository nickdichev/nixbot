"""Shared helpers for nixbot_effects tests."""

from __future__ import annotations

import subprocess
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


def git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        text=True,
        capture_output=True,
    ).stdout.strip()


def init_repo(tmp_path: Path, files: dict[str, str] | None = None) -> tuple[Path, str]:
    """Create tmp_path/repo on branch main with one commit; return (repo, rev)."""
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init", "-b", "main")
    git(repo, "config", "user.name", "test")
    git(repo, "config", "user.email", "test@test")
    for name, content in (files or {"file.txt": "content"}).items():
        (repo / name).write_text(content)
    git(repo, "add", ".")
    git(repo, "commit", "-m", "initial")
    return repo, git(repo, "rev-parse", "HEAD")
