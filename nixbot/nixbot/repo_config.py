"""Per-repository configuration (`nixbot.toml`), read from the
build worktree instead of via a buildbot remote command."""

from __future__ import annotations

import json
import tomllib
from fnmatch import fnmatch
from typing import TYPE_CHECKING, Literal, Self

from pydantic import BaseModel, Field, ValidationError

if TYPE_CHECKING:
    from pathlib import Path


# Preference order; the legacy buildbot-nix name keeps repositories
# migrating from buildbot-nix working without a rename.
CONFIG_FILENAMES = ("nixbot.toml", "buildbot-nix.toml")
DEFAULT_EVAL_KEY = '[".","flake.lock","checks"]'
EVAL_KEY_ATTRIBUTE_INDEX = 2

AttributeEvent = Literal["push", "pull_request"]


class RepoConfigError(Exception):
    pass


def _validate_flake_dir(flake_dir: str, repo_root: Path) -> None:
    """Validate that flake_dir is a safe relative path within the repo root."""
    resolved = (repo_root / flake_dir).resolve()
    if ":" in flake_dir or not resolved.is_relative_to(repo_root.resolve()):
        msg = f"Invalid flake_dir {flake_dir}"
        raise RepoConfigError(msg)


def eval_key_for(flake_dir: str, lock_file: str, attribute: str) -> str:
    """Stable identity for one flake evaluation selection."""
    return json.dumps([flake_dir, lock_file, attribute], separators=(",", ":"))


def eval_attribute_from_key(eval_key: str | None) -> str:
    """Recover the selected top-level flake attribute from a stored key.

    Older rows may carry just the attribute name; keep that readable so
    status replay remains correct across migrations.
    """
    if not eval_key:
        return "checks"
    try:
        parts = json.loads(eval_key)
    except json.JSONDecodeError:
        return eval_key.rsplit("\0", 1)[-1]
    if (
        isinstance(parts, list)
        and len(parts) > EVAL_KEY_ATTRIBUTE_INDEX
        and isinstance(parts[EVAL_KEY_ATTRIBUTE_INDEX], str)
    ):
        return parts[EVAL_KEY_ATTRIBUTE_INDEX]
    return "checks"


class BranchAttributeRule(BaseModel):
    match: str
    attribute: str
    events: list[AttributeEvent] = Field(
        default_factory=lambda: ["push", "pull_request"]
    )


class BranchConfig(BaseModel):
    flake_dir: str = "."
    lock_file: str = "flake.lock"
    attribute: str = "checks"
    attribute_branches: list[BranchAttributeRule] = Field(default_factory=list)
    effects_on_pull_requests: bool = False
    effects_branches: list[str] = []

    @property
    def eval_key(self) -> str:
        return eval_key_for(self.flake_dir, self.lock_file, self.attribute)

    def resolve_for_event(self, branch: str, is_pull_request: bool) -> Self:
        """Apply the first matching branch attribute override."""
        event: AttributeEvent = "pull_request" if is_pull_request else "push"
        for rule in self.attribute_branches:
            if event in rule.events and fnmatch(branch, rule.match):
                return self.model_copy(update={"attribute": rule.attribute})
        return self

    @classmethod
    def loads(cls, text: str | None) -> Self:
        """Parse `nixbot.toml` content (e.g. read from a git ref);
        defaults on absence or invalid content."""
        if text is None:
            return cls()
        try:
            return cls.model_validate(tomllib.loads(text))
        except (tomllib.TOMLDecodeError, ValidationError):
            return cls()

    @classmethod
    def load(cls, repo_root: Path) -> Self:
        """Read `nixbot.toml` (or the legacy `buildbot-nix.toml`)
        from a checkout; defaults on absence or invalid content
        (matching the buildbot-era behavior)."""
        for filename in CONFIG_FILENAMES:
            try:
                text = (repo_root / filename).read_text()
            except OSError:
                continue
            break
        else:
            return cls()
        config = cls.loads(text)
        try:
            _validate_flake_dir(config.flake_dir, repo_root)
        except RepoConfigError:
            return cls()
        return config
