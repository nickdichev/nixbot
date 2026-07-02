"""Per-repository config file loading.

Repositories migrating from buildbot-nix still carry a
`buildbot-nix.toml`; it must keep working until renamed.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from nixbot.repo_config import BranchConfig, eval_attribute_from_key

if TYPE_CHECKING:
    from pathlib import Path


def test_load_nixbot_toml(tmp_path: Path) -> None:
    (tmp_path / "nixbot.toml").write_text('attribute = "hydraJobs"')
    assert BranchConfig.load(tmp_path).attribute == "hydraJobs"


def test_load_legacy_buildbot_nix_toml(tmp_path: Path) -> None:
    (tmp_path / "buildbot-nix.toml").write_text('attribute = "hydraJobs"')
    assert BranchConfig.load(tmp_path).attribute == "hydraJobs"


def test_nixbot_toml_wins_over_legacy(tmp_path: Path) -> None:
    (tmp_path / "nixbot.toml").write_text('attribute = "new"')
    (tmp_path / "buildbot-nix.toml").write_text('attribute = "old"')
    assert BranchConfig.load(tmp_path).attribute == "new"


def test_missing_files_default(tmp_path: Path) -> None:
    assert BranchConfig.load(tmp_path).attribute == "checks"


def test_branch_config_defaults(tmp_path: Path) -> None:
    config = BranchConfig.load(tmp_path)
    assert config.flake_dir == "."
    assert config.lock_file == "flake.lock"
    assert config.attribute == "checks"
    assert not config.effects_on_pull_requests


def test_branch_config_from_toml(tmp_path: Path) -> None:
    (tmp_path / "nixbot.toml").write_text(
        'flake_dir = "subdir"\nlock_file = "dev.lock"\nattribute = "hydraJobs"\n'
        "effects_on_pull_requests = true\n"
    )
    config = BranchConfig.load(tmp_path)
    assert config.flake_dir == "subdir"
    assert config.lock_file == "dev.lock"
    assert config.attribute == "hydraJobs"
    assert config.effects_on_pull_requests


def test_attribute_branch_rules_resolve_by_branch_and_event() -> None:
    config = BranchConfig.loads(
        """
        attribute = "checks"

        [[attribute_branches]]
        match = "main"
        attribute = "packages"
        events = ["push"]

        [[attribute_branches]]
        match = "release-*"
        attribute = "packages"
        events = ["push"]
        """
    )

    assert config.attribute == "checks"
    assert config.resolve_for_event("main", is_pull_request=False).attribute == (
        "packages"
    )
    assert config.resolve_for_event("main", is_pull_request=True).attribute == "checks"
    assert config.resolve_for_event("release-1.0", is_pull_request=False).attribute == (
        "packages"
    )


def test_eval_key_contains_selected_attribute() -> None:
    config = BranchConfig(flake_dir="sub", lock_file="ci.lock", attribute="packages")
    assert eval_attribute_from_key(config.eval_key) == "packages"
    assert eval_attribute_from_key("checks") == "checks"


def test_branch_config_rejects_traversal(tmp_path: Path) -> None:
    (tmp_path / "nixbot.toml").write_text('flake_dir = "../../etc"\n')
    # Falls back to defaults on invalid config.
    assert BranchConfig.load(tmp_path).flake_dir == "."


def test_branch_config_invalid_toml(tmp_path: Path) -> None:
    (tmp_path / "nixbot.toml").write_text("not toml :::")
    assert BranchConfig.load(tmp_path).flake_dir == "."
