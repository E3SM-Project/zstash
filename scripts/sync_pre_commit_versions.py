#!/usr/bin/env python
"""Sync pinned QA tool versions from ``.pre-commit-config.yaml``.

``.pre-commit-config.yaml`` is the source of truth for the versions of the
quality-assurance tools (black, flake8, isort, mypy, ...).  The same versions
are pinned in ``conda/dev.yml`` and in the ``qa`` extra of ``pyproject.toml``,
so they have to be updated whenever ``pre-commit autoupdate`` (or the
``pre-commit-update`` GitHub workflow) bumps a ``rev``.

Run this script after updating ``.pre-commit-config.yaml``::

    python scripts/sync_pre_commit_versions.py

Use ``--check`` to verify that everything is already in sync without modifying
any files (exits with status 1 if it is not).
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Callable, Optional

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent

PRE_COMMIT_CONFIG = REPO_ROOT / ".pre-commit-config.yaml"
DEV_YML = REPO_ROOT / "conda" / "dev.yml"
PYPROJECT_TOML = REPO_ROOT / "pyproject.toml"

# Some pre-commit repos are mirrors of the package they run, e.g.
# https://github.com/pre-commit/mirrors-mypy provides `mypy`.
MIRROR_PREFIXES = ("mirrors-", "mirror-")

# A pinned dependency in `conda/dev.yml`, e.g. `  - black ==25.1.0` or
# `  - tbump=6.9.0`.  Ranges such as `- numpy >=2.0,<3.0` are not matched.
CONDA_PIN = re.compile(
    r"^(?P<prefix>\s*-\s+)"
    r"(?P<name>[A-Za-z0-9._-]+)"
    r"(?P<pre_op>\s*)(?P<op>==|=)(?P<post_op>\s*)"
    r"(?P<version>[^\s#]+)"
    r"(?P<suffix>.*)$"
)

# A pinned dependency in a `pyproject.toml` requirement list, e.g.
# `    "black==25.1.0",`.
PYPI_PIN = re.compile(
    r"^(?P<prefix>\s*\")"
    r"(?P<name>[A-Za-z0-9._-]+)"
    r"(?P<pre_op>\s*)(?P<op>==)(?P<post_op>\s*)"
    r"(?P<version>[^\"\s]+)"
    r"(?P<suffix>\".*)$"
)

# The `qa = [...]` list in `pyproject.toml`.
PYPROJECT_QA = re.compile(r"^qa\s*=\s*\[\s*$")


def normalize(name: str) -> str:
    """Normalize a package name the way PEP 503 does."""
    return re.sub(r"[-_.]+", "-", name).lower()


def package_names(repo_url: str, hook_ids: list[str]) -> set[str]:
    """Guess the package names provided by a pre-commit repo.

    The hook ids are usually the package name (``black``, ``isort``, ...), and
    so is the last component of the repo URL, once any ``mirrors-`` prefix has
    been stripped.
    """
    repo_name = repo_url.rstrip("/").rsplit("/", 1)[-1]
    if repo_name.endswith(".git"):
        repo_name = repo_name[: -len(".git")]
    for prefix in MIRROR_PREFIXES:
        if repo_name.startswith(prefix):
            repo_name = repo_name[len(prefix) :]
    return {normalize(name) for name in [repo_name, *hook_ids]}


def collect_versions(config_path: Path) -> dict[str, str]:
    """Map normalized package names to versions from ``.pre-commit-config.yaml``.

    Both the ``rev`` of each repo and any ``additional_dependencies`` pinned
    with ``==`` are collected.
    """
    with open(config_path) as f:
        config = yaml.safe_load(f)

    versions: dict[str, str] = {}

    def add(name: str, version: str, source: str) -> None:
        name = normalize(name)
        previous = versions.get(name)
        if previous is not None and previous != version:
            print(
                f"Warning: conflicting versions for {name} in {config_path.name}: "
                f"{previous} and {version} (from {source}); keeping {previous}"
            )
            return
        versions[name] = version

    for repo in config.get("repos", []):
        repo_url = repo.get("repo", "")
        if repo_url in ("local", "meta"):
            continue
        rev = repo.get("rev")
        hooks = repo.get("hooks", []) or []
        hook_ids = [hook["id"] for hook in hooks if "id" in hook]
        if rev is not None:
            # `rev` is a git tag, which is often prefixed with a `v`.
            version = str(rev).lstrip("v")
            for name in package_names(repo_url, hook_ids):
                add(name, version, f"rev of {repo_url}")
        for hook in hooks:
            for dependency in hook.get("additional_dependencies", []) or []:
                if "==" not in dependency:
                    continue
                name, _, version = dependency.partition("==")
                add(
                    name.strip(),
                    version.strip(),
                    f"additional_dependencies of {hook.get('id')}",
                )

    return versions


def sync_lines(
    lines: list[str],
    versions: dict[str, str],
    pattern: re.Pattern[str],
    path: Path,
    line_range: Optional[tuple[int, int]] = None,
) -> tuple[list[str], list[str]]:
    """Update pinned versions in ``lines``, returning the new lines and a log."""
    start, end = line_range if line_range is not None else (0, len(lines))
    updated = list(lines)
    changes: list[str] = []

    for index in range(start, end):
        content = lines[index].rstrip("\r\n")
        line_ending = lines[index][len(content) :]
        match = pattern.match(content)
        if match is None:
            continue
        name = normalize(match.group("name"))
        version = versions.get(name)
        if version is None or version == match.group("version"):
            continue
        updated[index] = (
            "{prefix}{name}{pre_op}{op}{post_op}{version}{suffix}".format(
                **{**match.groupdict(), "version": version}
            )
            + line_ending
        )
        changes.append(
            f"{path.relative_to(REPO_ROOT)}:{index + 1}: "
            f"{match.group('name')} {match.group('version')} -> {version}"
        )

    return updated, changes


def find_qa_block(lines: list[str]) -> tuple[int, int]:
    """Find the line range of the ``qa = [...]`` list in ``pyproject.toml``."""
    for index, line in enumerate(lines):
        if PYPROJECT_QA.match(line):
            for end in range(index + 1, len(lines)):
                if lines[end].startswith("]"):
                    return index + 1, end
            raise ValueError("unterminated `qa = [` list in pyproject.toml")
    raise ValueError("no `qa = [` list found in pyproject.toml")


def sync_file(
    path: Path,
    versions: dict[str, str],
    pattern: re.Pattern[str],
    check: bool,
    find_range: Optional[Callable[[list[str]], tuple[int, int]]] = None,
) -> list[str]:
    """Sync one file, writing it back unless ``check`` is set."""
    lines = path.read_text().splitlines(keepends=True)
    line_range = find_range(lines) if find_range is not None else None
    updated, changes = sync_lines(lines, versions, pattern, path, line_range)
    if changes and not check:
        path.write_text("".join(updated))
    return changes


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="report out-of-sync versions without modifying any files",
    )
    args = parser.parse_args()

    versions = collect_versions(PRE_COMMIT_CONFIG)

    changes = sync_file(DEV_YML, versions, CONDA_PIN, args.check)
    changes += sync_file(
        PYPROJECT_TOML, versions, PYPI_PIN, args.check, find_range=find_qa_block
    )

    if not changes:
        print(f"All versions are in sync with {PRE_COMMIT_CONFIG.name}.")
        return 0

    for change in changes:
        print(change)

    if args.check:
        print(
            f"\nRun `python {Path(__file__).relative_to(REPO_ROOT)}` "
            "to apply these updates."
        )
        return 1

    print(f"\nUpdated {len(changes)} pinned version(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
