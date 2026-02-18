import argparse
import re
from datetime import date
from pathlib import Path


PYPROJECT = Path("pyproject.toml")
CHANGELOG = Path("CHANGELOG.md")


def strip_v(tag: str) -> str:
    """Strip v if present. (e.g, `v1.0.2` -> `1.0.2`)"""
    return tag[1:] if tag.startswith("v") else tag


def bump_pyproject_version(new_version: str) -> None:
    """
    Automatic replace version in `pyproject.toml` on update. 
    Fail if more than one version line encountered.
    """
    text = PYPROJECT.read_text(encoding="utf-8")
    # expects: version = "0.0.0"
    new_text, n = re.subn(r'(?m)^version\s*=\s*"[0-9]+\.[0-9]+\.[0-9]+"\s*$', f'version = "{new_version}"', text)
    if n != 1:
        raise SystemExit("pyproject.toml: expected exactly one version = \"x.y.z\" line")
    PYPROJECT.write_text(new_text, encoding="utf-8")


def update_changelog(tag: str) -> None:
    """
    Update `CHANGELOG.md`.
    """
    today = date.today().isoformat()
    text = CHANGELOG.read_text(encoding="utf-8")

    if f"## {tag}" in text:     # fail on duplicate tags.
        raise SystemExit(f"CHANGELOG already contains {tag}")

    marker = "## Unreleased\n"
    if marker not in text:
        raise SystemExit("CHANGELOG.md: missing '## Unreleased' section")

    # Insert a new version section right after Unreleased header.
    parts = text.split(marker, 1)
    head, rest = parts[0], parts[1]

    # Rest starts with whatever is under Unreleased.
    new_version_block = f"## {tag} - {today}\n"
    new_text = head + marker + "\n" + new_version_block + rest.lstrip()
    CHANGELOG.write_text(new_text, encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", required=True, help="e.g. v0.1.0")
    args = ap.parse_args()

    tag = args.version
    numeric = strip_v(tag)

    bump_pyproject_version(numeric)
    update_changelog(tag)


if __name__ == "__main__":
    main()
