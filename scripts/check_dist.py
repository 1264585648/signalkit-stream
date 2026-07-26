from __future__ import annotations

import email
from pathlib import Path
import tarfile
import tomllib
import zipfile


ROOT = Path(__file__).resolve().parents[1]
DIST = ROOT / "dist"


def _project() -> tuple[str, str]:
    with (ROOT / "pyproject.toml").open("rb") as handle:
        payload = tomllib.load(handle)
    project = payload["project"]
    return str(project["name"]), str(project["version"])


def _single(pattern: str) -> Path:
    matches = sorted(DIST.glob(pattern))
    if len(matches) != 1:
        raise SystemExit(f"expected exactly one {pattern} artifact, found: {matches}")
    return matches[0]


def check_wheel(name: str, version: str) -> None:
    wheel = _single("*.whl")
    normalized = name.replace("-", "_")
    expected_prefix = f"{normalized}-{version}-"
    if not wheel.name.startswith(expected_prefix):
        raise SystemExit(
            f"wheel filename {wheel.name!r} does not start with {expected_prefix!r}"
        )

    with zipfile.ZipFile(wheel) as archive:
        metadata_names = [
            item for item in archive.namelist() if item.endswith(".dist-info/METADATA")
        ]
        if len(metadata_names) != 1:
            raise SystemExit(f"expected one wheel METADATA file, found {metadata_names}")
        metadata = email.message_from_bytes(archive.read(metadata_names[0]))
        if metadata.get("Name") != name:
            raise SystemExit(
                f"wheel metadata name {metadata.get('Name')!r} does not match {name!r}"
            )
        if metadata.get("Version") != version:
            raise SystemExit(
                f"wheel metadata version {metadata.get('Version')!r} does not match {version!r}"
            )

        entry_points = [
            item for item in archive.namelist() if item.endswith(".dist-info/entry_points.txt")
        ]
        if len(entry_points) != 1:
            raise SystemExit(f"expected one wheel entry_points.txt, found {entry_points}")
        text = archive.read(entry_points[0]).decode("utf-8")
        if "signalkit = signalkit_stream.cli:main" not in text:
            raise SystemExit("wheel is missing the signalkit console entry point")


def check_sdist(name: str, version: str) -> None:
    sdist = _single("*.tar.gz")
    normalized = name.replace("-", "_")
    expected_name = f"{normalized}-{version}.tar.gz"
    if sdist.name != expected_name:
        raise SystemExit(f"sdist filename {sdist.name!r} does not match {expected_name!r}")

    expected_root = f"{normalized}-{version}"
    required = {
        "CHANGELOG.md",
        "LICENSE",
        "README.md",
        "pyproject.toml",
        "docs/ARCHITECTURE.md",
        "docs/COMPATIBILITY.md",
        "docs/PUBLIC_API.md",
        "docs/RELEASE.md",
    }
    with tarfile.open(sdist, "r:gz") as archive:
        members = {member.name for member in archive.getmembers()}

    missing = sorted(
        item for item in required if f"{expected_root}/{item}" not in members
    )
    if missing:
        raise SystemExit(f"sdist is missing release files: {', '.join(missing)}")


def main() -> int:
    name, version = _project()
    check_wheel(name, version)
    check_sdist(name, version)
    print(f"distribution check passed for {name} {version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
