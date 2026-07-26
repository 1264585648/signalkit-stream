from pathlib import Path
import tomllib

import signalkit_stream


def test_pyproject_and_runtime_versions_match() -> None:
    root = Path(__file__).resolve().parents[1]
    with (root / "pyproject.toml").open("rb") as handle:
        payload = tomllib.load(handle)

    assert payload["project"]["version"] == signalkit_stream.__version__


def test_supported_python_classifiers_match_ci_policy() -> None:
    root = Path(__file__).resolve().parents[1]
    with (root / "pyproject.toml").open("rb") as handle:
        payload = tomllib.load(handle)

    classifiers = set(payload["project"]["classifiers"])
    assert {
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    } <= classifiers
