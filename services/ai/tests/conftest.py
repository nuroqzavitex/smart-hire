from pathlib import Path

import pytest


def _find_contracts_dir() -> Path:
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "contracts" / "events"
        if candidate.is_dir():
            return candidate
    raise RuntimeError("contracts/events not found above " + str(Path(__file__).resolve()))


@pytest.fixture(scope="session")
def contracts_dir() -> Path:
    """Directory holding the JSON Schemas of the Kafka events (contracts/events)."""
    return _find_contracts_dir()
