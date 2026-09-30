"""
Loads and validates the authorized source registry (sources.yaml).
"""
import os

import yaml

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SOURCES_FILE = os.path.join(PROJECT_ROOT, "sources.yaml")

REQUIRED_FIELDS = ["name", "url", "source_type", "priority", "interval_minutes"]
VALID_PRIORITIES = {"fast", "standard", "bulk"}


def load_sources(path: str = SOURCES_FILE) -> list[dict]:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    sources = data.get("sources", [])
    seen_names = set()

    for src in sources:
        missing = [field for field in REQUIRED_FIELDS if field not in src]
        if missing:
            raise ValueError(f"Source {src.get('name', '<unnamed>')} missing fields: {missing}")
        if src["priority"] not in VALID_PRIORITIES:
            raise ValueError(
                f"Source {src['name']} has invalid priority '{src['priority']}' "
                f"(must be one of {VALID_PRIORITIES})"
            )
        if src["name"] in seen_names:
            raise ValueError(f"Duplicate source name: {src['name']}")
        seen_names.add(src["name"])
        src.setdefault("enabled", True)
        src.setdefault("timeout_seconds", 120)

    return sources


def get_source(name: str, path: str = SOURCES_FILE) -> dict | None:
    for src in load_sources(path):
        if src["name"] == name:
            return src
    return None
