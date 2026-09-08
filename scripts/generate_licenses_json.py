#!/usr/bin/env python3
"""
Generate docs/licenses.json from manifest.yaml and license_registry.yaml.

Produces a static JSON file that makes license data queryable:
- summary stats (commercial OK, non-commercial, commercial unknown, unverified)
- per-license breakdown with counts
- flat source list with license fields for filtering

Usage:
  python scripts/generate_licenses_json.py
"""

import json
import os
from collections import Counter
from datetime import datetime, timezone

import yaml

MANIFEST = "manifest.yaml"
REGISTRY = "license_registry.yaml"
OUTPUT = "docs/licenses.json"


def commercial_use(record, label):
    """Preserve explicit booleans; absent/null is unknown, never inferred."""
    value = record.get("commercial_use")
    if value is not None and type(value) is not bool:
        raise ValueError(f"{label}: commercial_use must be a boolean or null")
    return value


def main():
    with open(MANIFEST) as f:
        manifest = yaml.safe_load(f)
    with open(REGISTRY) as f:
        registry = yaml.safe_load(f)["licenses"]

    complete = [s for s in manifest["sources"] if s.get("status") == "complete"]

    # Build source list
    sources = []
    for s in sorted(complete, key=lambda x: x["id"]):
        sources.append({
            "id": s["id"],
            "country": str(s.get("country", "")),
            "name": s.get("name", ""),
            "license_id": s.get("license_id", ""),
            "license_name": s.get("license_name", ""),
            "license_url": s.get("license_url"),
            "commercial_use": commercial_use(s, s["id"]),
        })

    # Summary
    commercial_ok = sum(1 for s in sources if s["commercial_use"] is True)
    non_commercial = sum(1 for s in sources if s["commercial_use"] is False)
    commercial_unknown = sum(1 for s in sources if s["commercial_use"] is None)
    unverified = sum(1 for s in sources if s["license_id"] == "unverified")
    id_counts = Counter(s["license_id"] for s in sources)

    # Per-license breakdown
    by_license = {}
    for lid, count in sorted(id_counts.items(), key=lambda x: -x[1]):
        reg = registry.get(lid, {})
        by_license[lid] = {
            "display_name": reg.get("display_name", lid),
            "url": reg.get("url"),
            "commercial_use": commercial_use(reg, f"registry {lid}"),
            "count": count,
        }

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "total_complete": len(sources),
            "commercial_ok": commercial_ok,
            "non_commercial": non_commercial,
            "commercial_unknown": commercial_unknown,
            "unverified": unverified,
            "unique_license_ids": len(id_counts),
        },
        "by_license": by_license,
        "sources": sources,
    }

    os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
    with open(OUTPUT, "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"Written {OUTPUT}")
    print(f"  Sources: {len(sources)}")
    print(f"  Commercial OK: {commercial_ok}")
    print(f"  Non-commercial: {non_commercial}")
    print(f"  Commercial unknown: {commercial_unknown}")
    print(f"  Unverified: {unverified}")


if __name__ == "__main__":
    main()
