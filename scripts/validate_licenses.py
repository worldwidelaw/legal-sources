#!/usr/bin/env python3
"""
Validate license data in manifest.yaml against license_registry.yaml.

Checks:
  1. Every complete source has license_id, license_name; commercial_use is boolean/null
  2. license_id exists in the registry
  3. commercial_use is consistent with the registry
  4. Tier 1/2 licenses have license_url
  5. Reports unverified count as warning

Usage:
  python scripts/validate_licenses.py           # validate
  python scripts/validate_licenses.py --strict  # fail on unverified sources too
"""

import sys
import yaml

MANIFEST = "manifest.yaml"
REGISTRY = "license_registry.yaml"

# Tier 1/2 licenses that MUST have a license_url
MUST_HAVE_URL = {
    "cc0-1.0", "cc-by-3.0", "cc-by-4.0", "cc-by-sa-3.0", "cc-by-sa-4.0",
    "cc-by-nc", "cc-by-nc-sa", "cc-by-nc-nd", "cc-by-nd-4.0", "odbl-1.0", "mit",
    "ogl-3.0", "ogl-canada", "licence-ouverte-2.0", "iodl-2.0", "nlod-2.0",
    "dl-de-2.0", "eur-lex", "pd-us", "pd-de",
}


def main():
    strict = "--strict" in sys.argv

    with open(MANIFEST) as f:
        manifest = yaml.safe_load(f)
    with open(REGISTRY) as f:
        registry = yaml.safe_load(f)["licenses"]

    valid_ids = set(registry.keys())
    complete = [s for s in manifest["sources"] if s.get("status") == "complete"]

    errors = []
    warnings = []

    for source in complete:
        sid = source["id"]

        # Check required fields
        if not source.get("license_id"):
            errors.append(f"{sid}: missing license_id")
            continue

        lid = source["license_id"]

        if not source.get("license_name"):
            errors.append(f"{sid}: missing license_name")

        src_cu = source.get("commercial_use")
        if src_cu is not None and type(src_cu) is not bool:
            errors.append(f"{sid}: commercial_use must be a boolean or null")
        elif src_cu is None:
            warnings.append(f"{sid}: commercial use unknown — needs research")

        # Check registry membership
        if lid not in valid_ids:
            errors.append(f"{sid}: unknown license_id '{lid}'")
            continue

        # Check commercial_use consistency
        reg_cu = registry[lid].get("commercial_use")
        if reg_cu is not None and type(reg_cu) is not bool:
            errors.append(f"{sid}: registry {lid} commercial_use must be a boolean or null")
        if reg_cu is not None and src_cu is not None and reg_cu != src_cu:
            errors.append(f"{sid}: commercial_use={src_cu} but registry says {reg_cu} for {lid}")

        # Check URL for tier 1/2
        if lid in MUST_HAVE_URL and not source.get("license_url"):
            warnings.append(f"{sid}: {lid} should have license_url")

        # Flag unverified
        if lid == "unverified":
            warnings.append(f"{sid}: license unverified — needs research")

    # Report
    print(f"Validated {len(complete)} complete sources")
    print(f"  Errors:   {len(errors)}")
    print(f"  Warnings: {len(warnings)}")

    if errors:
        print("\n--- ERRORS ---")
        for e in errors:
            print(f"  {e}")

    if warnings:
        print(f"\n--- WARNINGS ({len(warnings)}) ---")
        for w in warnings:
            print(f"  {w}")

    # Summary
    from collections import Counter
    ids = Counter(s.get("license_id") for s in complete if s.get("license_id"))
    nc = sum(1 for s in complete if s.get("commercial_use") is False)
    ok = sum(1 for s in complete if s.get("commercial_use") is True)
    unknown = sum(1 for s in complete if s.get("commercial_use") is None)
    unv = sum(1 for s in complete if s.get("license_id") == "unverified")
    print(f"\n--- Summary ---")
    print(f"  Unique license IDs: {len(ids)}")
    print(f"  Commercial use OK:  {ok}")
    print(f"  Non-commercial:     {nc}")
    print(f"  Commercial unknown: {unknown}")
    print(f"  Unverified:         {unv}")

    if errors:
        sys.exit(1)
    if strict and unv > 0:
        print(f"\n--strict: failing due to {unv} unverified sources")
        sys.exit(1)

    print("\nPASS")
    sys.exit(0)


if __name__ == "__main__":
    main()
