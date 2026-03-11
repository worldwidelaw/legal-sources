#!/usr/bin/env python3
"""
World Wide Law — Dashboard Data Generator

Reads manifest.yaml, status.yaml files, and sample data.
Outputs docs/status.json for the GitHub Pages dashboard.

Usage:
    python3 generate_dashboard.py
"""

import json
import yaml
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
DOCS_DIR = PROJECT_ROOT / "docs"

COUNTRY_NAMES = {
    # Supranational
    "EU": "European Union", "CoE": "Council of Europe",
    # EU member states
    "AT": "Austria", "BE": "Belgium", "BG": "Bulgaria", "CY": "Cyprus",
    "CZ": "Czechia", "DE": "Germany", "DK": "Denmark", "EE": "Estonia",
    "ES": "Spain", "FI": "Finland", "FR": "France", "GR": "Greece",
    "HR": "Croatia", "HU": "Hungary", "IE": "Ireland", "IT": "Italy",
    "LT": "Lithuania", "LU": "Luxembourg", "LV": "Latvia", "MT": "Malta",
    "NL": "Netherlands", "PL": "Poland", "PT": "Portugal", "RO": "Romania",
    "SE": "Sweden", "SI": "Slovenia", "SK": "Slovakia",
    # EFTA / EEA
    "CH": "Switzerland", "NO": "Norway", "IS": "Iceland", "LI": "Liechtenstein",
    # Other CoE members / candidates
    "UK": "United Kingdom", "TR": "Turkey", "UA": "Ukraine",
    "GE": "Georgia", "AM": "Armenia", "AZ": "Azerbaijan", "MD": "Moldova",
    # Western Balkans
    "RS": "Serbia", "BA": "Bosnia & Herzegovina", "ME": "Montenegro",
    "AL": "Albania", "MK": "North Macedonia", "XK": "Kosovo",
    # Microstates
    "AD": "Andorra", "MC": "Monaco", "SM": "San Marino",
}
COUNTRY_FLAGS = {
    # Supranational
    "EU": "\U0001F1EA\U0001F1FA", "CoE": "\U0001F3F0",
    # EU member states
    "AT": "\U0001F1E6\U0001F1F9", "BE": "\U0001F1E7\U0001F1EA",
    "BG": "\U0001F1E7\U0001F1EC", "CY": "\U0001F1E8\U0001F1FE",
    "CZ": "\U0001F1E8\U0001F1FF", "DE": "\U0001F1E9\U0001F1EA",
    "DK": "\U0001F1E9\U0001F1F0", "EE": "\U0001F1EA\U0001F1EA",
    "ES": "\U0001F1EA\U0001F1F8", "FI": "\U0001F1EB\U0001F1EE",
    "FR": "\U0001F1EB\U0001F1F7", "GR": "\U0001F1EC\U0001F1F7",
    "HR": "\U0001F1ED\U0001F1F7", "HU": "\U0001F1ED\U0001F1FA",
    "IE": "\U0001F1EE\U0001F1EA", "IT": "\U0001F1EE\U0001F1F9",
    "LT": "\U0001F1F1\U0001F1F9", "LU": "\U0001F1F1\U0001F1FA",
    "LV": "\U0001F1F1\U0001F1FB", "MT": "\U0001F1F2\U0001F1F9",
    "NL": "\U0001F1F3\U0001F1F1", "PL": "\U0001F1F5\U0001F1F1",
    "PT": "\U0001F1F5\U0001F1F9", "RO": "\U0001F1F7\U0001F1F4",
    "SE": "\U0001F1F8\U0001F1EA", "SI": "\U0001F1F8\U0001F1EE",
    "SK": "\U0001F1F8\U0001F1F0",
    # EFTA / EEA
    "CH": "\U0001F1E8\U0001F1ED", "NO": "\U0001F1F3\U0001F1F4",
    "IS": "\U0001F1EE\U0001F1F8", "LI": "\U0001F1F1\U0001F1EE",
    # Other CoE members / candidates
    "UK": "\U0001F1EC\U0001F1E7", "TR": "\U0001F1F9\U0001F1F7",
    "UA": "\U0001F1FA\U0001F1E6", "GE": "\U0001F1EC\U0001F1EA",
    "AM": "\U0001F1E6\U0001F1F2", "AZ": "\U0001F1E6\U0001F1FF",
    "MD": "\U0001F1F2\U0001F1E9",
    # Western Balkans
    "RS": "\U0001F1F7\U0001F1F8", "BA": "\U0001F1E7\U0001F1E6",
    "ME": "\U0001F1F2\U0001F1EA", "AL": "\U0001F1E6\U0001F1F1",
    "MK": "\U0001F1F2\U0001F1F0", "XK": "\U0001F1FD\U0001F1F0",
    # Microstates
    "AD": "\U0001F1E6\U0001F1E9", "MC": "\U0001F1F2\U0001F1E8",
    "SM": "\U0001F1F8\U0001F1F2",
}


def load_manifest():
    with open(PROJECT_ROOT / "manifest.yaml") as f:
        return yaml.safe_load(f)


def get_source_details(source_id):
    """Read status.yaml and count samples for a source."""
    parts = source_id.split('/')
    if len(parts) != 2:
        return {}
    country, name = parts
    country_dir = PROJECT_ROOT / "sources" / country
    if not country_dir.exists():
        return {}
    source_dir = None
    for d in country_dir.iterdir():
        if d.is_dir() and d.name.lower() == name.lower():
            source_dir = d
            break
    if not source_dir:
        source_dir = country_dir / name
    if not source_dir or not source_dir.exists():
        return {}

    details = {}
    status_path = source_dir / "status.yaml"
    if status_path.exists():
        try:
            with open(status_path) as f:
                status_data = yaml.safe_load(f) or {}
            details["total_records"] = status_data.get("total_records", 0)
        except Exception:
            pass

    # Check for retrieve.py and test cases
    details["has_retrieve"] = (source_dir / "retrieve.py").exists()
    test_file = source_dir / "retrieve_tests.json"
    if test_file.exists():
        try:
            with open(test_file) as f:
                tests = json.load(f)
            details["retrieve_test_count"] = len(tests)
        except Exception:
            details["retrieve_test_count"] = 0
    else:
        details["retrieve_test_count"] = 0

    # Count sample files and compute avg text length
    sample_dir = source_dir / "sample"
    if sample_dir.exists():
        sample_files = [f for f in sample_dir.glob("*.json") if f.name != "all_samples.json"]
        details["sample_count"] = len(sample_files)
        total_text_len = 0
        count = 0
        for sf in sample_files[:10]:
            try:
                with open(sf) as f:
                    data = json.load(f)
                if isinstance(data, dict):
                    text = data.get("text", data.get("content", ""))
                    if isinstance(text, str):
                        total_text_len += len(text)
                        count += 1
                elif isinstance(data, list) and data:
                    text = data[0].get("text", data[0].get("content", "")) if isinstance(data[0], dict) else ""
                    if isinstance(text, str):
                        total_text_len += len(text)
                        count += 1
            except Exception:
                pass
        details["avg_text_length"] = total_text_len // count if count > 0 else 0
    else:
        details["sample_count"] = 0
        details["avg_text_length"] = 0

    return details


def generate():
    manifest = load_manifest()
    sources = manifest.get("sources", [])

    by_status = {}
    by_country = {}
    for s in sources:
        status = s.get("status", "unknown")
        country = s.get("country", "??")
        by_status[status] = by_status.get(status, 0) + 1
        if country not in by_country:
            by_country[country] = {
                "name": COUNTRY_NAMES.get(country, country),
                "flag": COUNTRY_FLAGS.get(country, ""),
                "total": 0, "complete": 0, "planned": 0, "blocked": 0, "needs_maintenance": 0,
                "has_consolidated_codes": False,
                "preferred_legislation_source": None,
            }
        by_country[country]["total"] += 1
        by_country[country][status] = by_country[country].get(status, 0) + 1
        preferred = s.get("preferred_for", [])
        if "legislation" in preferred:
            by_country[country]["has_consolidated_codes"] = True
            by_country[country]["preferred_legislation_source"] = s.get("id")

    total = len(sources)
    complete = by_status.get("complete", 0)

    sources_out = []
    for s in sources:
        source_data = {
            "id": s.get("id", ""),
            "country": s.get("country", ""),
            "name": s.get("name", ""),
            "status": s.get("status", "unknown"),
            "priority": s.get("priority", 99),
            "data_types": s.get("data_types", []),
            "preferred_for": s.get("preferred_for", []),
            "url": s.get("url", ""),
            "notes": (s.get("notes") or "")[:200],
            "auth": s.get("auth", "none"),
        }
        if s.get("status") != "planned":
            details = get_source_details(s.get("id", ""))
            source_data.update(details)
        sources_out.append(source_data)

    status_order = {"complete": 0, "needs_maintenance": 1, "blocked": 2, "in_progress": 3, "planned": 4}
    sources_out.sort(key=lambda x: (status_order.get(x["status"], 9), x["priority"], x["id"]))

    countries_with_legislation = [c for c, d in by_country.items()
                                  if any("legislation" in s.get("data_types", [])
                                         for s in sources if s.get("country") == c)]
    countries_with_consolidated = [c for c, d in by_country.items() if d.get("has_consolidated_codes")]
    countries_gazette_only = [c for c in countries_with_legislation if c not in countries_with_consolidated]

    sources_with_samples = sum(1 for s in sources_out if s.get("sample_count", 0) > 0)
    sources_with_retrieve = sum(1 for s in sources_out if s.get("has_retrieve"))

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {
            "total": total,
            "complete": complete,
            "planned": by_status.get("planned", 0),
            "blocked": by_status.get("blocked", 0),
            "needs_maintenance": by_status.get("needs_maintenance", 0),
            "percent_complete": round(complete / total * 100, 1) if total > 0 else 0,
        },
        "consolidated_coverage": {
            "countries_with_consolidated_codes": sorted(countries_with_consolidated),
            "countries_gazette_only": sorted(countries_gazette_only),
            "coverage_percent": round(
                len(countries_with_consolidated) / len(countries_with_legislation) * 100, 1
            ) if countries_with_legislation else 0,
        },
        "retrieve_coverage": {
            "total_with_samples": sources_with_samples,
            "total_with_retrieve": sources_with_retrieve,
            "percent": round(sources_with_retrieve / sources_with_samples * 100, 1) if sources_with_samples else 0,
        },
        "by_country": by_country,
        "sources": sources_out,
    }

    DOCS_DIR.mkdir(exist_ok=True)
    with open(DOCS_DIR / "status.json", "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"Dashboard data generated: {complete}/{total} sources complete ({output['summary']['percent_complete']}%)")


if __name__ == "__main__":
    generate()
