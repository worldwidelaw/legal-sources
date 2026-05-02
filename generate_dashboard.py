#!/usr/bin/env python3
"""
Legal Data Hunter — Dashboard Data Generator

Reads manifest.yaml, BLOCKED.md, status.yaml files, sample data, and session logs.
Queries Neon PostgreSQL directly for live indexing metrics when NEON_DATABASE_URL is set,
falls back to INDEX.yaml when offline.

Outputs docs/status.json for the GitHub Pages dashboard.

Usage:
    python3 generate_dashboard.py                          # uses INDEX.yaml fallback
    NEON_DATABASE_URL=postgresql://... python3 generate_dashboard.py   # live Neon query
"""

import json
import os
import re
import sys
import yaml
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent
DOCS_DIR = PROJECT_ROOT / "docs"
PIPELINE_INDEX = Path.home() / "legal-data-pipeline" / "INDEX.yaml"
CONTRIBUTORS_FILE = DOCS_DIR / "contributors.json"
PIPELINE_REPO_URL = "https://github.com/ZachLaik/legal-data-pipeline"
JURISDICTIONS_FILE = PROJECT_ROOT / "jurisdictions.yaml"

# ─── Neon connection (optional) ───
# Set NEON_DATABASE_URL env var or create .env in project root.
# If unavailable, falls back to reading INDEX.yaml.
_ENV_FILE = PROJECT_ROOT / ".env"
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



# ─── Jurisdiction tree (loaded from jurisdictions.yaml) ───

def _load_subdivision_tree():
    """Load jurisdictions.yaml and build subdivision tree + enrich COUNTRY_NAMES/FLAGS."""
    subdivision_tree = {}
    if not JURISDICTIONS_FILE.exists():
        return subdivision_tree
    with open(JURISDICTIONS_FILE) as f:
        data = yaml.safe_load(f) or {}
    for code, info in data.get("jurisdictions", {}).items():
        code = str(code)
        if not isinstance(info, dict):
            continue
        if code not in COUNTRY_NAMES:
            COUNTRY_NAMES[code] = info.get("name", code)
        if code not in COUNTRY_FLAGS and len(code) == 2 and code.isalpha():
            COUNTRY_FLAGS[code] = "".join(chr(0x1F1E6 + ord(c) - ord("A")) for c in code.upper())
        subs = info.get("subdivisions", {})
        if subs:
            subdivision_tree[code] = {}
            for sub_code, sub_info in subs.items():
                sub_code = str(sub_code)
                if isinstance(sub_info, dict):
                    subdivision_tree[code][sub_code] = {
                        "name": sub_info.get("name", sub_code),
                        "legally_distinct": sub_info.get("legally_distinct", False),
                    }
    return subdivision_tree


SUBDIVISION_TREE = _load_subdivision_tree()


def _resolve_source_subdivisions(source):
    """Resolve a source's jurisdictions field to subdivision codes."""
    jurisdictions = source.get("jurisdictions")
    country = source.get("country", "")
    if not jurisdictions:
        return set(), True
    own_subs = set()
    is_country_wide = False
    country_subs = SUBDIVISION_TREE.get(country, {})
    for j in jurisdictions:
        code = j.get("code", "")
        if code == country:
            is_country_wide = True
        elif code.endswith("-*"):
            parent = code[:-2]
            if parent == country:
                is_country_wide = True
            else:
                own_subs.update(SUBDIVISION_TREE.get(parent, {}).keys())
        elif code in country_subs:
            own_subs.add(code)
        elif "-" in code:
            own_subs.add(code)
    return own_subs, is_country_wide


def load_contributors():
    """Load contributor data from docs/contributors.json (generated by scripts/fetch_contributors.py)."""
    if not CONTRIBUTORS_FILE.exists():
        return {}
    try:
        with open(CONTRIBUTORS_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def load_manifest():
    with open(PROJECT_ROOT / "manifest.yaml") as f:
        return yaml.safe_load(f)


def _load_neon_database_url():
    """Load NEON_DATABASE_URL from environment or .env file."""
    url = os.environ.get("NEON_DATABASE_URL")
    if url:
        return url
    # Try .env file in project root
    if _ENV_FILE.exists():
        for line in _ENV_FILE.read_text().splitlines():
            line = line.strip()
            if line.startswith("NEON_DATABASE_URL=") and not line.startswith("#"):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return None


def _query_neon_live(database_url):
    """Query Neon PostgreSQL directly for live indexing metrics.

    Returns a lookup dict keyed by source ID with per-table row counts,
    so sources present in multiple tables (e.g., AT/RIS in legislation AND case_law)
    are correctly counted without overwriting.
    """
    try:
        import psycopg2
    except ImportError:
        print("  psycopg2 not installed — pip install psycopg2-binary", file=sys.stderr)
        return None

    try:
        conn = psycopg2.connect(database_url, connect_timeout=10)
    except Exception as e:
        print(f"  Neon connection failed: {e}", file=sys.stderr)
        return None

    lookup = {}
    try:
        with conn.cursor() as cur:
            # ── Per-source row counts for all tables ──
            # Store per-table counts separately to avoid overwriting
            for table in ("legislation", "case_law", "doctrine"):
                cur.execute(f"""
                    SELECT source, COUNT(*) FROM {table} GROUP BY source
                """)
                for source, count in cur.fetchall():
                    if source not in lookup:
                        lookup[source] = {
                            "legislation_rows": 0,
                            "case_law_rows": 0,
                            "doctrine_rows": 0,
                            "neon_rows": 0,
                            "status": "pending",
                        }
                    lookup[source][f"{table}_rows"] = count

            # ── Compute totals and determine primary data_type ──
            for source, data in lookup.items():
                leg = data.get("legislation_rows", 0)
                case = data.get("case_law_rows", 0)
                doc = data.get("doctrine_rows", 0)
                total = leg + case + doc
                data["neon_rows"] = total
                data["status"] = "ok" if total > 0 else "pending"
                # Set data_type to the table with most rows (for backwards compatibility)
                if leg >= case and leg >= doc:
                    data["data_type"] = "legislation"
                elif case >= leg and case >= doc:
                    data["data_type"] = "case_law"
                else:
                    data["data_type"] = "doctrine"

            # ── Date ranges per source (all tables) ──
            for table in ("legislation", "case_law", "doctrine"):
                cur.execute(f"""
                    SELECT source,
                           EXTRACT(YEAR FROM MIN(date))::int,
                           EXTRACT(YEAR FROM MAX(date))::int,
                           COUNT(*)
                    FROM {table}
                    WHERE date IS NOT NULL
                    GROUP BY source
                """)
                for source, min_yr, max_yr, cnt in cur.fetchall():
                    if source in lookup:
                        # Merge date ranges across tables
                        existing = lookup[source].get("date_range")
                        if existing:
                            lookup[source]["date_range"] = {
                                "min_year": min(existing["min_year"], min_yr) if existing["min_year"] and min_yr else (existing["min_year"] or min_yr),
                                "max_year": max(existing["max_year"], max_yr) if existing["max_year"] and max_yr else (existing["max_year"] or max_yr),
                                "total_with_date": existing["total_with_date"] + cnt,
                            }
                        else:
                            lookup[source]["date_range"] = {
                                "min_year": min_yr,
                                "max_year": max_yr,
                                "total_with_date": cnt,
                            }

            # ── Last ingested timestamp per source (all tables) ──
            for table in ("legislation", "case_law", "doctrine"):
                cur.execute(f"""
                    SELECT source, MAX(ingested_at) FROM {table} GROUP BY source
                """)
                for source, ts in cur.fetchall():
                    if source in lookup and ts:
                        existing = lookup[source].get("last_ingested")
                        if existing:
                            # Keep the most recent timestamp
                            if ts.isoformat() > existing:
                                lookup[source]["last_ingested"] = ts.isoformat()
                        else:
                            lookup[source]["last_ingested"] = ts.isoformat()

    except Exception as e:
        print(f"  Neon query error: {e}", file=sys.stderr)
        conn.close()
        return None

    conn.close()

    # Build normalized variants for fuzzy matching (same as INDEX.yaml loader)
    normalized = {}
    for sid, val in lookup.items():
        normalized[sid] = val
        normalized[sid.lower()] = val
        normalized[sid.lower().replace("_", ".")] = val
        normalized[sid.lower().replace(".", "_")] = val

    return normalized


def _load_pipeline_index_yaml():
    """Fallback: load indexing data from the legal-data-pipeline INDEX.yaml.

    Builds a lookup keyed by normalized source ID (lowercase) with both
    dot-separated and underscore-separated variants for fuzzy matching.
    """
    if not PIPELINE_INDEX.exists():
        return {}
    try:
        with open(PIPELINE_INDEX) as f:
            data = yaml.safe_load(f) or {}
        raw = data.get("sources", {})
        lookup = {}
        for sid, val in raw.items():
            lookup[sid] = val
            lookup[sid.lower()] = val
            lookup[sid.lower().replace("_", ".")] = val
            lookup[sid.lower().replace(".", "_")] = val
        return lookup
    except Exception:
        return {}


def load_pipeline_index():
    """Load indexing data — live from Neon if available, else INDEX.yaml fallback."""
    neon_url = _load_neon_database_url()
    if neon_url:
        print("Querying Neon PostgreSQL for live indexing data...")
        result = _query_neon_live(neon_url)
        if result is not None:
            source_count = len({k for k in result if "/" in k and k == k})  # rough unique
            print(f"  Live data loaded: {len(result)} entries from Neon")
            return result
        print("  Falling back to INDEX.yaml...")

    print("Loading indexing data from INDEX.yaml...")
    return _load_pipeline_index_yaml()


def parse_blocked_md():
    path = PROJECT_ROOT / "BLOCKED.md"
    if not path.exists():
        return []
    content = path.read_text()
    blockers = []
    # Split on ### headers
    sections = re.split(r'^### ', content, flags=re.MULTILINE)
    for section in sections[1:]:  # skip preamble
        lines = section.strip().split('\n')
        header = lines[0].strip()
        # Extract source_id from header (e.g., "BE/MoniteurBelge — description")
        source_id = header.split(' ')[0] if ' ' in header else header
        description = header.split(' — ', 1)[1] if ' — ' in header else ''
        body = '\n'.join(lines[1:])
        # Extract status
        status_match = re.search(r'\*\*Status:\*\*\s*(.+)', body)
        status = status_match.group(1).strip() if status_match else 'unknown'
        # Extract reason
        reason_match = re.search(r'\*\*Reason:\*\*\s*(.+)', body)
        reason = reason_match.group(1).strip() if reason_match else description
        blockers.append({
            "source_id": source_id,
            "description": description,
            "status": status,
            "reason": reason,
            "details": body.strip()[:500],
        })
    return blockers


def get_source_details(source_id):
    """Read status.yaml and count samples for a source."""
    parts = source_id.split('/')
    if len(parts) != 2:
        return {}
    country, name = parts
    # Find the actual directory (case-insensitive match)
    country_dir = PROJECT_ROOT / "sources" / country
    if not country_dir.exists():
        return {}
    source_dir = None
    for d in country_dir.iterdir():
        if d.is_dir() and d.name.lower() == name.lower():
            source_dir = d
            break
    if not source_dir:
        # Try exact match
        source_dir = country_dir / name
    if not source_dir or not source_dir.exists():
        return {}

    details = {}
    # Read status.yaml
    status_path = source_dir / "status.yaml"
    if status_path.exists():
        try:
            with open(status_path) as f:
                status_data = yaml.safe_load(f) or {}
            details["last_run"] = status_data.get("last_run")
            details["total_records"] = status_data.get("total_records", 0)
            details["last_error"] = status_data.get("last_error")
            history = status_data.get("run_history", [])
            if history:
                last = history[-1]
                details["last_records_fetched"] = last.get("records_fetched", 0)
                details["last_samples_saved"] = last.get("sample_records_saved", 0)
                details["last_errors"] = last.get("errors", 0)
        except Exception:
            pass

    # Fallback: read total_records from data/index.json if status.yaml didn't have it
    if not details.get("total_records"):
        data_index = source_dir / "data" / "index.json"
        if data_index.exists():
            try:
                with open(data_index) as f:
                    idx = json.load(f)
                details["total_records"] = len(idx)
            except Exception:
                pass

    # Count sample files and compute avg text length
    sample_dir = source_dir / "sample"
    if sample_dir.exists():
        sample_files = [f for f in sample_dir.glob("*.json") if f.name != "all_samples.json"]
        details["sample_count"] = len(sample_files)
        total_text_len = 0
        count = 0
        for sf in sample_files[:10]:  # only check first 10 for performance
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


def get_session_logs(limit=10):
    """Read recent session logs."""
    logs_dir = PROJECT_ROOT / "logs"
    if not logs_dir.exists():
        return [], ""
    log_files = sorted(logs_dir.glob("session_*.log"), reverse=True)
    sessions = []
    latest_log = ""
    for i, lf in enumerate(log_files[:limit]):
        try:
            content = lf.read_text()
            # Parse timestamp from filename: session_YYYYMMDD-HHMMSS.log
            match = re.search(r'session_(\d{8})-(\d{6})\.log', lf.name)
            if match:
                ts = f"{match.group(1)[:4]}-{match.group(1)[4:6]}-{match.group(1)[6:8]}T{match.group(2)[:2]}:{match.group(2)[2:4]}:{match.group(2)[4:6]}"
            else:
                ts = lf.name
            # Parse end time and compute duration
            end_match = re.search(r'Session ended at (.+?)(?:\n|$)', content)
            duration = None
            if end_match and match:
                try:
                    start = datetime.strptime(f"{match.group(1)}{match.group(2)}", "%Y%m%d%H%M%S")
                    # Try to parse end time
                    end_str = end_match.group(1).strip().split(' (')[0]
                    # Duration from filename diff
                except Exception:
                    pass
            # Extract a useful snippet
            lines = content.strip().split('\n')
            snippet_lines = [l for l in lines if l.strip() and not l.startswith('=')]
            snippet = '\n'.join(snippet_lines[:5]) if snippet_lines else "(empty)"

            session = {
                "timestamp": ts,
                "filename": lf.name,
                "snippet": snippet[:300],
                "has_error": "Error:" in content or "fatal:" in content,
                "pushed": "Safety net: pushing" in content or "git push" in content.lower(),
            }
            sessions.append(session)
            if i == 0:
                latest_log = content
        except Exception:
            pass
    return sessions, latest_log


def generate():
    manifest = load_manifest()
    sources = manifest.get("sources", [])
    neon_live = bool(_load_neon_database_url())
    pipeline_index = load_pipeline_index()
    contributors = load_contributors()

    # Summary
    by_status = {}
    by_country = {}
    subdivision_sources = {}
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
        # Track consolidated code coverage
        preferred = s.get("preferred_for", [])
        if "legislation" in preferred:
            by_country[country]["has_consolidated_codes"] = True
            by_country[country]["preferred_legislation_source"] = s.get("id")

        # Resolve subdivision mappings
        if country in SUBDIVISION_TREE:
            own_subs, is_country_wide = _resolve_source_subdivisions(s)
            if country not in subdivision_sources:
                subdivision_sources[country] = {
                    sub_code: {"own": [], "inherited": []}
                    for sub_code in SUBDIVISION_TREE[country]
                }
            for sub_code in SUBDIVISION_TREE[country]:
                if sub_code not in subdivision_sources[country]:
                    subdivision_sources[country][sub_code] = {"own": [], "inherited": []}
                if sub_code in own_subs:
                    subdivision_sources[country][sub_code]["own"].append(s)
                elif is_country_wide:
                    subdivision_sources[country][sub_code]["inherited"].append(s)

    # Build subdivisions summary into by_country
    for country_code, subs_data in subdivision_sources.items():
        if country_code not in by_country:
            continue
        subdivisions = {}
        for sub_code, sub_info in SUBDIVISION_TREE.get(country_code, {}).items():
            src_data = subs_data.get(sub_code, {"own": [], "inherited": []})
            own = src_data["own"]
            inherited = src_data["inherited"]
            own_complete = sum(1 for x in own if x.get("status") == "complete")
            inherited_complete = sum(1 for x in inherited if x.get("status") == "complete")
            total_src = len(own) + len(inherited)
            total_cmp = own_complete + inherited_complete
            if own or inherited:
                subdivisions[sub_code] = {
                    "name": sub_info.get("name", sub_code),
                    "own_sources": len(own),
                    "inherited_sources": len(inherited),
                    "own_complete": own_complete,
                    "inherited_complete": inherited_complete,
                    "status": (
                        "complete" if total_cmp == total_src and total_src > 0
                        else "partial" if total_cmp > 0
                        else "planned"
                    ),
                }
        if subdivisions:
            by_country[country_code]["subdivisions"] = subdivisions

    total = len(sources)
    complete = by_status.get("complete", 0)

    # Sources with details
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
            "jurisdictions": s.get("jurisdictions"),
        }
        # Add details for non-planned sources
        if s.get("status") != "planned":
            details = get_source_details(s.get("id", ""))
            source_data.update(details)
        # Merge pipeline indexing data (try exact, lowercase, dot/underscore variants)
        sid = s.get("id", "")
        pi = pipeline_index.get(sid) or pipeline_index.get(sid.lower())
        if pi:
            source_data["indexing"] = {
                "status": pi.get("status", "pending"),
                "neon_rows": pi.get("neon_rows", 0),
                "legislation_rows": pi.get("legislation_rows", 0),
                "case_law_rows": pi.get("case_law_rows", 0),
                "doctrine_rows": pi.get("doctrine_rows", 0),
                "data_type": pi.get("data_type"),
                "data_source": pi.get("data_source"),
                "last_ingested": pi.get("last_ingested"),
                "date_range": pi.get("date_range"),
            }
        # Add contributors from community
        source_contribs = contributors.get(sid, [])
        if source_contribs:
            source_data["contributors"] = [
                {"login": c["login"], "type": c["type"], "avatar_url": c.get("avatar_url", "")}
                for c in source_contribs
            ]
        sources_out.append(source_data)

    # Sort: complete first, then maintenance, then blocked, then planned; within each by priority
    status_order = {"complete": 0, "needs_maintenance": 1, "blocked": 2, "in_progress": 3, "planned": 4}
    priority_map = {"high": 1, "medium": 2, "low": 3}
    def _pri(v):
        if isinstance(v, int):
            return v
        try:
            return int(v)
        except (ValueError, TypeError):
            return priority_map.get(str(v).lower(), 99)
    sources_out.sort(key=lambda x: (status_order.get(x["status"], 9), _pri(x.get("priority", 99)), x["id"]))

    # Blockers
    blockers = parse_blocked_md()

    # Session logs
    sessions, latest_log = get_session_logs(limit=10)

    # Next source (planned first, then needs_maintenance)
    actionable = [s for s in sources if s.get("status") in ("planned", "needs_maintenance")]
    actionable.sort(key=lambda s: (
        0 if s.get("status") == "planned" else 1,
        s.get("priority", 99),
        s.get("id", ""),
    ))
    next_source = actionable[0]["id"] if actionable else None

    # Indexing summary
    indexed_sources = [s for s in sources_out if s.get("indexing")]
    total_neon_rows = sum(s["indexing"]["neon_rows"] for s in indexed_sources)
    indexed_ok = sum(1 for s in indexed_sources if s["indexing"]["status"] == "ok")
    pipeline_source_count = len(indexed_sources)
    # Use per-table row counts if available (from Neon live), otherwise fall back to data_type filter
    legislation_rows = sum(
        s["indexing"].get("legislation_rows", 0) or (
            s["indexing"]["neon_rows"] if s["indexing"].get("data_type") == "legislation" else 0
        )
        for s in indexed_sources if s["indexing"]["status"] == "ok"
    )
    case_law_rows = sum(
        s["indexing"].get("case_law_rows", 0) or (
            s["indexing"]["neon_rows"] if s["indexing"].get("data_type") == "case_law" else 0
        )
        for s in indexed_sources if s["indexing"]["status"] == "ok"
    )
    doctrine_rows = sum(
        s["indexing"].get("doctrine_rows", 0) or (
            s["indexing"]["neon_rows"] if s["indexing"].get("data_type") == "doctrine" else 0
        )
        for s in indexed_sources if s["indexing"]["status"] == "ok"
    )

    # Consolidated codes coverage
    countries_with_legislation = [c for c, d in by_country.items()
                                  if any("legislation" in s.get("data_types", [])
                                         for s in sources if s.get("country") == c)]
    countries_with_consolidated = [c for c, d in by_country.items() if d.get("has_consolidated_codes")]
    countries_gazette_only = [c for c in countries_with_legislation if c not in countries_with_consolidated]

    # Build output
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "indexing_data_source": "neon_live" if neon_live else "index_yaml",
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
        "indexing_summary": {
            "total_pipeline_sources": pipeline_source_count,
            "indexed_ok": indexed_ok,
            "total_neon_rows": total_neon_rows,
            "legislation_rows": legislation_rows,
            "case_law_rows": case_law_rows,
            "doctrine_rows": doctrine_rows,
        },
        "next_source": next_source,
        "by_country": by_country,
        "sources": sources_out,
        "blockers": blockers,
        "recent_sessions": sessions,
        "latest_session_log": latest_log[:5000],
    }

    # Write output
    DOCS_DIR.mkdir(exist_ok=True)
    with open(DOCS_DIR / "status.json", "w") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"Dashboard data generated: {complete}/{total} sources complete ({output['summary']['percent_complete']}%)")

    # Also regenerate licenses.json
    try:
        from scripts.generate_licenses_json import main as generate_licenses
        generate_licenses()
    except Exception as e:
        print(f"Warning: could not generate licenses.json: {e}")


if __name__ == "__main__":
    generate()
