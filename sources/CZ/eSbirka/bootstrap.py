#!/usr/bin/env python3
"""
Czech e-Sbírka (Collection of Laws) Data Fetcher — SPARQL edition

Official open data from the Czech Ministry of Interior
https://www.e-sbirka.cz / https://zakony.gov.cz

Uses the SPARQL endpoint at opendata.eselpoint.gov.cz to fetch legal acts
and their full text fragment-by-fragment.  No bulk downloads, no OOM.

92,000+ acts available.  CC BY 4.0.
"""

import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import quote

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SPARQL_ENDPOINT = "https://opendata.eselpoint.gov.cz/sparql"
ESBIRKA_BASE = "https://e-sbirka.gov.cz"
PREFIX_ACT = "https://slovník.gov.cz/datový/sbírka/pojem/"
SOURCE_ID = "CZ/eSbirka"
SAMPLE_DIR = Path(__file__).parent / "sample"

# Consecutive per-act SPARQL failures tolerated before aborting the crawl.
MAX_CONSECUTIVE_ERRORS = 10

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "LegalDataHunter/1.0 (legal-data research project)",
}

# ───────────────────────── SPARQL helpers ─────────────────────────


class SparqlError(RuntimeError):
    """The SPARQL endpoint could not be queried after retries."""


def _sparql(query: str, timeout: int = 120) -> List[dict]:
    """Execute a SPARQL SELECT and return the list of bindings.

    The query is stripped before sending: opendata.eselpoint.gov.cz answers 403
    to any `query` parameter that begins with whitespace (leading newline, space
    or tab), which is exactly what a triple-quoted Python query literal produces.
    Internal newlines and a trailing newline are accepted, so only the leading
    edge needs trimming (issue #1472).
    """
    query = query.strip()
    last_error: Optional[str] = None
    for attempt in range(3):
        try:
            r = requests.get(
                SPARQL_ENDPOINT,
                params={"default-graph-uri": "", "query": query},
                headers=HEADERS,
                timeout=timeout,
            )
            r.raise_for_status()
            return r.json()["results"]["bindings"]
        except Exception as e:
            last_error = str(e)
            logger.warning("SPARQL attempt %d failed: %s", attempt + 1, e)
            time.sleep(2 * (attempt + 1))
    # Never degrade to an empty result set: a silent [] here is what let a total
    # endpoint failure look like "0 acts found" and fall back to the samples.
    raise SparqlError(f"SPARQL endpoint {SPARQL_ENDPOINT} failed after 3 attempts: {last_error}")


def _val(binding: dict, key: str) -> Optional[str]:
    """Extract value from a SPARQL binding."""
    b = binding.get(key)
    return b["value"] if b else None


# ───────────────────────── Discovery ─────────────────────────


def list_acts(offset: int = 0, limit: int = 1000) -> List[dict]:
    """Get a page of legal acts with their citation, year, and number."""
    q = f"""
    SELECT ?act ?citation ?year ?number WHERE {{
      ?act a <{PREFIX_ACT}právní-akt> ;
           <{PREFIX_ACT}citace-právního-aktu> ?citation ;
           <{PREFIX_ACT}rok-předpisu> ?year ;
           <{PREFIX_ACT}číslo-předpisu> ?number .
    }}
    ORDER BY DESC(?year) DESC(?number)
    OFFSET {offset} LIMIT {limit}
    """
    return _sparql(q)


def get_latest_version(act_uri: str) -> Optional[str]:
    """Get the URI of the latest version of a legal act."""
    q = f"""
    SELECT ?ver WHERE {{
      <{act_uri}> <{PREFIX_ACT}má-poslední-znění> ?ver .
    }} LIMIT 1
    """
    rows = _sparql(q)
    return _val(rows[0], "ver") if rows else None


def get_full_text(version_uri: str) -> str:
    """Get the full text of a law version by fetching all ordered fragments."""
    q = f"""
    SELECT ?text ?order WHERE {{
      <{version_uri}> <{PREFIX_ACT}má-fragment-znění> ?frag .
      ?frag <{PREFIX_ACT}obsahuje-fragment> ?innerFrag .
      ?frag <{PREFIX_ACT}pořadí-fragmentu-znění-právního-aktu> ?order .
      ?innerFrag <{PREFIX_ACT}text-fragmentu> ?text .
    }}
    ORDER BY ?order
    """
    rows = _sparql(q, timeout=180)
    if not rows:
        return ""
    parts = []
    for r in rows:
        txt = _val(r, "text") or ""
        txt = _clean_html(txt)
        if txt:
            parts.append(txt)
    return "\n".join(parts)


def _clean_html(text: str) -> str:
    """Remove HTML tags and decode entities from fragment text."""
    import html as html_mod

    text = re.sub(r"</?var>", "", text)
    text = re.sub(r'<a[^>]*class="ext_odkaz"[^>]*>([^<]*)</a>', r"\1", text)
    text = re.sub(r'<a[^>]*class="lz_plna"[^>]*>([^<]*)</a>', r"\1", text)
    text = re.sub(r"<a[^>]*>([^<]*)</a>", r"\1", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = html_mod.unescape(text)
    return text.strip()


# ───────────────────────── Normalization ─────────────────────────


def _safe_int(value: str) -> Optional[int]:
    """Parse an int from a possibly noisy value (e.g. 'n9' → 9). None if no digits."""
    if value is None:
        return None
    digits = re.sub(r"\D", "", str(value))
    return int(digits) if digits else None


def normalize(act_uri: str, citation: str, year: str, number: str, text: str) -> Dict[str, Any]:
    """Normalize a Czech legal act into the standard schema."""
    eli_path = act_uri.split("/esel-esb/")[-1] if "/esel-esb/" in act_uri else ""
    year_i = _safe_int(year)
    return {
        "_id": f"CZ-{year}-{number}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": citation,
        "text": text,
        "date": f"{year_i}-01-01" if year_i else None,
        "url": f"{ESBIRKA_BASE}/{eli_path}" if eli_path else ESBIRKA_BASE,
        "citation": citation,
        "year": year_i,
        "number": _safe_int(number),
        "language": "cs",
        "eli_uri": act_uri,
    }


# ───────────────────────── Fetcher interface ─────────────────────────


def fetch_all() -> Iterator[Dict[str, Any]]:
    """Yield all legal acts with full text."""
    offset = 0
    page_size = 500
    total = 0
    consecutive_errors = 0
    while True:
        acts = list_acts(offset=offset, limit=page_size)
        if not acts:
            break
        for act in acts:
            act_uri = _val(act, "act")
            citation = _val(act, "citation") or ""
            year = _val(act, "year") or ""
            number = _val(act, "number") or ""
            if not act_uri:
                continue

            # An isolated per-act failure is tolerable; a run of them means the
            # endpoint has gone away and the rest of the crawl would be a
            # silently truncated corpus, so escalate.
            try:
                version_uri = get_latest_version(act_uri)
                text = get_full_text(version_uri) if version_uri else ""
            except SparqlError as e:
                consecutive_errors += 1
                logger.warning("SPARQL failure on %s (%d in a row): %s",
                               citation, consecutive_errors, e)
                if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                    raise SparqlError(
                        f"{consecutive_errors} consecutive SPARQL failures after "
                        f"{total} acts — aborting rather than writing a truncated corpus"
                    ) from e
                time.sleep(5)
                continue
            consecutive_errors = 0

            if not version_uri:
                logger.debug("No version for %s, skipping", citation)
                continue
            if not text:
                logger.debug("No text for %s, skipping", citation)
                continue

            total += 1
            yield normalize(act_uri, citation, year, number, text)

            if total % 100 == 0:
                logger.info("Fetched %d acts so far", total)
            time.sleep(1)  # rate limit

        offset += page_size

    logger.info("Total acts fetched: %d", total)


def fetch_updates(since: str) -> Iterator[Dict[str, Any]]:
    """Yield acts updated since a given date (not yet implemented)."""
    logger.warning("fetch_updates not yet implemented for SPARQL approach")
    return iter([])


# ───────────────────────── CLI ─────────────────────────


def bootstrap_sample(count: int = 12):
    """Fetch a small sample of recent acts for validation."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    # Get recent acts (last few years)
    acts = list_acts(offset=0, limit=count * 2)
    if not acts:
        logger.error("No acts found")
        sys.exit(1)

    saved = 0
    for act in acts:
        if saved >= count:
            break

        act_uri = _val(act, "act")
        citation = _val(act, "citation") or ""
        year = _val(act, "year") or ""
        number = _val(act, "number") or ""
        if not act_uri:
            continue

        logger.info("Fetching [%d/%d] %s ...", saved + 1, count, citation)

        version_uri = get_latest_version(act_uri)
        if not version_uri:
            logger.warning("  No version, skipping")
            continue

        text = get_full_text(version_uri)
        if not text or len(text) < 100:
            logger.warning("  Text too short (%d chars), skipping", len(text))
            continue

        record = normalize(act_uri, citation, year, number, text)
        fname = SAMPLE_DIR / f"{year}_{number}.json"
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        logger.info("  Saved %s (%d chars)", fname.name, len(text))
        saved += 1
        time.sleep(2)  # be polite

    logger.info("Saved %d sample records to %s", saved, SAMPLE_DIR)
    return saved


def bootstrap_full() -> int:
    """Fetch all acts and stream them to data/records.jsonl."""
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)
    out_path = data_dir / "records.jsonl"
    count = 0
    with open(out_path, "w", encoding="utf-8") as f:
        for record in fetch_all():
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1
    logger.info("Wrote %d records to %s", count, out_path)
    return count


def main():
    import argparse

    parser = argparse.ArgumentParser(description="CZ/eSbirka bootstrap (SPARQL)")
    # Accept the fleet's bootstrap/bootstrap-fast commands and ignore extra flags
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch sample data only")
    parser.add_argument("--full", action="store_true", help="Full pull (default for non-sample)")
    parser.add_argument("--count", type=int, default=12, help="Number of sample records")
    args, _unknown = parser.parse_known_args()

    if args.sample:
        n = bootstrap_sample(count=args.count)
        if n < 10:
            logger.error("Only %d samples (need 10+). Check SPARQL endpoint.", n)
            sys.exit(1)
    else:
        bootstrap_full()


if __name__ == "__main__":
    main()
