#!/usr/bin/env python3
"""
NL/CRvB - Dutch Central Appeals Tribunal (Centrale Raad van Beroep)

Fetches case law from the Centrale Raad van Beroep via the Rechtspraak Open Data API.
Filters by instantie to get only CRvB decisions.

CRvB is the highest administrative court for social security and civil service cases.

Data source:
- https://data.rechtspraak.nl/uitspraken/zoeken

Usage:
    python bootstrap.py bootstrap --sample   # Fetch sample records
    python bootstrap.py bootstrap --full     # Full fetch all decisions
"""

import argparse
import html
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional, List, Dict
from xml.etree import ElementTree as ET

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Constants
SOURCE_ID = "NL/CRvB"
COURT_NAME = "Centrale Raad van Beroep"
COURT_URI = "http://standaarden.overheid.nl/owms/terms/Centrale_Raad_van_Beroep"
API_BASE = "https://data.rechtspraak.nl"
SEARCH_URL = f"{API_BASE}/uitspraken/zoeken"
CONTENT_URL = f"{API_BASE}/uitspraken/content"

RATE_LIMIT_DELAY = 2.0
USER_AGENT = "WorldWideLaw/1.0 (Open Data Research)"

SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"

# Namespaces in Rechtspraak XML
NS = {
    "atom": "http://www.w3.org/2005/Atom",
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "dcterms": "http://purl.org/dc/terms/",
    "psi": "http://psi.rechtspraak.nl/",
}
RS_NS = "{http://www.rechtspraak.nl/schema/rechtspraak-1.0}"


def create_session() -> requests.Session:
    """Create a requests session with retry logic."""
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=5, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "application/xml, application/atom+xml, text/xml, */*",
    })
    return session


def search_eclis(session: requests.Session, max_results: int = 100, offset: int = 0) -> List[Dict]:
    """Search for CRvB ECLIs via the Rechtspraak API."""
    params = {
        "max": str(max_results),
        "from": str(offset),
        "sort": "DESC",
        "type": "Uitspraak",
        "creator": COURT_URI,
    }

    try:
        resp = session.get(SEARCH_URL, params=params, timeout=60)
        resp.raise_for_status()
    except Exception as e:
        print(f"Search error: {e}", file=sys.stderr)
        return []

    results = []
    try:
        root = ET.fromstring(resp.content)
        for entry in root.findall("atom:entry", NS):
            ecli_el = entry.find("atom:id", NS)
            title_el = entry.find("atom:title", NS)
            summary_el = entry.find("atom:summary", NS)

            if ecli_el is not None and ecli_el.text:
                results.append({
                    "ecli": ecli_el.text,
                    "title": title_el.text if title_el is not None else "",
                    "summary": summary_el.text if summary_el is not None else "",
                })
    except ET.ParseError as e:
        print(f"XML parse error in search: {e}", file=sys.stderr)

    return results


def fetch_document(session: requests.Session, ecli: str) -> Optional[str]:
    """Fetch the full XML document for an ECLI."""
    try:
        resp = session.get(CONTENT_URL, params={"id": ecli}, timeout=60)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        print(f"  Error fetching {ecli}: {e}", file=sys.stderr)
        return None


def extract_text_recursive(elem: ET.Element) -> str:
    """Recursively extract all text from an element."""
    texts = []
    if elem.text:
        texts.append(elem.text.strip())
    for child in elem:
        child_text = extract_text_recursive(child)
        if child_text:
            texts.append(child_text)
        if child.tail:
            texts.append(child.tail.strip())
    full = " ".join(t for t in texts if t)
    full = html.unescape(full)
    full = re.sub(r"\s+", " ", full)
    return full.strip()


def parse_document(xml_content: str) -> Dict:
    """Parse a Rechtspraak XML document."""
    try:
        root = ET.fromstring(xml_content.encode("utf-8"))
    except ET.ParseError as e:
        print(f"  XML parse error: {e}", file=sys.stderr)
        return {}

    result = {}

    # RDF metadata
    rdf = root.find(".//{http://www.w3.org/1999/02/22-rdf-syntax-ns#}RDF")
    if rdf is not None:
        desc = rdf.find("{http://www.w3.org/1999/02/22-rdf-syntax-ns#}Description")
        if desc is not None:
            for field in ['identifier', 'title', 'date', 'issued', 'modified', 'creator', 'publisher', 'type']:
                el = desc.find(f"{{{NS['dcterms']}}}{field}")
                if el is not None and el.text:
                    result[field] = el.text.strip()

            # Multi-value fields
            for field in ['subject', 'procedure']:
                ns_key = 'dcterms' if field == 'subject' else 'psi'
                els = desc.findall(f"{{{NS[ns_key]}}}{field}")
                result[field] = [el.text.strip() for el in els if el.text]

    # Full text extraction
    text_parts = []

    # Inhoudsindicatie (summary)
    inhoud = root.find(f".//{RS_NS}inhoudsindicatie")
    if inhoud is None:
        inhoud = root.find(".//inhoudsindicatie")
    if inhoud is not None:
        t = extract_text_recursive(inhoud)
        if t and t != "-":
            text_parts.append(f"=== INHOUDSINDICATIE ===\n{t}")

    # Uitspraak (judgment)
    uitspraak = root.find(f".//{RS_NS}uitspraak")
    if uitspraak is None:
        uitspraak = root.find(".//uitspraak")
    if uitspraak is not None:
        t = extract_text_recursive(uitspraak)
        if t:
            text_parts.append(f"=== UITSPRAAK ===\n{t}")

    # Conclusie
    conclusie = root.find(f".//{RS_NS}conclusie")
    if conclusie is None:
        conclusie = root.find(".//conclusie")
    if conclusie is not None:
        t = extract_text_recursive(conclusie)
        if t:
            text_parts.append(f"=== CONCLUSIE ===\n{t}")

    result["text"] = "\n\n".join(text_parts)
    return result


def normalize(raw: Dict) -> Dict:
    """Normalize a raw record into the standard schema."""
    ecli = raw.get("identifier", "") or raw.get("ecli", "")
    subjects = raw.get("subject", [])
    if isinstance(subjects, list):
        subjects = ", ".join(subjects)
    procedures = raw.get("procedure", [])
    if isinstance(procedures, list):
        procedures = ", ".join(procedures)

    return {
        "_id": ecli,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": raw.get("title", ecli),
        "text": raw.get("text", ""),
        "date": raw.get("date", ""),
        "url": f"https://uitspraken.rechtspraak.nl/details?id={ecli}" if ecli else "",
        "ecli": ecli,
        "court": raw.get("creator", COURT_NAME),
        "subject": subjects,
        "procedure": procedures,
        "summary": raw.get("_search_summary", ""),
    }


def fetch_all(sample: bool = False) -> Generator[Dict, None, None]:
    """Fetch all CRvB decisions."""
    session = create_session()
    batch_size = 100 if sample else 500
    offset = 0
    total_fetched = 0
    limit = 15 if sample else 999999

    while total_fetched < limit:
        print(f"Searching ECLIs (offset={offset})...", file=sys.stderr)
        results = search_eclis(session, max_results=batch_size, offset=offset)
        if not results:
            break

        for item in results:
            if total_fetched >= limit:
                break
            ecli = item["ecli"]
            print(f"  [{total_fetched+1}] Fetching: {ecli}", file=sys.stderr)

            time.sleep(RATE_LIMIT_DELAY)
            xml = fetch_document(session, ecli)
            if not xml:
                continue

            parsed = parse_document(xml)
            if not parsed.get("text"):
                print(f"    No text for {ecli}, skipping", file=sys.stderr)
                continue

            parsed["_search_summary"] = item.get("summary", "")
            record = normalize(parsed)
            yield record
            total_fetched += 1

        offset += batch_size
        time.sleep(1)

    print(f"Fetched {total_fetched} CRvB decisions", file=sys.stderr)


def bootstrap(sample: bool = True):
    """Bootstrap the data source."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    count = 0
    for record in fetch_all(sample=sample):
        safe_id = re.sub(r'[^a-zA-Z0-9_.-]', '_', record['_id'])
        fname = SAMPLE_DIR / f"{safe_id}.json"
        with open(fname, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        print(f"    Saved: {record['ecli']} ({len(record['text'])} chars)", file=sys.stderr)

    print(f"\nBootstrap complete: {count} records saved to {SAMPLE_DIR}", file=sys.stderr)
    return count


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='NL/CRvB Bootstrap')
    sub = parser.add_subparsers(dest='command')
    boot = sub.add_parser('bootstrap')
    boot.add_argument('--sample', action='store_true', default=True)
    boot.add_argument('--full', action='store_true')
    args = parser.parse_args()

    if args.command == 'bootstrap':
        sample = not args.full
        bootstrap(sample=sample)
    else:
        parser.print_help()
