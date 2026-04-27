#!/usr/bin/env python3
"""
AW/Courts -- Aruba Court Decisions via rechtspraak.nl Open Data API

Fetches decisions from the Gemeenschappelijk Hof van Justitie via the Dutch
Open Data API at data.rechtspraak.nl. Full text is available in structured
XML which we convert to plain text.

Strategy:
  1. Search API with creator filter to list ECLIs for this court
  2. Content API to fetch full XML for each decision
  3. Extract plain text from XML structure

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample records
  python bootstrap.py bootstrap --full     # Full bootstrap (all ~4000 decisions)
  python bootstrap.py test-api             # Quick connectivity test
"""

import argparse
import json
import logging
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "AW/Courts"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AW.Courts")

API_BASE = "https://data.rechtspraak.nl/uitspraken"
SEARCH_URL = f"{API_BASE}/zoeken"
CONTENT_URL = f"{API_BASE}/content"

# psi.rechtspraak.nl identifier for Gemeenschappelijk Hof
CREATOR_ID = "http://psi.rechtspraak.nl/GHACSMBES"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "application/xml, text/xml, */*",
}

ATOM_NS = "http://www.w3.org/2005/Atom"
RS_NS = "http://www.rechtspraak.nl/schema/rechtspraak-1.0"
RDF_NS = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
DCTERMS_NS = "http://purl.org/dc/terms/"
PSI_NS = "http://psi.rechtspraak.nl/"
RDFS_NS = "http://www.w3.org/2000/01/rdf-schema#"

session = requests.Session()
session.headers.update(HEADERS)


def search_eclis(max_results: int = 1000, from_date: Optional[str] = None) -> list[dict]:
    """Search the ECLI index for decisions from this court."""
    eclis = []
    offset = 0
    page_size = min(max_results, 100)

    while offset < max_results:
        params = {
            "creator": CREATOR_ID,
            "max": page_size,
            "from": offset,
        }
        if from_date:
            params["date"] = from_date

        try:
            resp = session.get(SEARCH_URL, params=params, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Search failed at offset {offset}: {e}")
            break

        root = ET.fromstring(resp.content)

        # Parse total count from subtitle
        subtitle = root.find(f"{{{ATOM_NS}}}subtitle")
        if subtitle is not None and subtitle.text:
            match = re.search(r"(\d+)", subtitle.text)
            if match:
                total = int(match.group(1))
                if offset == 0:
                    logger.info(f"Total ECLIs available: {total}")

        entries = root.findall(f"{{{ATOM_NS}}}entry")
        if not entries:
            break

        for entry in entries:
            ecli_id = entry.findtext(f"{{{ATOM_NS}}}id", "")
            title = entry.findtext(f"{{{ATOM_NS}}}title", "")
            summary = entry.findtext(f"{{{ATOM_NS}}}summary", "")
            updated = entry.findtext(f"{{{ATOM_NS}}}updated", "")

            if ecli_id:
                eclis.append({
                    "ecli": ecli_id,
                    "title": title,
                    "summary": summary if summary != "-" else "",
                    "updated": updated,
                })

        offset += len(entries)
        if len(entries) < page_size:
            break

        time.sleep(0.5)

    logger.info(f"Found {len(eclis)} ECLIs")
    return eclis


def extract_text_from_xml(xml_content: bytes) -> dict:
    """Extract full text and metadata from rechtspraak XML."""
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        logger.warning(f"XML parse error: {e}")
        return {}

    result = {}

    # Extract metadata from RDF
    rdf = root.find(f"{{{RDF_NS}}}RDF")
    if rdf is not None:
        desc = rdf.find(f"{{{RDF_NS}}}Description")
        if desc is not None:
            result["ecli"] = desc.findtext(f"{{{DCTERMS_NS}}}identifier", "")
            result["date"] = desc.findtext(f"{{{DCTERMS_NS}}}date", "")
            result["issued"] = desc.findtext(f"{{{DCTERMS_NS}}}issued", "")
            result["modified"] = desc.findtext(f"{{{DCTERMS_NS}}}modified", "")
            result["language"] = desc.findtext(f"{{{DCTERMS_NS}}}language", "nl")

            creator = desc.find(f"{{{DCTERMS_NS}}}creator")
            if creator is not None:
                result["court"] = creator.text or creator.get(f"{{{RDFS_NS}}}label", "")

            case_num = desc.find(f"{{{PSI_NS}}}zaaknummer")
            if case_num is not None:
                result["case_number"] = case_num.text or ""

            proc = desc.find(f"{{{PSI_NS}}}procedure")
            if proc is not None:
                result["procedure_type"] = proc.text or proc.get(f"{{{RDFS_NS}}}label", "")

            subject = desc.find(f"{{{DCTERMS_NS}}}subject")
            if subject is not None:
                result["subject_area"] = subject.text or subject.get(f"{{{RDFS_NS}}}label", "")

            # Get title from second Description block
            for d in rdf.findall(f"{{{RDF_NS}}}Description"):
                t = d.findtext(f"{{{DCTERMS_NS}}}title", "")
                if t:
                    result["title"] = t
                    break

    # Extract inhoudsindicatie (summary)
    inh = root.find(f"{{{RS_NS}}}inhoudsindicatie")
    if inh is None:
        inh = root.find("inhoudsindicatie")
    if inh is not None:
        result["summary"] = _element_text(inh).strip()

    # Extract full text from uitspraak or conclusie
    for tag in ["uitspraak", "conclusie"]:
        elem = root.find(f"{{{RS_NS}}}{tag}")
        if elem is None:
            elem = root.find(tag)
        if elem is not None:
            result["text"] = _element_text(elem).strip()
            break

    return result


def _element_text(elem) -> str:
    """Recursively extract text from an XML element, stripping tags."""
    parts = []
    if elem.text:
        parts.append(elem.text)
    for child in elem:
        tag_local = child.tag.split("}")[-1] if "}" in child.tag else child.tag
        if tag_local in ("title", "nr"):
            parts.append("\n\n")
            parts.append(_element_text(child))
            parts.append("\n")
        elif tag_local in ("para", "parablock"):
            parts.append("\n")
            parts.append(_element_text(child))
        elif tag_local == "emphasis":
            parts.append(_element_text(child))
        elif tag_local in ("listitem",):
            parts.append("\n- ")
            parts.append(_element_text(child))
        else:
            parts.append(_element_text(child))
        if child.tail:
            parts.append(child.tail)
    text = "".join(parts)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


def fetch_document(ecli: str) -> Optional[dict]:
    """Fetch full document content for an ECLI."""
    try:
        resp = session.get(CONTENT_URL, params={"id": ecli}, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.warning(f"Failed to fetch {ecli}: {e}")
        return None

    return extract_text_from_xml(resp.content)


def normalize(data: dict) -> dict:
    """Transform to standard schema."""
    ecli = data.get("ecli", "")
    doc_id = ecli.replace(":", "-").lower() if ecli else f"aw-courts-{hash(data.get('title', ''))}"

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": data.get("title", ecli),
        "text": data.get("text", ""),
        "date": data.get("date") or None,
        "url": f"https://uitspraken.rechtspraak.nl/details?id={ecli}" if ecli else "",
        "ecli": ecli,
        "case_number": data.get("case_number", ""),
        "court": data.get("court", "Gemeenschappelijk Hof van Justitie"),
        "procedure_type": data.get("procedure_type", ""),
        "subject_area": data.get("subject_area", ""),
        "summary": data.get("summary", ""),
        "language": data.get("language", "nl"),
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Yield all documents. If sample=True, fetch only ~15 records."""
    max_search = 20 if sample else 100000
    eclis = search_eclis(max_results=max_search)

    count = 0
    limit = 15 if sample else len(eclis)

    for entry in eclis:
        if count >= limit:
            break

        ecli = entry["ecli"]
        time.sleep(1.0)

        data = fetch_document(ecli)
        if not data or not data.get("text"):
            logger.warning(f"No text for {ecli}, skipping")
            continue

        record = normalize(data)
        if record["text"] and len(record["text"]) > 50:
            count += 1
            yield record
            logger.info(f"  [{count}] {ecli} — {len(record['text'])} chars")

    logger.info(f"Total documents fetched: {count}")


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """Yield documents modified since a date (YYYY-MM-DD)."""
    eclis = search_eclis(max_results=100000, from_date=since)

    count = 0
    for entry in eclis:
        ecli = entry["ecli"]
        time.sleep(1.0)

        data = fetch_document(ecli)
        if not data or not data.get("text"):
            continue

        record = normalize(data)
        if record["text"] and len(record["text"]) > 50:
            count += 1
            yield record

    logger.info(f"Updates since {since}: {count} documents")


def save_sample(records: list[dict]) -> None:
    """Save sample records to sample/ directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    for old in SAMPLE_DIR.glob("record_*.json"):
        old.unlink()
    for i, record in enumerate(records):
        path = SAMPLE_DIR / f"record_{i+1:03d}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
    logger.info(f"Saved {len(records)} sample records to {SAMPLE_DIR}")


def test_api() -> bool:
    """Test connectivity to rechtspraak.nl Open Data API."""
    try:
        eclis = search_eclis(max_results=3)
        if not eclis:
            logger.error("No ECLIs found")
            return False

        logger.info(f"Search OK: {len(eclis)} ECLIs")

        time.sleep(1.0)
        data = fetch_document(eclis[0]["ecli"])
        if data and data.get("text"):
            logger.info(f"Content OK: {eclis[0]['ecli']} — {len(data['text'])} chars")
            return True
        else:
            logger.warning("Content fetch returned no text")
            return False
    except Exception as e:
        logger.error(f"API test failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="AW/Courts data fetcher")
    parser.add_argument("command", choices=["bootstrap", "test-api"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (~15 records)")
    parser.add_argument("--full", action="store_true", help="Full bootstrap")
    args = parser.parse_args()

    if args.command == "test-api":
        success = test_api()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        is_sample = args.sample or not args.full
        records = []
        for doc in fetch_all(sample=is_sample):
            records.append(doc)

        if records:
            save_sample(records)
            logger.info(f"Bootstrap complete: {len(records)} records")
            texts = [r for r in records if r.get("text") and len(r["text"]) > 50]
            logger.info(f"Records with full text: {len(texts)}/{len(records)}")
            if texts:
                avg_len = sum(len(r["text"]) for r in texts) // len(texts)
                logger.info(f"Average text length: {avg_len} chars")
        else:
            logger.error("No records fetched!")
            sys.exit(1)


if __name__ == "__main__":
    main()
