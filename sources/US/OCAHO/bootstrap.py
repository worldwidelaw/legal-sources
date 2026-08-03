#!/usr/bin/env python3
"""
US/OCAHO -- Office of the Chief Administrative Hearing Officer Decisions

Fetches OCAHO published decisions from justice.gov EOIR. OCAHO is the EOIR
adjudicatory body whose Administrative Law Judges decide contested cases under
the Immigration and Nationality Act's employer-sanctions (8 U.S.C. 1324a),
document-fraud (1324c) and unfair immigration-related employment-practice
(1324b) provisions. Each published ruling resolves a specific case = case_law;
US federal-government works, public domain (17 U.S.C. 105).

Strategy:
  - GET the OCAHO decisions index page and harvest every volume-listing link.
    Two layouts coexist:
      * Volumes 1-11 (bound/looseleaf):
        /eoir/OcahoMain/publisheddecisions/{Hardbound|Looseleaf}/Volume{N}/
        vol{N}listforInternet.htm  -> PDFs under the same Volume{N}/ folder.
      * Volumes 12+ (modern):
        /eoir/listing-volume-{N}-decisions  -> PDFs at /d9/YYYY-MM/{ID}.pdf.
  - Each listing is a table whose rows carry the case caption, the OCAHO case
    number, the decision date and a "{ID} (PDF)" link. A single row-based
    parser handles both layouts. Continuation rows (same PDF, e.g. consolidated
    subpoena rulings) are de-duplicated by PDF URL.
  - Download each decision PDF and extract full text via common.pdf_extract
    (born-digital text layer; no OCR needed).

Usage:
  python bootstrap.py bootstrap --sample   # ~15 sample decisions
  python bootstrap.py bootstrap             # Full extraction -> data/records.jsonl
  python bootstrap.py bootstrap-fast        # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api              # Test connectivity
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional, Union

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "US/OCAHO"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.OCAHO")

BASE_URL = "https://www.justice.gov"
INDEX_URL = f"{BASE_URL}/eoir/office-of-the-chief-administrative-hearing-officer-decisions"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal data research; +https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html,application/xhtml+xml,application/pdf",
    "Accept-Language": "en-US,en;q=0.9",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)

CRAWL_DELAY = 2.0  # seconds between requests


def fetch_url(url: str, binary: bool = False) -> Optional[Union[bytes, str]]:
    """Fetch a URL with retry logic (follows the EOIR PDF redirects)."""
    for attempt in range(3):
        try:
            resp = SESSION.get(url, timeout=60, allow_redirects=True)
            if resp.status_code == 404:
                return None
            resp.raise_for_status()
            return resp.content if binary else resp.text
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
            if attempt < 2:
                time.sleep(5 * (attempt + 1))
            else:
                logger.warning(f"Failed to fetch {url}: {e}")
                return None
        except Exception as e:
            logger.warning(f"Error fetching {url}: {e}")
            return None


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    """Extract text from PDF using centralized extractor."""
    return extract_pdf_markdown(
        source="US/OCAHO",
        source_id="",
        pdf_bytes=pdf_bytes,
        table="case_law",
    ) or ""


TAG_RE = re.compile(r"<[^>]+>")
ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.IGNORECASE | re.DOTALL)
CELL_RE = re.compile(r"<t[dh][^>]*>(.*?)</t[dh]>", re.IGNORECASE | re.DOTALL)
HREF_RE = re.compile(r'href="([^"]+)"', re.IGNORECASE)
VOL_LINK_RE = re.compile(
    r'href="([^"]*(?:vol\d+listforInternet\.htm|listing-volume-\d+-decisions))"',
    re.IGNORECASE,
)
DATE_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{2,4})\b")
ID_RE = re.compile(r"(\d+[a-zA-Z]?)\s*\(?\s*PDF", re.IGNORECASE)
# OCAHO case numbers look like 88S004A0, 2024B00109, 92B00123, etc.
CASENO_RE = re.compile(r"^\d{2,4}[A-Za-z]\d{3,}[A-Za-z0-9]*$")


def _clean(s: str) -> str:
    """Strip tags, unescape entities, collapse whitespace."""
    return re.sub(r"\s+", " ", unescape(TAG_RE.sub(" ", s or ""))).strip()


def _volume_from_url(url: str) -> Optional[int]:
    m = re.search(r"Volume(\d+)", url)
    if m:
        return int(m.group(1))
    m = re.search(r"listing-volume-(\d+)-decisions", url)
    if m:
        return int(m.group(1))
    return None


def _parse_date(cells: list) -> Optional[str]:
    """Return ISO date from the first cell holding a M/D/Y date."""
    for c in cells:
        m = DATE_RE.search(c)
        if not m:
            continue
        month, day, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if year < 100:
            year += 1900 if year >= 80 else 2000
        try:
            return datetime(year, month, day).strftime("%Y-%m-%d")
        except ValueError:
            return None
    return None


def get_volume_index_urls() -> list:
    """Harvest every volume-listing URL from the OCAHO decisions index page."""
    html = fetch_url(INDEX_URL)
    if not html:
        logger.error("Failed to fetch OCAHO index page")
        return []
    seen = set()
    out = []
    for href in VOL_LINK_RE.findall(html):
        url = href if href.startswith("http") else f"{BASE_URL}{href}"
        vol = _volume_from_url(url)
        if vol is None or url in seen:
            continue
        seen.add(url)
        out.append((vol, url))
    out.sort(key=lambda t: t[0])
    return out


def parse_volume_index(html: str, volume_num: int) -> list:
    """Parse decisions from an OCAHO volume-listing table.

    Each usable row carries a "{ID} (PDF)" link. The caption / case number /
    date live in the surrounding cells; continuation rows (which lack a caption
    of their own) inherit the previous row's caption and case number. Records
    are de-duplicated by PDF URL so consolidated multi-ruling PDFs yield one
    record.
    """
    decisions = []
    seen_pdf = set()
    last_caption = ""
    last_caseno = ""

    for row in ROW_RE.findall(html):
        # Locate a decision PDF link within this row.
        pdf_url = None
        for href in HREF_RE.findall(row):
            low = href.lower()
            if low.endswith(".pdf") or ".pdf?" in low or "/dl" in low:
                pdf_url = href if href.startswith("http") else f"{BASE_URL}{href}"
                break
        if not pdf_url:
            continue

        cells = [_clean(c) for c in CELL_RE.findall(row)]
        if not cells:
            continue

        # Decision id: the "{ID} (PDF)" text, else the PDF filename stem.
        decision_id = None
        for c in cells:
            m = ID_RE.search(c)
            if m:
                decision_id = m.group(1)
                break
        if not decision_id:
            stem = re.search(r"/(\d+[a-zA-Z]?)(?:_\d+)?\.pdf", pdf_url)
            decision_id = stem.group(1) if stem else pdf_url.rsplit("/", 1)[-1]

        # Case number (best effort) and caption from the remaining cells.
        caseno = ""
        caption = ""
        for c in cells:
            if CASENO_RE.match(c):
                caseno = c
                break
        date_iso = _parse_date(cells)
        for c in cells:
            if not c or c is None:
                continue
            if ID_RE.search(c):
                continue
            if DATE_RE.search(c) and len(c) <= 12:
                continue
            if CASENO_RE.match(c):
                continue
            if c.upper() in ("X", "PDF"):
                continue
            if re.search(r"[A-Za-z]{3,}", c):
                caption = c
                break

        if caption:
            last_caption = caption
            last_caseno = caseno or last_caseno
        else:
            caption = last_caption
            caseno = caseno or last_caseno

        if pdf_url in seen_pdf:
            continue
        seen_pdf.add(pdf_url)

        decisions.append({
            "decision_id": decision_id,
            "caption": caption or f"OCAHO Decision {decision_id}",
            "case_number": caseno,
            "date": date_iso,
            "volume": volume_num,
            "pdf_url": pdf_url,
        })

    return decisions


def get_volume_decisions(url: str, volume_num: int) -> list:
    html = fetch_url(url)
    if not html:
        logger.warning(f"Failed to fetch volume {volume_num} index ({url})")
        return []
    return parse_volume_index(html, volume_num)


def normalize(raw: dict, text: str) -> dict:
    """Normalize an OCAHO decision record."""
    decision_id = raw["decision_id"]
    caption = raw["caption"]
    volume = raw["volume"]

    cite = f"{volume} OCAHO no. {decision_id}"
    title = f"{caption} ({cite})" if caption else cite

    return {
        "_id": f"US/OCAHO/vol{volume}-{decision_id}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": raw.get("date"),
        "url": raw["pdf_url"],
        "caption": caption,
        "case_number": raw.get("case_number") or None,
        "citation": cite,
        "volume": volume,
        "decision_number": decision_id,
        "authority": "OCAHO",
        "jurisdiction": "US",
        "language": "en",
    }


def fetch_all() -> Generator[dict, None, None]:
    """Yield all OCAHO published decisions with full text."""
    volumes = get_volume_index_urls()
    if not volumes:
        logger.error("No volume index URLs discovered")
        return
    total = 0
    # Newest volumes first.
    for volume_num, url in sorted(volumes, key=lambda t: t[0], reverse=True):
        logger.info(f"Processing Volume {volume_num} ({url})...")
        decisions = get_volume_decisions(url, volume_num)
        time.sleep(CRAWL_DELAY)

        if not decisions:
            logger.warning(f"  No decisions found for volume {volume_num}")
            continue
        logger.info(f"  Found {len(decisions)} decisions")

        for raw in decisions:
            pdf_bytes = fetch_url(raw["pdf_url"], binary=True)
            time.sleep(CRAWL_DELAY)
            if not pdf_bytes:
                logger.warning(f"  Failed to download PDF {raw['pdf_url']}")
                continue
            text = extract_text_from_pdf(pdf_bytes)
            if not text or len(text) < 100:
                logger.warning(f"  No text extracted for {raw['pdf_url']}")
                continue
            record = normalize(raw, text)
            total += 1
            if total % 50 == 0:
                logger.info(f"  Progress: {total} decisions fetched")
            yield record

    logger.info(f"Total decisions with full text: {total}")


def fetch_sample(count: int = 15) -> list:
    """Fetch sample records across old and modern volume layouts."""
    records = []
    volumes = {v: u for v, u in get_volume_index_urls()}
    # Exercise both PDF-URL formats: modern (22, 21, 12) and bound (1).
    for vol in [22, 21, 12, 1]:
        if len(records) >= count:
            break
        url = volumes.get(vol)
        if not url:
            continue
        logger.info(f"Sampling Volume {vol}...")
        decisions = get_volume_decisions(url, vol)
        time.sleep(CRAWL_DELAY)
        if not decisions:
            continue
        take = min(5, count - len(records), len(decisions))
        for raw in decisions[:take]:
            pdf_bytes = fetch_url(raw["pdf_url"], binary=True)
            time.sleep(CRAWL_DELAY)
            if not pdf_bytes:
                continue
            text = extract_text_from_pdf(pdf_bytes)
            if not text or len(text) < 100:
                continue
            record = normalize(raw, text)
            records.append(record)
            logger.info(f"  [{len(records)}] {record['title'][:60]} - {len(text)} chars")
    return records


def test_api():
    """Test connectivity to the OCAHO decisions pages."""
    logger.info("Testing OCAHO connectivity...")
    volumes = get_volume_index_urls()
    if not volumes:
        logger.error("No volume index URLs discovered")
        return False
    logger.info(f"Discovered {len(volumes)} volume indexes: "
                f"{[v for v, _ in volumes]}")

    vol, url = volumes[-1]  # newest
    html = fetch_url(url)
    if not html:
        logger.error(f"Volume {vol} index unreachable")
        return False
    decisions = parse_volume_index(html, vol)
    if not decisions:
        logger.error(f"No decisions parsed from volume {vol}")
        return False
    logger.info(f"Parsed {len(decisions)} decisions from volume {vol}")

    time.sleep(CRAWL_DELAY)
    raw = decisions[0]
    pdf_bytes = fetch_url(raw["pdf_url"], binary=True)
    if not pdf_bytes:
        logger.error("PDF download failed")
        return False
    logger.info(f"PDF download OK - {len(pdf_bytes)} bytes")
    text = extract_text_from_pdf(pdf_bytes)
    if not text:
        logger.error("PDF text extraction failed")
        return False
    logger.info(f"Text extraction OK - {len(text)} chars")
    logger.info(f"Preview: {text[:200]}...")
    return True


def bootstrap_sample():
    """Fetch and save sample records."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    records = fetch_sample(count=15)
    if not records:
        logger.error("No records fetched!")
        return False

    for i, record in enumerate(records, 1):
        safe_id = re.sub(r'[^\w\-]', '_', record["_id"])[:80]
        filename = f"sample_{i:02d}_{safe_id}.json"
        with open(SAMPLE_DIR / filename, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    logger.info(f"\nSaved {len(records)} sample records to {SAMPLE_DIR}")
    text_lengths = [len(r.get("text", "")) for r in records]
    avg_text = sum(text_lengths) / len(text_lengths) if text_lengths else 0
    logger.info("Validation:")
    logger.info(f"  - Records with text: {sum(1 for t in text_lengths if t > 0)}/{len(records)}")
    logger.info(f"  - Avg text length: {avg_text:.0f} chars")
    logger.info(f"  - Min text length: {min(text_lengths) if text_lengths else 0}")
    logger.info(f"  - Max text length: {max(text_lengths) if text_lengths else 0}")
    return len(records) >= 10 and avg_text > 500


def bootstrap_full():
    """Full pull -- stream every decision to data/records.jsonl."""
    data_dir = SOURCE_DIR / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    out_path = data_dir / "records.jsonl"
    count = 0
    with open(out_path, "w", encoding="utf-8") as fh:
        for record in fetch_all():
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1
    logger.info(f"Processed {count} records -> {out_path}")
    return count


def main():
    parser = argparse.ArgumentParser(description="US/OCAHO Data Fetcher")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "test-api"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == "test-api":
        success = test_api()
        sys.exit(0 if success else 1)
    else:  # bootstrap / bootstrap-fast
        if args.sample:
            success = bootstrap_sample()
            sys.exit(0 if success else 1)
        logger.info("Full bootstrap mode")
        bootstrap_full()


if __name__ == "__main__":
    main()
