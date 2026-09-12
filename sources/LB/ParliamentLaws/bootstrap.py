#!/usr/bin/env python3
"""
LB/ParliamentLaws — Lebanese Parliament Approved Laws

Fetches legislation approved by the Lebanese Parliament from lp.gov.lb.

Strategy:
  - GetLawsByYear (paginated) → list of year values (1994–2026)
  - GetLawsBySection (per year, paginated) → list of session/section names
  - GetLaws (per section, paginated) → law metadata (Id, Title, Year, PublishDate)
  - GetLawFile (per law ID) → base64-encoded PDF
  - Extract full text from PDF using PyMuPDF (fitz)

All endpoints are ASP.NET ASMX web services accepting form-encoded POST.

Data:
  - ~500+ laws spanning 1994–2026 across 27 legislative years
  - Full text in Arabic (PDF)
  - License: Public Domain (Government Works)

Usage:
  python3 bootstrap.py bootstrap          # Full initial pull
  python3 bootstrap.py bootstrap --sample # Fetch 15 sample records
"""

import base64
import json
import logging
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://www.lp.gov.lb"
WEBSERVICE = BASE_URL + "/Webservice.asmx"
DELAY = 1.5
HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
}

try:
    import fitz  # PyMuPDF
except ImportError:
    fitz = None
    logger.warning("PyMuPDF (fitz) not available — PDF extraction disabled")


def _post(endpoint: str, data: Dict[str, str], timeout: int = 30) -> Optional[str]:
    """POST to an ASMX endpoint with form-encoded data, return response text."""
    url = f"{WEBSERVICE}/{endpoint}"
    encoded = urllib.parse.urlencode(data).encode("utf-8")
    req = urllib.request.Request(url, data=encoded, headers=HEADERS, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except Exception as e:
        logger.warning(f"POST {endpoint} failed: {e}")
        return None


def get_all_years() -> List[str]:
    """Retrieve all available legislative years."""
    years = []
    page = 1
    while True:
        raw = _post("GetLawsByYear", {"pageNumber": str(page)})
        if not raw:
            break
        try:
            batch = json.loads(raw)
        except json.JSONDecodeError:
            break
        if not batch:
            break
        years.extend(batch)
        if len(batch) < 10:
            break
        page += 1
        time.sleep(DELAY)
    return years


def get_sections_for_year(year: str) -> List[str]:
    """Get all session/section names for a given year."""
    sections = []
    # Get total count
    raw_count = _post("GetLawSectionNumber", {"Year": year})
    if not raw_count:
        return sections
    try:
        total = int(raw_count.strip())
    except (ValueError, TypeError):
        return sections

    if total == 0:
        return sections

    page = 1
    while True:
        raw = _post("GetLawsBySection", {"pageNumber": str(page), "Year": year})
        if not raw:
            break
        try:
            batch = json.loads(raw)
        except json.JSONDecodeError:
            break
        if not batch:
            break
        sections.extend(batch)
        if len(sections) >= total:
            break
        page += 1
        time.sleep(DELAY)
    return sections


def get_laws_for_section(section: str) -> List[Dict[str, Any]]:
    """Get all law metadata records for a given section name."""
    laws = []
    # Get count
    raw_count = _post("GetLawNumber", {"Section": section})
    if not raw_count:
        return laws
    try:
        total = int(raw_count.strip())
    except (ValueError, TypeError):
        return laws

    if total == 0:
        return laws

    page = 1
    while True:
        raw = _post("GetLaws", {"pageNumber": str(page), "Section": section})
        if not raw:
            break
        try:
            batch = json.loads(raw)
        except json.JSONDecodeError:
            break
        if not batch:
            break
        laws.extend(batch)
        if len(laws) >= total:
            break
        page += 1
        time.sleep(DELAY)
    return laws


def get_law_pdf_text(law_id: int) -> Optional[str]:
    """Download PDF for a law and extract text using PyMuPDF."""
    if fitz is None:
        logger.error("PyMuPDF not available, cannot extract PDF text")
        return None

    raw = _post("GetLawFile", {"ID": str(law_id)}, timeout=120)
    if not raw:
        return None

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        logger.warning(f"GetLawFile({law_id}): invalid JSON response")
        return None

    b64 = data.get("base64", "")
    if not b64:
        logger.warning(f"GetLawFile({law_id}): empty base64 field")
        return None

    # Strip data URI prefix if present
    if b64.startswith("data:"):
        b64 = b64.split(",", 1)[1]

    try:
        pdf_bytes = base64.b64decode(b64)
    except Exception as e:
        logger.warning(f"GetLawFile({law_id}): base64 decode failed: {e}")
        return None

    if len(pdf_bytes) < 100:
        logger.warning(f"GetLawFile({law_id}): PDF too small ({len(pdf_bytes)} bytes)")
        return None

    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        text_parts = []
        for page in doc:
            page_text = page.get_text()
            if page_text:
                text_parts.append(page_text.strip())
        doc.close()
        full_text = "\n\n".join(text_parts)
        if len(full_text) < 50:
            logger.warning(f"GetLawFile({law_id}): very little text extracted ({len(full_text)} chars)")
            return None
        return full_text
    except Exception as e:
        logger.warning(f"GetLawFile({law_id}): PDF extraction error: {e}")
        return None


def normalize(law: Dict[str, Any], text: str) -> Dict[str, Any]:
    """Transform raw law data into normalized record."""
    law_id = law.get("Id", 0)
    title = (law.get("Title") or "").strip()
    year = law.get("Year", "")
    section = law.get("Section", "")
    publish_date = law.get("PublishDate", "")

    # Parse date from "Mon DD, YYYY" to ISO
    iso_date = None
    if publish_date:
        try:
            dt = datetime.strptime(publish_date, "%b %d, %Y")
            iso_date = dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            pass

    return {
        "_id": f"LB/ParliamentLaws/{law_id}",
        "_source": "LB/ParliamentLaws",
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": iso_date,
        "url": f"{BASE_URL}/ViewLaws.aspx?Section={urllib.parse.quote(section)}",
        "year": year,
        "section": section,
        "law_id": law_id,
    }


def fetch_all(limit: int = 0) -> Iterator[Dict[str, Any]]:
    """Yield all normalized law records with full text."""
    years = get_all_years()
    logger.info(f"Found {len(years)} legislative years: {years[:5]}...{years[-3:]}")

    yielded = 0
    for year in years:
        sections = get_sections_for_year(year)
        logger.info(f"Year {year}: {len(sections)} sections")
        time.sleep(DELAY)

        for section in sections:
            laws = get_laws_for_section(section)
            logger.info(f"  Section '{section[:50]}': {len(laws)} laws")
            time.sleep(DELAY)

            for law in laws:
                law_id = law.get("Id", 0)
                title = (law.get("Title") or "")[:60]
                logger.info(f"    Fetching law {law_id}: {title}...")

                text = get_law_pdf_text(law_id)
                if not text:
                    logger.warning(f"    Skipped law {law_id}: no text extracted")
                    continue

                record = normalize(law, text)
                yield record
                yielded += 1

                if limit and yielded >= limit:
                    logger.info(f"Reached limit of {limit} records")
                    return

                time.sleep(DELAY)

    logger.info(f"Fetch complete: {yielded} laws with full text")


def bootstrap_sample(sample_dir: Path, count: int = 15) -> int:
    """Fetch sample records and save to sample directory."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    saved = 0
    for doc in fetch_all(limit=count):
        text_len = len(doc.get("text", ""))
        logger.info(f"  [{saved+1}/{count}] {doc.get('title', 'N/A')[:70]}")
        logger.info(f"    Text: {text_len} chars, Date: {doc.get('date')}")

        out_file = sample_dir / f"record_{saved:04d}.json"
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)

        saved += 1
        if saved >= count:
            break

    # Save combined file
    if saved > 0:
        all_records = []
        for i in range(saved):
            fp = sample_dir / f"record_{i:04d}.json"
            with open(fp, "r", encoding="utf-8") as f:
                all_records.append(json.load(f))
        combined = sample_dir / "all_samples.json"
        with open(combined, "w", encoding="utf-8") as f:
            json.dump(all_records, f, ensure_ascii=False, indent=2)

    logger.info(f"Bootstrap complete: {saved} documents saved to {sample_dir}")
    return saved


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    source_dir = Path(__file__).parent
    sample_dir = source_dir / "sample"

    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap":
        sample_flag = "--sample" in sys.argv
        count = 15 if sample_flag else 200
        saved = bootstrap_sample(sample_dir, count)
        if saved < 10:
            logger.error(f"Only {saved} documents saved, expected at least 10")
            sys.exit(1)
    else:
        print("Usage: python3 bootstrap.py bootstrap [--sample]")
        print("  bootstrap --sample  Fetch 15 sample documents")
        print("  bootstrap           Fetch up to 200 documents")
