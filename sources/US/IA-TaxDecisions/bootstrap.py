#!/usr/bin/env python3
"""
US/IA-TaxDecisions -- Iowa Department of Revenue, Administrative Orders &
Declaratory Orders (Iowa Revenue Research Library)

Fetches the full text of the Iowa Department of Revenue's published
administrative documents: Declaratory Orders (binding rulings on a
petitioner's specific facts, Iowa Code 17A.9), contested-case Orders
(final adjudications of a taxpayer protest, case_law), Director Orders
(e.g. annual rate certifications, doctrine), and petition-for-rulemaking
rulings. All are public, born-digital PDFs.

Access (no JavaScript needed, no CAPTCHA, no auth):
  The documents are served by the state's "DocsIowaGov" portal
  (documents.iowa.gov), an OpenText-backed document library shared by six
  agencies. The Revenue subset (organization "IDR", OpenText category
  10350462) is enumerated via the portal's server-side DataTables search
  endpoint POST /home/search, filtered by the criterion
  `(OTDCategory:10350462)`. Each result row carries: id (the document id),
  name (the caption, e.g. "VERTEX INC (DO) 2026"), mime_type, create_date
  (M/D/YYYY), size, and a short_summary. ~1,400 Revenue documents.

Strategy:
  1. POST /home/search paginated (start, length=100) with the Revenue
     OTDCategory criterion; collect every row's id + metadata.
  2. For each row, GET /home/download/{id} -> raw PDF bytes.
  3. Extract text via the shared, OOM-hardened common.pdf_extract helper
     (born-digital, no OCR). date = create_date parsed to ISO.
  4. Classify _type: Director Orders / rulemaking petitions / policy ->
     doctrine; declaratory & contested-case orders -> case_law.

  reCAPTCHA on the portal gates only the ADA-remediation request form; the
  search and download endpoints are open.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.IA-TaxDecisions")

BASE = "https://documents.iowa.gov"
SEARCH_URL = f"{BASE}/home/search"
DOWNLOAD_URL = f"{BASE}/home/download/{{id}}"
# OpenText category id for the Revenue (IDR) organization in DocsIowaGov.
REVENUE_CATEGORY = "10350462"
PAGE_LEN = 100
MIN_TEXT_CHARS = 200

DATE_RE = re.compile(r"(\d{1,2})/(\d{1,2})/(\d{4})")

# Name suffix / keyword codes that indicate interpretive guidance (doctrine)
# rather than an adjudication of a specific contested matter (case_law).
DOCTRINE_HINTS = (
    "DIRECTOR ORDER",
    "DIRECTOR'S ORDER",
    "(PRM)",          # Petition for RuleMaking
    "RULEMAKING",
    "POLICY",
    "GUIDANCE",
    "CERTIFY",
    "CERTIFICATION",
    "CERTIFYING",
)


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _iso_date(value: str) -> str | None:
    m = DATE_RE.search(value or "")
    if not m:
        return None
    mo, d, y = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if 1980 <= y <= 2100 and 1 <= mo <= 12 and 1 <= d <= 31:
        return f"{y}-{mo:02d}-{d:02d}"
    return None


def _classify(name: str) -> str:
    upper = (name or "").upper()
    for hint in DOCTRINE_HINTS:
        if hint in upper:
            return "doctrine"
    return "case_law"


class IATaxDecisionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Accept-Language": "en-US,en;q=0.9",
                "X-Requested-With": "XMLHttpRequest",
                "Origin": BASE,
                "Referer": f"{BASE}/",
            },
            timeout=90,
        )
        self.delay = 1.0

    # ---- fetch helpers -------------------------------------------------

    def _search_page(self, start: int, length: int, retries: int = 4) -> dict | None:
        data = {
            "draw": "1",
            "start": str(start),
            "length": str(length),
            "categoryId": REVENUE_CATEGORY,
            "criteria": f'["OTDCategory:{REVENUE_CATEGORY}"]',
            "query": f"(OTDCategory:{REVENUE_CATEGORY})",
        }
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.post(SEARCH_URL, data=data)
                if resp.status_code == 200:
                    payload = resp.json()
                    # OpenText backend occasionally returns {"result": false}
                    if isinstance(payload, dict) and payload.get("result") is False:
                        logger.warning(
                            f"Search backend error at start={start}: "
                            f"{payload.get('data')}"
                        )
                    else:
                        return payload
                else:
                    logger.warning(f"HTTP {resp.status_code} for search start={start}")
            except Exception as e:
                logger.warning(
                    f"Error searching start={start} (attempt {attempt + 1}): {e}"
                )
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    def _get_pdf(self, doc_id: int, retries: int = 3) -> bytes | None:
        url = DOWNLOAD_URL.format(id=doc_id)
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200 and resp.content:
                    if resp.content[:5] == b"%PDF-":
                        return resp.content
                    ctype = resp.headers.get("Content-Type", "").lower()
                    logger.warning(f"Non-PDF response for doc {doc_id} ({ctype})")
                    return None
                logger.warning(f"HTTP {resp.status_code} for doc {doc_id}")
            except Exception as e:
                logger.warning(
                    f"Error fetching doc {doc_id} (attempt {attempt + 1}): {e}"
                )
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    # ---- discovery -----------------------------------------------------

    def discover_documents(self) -> Generator[dict, None, None]:
        first = self._search_page(0, PAGE_LEN)
        if not first:
            logger.error("Failed to fetch first Revenue search page")
            return
        total = int(first.get("recordsTotal") or 0)
        logger.info(f"Iowa DoR Revenue library reports {total} documents")
        seen: set[int] = set()
        start = 0
        page = first
        emitted = 0
        while True:
            rows = page.get("data") or []
            if not rows:
                break
            for row in rows:
                doc_id = row.get("id")
                if doc_id is None or doc_id in seen:
                    continue
                seen.add(doc_id)
                name = (row.get("name") or "").strip()
                # normalize a slug from the id (stable, filename-safe)
                yield {
                    "id": doc_id,
                    "name": name or str(doc_id),
                    "mime_type": row.get("mime_type"),
                    "date": _iso_date(row.get("create_date") or ""),
                    "summary": (row.get("short_summary") or "").strip() or None,
                    "url": DOWNLOAD_URL.format(id=doc_id),
                }
                emitted += 1
            start += PAGE_LEN
            if start >= total:
                break
            page = self._search_page(start, PAGE_LEN)
            if not page:
                logger.warning(f"Stopping discovery early at start={start}")
                break
        logger.info(f"Discovered {emitted} Iowa DoR documents")

    # ---- build ---------------------------------------------------------

    def _build_raw(self, doc: dict) -> dict | None:
        mime = (doc.get("mime_type") or "").lower()
        # Only PDFs carry extractable born-digital text via pdf_extract; the
        # rare .docx/.xlsx entries are skipped.
        if mime and "pdf" not in mime:
            logger.info(f"Skipping non-PDF doc {doc['id']} ({mime})")
            return None
        pdf_bytes = self._get_pdf(doc["id"])
        if not pdf_bytes:
            return None
        _type = _classify(doc["name"])
        text = extract_pdf_markdown(
            "US/IA-TaxDecisions",
            str(doc["id"]),
            pdf_bytes=pdf_bytes,
            table=_type,
            force=True,
        )
        text = clean_text(text or "")
        if len(text) < MIN_TEXT_CHARS:
            logger.warning(
                f"Insufficient text ({len(text)} chars), likely scanned: "
                f"{doc['id']} {doc['name']}"
            )
            return None
        doc = dict(doc)
        doc["text"] = text
        doc["_type"] = _type
        return doc

    # ---- public interface ----------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing Iowa DoR documents portal...")
        try:
            docs = []
            for d in self.discover_documents():
                docs.append(d)
                if len(docs) >= 5:
                    break
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)}+ documents (partial crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) >= MIN_TEXT_CHARS:
                logger.info(
                    f"  Text extraction OK ({len(raw['text'])} chars) — "
                    f"{raw['name']} [{raw['_type']}]"
                )
            else:
                logger.error("  Text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        name = (raw.get("name") or "").strip()
        # Strip a trailing file extension from the caption for a clean title.
        cite = re.sub(r"\.(pdf|docx?|xlsx?)$", "", name, flags=re.I).strip()
        title = f"Iowa Department of Revenue — {cite}"[:300]
        return {
            "_id": f"US/IA-TaxDecisions/{raw['id']}",
            "_source": "US/IA-TaxDecisions",
            "_type": raw.get("_type") or "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "doc_id": str(raw["id"]),
            "cite": cite or None,
            "issuer": "Iowa Department of Revenue",
            "summary": raw.get("summary"),
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-IA",
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        examined = 0
        for doc in self.discover_documents():
            examined += 1
            raw = self._build_raw(doc)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return
            if sample and examined >= 40:
                return

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/IA-TaxDecisions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = IATaxDecisionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
