#!/usr/bin/env python3
"""
US/IL-TaxTribunal -- Illinois Independent Tax Tribunal (Issued Decisions/Rulings)

Fetches the full text of every issued decision / ruling of the Illinois
Independent Tax Tribunal, the executive-branch tribunal (created by the
Illinois Independent Tax Tribunal Act of 2012) that adjudicates disputes
between taxpayers and the Illinois Department of Revenue over notices of
tax liability, penalties and refund denials (income, sales/use, excise,
etc.). Each document resolves a specific tax controversy, so the corpus
is case_law.

The Tribunal publishes its final decisions and rulings on its website's
"Decisions/Rulings" page. That page is an Adobe AEM site whose decision
table is loaded client-side from a JSON endpoint:

  /content/soi/taxtribunal/en/decisions-rulings/jcr:content/
      responsivegrid/container/data_table.datatablejson.json

The JSON returns one row per decision with the case number (hyperlinked
to the decision PDF under /content/dam/.../rules-decisions/), the year,
the decision date, the case caption and the document subject. The PDFs
are born-digital text-layer documents (no auth, no CAPTCHA).

Strategy:
  1. GET the datatable JSON and parse each row.
  2. Extract the PDF href + case number from the linked first column.
  3. Download each PDF and extract its text layer via common.pdf_extract.
  4. Normalize into the standard case_law schema.

Usage:
  python bootstrap.py bootstrap            # Full pull (all decisions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample decisions
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import subprocess
import time
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common import pdf_extract

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.IL-TaxTribunal")

BASE_URL = "https://taxtribunal.illinois.gov"
DATA_JSON = (
    "/content/soi/taxtribunal/en/decisions-rulings/jcr:content/"
    "responsivegrid/container/data_table.datatablejson.json"
)
HREF_RE = re.compile(r'href="([^"]+)"', re.I)
TAG_RE = re.compile(r"<[^>]+>")


class ILTaxTribunalScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120 Safari/537.36"
        )

    def _curl_bytes(self, url: str) -> bytes | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "90", "-A", self._ua,
                     "-H", "Accept: */*", url],
                    capture_output=True, timeout=120,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    @staticmethod
    def _clean(cell: str | None) -> str:
        if not cell:
            return ""
        txt = TAG_RE.sub(" ", cell)
        txt = urllib.parse.unquote(txt) if False else txt
        return re.sub(r"\s+", " ", txt).strip()

    @staticmethod
    def _norm_date(raw: str | None) -> str | None:
        if not raw:
            return None
        raw = raw.strip()
        m = re.match(r"(\d{1,2})/(\d{1,2})/(\d{4})", raw)
        if m:
            mo, d, y = (int(x) for x in m.groups())
            if 1 <= mo <= 12 and 1 <= d <= 31 and 2000 <= y <= 2035:
                return f"{y:04d}-{mo:02d}-{d:02d}"
        m = re.match(r"(\d{4})", raw)
        if m:
            return f"{int(m.group(1)):04d}-01-01"
        return None

    @staticmethod
    def _slug(pdf_url: str) -> str:
        stem = urllib.parse.unquote(pdf_url.rsplit("/", 1)[-1])
        stem = re.sub(r"\.pdf$", "", stem, flags=re.I)
        stem = re.sub(r"[^A-Za-z0-9._-]+", "-", stem).strip("-")
        return stem[:180]

    def discover_documents(self, sample: bool = False) -> list[dict]:
        """Return one dict per decision row (case_number, date, case_name,
        subject, pdf_url, slug)."""
        raw = self._curl_bytes(BASE_URL + DATA_JSON)
        if not raw:
            logger.error("Failed to fetch decisions datatable JSON")
            return []
        try:
            rows = json.loads(raw.decode("utf-8", "replace")).get("data", [])
        except Exception as e:
            logger.error(f"JSON parse failed: {e}")
            return []

        out = []
        seen = set()
        for row in rows:
            if not isinstance(row, list) or not row:
                continue
            col0 = row[0] or ""
            m = HREF_RE.search(col0)
            if not m:
                continue
            href = m.group(1)
            if not href.lower().endswith(".pdf"):
                continue
            pdf_url = urllib.parse.urljoin(BASE_URL, href.strip())
            if pdf_url in seen:
                continue
            seen.add(pdf_url)
            case_number = self._clean(col0)
            year = self._clean(row[1]) if len(row) > 1 else ""
            date = self._clean(row[2]) if len(row) > 2 else ""
            case_name = self._clean(row[3]) if len(row) > 3 else ""
            subject = self._clean(row[4]) if len(row) > 4 else ""
            out.append({
                "case_number": case_number,
                "year": year,
                "date": self._norm_date(date) or self._norm_date(year),
                "case_name": case_name,
                "subject": subject,
                "pdf_url": pdf_url,
                "slug": self._slug(pdf_url),
            })
        # Newest first by date.
        out.sort(key=lambda r: r.get("date") or "0000", reverse=True)
        logger.info(f"Discovered {len(out)} Tax Tribunal decisions")
        return out

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._curl_bytes(doc["pdf_url"])
        if not pdf_bytes or pdf_bytes[:4] != b"%PDF":
            logger.warning(f"PDF download failed or not a PDF: {doc['pdf_url']}")
            return None
        text = pdf_extract.extract_pdf_markdown(
            "US/IL-TaxTribunal", doc["slug"], pdf_bytes=pdf_bytes,
            table="case_law", force=True,
        )
        if not text or len(text.strip()) < 150:
            logger.warning(f"No usable text for {doc['pdf_url']} "
                           f"({len(text) if text else 0} chars)")
            return None
        doc = dict(doc)
        doc["text"] = text.strip()
        return doc

    def test_api(self) -> bool:
        logger.info("Testing Illinois Independent Tax Tribunal datatable + PDF...")
        try:
            docs = self.discover_documents()
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} decisions")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) > 150:
                logger.info(f"  PDF text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('case_number')} {raw.get('case_name')}")
            else:
                logger.error("  PDF text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard case_law schema."""
        case_name = (raw.get("case_name") or "").strip()
        case_number = (raw.get("case_number") or "").strip()
        subject = (raw.get("subject") or "").strip()
        if case_name:
            title = case_name
        else:
            title = f"Illinois Independent Tax Tribunal {subject or 'Decision'}"
        if case_number and case_number not in title:
            title = f"{title} ({case_number})"
        title = title[:300]
        return {
            "_id": f"US/IL-TaxTribunal/{raw['slug']}",
            "_source": "US/IL-TaxTribunal",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "case_number": case_number or None,
            "document_type": subject or None,
            "court": "Illinois Independent Tax Tribunal",
            "case_name": case_name or None,
            "title": title,
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for doc in self.discover_documents(sample=sample):
            raw = self._build_raw(doc)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 12:
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

    parser = argparse.ArgumentParser(description="US/IL-TaxTribunal bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = ILTaxTribunalScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    gen = scraper.fetch_sample() if args.sample else scraper.fetch_all()

    count = 0
    for raw in gen:
        record = scraper.normalize(raw)
        safe_id = record["_id"].replace("/", "_")
        out_path = sample_dir / f"{safe_id}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        logger.info(f"Saved: {record['_id']} ({len(record['text'])} chars)")

    logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")


if __name__ == "__main__":
    main()
