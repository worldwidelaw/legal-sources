#!/usr/bin/env python3
"""
US/OK-TaxDecisions -- Oklahoma Tax Commission, Commission Decisions

Fetches the full text of the Oklahoma Tax Commission's Commission Decisions --
final determinations by the Tax Commissioners in an adversarial hearing on a
taxpayer's tax protest or claim (case_law). Each decision is a public,
taxpayer-identity-redacted, born-digital PDF captioned e.g. "PRECEDENTIAL
DECISION OKLAHOMA TAX COMMISSION" with a structured header (JURISDICTION,
CITE, ID, DATE, DISPOSITION, TAX TYPE, APPEAL) followed by the ORDER body.

Decisions are classified Precedential (relied upon prospectively by the
Commission and the public) or Non-precedential; both are published.

Access (no JavaScript needed, no CAPTCHA, no auth):
  The Commission Decisions page (oklahoma.gov, an AEM site) is backed by a
  master CSV index whose URL is exposed in the page's `data-csv-table-api`
  attribute. Each CSV row carries: TITLE (the cite/date, YYYY-MM-DD-NN),
  CATEGORY (tax type), PRECEDENTIAL ("Precedential" or blank), DOWNLOAD (an
  Excel =DOWNLOAD("/content/dam/.../<file>.pdf") formula wrapping the decision
  PDF's absolute path), and Keyword. ~1,800 decisions.

Strategy:
  1. Fetch the hub page; read the CSV URL from `data-csv-table-api` (fall back
     to the last-known CSV path if absent).
  2. Parse the CSV; for each row, unwrap the PDF path from the DOWNLOAD
     formula and resolve it against oklahoma.gov.
  3. Download each PDF and extract its text via the shared, OOM-hardened
     common.pdf_extract helper (born-digital, no OCR). date = TITLE's leading
     YYYY-MM-DD.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import csv
import io
import json
import logging
import re
import html as _htmllib
import time
import urllib.parse
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
logger = logging.getLogger("legal-data-hunter.US.OK-TaxDecisions")

BASE = "https://oklahoma.gov"
HUB_URL = ("https://oklahoma.gov/tax/reporting-resources/rules-policies/"
           "commission-decisions.html")
# Last-known master CSV path, used only if the hub attribute can't be read.
FALLBACK_CSV = ("/content/dam/ok/en/tax/documents/resources/rules-and-policies/"
                "commission-decisions/commission-decisions-9-30-25.csv")

MIN_TEXT_CHARS = 200

CSV_API_RE = re.compile(r'data-csv-table-api="([^"]+\.csv)"', re.I)
DOWNLOAD_RE = re.compile(r'DOWNLOAD\("([^"]+\.pdf)"\)', re.I)
DATE_RE = re.compile(r"(\d{4})-(\d{2})-(\d{2})")


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _iso_date(title: str) -> str | None:
    m = DATE_RE.search(title or "")
    if not m:
        return None
    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    if 1980 <= y <= 2100 and 1 <= mo <= 12 and 1 <= d <= 31:
        return f"{y}-{mo:02d}-{d:02d}"
    return None


class OKTaxDecisionsScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=90,
        )
        self.delay = 1.0

    # ---- fetch helpers -------------------------------------------------

    def _get(self, url: str, retries: int = 4) -> str:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200:
                    return resp.text
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return ""

    def _get_bytes(self, url: str, retries: int = 3) -> bytes | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200 and resp.content:
                    ctype = resp.headers.get("Content-Type", "").lower()
                    if "pdf" in ctype or resp.content[:5] == b"%PDF-":
                        return resp.content
                    logger.warning(f"Non-PDF response for {url} ({ctype})")
                    return None
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching PDF {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    # ---- discovery -----------------------------------------------------

    def _csv_url(self) -> str:
        html = self._get(HUB_URL)
        m = CSV_API_RE.search(html) if html else None
        path = _htmllib.unescape(m.group(1)) if m else FALLBACK_CSV
        return urllib.parse.urljoin(BASE, path)

    def discover_documents(self) -> Generator[dict, None, None]:
        csv_url = self._csv_url()
        raw = self._get(csv_url)
        if not raw:
            logger.error(f"Failed to fetch decisions CSV: {csv_url}")
            return
        # The CSV is UTF-8 with a BOM; strip it so the "TITLE" header parses.
        reader = csv.DictReader(io.StringIO(raw.lstrip("﻿")))
        seen: set[str] = set()
        total = 0
        for row in reader:
            title = (row.get("TITLE") or "").strip()
            download = row.get("DOWNLOAD") or ""
            dm = DOWNLOAD_RE.search(download)
            if not dm:
                continue
            pdf_path = _htmllib.unescape(dm.group(1)).strip()
            pdf_url = urllib.parse.urljoin(BASE, pdf_path)
            slug = re.sub(r"[^A-Za-z0-9._-]+", "-",
                          pdf_path.rsplit("/", 1)[-1].rsplit(".", 1)[0]).strip("-")[:80]
            if not slug or slug in seen:
                continue
            seen.add(slug)
            total += 1
            yield {
                "slug": slug,
                "title": title or slug,
                "url": pdf_url,
                "category": (row.get("CATEGORY") or "").strip() or None,
                "precedential": (row.get("PRECEDENTIAL") or "").strip().lower()
                == "precedential",
                "date": _iso_date(title) or _iso_date(slug),
            }
        logger.info(f"Discovered {total} Oklahoma Tax Commission decisions")

    # ---- build ---------------------------------------------------------

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._get_bytes(doc["url"])
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/OK-TaxDecisions",
            doc["slug"],
            pdf_bytes=pdf_bytes,
            table="case_law",
            force=True,
        )
        text = clean_text(text or "")
        if len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text ({len(text)} chars), likely "
                           f"scanned: {doc['slug']}")
            return None
        doc = dict(doc)
        doc["text"] = text
        return doc

    # ---- public interface ----------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing Oklahoma Tax Commission decisions...")
        try:
            docs = []
            for d in self.discover_documents():
                docs.append(d)
                if len(docs) >= 5:
                    break
            if not docs:
                logger.error("  No decisions discovered")
                return False
            logger.info(f"  Discovered {len(docs)}+ decisions (partial crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) >= MIN_TEXT_CHARS:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw['title']}")
            else:
                logger.error("  Text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        cite = (raw.get("title") or "").strip()
        cat = raw.get("category")
        prec = "Precedential" if raw.get("precedential") else "Non-precedential"
        parts = ["Oklahoma Tax Commission Decision", cite]
        if cat:
            parts.append(f"({cat})")
        title = " ".join(parts)[:300]
        return {
            "_id": f"US/OK-TaxDecisions/{raw['slug']}",
            "_source": "US/OK-TaxDecisions",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "cite": cite or None,
            "tax_type": cat,
            "precedential": bool(raw.get("precedential")),
            "precedential_label": prec,
            "issuer": "Oklahoma Tax Commission",
            "title": title,
            "text": raw["text"],
            "url": raw["url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-OK",
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

    parser = argparse.ArgumentParser(description="US/OK-TaxDecisions bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = OKTaxDecisionsScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
