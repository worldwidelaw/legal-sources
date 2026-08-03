#!/usr/bin/env python3
"""
US/CA-PERB-FactFinding -- California PERB Fact-Finding Reports

Fetches the full text of every published fact-finding report of the
California Public Employment Relations Board (PERB). Under California's
public-sector labor-relations statutes (the MMBA, EERA, HEERA, etc.), when
a public employer and a union reach impasse in bargaining, a tripartite
fact-finding panel chaired by a neutral factfinder holds hearings, makes
written findings of fact on the disputed issues, and issues a report with
recommended terms of settlement for that specific dispute. Each report
resolves a specific contested impasse between named parties = case_law.
These are official California state-government works in the public domain
(government edicts).

Access (no CAPTCHA, no auth):
  perb.ca.gov is a WordPress site. Fact-finding reports are a custom post
  type `fact-finder-report` enumerable via the public WP REST API:

      /wp-json/wp/v2/fact-finder-report?per_page=100&page={N}   (~914 posts)

  Each post's attached born-digital PDF (the actual report body) is its
  single child media item, obtained via:

      /wp-json/wp/v2/media?parent={postId}

  yielding a source_url like /wp-content/uploads/YYYY/MM/FR{NNNN}.pdf where
  FR{NNNN} is the fact-finding report number. Text via common.pdf_extract.

Strategy:
  1. Enumerate every `fact-finder-report` post (title = parties, post date).
  2. For each post, look up its child media attachment (the PDF URL).
  3. Download the PDF, extract text via common.pdf_extract, and normalize
     into the case_law schema (title = parties, date = post date, number =
     FR#### from the filename).

Usage:
  python bootstrap.py bootstrap            # Full pull (~914 reports)
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
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
logger = logging.getLogger("legal-data-hunter.US.CA-PERB-FactFinding")

BASE_URL = "https://perb.ca.gov"
POST_TEMPLATE = (
    BASE_URL + "/wp-json/wp/v2/fact-finder-report"
    "?per_page=100&page={page}&_fields=id,slug,date,link,title"
)
MEDIA_PARENT_TEMPLATE = (
    BASE_URL + "/wp-json/wp/v2/media?parent={pid}&_fields=source_url,mime_type"
)

TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")
FR_RE = re.compile(r"(FR\d+)", re.I)
DATE_RE = re.compile(
    r"(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{1,2}),\s+(\d{4})",
    re.I,
)
MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}


class CAPERBFactFindingScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"
        )

    # ---------------------------------------------------------------- http
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

    def _curl_json(self, url: str):
        b = self._curl_bytes(url)
        if not b:
            return None
        try:
            return json.loads(b.decode("utf-8", "replace"))
        except Exception:
            return None

    # ------------------------------------------------------------- helpers
    @classmethod
    def _clean_html(cls, html: str) -> str:
        import html as _html
        txt = TAG_RE.sub(" ", html or "")
        txt = _html.unescape(txt)
        return WS_RE.sub(" ", txt).strip()

    @staticmethod
    def _report_number(url: str) -> str | None:
        filename = url.rsplit("/", 1)[-1]
        m = FR_RE.search(filename)
        return m.group(1).upper() if m else None

    @staticmethod
    def _doc_id(url: str, post_id) -> str:
        filename = url.rsplit("/", 1)[-1]
        stem = re.sub(r"\.pdf$", "", filename, flags=re.I)
        stem = re.sub(r"[^A-Za-z0-9_-]", "_", stem)
        return stem or f"post-{post_id}"

    # ---------------------------------------------------------- attachment
    def _pdf_url(self, post_id) -> str | None:
        data = self._curl_json(MEDIA_PARENT_TEMPLATE.format(pid=post_id))
        if not isinstance(data, list):
            return None
        for a in data:
            url = a.get("source_url") or ""
            if url.lower().endswith(".pdf"):
                return url
        return None

    # --------------------------------------------------------- discovery
    def discover_documents(self, sample: bool = False) -> Generator[dict, None, None]:
        total = 0
        page = 1
        while True:
            data = self._curl_json(POST_TEMPLATE.format(page=page))
            if not isinstance(data, list) or not data:
                break
            for post in data:
                pid = post.get("id")
                title = self._clean_html((post.get("title") or {}).get("rendered", ""))
                pdf_url = self._pdf_url(pid)
                if not pdf_url:
                    logger.info(f"  post {pid} ({title[:40]}...) has no PDF attachment — skip")
                    continue
                total += 1
                yield {
                    "post_id": pid,
                    "title": title or f"PERB Fact-Finding Report ({post.get('slug')})",
                    "post_date": (post.get("date") or "")[:10] or None,
                    "link": post.get("link"),
                    "doc_url": pdf_url,
                    "number": self._report_number(pdf_url),
                }
                if sample and total >= 16:
                    return
            logger.info(f"  post page {page}: total {total} reports with PDFs")
            if len(data) < 100:  # last page
                break
            page += 1
            if page > 40:  # safety (~4,000 posts)
                logger.warning("Reached post page safety cap (40)")
                break
        logger.info(f"Discovered {total} CA PERB fact-finding report PDFs")

    # ------------------------------------------------------- build record
    def _build_raw(self, doc: dict) -> dict | None:
        blob = self._curl_bytes(doc["doc_url"])
        if not blob:
            logger.warning(f"Download failed: {doc['doc_url']}")
            return None
        if blob[:4] != b"%PDF":
            logger.warning(f"Not a PDF ({blob[:8]!r}): {doc['doc_url']}")
            return None
        doc_id = self._doc_id(doc["doc_url"], doc["post_id"])
        text = pdf_extract.extract_pdf_markdown(
            "US/CA-PERB-FactFinding", doc_id, pdf_bytes=blob,
            table="case_law", force=True,
        )
        if not text or len(text.strip()) < 200:
            logger.warning(f"No usable text for {doc['doc_url']} "
                           f"({len(text) if text else 0} chars)")
            return None
        doc = dict(doc)
        doc["doc_id"] = doc_id
        doc["text"] = text.strip()
        doc["date"] = doc.get("post_date") or self._body_date(doc["text"])
        return doc

    @staticmethod
    def _body_date(text: str) -> str | None:
        best = None
        for m in DATE_RE.finditer(text):
            mo = MONTHS.get(m.group(1).lower())
            d, y = int(m.group(2)), int(m.group(3))
            if mo and 1 <= d <= 31 and 1975 <= y <= 2035:
                best = f"{y:04d}-{mo:02d}-{d:02d}"
        return best

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing CA PERB fact-finding enumeration + PDF extraction...")
        try:
            docs = list(self.discover_documents(sample=True))
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} PDFs (sample crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) > 200:
                logger.info(f"  PDF text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('number')} [{raw.get('date')}] {raw.get('title')[:50]}")
            else:
                logger.error("  PDF text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    # --------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        num = raw.get("number")
        parties = raw.get("title") or "PERB Fact-Finding Report"
        title = f"PERB Fact-Finding Report {num}: {parties}" if num else \
                f"PERB Fact-Finding Report: {parties}"
        return {
            "_id": f"US/CA-PERB-FactFinding/{raw['doc_id']}",
            "_source": "US/CA-PERB-FactFinding",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "doc_id": raw["doc_id"],
            "report_number": num,
            "parties": parties,
            "issuer": "California Public Employment Relations Board (PERB) — Fact-Finding Panel",
            "title": title,
            "text": raw["text"],
            "url": raw.get("link") or raw["doc_url"],
            "pdf_url": raw["doc_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-CA",
        }

    # ------------------------------------------------------------- fetch
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

    parser = argparse.ArgumentParser(description="US/CA-PERB-FactFinding bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CAPERBFactFindingScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
