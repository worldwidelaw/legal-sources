#!/usr/bin/env python3
"""
US/CA-PERB -- California Public Employment Relations Board (PERB) Decisions

Fetches the full text of every published Board Decision of the California
Public Employment Relations Board (PERB). PERB is California's quasi-judicial
administrative agency that adjudicates unfair-practice charges, representation
disputes and related matters under the state's public-sector labor-relations
statutes (EERA, HEERA, the Dills Act, the MMBA, the TEERA, the Trial Court
Act, the Court Interpreter Act, etc.). Each Board Decision resolves a specific
contested case = case_law. These decisions are official California
state-government works in the public domain (government edicts).

Access (no CAPTCHA, no auth):
  perb.ca.gov is a WordPress site. Every Board decision is a born-digital,
  text-layer PDF stored in the WordPress media library under
  /wp-content/uploads/decisionbank/ . The full media set is enumerable via
  the public WP REST API:

      /wp-json/wp/v2/media?search=decisionbank&per_page=100&page={N}

  which returns ~3,900 PDFs (100% under /decisionbank/). Filenames are
  decision-<NUM>.pdf (e.g. decision-2995e.pdf, decision-2422H.pdf) plus a
  handful of order-<num>.pdf / <NUM>.pdf "J"-series items. <NUM> is the PERB
  Decision number followed by a one-letter sector suffix: E = EERA
  (K-12/community-college), H = HEERA (higher education), M = MMBA (local
  government), S = Dills Act (state employees), C = court employees, etc.

  Rich per-decision metadata (official issue date, a Description/Disposition
  summary, canonical decision-page URL) lives in the WP `decision` custom
  post type (~4,005 posts) at /wp-json/wp/v2/decision , keyed by a slug
  equal to <NUM> lower-cased. We build that map once and join it to each PDF.

Strategy:
  1. Enumerate every /decisionbank/*.pdf via the media REST API (page N).
  2. Build a slug->metadata map from the `decision` post type (issue date,
     summary, canonical link).
  3. For each PDF: download, extract text via common.pdf_extract, parse the
     decision number from the filename, prefer the official issue date from
     the decision post (fall back to the last date in the body), and
     normalize into the case_law schema.

Usage:
  python bootstrap.py bootstrap            # Full pull (~3,900 decisions)
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
logger = logging.getLogger("legal-data-hunter.US.CA-PERB")

BASE_URL = "https://perb.ca.gov"
MEDIA_TEMPLATE = (
    BASE_URL + "/wp-json/wp/v2/media?search=decisionbank"
    "&per_page=100&page={page}&_fields=source_url,title"
)
DECISION_TEMPLATE = (
    BASE_URL + "/wp-json/wp/v2/decision?per_page=100&page={page}"
    "&_fields=slug,date,link,content"
)

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
TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")
SECTORS = {
    "E": "EERA (public schools / community colleges)",
    "H": "HEERA (higher education)",
    "M": "MMBA (local government)",
    "S": "Dills Act (state employees)",
    "C": "Court employees (Trial Court / Court Interpreter Acts)",
    "I": "In-Home Supportive Services",
}


class CAPERBScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"
        )
        self._meta: dict | None = None  # slug(lower) -> {date, link, summary}

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
    @staticmethod
    def _decision_number(filename: str) -> str | None:
        """decision-2995e.pdf -> 2995E ; order-j024e.pdf -> J024E ; J025.pdf -> J025"""
        stem = re.sub(r"\.pdf$", "", filename, flags=re.I)
        stem = re.sub(r"^(?:decision|order)[-_]?", "", stem, flags=re.I)
        stem = stem.strip("-_ ")
        return stem.upper() or None

    @staticmethod
    def _doc_id(filename: str) -> str:
        stem = re.sub(r"\.pdf$", "", filename, flags=re.I)
        return re.sub(r"[^A-Za-z0-9_-]", "_", stem)

    @classmethod
    def _clean_html(cls, html: str) -> str:
        import html as _html
        txt = TAG_RE.sub(" ", html or "")
        txt = _html.unescape(txt)
        return WS_RE.sub(" ", txt).strip()

    @staticmethod
    def _sector(number: str | None) -> str | None:
        if not number:
            return None
        m = re.search(r"([A-Za-z])$", number)
        return SECTORS.get(m.group(1).upper()) if m else None

    # ---------------------------------------------------- decision metadata
    def _load_meta(self) -> dict:
        if self._meta is not None:
            return self._meta
        meta: dict = {}
        page = 1
        while True:
            data = self._curl_json(DECISION_TEMPLATE.format(page=page))
            if not isinstance(data, list) or not data:
                break
            for post in data:
                slug = (post.get("slug") or "").lower()
                if not slug:
                    continue
                summary = self._clean_html(
                    (post.get("content") or {}).get("rendered", "")
                )
                date = (post.get("date") or "")[:10] or None
                meta[slug] = {
                    "date": date,
                    "link": post.get("link"),
                    "summary": summary[:4000] if summary else None,
                }
            logger.info(f"  decision-post map: page {page} ({len(meta)} so far)")
            page += 1
            if page > 60:  # safety (~6,000 posts)
                break
        self._meta = meta
        logger.info(f"Loaded metadata for {len(meta)} PERB decision posts")
        return meta

    # --------------------------------------------------------- discovery
    def discover_documents(self, sample: bool = False) -> Generator[dict, None, None]:
        seen: set[str] = set()
        total = 0
        page = 1
        while True:
            data = self._curl_json(MEDIA_TEMPLATE.format(page=page))
            if not isinstance(data, list) or not data:
                break
            new_on_page = 0
            for item in data:
                url = item.get("source_url") or ""
                if "/decisionbank/" not in url or not url.lower().endswith(".pdf"):
                    continue
                filename = url.rsplit("/", 1)[-1]
                key = filename.lower()
                if key in seen:
                    continue
                seen.add(key)
                total += 1
                new_on_page += 1
                yield {
                    "file": filename,
                    "doc_url": url,
                    "number": self._decision_number(filename),
                }
            logger.info(f"  media page {page}: {new_on_page} decision PDFs (total {total})")
            if sample and total >= 16:
                break
            if len(data) < 100:  # last page
                break
            page += 1
            if page > 60:  # safety (~6,000 PDFs)
                logger.warning("Reached media page safety cap (60)")
                break
        logger.info(f"Discovered {total} CA PERB decision PDFs")

    # ------------------------------------------------------- build record
    def _build_raw(self, doc: dict) -> dict | None:
        blob = self._curl_bytes(doc["doc_url"])
        if not blob:
            logger.warning(f"Download failed: {doc['doc_url']}")
            return None
        if blob[:4] != b"%PDF":
            logger.warning(f"Not a PDF ({blob[:8]!r}): {doc['doc_url']}")
            return None
        doc_id = self._doc_id(doc["file"])
        text = pdf_extract.extract_pdf_markdown(
            "US/CA-PERB", doc_id, pdf_bytes=blob,
            table="case_law", force=True,
        )
        if not text or len(text.strip()) < 200:
            logger.warning(f"No usable text for {doc['doc_url']} "
                           f"({len(text) if text else 0} chars)")
            return None
        doc = dict(doc)
        doc["doc_id"] = doc_id
        doc["text"] = text.strip()

        meta = self._load_meta()
        m = meta.get((doc.get("number") or "").lower(), {})
        doc["date"] = m.get("date") or self._decision_date(doc["text"])
        doc["summary"] = m.get("summary")
        doc["decision_page"] = m.get("link")
        return doc

    @staticmethod
    def _decision_date(text: str) -> str | None:
        best = None
        for m in DATE_RE.finditer(text):
            mo = MONTHS.get(m.group(1).lower())
            d, y = int(m.group(2)), int(m.group(3))
            if mo and 1 <= d <= 31 and 1975 <= y <= 2035:
                best = f"{y:04d}-{mo:02d}-{d:02d}"
        return best

    # -------------------------------------------------------------- test
    def test_api(self) -> bool:
        logger.info("Testing CA PERB media enumeration + PDF extraction...")
        try:
            docs = list(self.discover_documents(sample=True))
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} PDFs (sample crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) > 200:
                logger.info(f"  PDF text extraction OK ({len(raw['text'])} chars) — "
                            f"Decision No. {raw.get('number')} [{raw.get('date')}]")
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
        title = f"California PERB Decision No. {num}" if num else "California PERB Decision"
        return {
            "_id": f"US/CA-PERB/{raw['doc_id']}",
            "_source": "US/CA-PERB",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "doc_id": raw["doc_id"],
            "decision_number": num,
            "sector": self._sector(num),
            "issuer": "California Public Employment Relations Board (PERB)",
            "title": title,
            "summary": raw.get("summary"),
            "text": raw["text"],
            "url": raw.get("decision_page") or raw["doc_url"],
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

    parser = argparse.ArgumentParser(description="US/CA-PERB bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CAPERBScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
