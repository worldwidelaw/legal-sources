#!/usr/bin/env python3
"""
Legal Data Hunter - UK IPO Registered Designs Hearing Decisions Scraper

Fetches the hearing decisions of the UK Intellectual Property Office (IPO)
concerning registered designs — invalidation, entitlement and related disputes
decided by an IPO Hearing Officer under the Registered Designs Act 1949. These
are published on GOV.UK (NOT on the Cloudflare-gated ipo.gov.uk site, so this
path is reachable without browser automation — cf. blocked UK/IPO).

Access (the shared GOV.UK finder + Content Store recipe used by UK/SchoolsAdjudicator,
UK/RPT, UK/ET):
  - GET /api/search.json?filter_format=design_decision (discovery)
  - GET /api/content/{path} (per-decision metadata + PDF attachment URL)
  - Download the born-digital decision PDF and extract full text.

On GOV.UK each `design_decision` entry carries only a short summary in
details.body (hidden_indexable_content is empty); the full decision is the
PDF attachment on assets.publishing.service.gov.uk — born-digital, extracted
via the shared PDF extractor (no OCR needed). Rich metadata: the British
Library decision number (e.g. O/0543/26), decision date, hearing officer and
litigants.

Coverage: ~134 design hearing decisions.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records for validation
  python bootstrap.py bootstrap-fast     # Alias for full pull (fleet runner)
"""

import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common import pdf_extract

MIN_PDF_TEXT = 200

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("UK/IPO-Designs")


class UKIPODesignsScraper(BaseScraper):
    """Scraper for UK IPO registered-design hearing decisions on GOV.UK."""

    BASE_URL = "https://www.gov.uk"
    SEARCH_URL = "/api/search.json"
    CONTENT_URL = "/api/content"
    FILTER_FORMAT = "design_decision"
    PAGE_SIZE = 200

    SEARCH_FIELDS = ["title", "link", "public_timestamp"]

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=self.BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (legal research project)",
                "Accept": "application/json",
            },
            timeout=30,
        )

    def _search(self, start: int = 0) -> dict:
        params = {
            "filter_format": self.FILTER_FORMAT,
            "count": self.PAGE_SIZE,
            "start": start,
            "order": "-public_timestamp",
            "fields": ",".join(self.SEARCH_FIELDS),
        }
        self.rate_limiter.wait()
        try:
            resp = self.client.get(self.SEARCH_URL, params=params)
            if resp.status_code == 200:
                return resp.json()
            logger.warning(f"Search API returned {resp.status_code} for start={start}")
            return {}
        except Exception as e:
            logger.error(f"Search API failed: {e}")
            return {}

    def _fetch_content(self, path: str) -> Optional[dict]:
        if not path.startswith("/"):
            path = "/" + path
        url = f"{self.CONTENT_URL}{path}"
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 404:
                logger.debug(f"Content not found: {path}")
            else:
                logger.warning(f"Content API returned {resp.status_code} for {path}")
            return None
        except Exception as e:
            logger.warning(f"Content API failed for {path}: {e}")
            return None

    @staticmethod
    def _pdf_attachments(content: dict) -> list:
        atts = content.get("details", {}).get("attachments", []) or []
        return [
            a for a in atts
            if (a.get("content_type") == "application/pdf" or
                str(a.get("url", "")).lower().endswith(".pdf"))
            and a.get("url")
        ]

    def _extract_pdf_text(self, content_id: str, url: str) -> str:
        try:
            text = pdf_extract.extract_pdf_markdown(
                "UK/IPO-Designs", content_id, pdf_url=url,
            )
        except Exception as e:
            logger.warning(f"PDF extraction failed for {url}: {e}")
            return ""
        return (text or "").strip()

    def _build_raw(self, item: dict, content: dict, text: str, pdf_url: str) -> dict:
        meta = content.get("details", {}).get("metadata", {})
        litigants = (meta.get("design_decision_litigants", "") or "").strip()
        return {
            "content_id": content.get("content_id", ""),
            "title": (content.get("title", "") or item.get("title", "")).strip(),
            "text": text,
            "description": content.get("description", ""),
            "reference": (meta.get("design_decision_british_library_number", "") or "").strip(),
            "decision_date": meta.get("design_decision_date", ""),
            "hearing_officer": (meta.get("design_decision_hearing_officer", "") or "").strip(),
            "litigants": litigants,
            "first_published_at": content.get("first_published_at", ""),
            "public_updated_at": content.get("public_updated_at", ""),
            "pdf_url": pdf_url,
            "link": item.get("link", ""),
        }

    def _iter(self, first: dict, total: int) -> Generator[dict, None, None]:
        result = first
        start = 0
        count = 0
        skipped = 0
        while True:
            results = result.get("results", [])
            if not results:
                break
            for item in results:
                link = item.get("link", "")
                if not link:
                    continue
                content = self._fetch_content(link)
                if not content:
                    skipped += 1
                    continue
                pdfs = self._pdf_attachments(content)
                if not pdfs:
                    skipped += 1
                    continue
                content_id = content.get("content_id", "") or link
                text = self._extract_pdf_text(content_id, pdfs[0]["url"])
                if not text or len(text) < MIN_PDF_TEXT:
                    skipped += 1
                    continue
                count += 1
                yield self._build_raw(item, content, text, pdfs[0]["url"])
                if count % 25 == 0:
                    logger.info(f"  {count} decisions fetched ({skipped} skipped)")
            start += len(results)
            if total and start >= total:
                break
            result = self._search(start)
        logger.info(f"Total: {count} decisions with text ({skipped} skipped)")

    def fetch_all(self) -> Generator[dict, None, None]:
        first = self._search(0)
        total = first.get("total", 0)
        logger.info(f"Total design decisions: {total}")
        yield from self._iter(first, total)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        since_str = since.strftime("%Y-%m-%d")
        params = {
            "filter_format": self.FILTER_FORMAT,
            "filter_public_timestamp": f"from:{since_str}",
            "count": self.PAGE_SIZE,
            "start": 0,
            "order": "-public_timestamp",
            "fields": ",".join(self.SEARCH_FIELDS),
        }
        self.rate_limiter.wait()
        try:
            resp = self.client.get(self.SEARCH_URL, params=params)
            if resp.status_code != 200:
                logger.warning(f"Updates search returned {resp.status_code}")
                return
            first = resp.json()
        except Exception as e:
            logger.error(f"Updates search failed: {e}")
            return
        total = first.get("total", 0)
        logger.info(f"Updates since {since_str}: {total} decisions")
        yield from self._iter(first, total)

    def normalize(self, raw: dict) -> dict:
        text = (raw.get("text", "") or "").strip()
        if not text:
            return None

        dd = raw.get("decision_date", "")
        if dd:
            date_iso = dd[:10]
        else:
            fp = raw.get("first_published_at", "") or raw.get("public_updated_at", "")
            date_iso = fp[:10] if fp else None

        return {
            "_id": f"UK/IPO-Designs/{raw.get('content_id', '')}",
            "_source": "UK/IPO-Designs",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "content_id": raw.get("content_id", ""),
            "title": raw.get("title", ""),
            "text": text,
            "description": raw.get("description", ""),
            "reference": raw.get("reference", "") or None,
            "hearing_officer": raw.get("hearing_officer", "") or None,
            "litigants": raw.get("litigants", "") or None,
            "date": date_iso,
            "url": f"{self.BASE_URL}{raw.get('link', '')}",
            "pdf_url": raw.get("pdf_url", ""),
            "updated_at": raw.get("public_updated_at", ""),
        }


# ── CLI entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    scraper = UKIPODesignsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py bootstrap [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd in ("bootstrap", "bootstrap-fast"):
        sample = "--sample" in sys.argv
        result = scraper.bootstrap(sample_mode=sample, sample_size=12)
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
