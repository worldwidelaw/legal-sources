#!/usr/bin/env python3
"""
Legal Data Hunter - UK Teaching Regulation Agency (TRA) Misconduct Decisions Scraper

Fetches teacher-misconduct professional-conduct panel decisions of the
Teaching Regulation Agency (TRA) from GOV.UK using:
  - GET /api/search.json?filter_format=decision&filter_organisations=teaching-regulation-agency (discovery)
  - GET /api/content/{path} (per-decision metadata + PDF attachment URL)
  - Download the born-digital panel-decision PDF and extract full text.

The Teaching Regulation Agency is an executive agency of the Department for
Education that regulates the teaching profession in England. Its independent
professional-conduct panels hear allegations of unacceptable professional
conduct, conduct that may bring the profession into disrepute, and relevant
criminal convictions; the Secretary of State, on the panel's recommendation,
decides whether to impose a prohibition order (a ban from teaching). These
published outcomes set out the panel's findings of fact, reasons and the
final decision — quasi-judicial professional-discipline case law.

Coverage: ~1,660 decisions. On GOV.UK the `decision` format entries for this
body carry a short summary/notice in details.body and the full panel decision
as a PDF attachment (assets.publishing.service.gov.uk) — born-digital,
extracted via the shared PDF extractor (no OCR needed).

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records for validation
  python bootstrap.py bootstrap-fast     # Alias for full pull (fleet runner)
"""

import re
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("UK/TeachingRegulationAgency")

# TRA panel decisions carry a "TRA reference: NNNNNNNN" (or "TRA Reference No: ...").
_REF_RE = re.compile(r"TRA\s+reference(?:\s+no)?[:\s]+([0-9]{4,10})", re.IGNORECASE)


class UKTeachingRegulationAgencyScraper(BaseScraper):
    """Scraper for the Teaching Regulation Agency misconduct decisions on GOV.UK."""

    BASE_URL = "https://www.gov.uk"
    SEARCH_URL = "/api/search.json"
    CONTENT_URL = "/api/content"
    FILTER_FORMAT = "decision"
    FILTER_ORG = "teaching-regulation-agency"
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
            "filter_organisations": self.FILTER_ORG,
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
        """Download the born-digital determination PDF and extract full text."""
        try:
            text = pdf_extract.extract_pdf_markdown(
                "UK/TeachingRegulationAgency", content_id, pdf_url=url,
            )
        except Exception as e:
            logger.warning(f"PDF extraction failed for {url}: {e}")
            return ""
        return (text or "").strip()

    def _build_raw(self, item: dict, content: dict, text: str, pdf_url: str) -> dict:
        title = content.get("title", "") or item.get("title", "")
        ref = ""
        m = _REF_RE.search(text)
        if m:
            ref = m.group(1)
        return {
            "content_id": content.get("content_id", ""),
            "title": title,
            "text": text,
            "description": content.get("description", ""),
            "reference": ref,
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
                    # Collection/guide pages without a determination PDF — skip.
                    skipped += 1
                    continue
                content_id = content.get("content_id", "") or link
                text = self._extract_pdf_text(content_id, pdfs[0]["url"])
                if not text or len(text) < 200:
                    skipped += 1
                    continue
                count += 1
                yield self._build_raw(item, content, text, pdfs[0]["url"])
                if count % 50 == 0:
                    logger.info(f"  {count} determinations fetched ({skipped} skipped)")
            start += len(results)
            if total and start >= total:
                break
            result = self._search(start)
        logger.info(f"Total: {count} determinations with text ({skipped} skipped)")

    def fetch_all(self) -> Generator[dict, None, None]:
        first = self._search(0)
        total = first.get("total", 0)
        logger.info(f"Total OSA decision entries: {total}")
        yield from self._iter(first, total)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        since_str = since.strftime("%Y-%m-%d")
        params = {
            "filter_format": self.FILTER_FORMAT,
            "filter_organisations": self.FILTER_ORG,
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
        logger.info(f"Updates since {since_str}: {total} entries")
        yield from self._iter(first, total)

    def normalize(self, raw: dict) -> dict:
        text = raw.get("text", "").strip()
        if not text:
            return None

        fp = raw.get("first_published_at", "") or raw.get("public_updated_at", "")
        date_iso = fp[:10] if fp else None

        return {
            "_id": f"UK/TeachingRegulationAgency/{raw.get('content_id', '')}",
            "_source": "UK/TeachingRegulationAgency",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "content_id": raw.get("content_id", ""),
            "title": raw.get("title", ""),
            "text": text,
            "description": raw.get("description", ""),
            "reference": raw.get("reference", ""),
            "date": date_iso,
            "url": f"{self.BASE_URL}{raw.get('link', '')}",
            "pdf_url": raw.get("pdf_url", ""),
            "updated_at": raw.get("public_updated_at", ""),
        }


# ── CLI entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    scraper = UKTeachingRegulationAgencyScraper()

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
