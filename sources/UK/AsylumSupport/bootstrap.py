#!/usr/bin/env python3
"""
Legal Data Hunter - UK First-tier Tribunal (Asylum Support) Decisions Scraper

Fetches decisions of the First-tier Tribunal (Social Entitlement Chamber) —
Asylum Support, published on GOV.UK. The tribunal hears appeals by asylum
seekers and failed asylum seekers against Home Office decisions to refuse or
discontinue asylum support / accommodation under sections 95, 98 and 4 of the
Immigration and Asylum Act 1999. Its decisions are binding case_law.

Access (the shared GOV.UK finder + Content Store recipe used by UK/RPT, UK/ET,
UK/FTT-Tax, UK/TrafficCommissioner):
  - GET /api/search.json?filter_format=asylum_support_decision (discovery)
  - GET /api/content/{path} (full text)

Full text is served in the Content Store `details.metadata.hidden_indexable_content`
field (born-digital, clean plain text — no OCR). Rich tribunal metadata:
categories, sub-categories, judges, reference number, decision date.

Coverage: ~101 decisions.

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

import requests
from bs4 import BeautifulSoup

# hidden_indexable_content shorter than this triggers a PDF-attachment fallback
# (a minority of decisions have empty hic but a born-digital decision PDF).
HIC_MIN = 500
MIN_PDF_TEXT = 200

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("UK/AsylumSupport")


class UKAsylumSupportScraper(BaseScraper):
    """Scraper for First-tier Tribunal (Asylum Support) decisions on GOV.UK."""

    BASE_URL = "https://www.gov.uk"
    SEARCH_URL = "/api/search.json"
    CONTENT_URL = "/api/content"
    FILTER_FORMAT = "asylum_support_decision"
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

    def _extract_text(self, content: dict) -> str:
        """Full text is normally in details.metadata.hidden_indexable_content.
        A minority of decisions have empty/short hic but a born-digital
        decision PDF attachment — fall back to extracting that."""
        details = content.get("details", {})
        meta = details.get("metadata", {})
        hic = (meta.get("hidden_indexable_content", "") or "").strip()
        if len(hic) >= HIC_MIN:
            return hic

        # Fallback 1: born-digital decision PDF attachment.
        pdfs = self._pdf_attachments(content)
        if pdfs:
            content_id = content.get("content_id", "") or details.get("body", "")[:32]
            try:
                pdf_text = (pdf_extract.extract_pdf_markdown(
                    "UK/AsylumSupport", content_id, pdf_url=pdfs[0]["url"],
                ) or "").strip()
            except Exception as e:
                logger.warning(f"PDF extraction failed for {pdfs[0]['url']}: {e}")
                pdf_text = ""
            if len(pdf_text) >= MIN_PDF_TEXT:
                return pdf_text

        # Fallback 2: whatever hic we have, else body HTML summary.
        if hic:
            return hic
        body = details.get("body", "")
        if body:
            soup = BeautifulSoup(body, "html.parser")
            for tag in soup(["script", "style"]):
                tag.decompose()
            lines = [ln.strip() for ln in soup.get_text("\n").splitlines()]
            return "\n".join(ln for ln in lines if ln).strip()
        return ""

    def _build_raw(self, item: dict, content: dict, text: str) -> dict:
        meta = content.get("details", {}).get("metadata", {})
        return {
            "content_id": content.get("content_id", ""),
            "title": (content.get("title", "") or "").strip(),
            "text": text,
            "description": content.get("description", ""),
            "categories": meta.get("tribunal_decision_categories", []),
            "sub_categories": meta.get("tribunal_decision_sub_categories", []),
            "judges": meta.get("tribunal_decision_judges", []),
            "reference_number": meta.get("tribunal_decision_reference_number", ""),
            "decision_date": meta.get("tribunal_decision_decision_date", ""),
            "landmark": meta.get("tribunal_decision_landmark", ""),
            "first_published_at": content.get("first_published_at", ""),
            "public_updated_at": content.get("public_updated_at", ""),
            "link": item.get("link", ""),
        }

    def _iter(self, initial: dict, total: int) -> Generator[dict, None, None]:
        result = initial
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
                text = self._extract_text(content)
                if not text or len(text) < 50:
                    skipped += 1
                    continue
                count += 1
                yield self._build_raw(item, content, text)
                if count % 25 == 0:
                    logger.info(f"  {count} decisions fetched ({skipped} skipped)")
            start += len(results)
            if start >= total:
                break
            result = self._search(start)
        logger.info(f"Total: {count} decisions with text ({skipped} skipped)")

    def fetch_all(self) -> Generator[dict, None, None]:
        first = self._search(0)
        total = first.get("total", 0)
        logger.info(f"Total Asylum Support decisions: {total}")
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
            result = resp.json()
        except Exception as e:
            logger.error(f"Updates search failed: {e}")
            return
        total = result.get("total", 0)
        logger.info(f"Updates since {since_str}: {total} decisions")
        yield from self._iter(result, total)

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

        def _titlecase(lst):
            return [s.replace("-", " ").strip().title() for s in (lst or []) if s]

        return {
            "_id": f"UK/AsylumSupport/{raw.get('content_id', '')}",
            "_source": "UK/AsylumSupport",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "content_id": raw.get("content_id", ""),
            "title": raw.get("title", ""),
            "text": text,
            "description": raw.get("description", ""),
            "date": date_iso,
            "reference_number": raw.get("reference_number", "") or None,
            "categories": _titlecase(raw.get("categories")),
            "sub_categories": _titlecase(raw.get("sub_categories")),
            "judges": raw.get("judges", []) or [],
            "url": f"{self.BASE_URL}{raw.get('link', '')}",
            "updated_at": raw.get("public_updated_at", ""),
        }


# ── CLI entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    scraper = UKAsylumSupportScraper()

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
