#!/usr/bin/env python3
"""
Legal Data Hunter - UK Central Arbitration Committee (CAC) Decisions Scraper

Fetches decisions of the Central Arbitration Committee from GOV.UK using:
  - GET /api/search.json?filter_format=decision&filter_organisations=central-arbitration-committee
  - GET /api/content/{path} (per-case metadata + decision attachment URLs)
  - For each attached decision document, extract the full text:
      * older cases attach born-digital decision PDF(s)
        (assets.publishing.service.gov.uk) → common.pdf_extract
      * newer cases attach html_publication documents
        (/government/publications/{slug}/{decision-slug}) → Content Store
        `details.body` HTML → stripped to text.

The CAC is the permanent independent statutory body (Trade Union and Labour
Relations (Consolidation) Act 1992) that adjudicates statutory trade-union
recognition and derecognition applications, disclosure-of-information for
collective-bargaining complaints, and European Works Council / information &
consultation disputes. Each case produces one or more binding, reasoned
decisions (Acceptance, Validity, Bargaining unit, Form of ballot, Declaration
of recognition, Method, etc.) — quasi-judicial case law.

Coverage: ~936 case pages, each with 1+ decision documents. Every decision
document (PDF or HTML) for a case is concatenated into one full-text record.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records for validation
  python bootstrap.py bootstrap-fast     # Alias for full pull (fleet runner)
"""

import re
import sys
import html
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
logger = logging.getLogger("UK/CentralArbitrationCommittee")


def _html_to_text(raw_html: str) -> str:
    """Strip tags/entities from a GOV.UK html_publication body into clean text."""
    if not raw_html:
        return ""
    # Drop script/style blocks entirely.
    raw_html = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", raw_html)
    # Preserve block boundaries as newlines before stripping tags.
    raw_html = re.sub(r"(?i)<(br|/p|/div|/li|/h[1-6]|/tr)\s*/?>", "\n", raw_html)
    text = re.sub(r"<[^>]+>", " ", raw_html)
    text = html.unescape(text)
    # Collapse whitespace while keeping paragraph breaks.
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n\s*", "\n\n", text)
    return text.strip()


class UKCentralArbitrationCommitteeScraper(BaseScraper):
    """Scraper for the Central Arbitration Committee decisions on GOV.UK."""

    BASE_URL = "https://www.gov.uk"
    SEARCH_URL = "/api/search.json"
    CONTENT_URL = "/api/content"
    FILTER_FORMAT = "decision"
    FILTER_ORG = "central-arbitration-committee"
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
    def _is_pdf(att: dict) -> bool:
        return (att.get("content_type") == "application/pdf" or
                str(att.get("url", "")).lower().endswith(".pdf"))

    def _attachment_text(self, content_id: str, idx: int, att: dict) -> str:
        """Extract full text from one decision attachment (PDF or HTML)."""
        url = att.get("url", "")
        if not url:
            return ""
        if self._is_pdf(att):
            try:
                t = pdf_extract.extract_pdf_markdown(
                    "UK/CentralArbitrationCommittee", f"{content_id}#{idx}", pdf_url=url,
                )
            except Exception as e:
                logger.warning(f"PDF extraction failed for {url}: {e}")
                return ""
            return (t or "").strip()
        # HTML decision document — served as an html_publication in the
        # Content Store under its own gov.uk path.
        path = url
        if path.startswith("http"):
            path = re.sub(r"^https?://[^/]+", "", path)
        sub = self._fetch_content(path)
        if not sub:
            return ""
        body = sub.get("details", {}).get("body", "")
        if isinstance(body, list):  # some formats return a list of {content}
            body = " ".join(b.get("content", "") for b in body if isinstance(b, dict))
        return _html_to_text(body)

    def _decision_attachments(self, content: dict) -> list:
        """Return decision documents, skipping the 'application progress' tracker."""
        atts = content.get("details", {}).get("attachments", []) or []
        out = []
        for a in atts:
            url = str(a.get("url", ""))
            if not url:
                continue
            # 'application-progress' pages are status trackers, not decisions.
            if "application-progress" in url.lower():
                continue
            out.append(a)
        return out

    def _extract_all(self, content_id: str, atts: list) -> str:
        chunks = []
        for i, a in enumerate(atts):
            t = self._attachment_text(content_id, i, a)
            if t:
                title = (a.get("title") or "").strip()
                header = f"## {title}\n\n" if title and len(atts) > 1 else ""
                chunks.append(header + t)
        return "\n\n".join(chunks).strip()

    def _build_raw(self, item: dict, content: dict, text: str, ref_url: str) -> dict:
        return {
            "content_id": content.get("content_id", ""),
            "title": content.get("title", "") or item.get("title", ""),
            "text": text,
            "description": content.get("description", ""),
            "first_published_at": content.get("first_published_at", ""),
            "public_updated_at": content.get("public_updated_at", ""),
            "ref_url": ref_url,
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
                atts = self._decision_attachments(content)
                if not atts:
                    skipped += 1
                    continue
                content_id = content.get("content_id", "") or link
                text = self._extract_all(content_id, atts)
                if not text or len(text) < 200:
                    skipped += 1
                    continue
                count += 1
                ref = atts[0].get("url", "") or f"{self.BASE_URL}{link}"
                yield self._build_raw(item, content, text, ref)
                if count % 50 == 0:
                    logger.info(f"  {count} decisions fetched ({skipped} skipped)")
            start += len(results)
            if total and start >= total:
                break
            result = self._search(start)
        logger.info(f"Total: {count} decisions with text ({skipped} skipped)")

    def fetch_all(self) -> Generator[dict, None, None]:
        first = self._search(0)
        total = first.get("total", 0)
        logger.info(f"Total CAC decision entries: {total}")
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
            "_id": f"UK/CentralArbitrationCommittee/{raw.get('content_id', '')}",
            "_source": "UK/CentralArbitrationCommittee",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "content_id": raw.get("content_id", ""),
            "title": raw.get("title", ""),
            "text": text,
            "description": raw.get("description", ""),
            "date": date_iso,
            "url": f"{self.BASE_URL}{raw.get('link', '')}",
            "ref_url": raw.get("ref_url", ""),
            "updated_at": raw.get("public_updated_at", ""),
        }


# ── CLI entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    scraper = UKCentralArbitrationCommitteeScraper()

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
