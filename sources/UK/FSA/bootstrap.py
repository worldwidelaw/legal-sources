#!/usr/bin/env python3
"""
Legal Data Hunter - UK Food Standards Agency Scraper

The FSA website (www.food.gov.uk) was migrated onto GOV.UK during 2026: the old
Drupal `/search-api` endpoint now returns 404 and every food.gov.uk content path
301s to www.gov.uk. This scraper therefore reads the FSA corpus from GOV.UK's two
official public APIs:

  1. Search API   GET https://www.gov.uk/api/search.json
                      ?filter_organisations=food-standards-agency
     — enumerates every FSA-attributed content item (metadata only).
  2. Content API  GET https://www.gov.uk/api/content{base_path}
     — returns the full document JSON, including the rendered body HTML.

Enumeration is partitioned by `content_store_document_type` so no single query
approaches the Search API's `start` ceiling.

Full text comes from `details.body` (HTML), `details.parts` (multi-part guides),
and — when the landing page itself is thin — the document's HTML and PDF
attachments on assets.publishing.service.gov.uk.

Coverage: ~900 documents — statutory guidance, detailed guidance, regulated-product
decisions, EU-withdrawal statutory instruments, corporate reports, research and
official statistics.

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch 12 sample records for validation
  python bootstrap.py bootstrap-fast      # Concurrent full pull (used by the fleet)
  python bootstrap.py test                # Connectivity check
"""

import re
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("UK/FSA")


def html_to_text(html: str) -> str:
    """Strip HTML tags and return clean text."""
    if not html:
        return ""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style"]):
        tag.decompose()
    text = soup.get_text(separator="\n")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _body_html(details: dict) -> str:
    """`details.body` is either an HTML string or a list of typed renderings."""
    body = details.get("body")
    if isinstance(body, str):
        return body
    if isinstance(body, list):
        for entry in body:
            if isinstance(entry, dict) and entry.get("content_type") == "text/html":
                return entry.get("content", "")
        for entry in body:
            if isinstance(entry, dict) and entry.get("content"):
                return entry["content"]
    return ""


class UKFSAScraper(BaseScraper):
    """Scraper for UK Food Standards Agency publications hosted on GOV.UK."""

    BASE_URL = "https://www.gov.uk"
    SEARCH_PATH = "/api/search.json"
    CONTENT_PATH = "/api/content"
    ORGANISATION = "food-standards-agency"
    PAGE_SIZE = 200

    # Below this many characters the landing page is just a stub in front of
    # attachments, so we go and read the attachments too.
    THIN_BODY_CHARS = 1200
    # Absolute floor for keeping a record at all.
    MIN_TEXT_CHARS = 200
    # Attachments are capped so a publication with 30 annexes cannot dominate a run.
    MAX_HTML_ATTACHMENTS = 8
    MAX_PDF_ATTACHMENTS = 3

    # Non-substantive GOV.UK page types (org landing pages, person profiles, …).
    SKIP_DOCUMENT_TYPES = {
        "organisation",
        "person",
        "role",
        "ministerial_role",
        "topical_event",
        "world_location",
        "contact",
        "finder",
        "official_statistics_announcement",
        "national_statistics_announcement",
        "recruitment",
    }

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=self.BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (legal research project)",
                "Accept": "application/json",
            },
            timeout=45,
        )

    # ── GOV.UK Search API ────────────────────────────────────────────

    def _search(self, params: dict) -> dict:
        self.rate_limiter.wait()
        resp = self.client.get(self.SEARCH_PATH, params=params)
        if resp.status_code != 200:
            raise RuntimeError(
                f"GOV.UK search API returned {resp.status_code} for {params}"
            )
        return resp.json()

    def _document_types(self) -> list:
        """Aggregate the FSA corpus by document type so each query stays small."""
        data = self._search(
            {
                "filter_organisations": self.ORGANISATION,
                "count": 0,
                "aggregate_content_store_document_type": 200,
            }
        )
        total = data.get("total", 0)
        options = (
            data.get("aggregates", {})
            .get("content_store_document_type", {})
            .get("options", [])
        )
        types = []
        for opt in options:
            slug = (opt.get("value") or {}).get("slug")
            if slug and slug not in self.SKIP_DOCUMENT_TYPES:
                types.append((slug, opt.get("documents", 0)))
        logger.info(
            f"FSA corpus on GOV.UK: {total} items across "
            f"{len(types)} substantive document types"
        )
        if not types:
            raise RuntimeError(
                "GOV.UK search API returned no document-type aggregates for "
                f"organisation={self.ORGANISATION} — enumeration would be silently empty"
            )
        return types

    def _list_type(self, doc_type: str) -> Generator[dict, None, None]:
        start = 0
        while True:
            data = self._search(
                {
                    "filter_organisations": self.ORGANISATION,
                    "filter_content_store_document_type": doc_type,
                    "count": self.PAGE_SIZE,
                    "start": start,
                    "order": "-public_timestamp",
                    "fields": ",".join(
                        [
                            "title",
                            "link",
                            "description",
                            "public_timestamp",
                            "content_store_document_type",
                            "format",
                        ]
                    ),
                }
            )
            results = data.get("results", [])
            if not results:
                return
            for r in results:
                yield r
            start += len(results)
            if start >= data.get("total", 0):
                return

    # ── GOV.UK Content API ───────────────────────────────────────────

    def _content(self, base_path: str) -> Optional[dict]:
        if not base_path.startswith("/"):
            base_path = "/" + base_path
        self.rate_limiter.wait()
        try:
            resp = self.client.get(self.CONTENT_PATH + base_path)
        except Exception as e:
            logger.warning(f"Content API error for {base_path}: {e}")
            return None
        if resp.status_code != 200:
            logger.warning(f"Content API returned {resp.status_code} for {base_path}")
            return None
        try:
            return resp.json()
        except ValueError:
            return None

    def _attachment_text(self, details: dict, source_id: str) -> str:
        """Read HTML and PDF attachments for publications whose body is a stub."""
        chunks = []
        html_used = 0
        pdf_used = 0
        for att in details.get("attachments") or []:
            ctype = (att.get("content_type") or "").lower()
            atype = (att.get("attachment_type") or "").lower()
            url = att.get("url") or ""
            title = att.get("title") or ""

            # HTML attachments are separate GOV.UK content items ("html publications").
            # They carry a relative base_path and no content_type.
            is_html = atype == "html" or ctype == "text/html"
            if is_html:
                if html_used >= self.MAX_HTML_ATTACHMENTS:
                    continue
                path = url.split("www.gov.uk", 1)[1] if "www.gov.uk" in url else url
                sub = self._content(path)
                if sub:
                    text = html_to_text(_body_html(sub.get("details", {})))
                    if len(text) >= 100:
                        chunks.append(f"{title}\n\n{text}" if title else text)
                        html_used += 1
                continue

            if ctype == "application/pdf" or url.lower().endswith(".pdf"):
                if pdf_used >= self.MAX_PDF_ATTACHMENTS:
                    continue
                try:
                    from common.pdf_extract import extract_pdf_markdown

                    md = extract_pdf_markdown(
                        "UK/FSA",
                        f"{source_id}#{pdf_used}",
                        pdf_url=url,
                        table="doctrine",
                        force=True,
                    )
                except Exception as e:
                    logger.debug(f"PDF extract failed for {url}: {e}")
                    md = None
                if md and len(md.strip()) >= 100:
                    chunks.append(f"{title}\n\n{md.strip()}" if title else md.strip())
                    pdf_used += 1
        return "\n\n---\n\n".join(chunks)

    # ── BaseScraper contract ─────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield raw search-result metadata for every FSA document on GOV.UK."""
        seen = set()
        count = 0
        for doc_type, expected in self._document_types():
            logger.info(f"Enumerating {doc_type} (~{expected} items)")
            for result in self._list_type(doc_type):
                link = result.get("link") or ""
                if not link or link in seen:
                    continue
                seen.add(link)
                count += 1
                yield result
        logger.info(f"Enumerated {count} FSA documents")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield FSA documents published or updated since a given date."""
        since_str = since.strftime("%Y-%m-%d")
        for doc_type, _ in self._document_types():
            for result in self._list_type(doc_type):
                ts = (result.get("public_timestamp") or "")[:10]
                if ts and ts < since_str:
                    break  # results are ordered newest-first
                yield result

    def normalize(self, raw: dict) -> Optional[dict]:
        """Fetch the document from the Content API and build the full-text record."""
        link = raw.get("link") or ""
        if not link.startswith("/"):
            return None

        source_id = link.strip("/").replace("/", "_")
        doc = self._content(link)
        if not doc:
            return None

        details = doc.get("details", {})

        pieces = []
        description = doc.get("description") or raw.get("description") or ""
        if isinstance(description, str) and description.strip():
            pieces.append(description.strip())

        body_text = html_to_text(_body_html(details))
        if body_text:
            pieces.append(body_text)

        for part in details.get("parts") or []:
            part_title = part.get("title") or ""
            part_body = html_to_text(part.get("body") or "")
            if part_body:
                pieces.append(
                    f"{part_title}\n\n{part_body}" if part_title else part_body
                )

        text = "\n\n".join(p for p in pieces if p).strip()

        if len(text) < self.THIN_BODY_CHARS:
            extra = self._attachment_text(details, source_id)
            if extra:
                text = (text + "\n\n" + extra).strip() if text else extra

        if len(text) < self.MIN_TEXT_CHARS:
            return None

        date_iso = None
        for key in ("first_published_at", "public_updated_at", "updated_at"):
            val = doc.get(key)
            if isinstance(val, str) and len(val) >= 10:
                date_iso = val[:10]
                break
        if not date_iso:
            ts = raw.get("public_timestamp")
            if isinstance(ts, str) and len(ts) >= 10:
                date_iso = ts[:10]

        organisations = []
        for org in (doc.get("links", {}).get("organisations") or []):
            name = org.get("title")
            if name:
                organisations.append(name)

        return {
            "_id": f"UK/FSA/{source_id}",
            "_source": "UK/FSA",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "id": source_id,
            "title": doc.get("title") or raw.get("title") or "",
            "text": text,
            "content_type": doc.get("document_type") or raw.get(
                "content_store_document_type", ""
            ),
            "filter_type": raw.get("content_store_document_type", ""),
            "date": date_iso,
            "organisations": organisations,
            "url": self.BASE_URL + link,
            "updated_at": doc.get("public_updated_at") or "",
        }


# ── CLI entry point ───────────────────────────────────────────────
def _test() -> int:
    scraper = UKFSAScraper()
    types = scraper._document_types()
    print(f"Document types: {len(types)}")
    first = next(scraper._list_type(types[0][0]))
    print(f"First item: {first.get('link')}")
    rec = scraper.normalize(first)
    if not rec:
        print("FAILED: normalize returned None")
        return 1
    print(f"Title: {rec['title']}")
    print(f"Text chars: {len(rec['text'])}")
    print("PASSED")
    return 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py bootstrap [--sample] | bootstrap-fast | test")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "test":
        sys.exit(_test())

    scraper = UKFSAScraper()
    if cmd in ("bootstrap", "bootstrap-fast"):
        sample = "--sample" in sys.argv
        if sample:
            result = scraper.bootstrap(sample_mode=True, sample_size=12)
        elif cmd == "bootstrap-fast":
            result = scraper.bootstrap_fast()
        else:
            result = scraper.bootstrap()
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
