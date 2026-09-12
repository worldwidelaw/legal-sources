#!/usr/bin/env python3
"""
TZ/FCC-Decisions — Tanzania Fair Competition Commission Decisions

Fetches competition decisions (merger determinations, complaint rulings,
provisional findings) and official speeches from the FCC's public REST API.

API base: https://www.fcc.go.tz/content/v1/
Endpoint: /publications/published-publications/list?page=N  (10 items/page)

Full text comes from two places:
  * merger determinations are published as the decision narrative in the
    publication's `description` field (there is no attachment on the API), and
  * provisional findings / speeches carry a PDF `attachment`, whose text is
    extracted with the shared PDF pipeline.

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py bootstrap-fast      # Full pull, concurrent normalize
  python bootstrap.py test-api            # Quick connectivity test
"""

import re
import sys
import json
import logging
import hashlib
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urlsplit, urlunsplit, quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TZ.FCC-Decisions")

SOURCE_ID = "TZ/FCC-Decisions"
API_BASE = "https://www.fcc.go.tz/content/v1"
LIST_ENDPOINT = f"{API_BASE}/publications/published-publications/list"
PUBLIC_HOST = "https://www.fcc.go.tz"
SOURCE_URL = "https://www.fcc.go.tz/decision"

# Categories to treat as case_law (by English name)
CASE_LAW_CATEGORIES = {"Decisions", "Provisional Findings"}

# Shortest body we accept as a real document rather than a stub.
MIN_TEXT_CHARS = 200


class SourceBlockedError(RuntimeError):
    """Raised when the API cannot be enumerated at all — fail loud, never 0."""


def _make_id(pub: dict) -> str:
    """Generate stable ID from publication ID."""
    pub_id = pub.get("publicationId", "")
    return f"TZ_FCC_{pub_id}" if pub_id else "TZ_FCC_" + hashlib.md5(
        json.dumps(pub.get("name", {}), sort_keys=True).encode()
    ).hexdigest()


def _en(field) -> str:
    """Pull the English variant out of a bilingual {en, sw} field."""
    if isinstance(field, dict):
        return (field.get("en") or "").strip()
    return str(field or "").strip()


def _clean(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _attachment_url(pub: dict) -> Optional[str]:
    """
    Public URL of a publication's PDF attachment.

    The CMS stores some attachment URLs against its internal address
    (http://10.1.90.177/attachments/...), which is unroutable from outside the
    FCC network. The files are served from the public host under the same path,
    so rewrite the authority and percent-encode the (often space-bearing) path.
    """
    att = pub.get("attachment")
    if not isinstance(att, dict):
        return None
    raw_url = att.get("url")
    if not raw_url:
        return None
    parts = urlsplit(raw_url)
    path = quote(parts.path, safe="/%")
    return urlunsplit(("https", "www.fcc.go.tz", path, parts.query, ""))


def _category(pub: dict) -> str:
    cat = pub.get("category")
    if isinstance(cat, dict):
        return _en(cat.get("name")) or "Unknown"
    return "Unknown"


class FCCDecisionsScraper(BaseScraper):
    SOURCE_ID = SOURCE_ID

    def __init__(self, source_dir=None):
        super().__init__(source_dir)
        self.http = HttpClient(headers={"Accept": "application/json"})

    # ------------------------------------------------------------ fetching

    def _list_page(self, page: int) -> dict:
        resp = self.http.get(LIST_ENDPOINT, params={"page": page})
        resp.raise_for_status()
        return resp.json()

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW publications; BaseScraper calls normalize() on each."""
        try:
            first = self._list_page(1)
        except Exception as exc:  # noqa: BLE001
            raise SourceBlockedError(
                f"cannot enumerate {LIST_ENDPOINT} — {exc}"
            ) from exc

        total = first.get("pagination", {}).get("totalItems")
        total_pages = first.get("pagination", {}).get("totalPages", 1) or 1
        logger.info(f"FCC publications: {total} items across {total_pages} pages")

        pubs = first.get("publications", [])
        if not pubs:
            raise SourceBlockedError(
                "page 1 returned no publications — the API answered but the "
                "corpus is empty, which never happens on a healthy endpoint"
            )

        yielded = 0
        page = 1
        while True:
            for pub in pubs:
                yielded += 1
                yield pub
            page += 1
            if page > total_pages:
                break
            pubs = self._list_page(page).get("publications", [])
            if not pubs:
                break

        logger.info(f"Enumerated {yielded} publications")

    def fetch_updates(self, since=None) -> Generator[dict, None, None]:
        """The API exposes no date filter — re-walk the (small) corpus."""
        since_str = since.isoformat() if hasattr(since, "isoformat") else (since or "")
        for pub in self.fetch_all():
            if not since_str or (pub.get("lastModified") or "") >= since_str:
                yield pub

    # ----------------------------------------------------------- normalize

    def normalize(self, raw: dict) -> Optional[dict]:
        title = _en(raw.get("name"))
        description = _clean(_en(raw.get("description")))
        category = _category(raw)
        doc_id = _make_id(raw)

        issue_date = raw.get("issueDate") or raw.get("datePublished")
        date = issue_date[:10] if isinstance(issue_date, str) and issue_date else None

        pdf_url = _attachment_url(raw)
        text = description
        if pdf_url:
            pdf_text = self._pdf_text(pdf_url, doc_id, category)
            if pdf_text and len(pdf_text) > len(text):
                text = pdf_text

        if len(text) < MIN_TEXT_CHARS:
            logger.warning(
                f"Skipping {doc_id} — only {len(text)} chars of text: {title[:60]}"
            )
            return None

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "case_law" if category in CASE_LAW_CATEGORIES else "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title[:500],
            "title_sw": (raw.get("name") or {}).get("sw", "").strip()
            if isinstance(raw.get("name"), dict) else "",
            "text": text,
            # The description doubles as the body for attachment-less
            # determinations; only keep it as a separate summary when the PDF
            # supplied a longer text.
            "summary": description if text != description else None,
            "date": date,
            "url": pdf_url or SOURCE_URL,
            "country": "TZ",
            "language": "en",
            "authority": "Fair Competition Commission",
            "category": category,
            "publication_id": raw.get("publicationId", ""),
            "attachment_url": pdf_url,
        }

    def _pdf_text(self, pdf_url: str, doc_id: str, category: str) -> str:
        """Download an attachment and run it through the shared PDF pipeline."""
        try:
            resp = self.http.get(pdf_url)
            resp.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"Attachment download failed ({pdf_url}): {exc}")
            return ""

        table = "case_law" if category in CASE_LAW_CATEGORIES else "doctrine"
        try:
            markdown = extract_pdf_markdown(
                SOURCE_ID, doc_id, pdf_bytes=resp.content, table=table
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"PDF extraction failed for {doc_id}: {exc}")
            return ""
        return _clean(markdown or "")

    # ---------------------------------------------------------------- test

    def test_api(self) -> bool:
        try:
            data = self._list_page(1)
        except Exception as exc:  # noqa: BLE001
            logger.error(f"API test FAILED: {exc}")
            return False
        pubs = data.get("publications", [])
        pagination = data.get("pagination", {})
        print(
            f"OK — {len(pubs)} publications on page 1, "
            f"total: {pagination.get('totalItems', '?')}"
        )
        if pubs:
            print(f"First: {_en(pubs[0].get('name'))[:80]}")
        return bool(pubs)


def main():
    parser = argparse.ArgumentParser(description="TZ/FCC-Decisions bootstrap")
    parser.add_argument(
        "command", choices=["bootstrap", "bootstrap-fast", "test-api", "updates"]
    )
    parser.add_argument("--sample", action="store_true",
                        help="Save sample records for validation")
    parser.add_argument("--full", action="store_true",
                        help="Fetch the whole corpus (default)")
    parser.add_argument("--count", type=int, default=15, help="Sample size")
    parser.add_argument("--since", type=str, help="ISO date for updates")
    args = parser.parse_args()

    scraper = FCCDecisionsScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    if args.command == "updates":
        for raw in scraper.fetch_updates(args.since):
            rec = scraper.normalize(raw)
            if rec:
                print(json.dumps(rec, ensure_ascii=False))
        return

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
    else:
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=args.count)
    logger.info(f"{args.command} complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
