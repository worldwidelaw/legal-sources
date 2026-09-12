#!/usr/bin/env python3
"""
JM/OUR-Decisions — Office of Utilities Regulation (Jamaica)

Fetches determination notices, decisions, regulations, and directives from
the OUR Jamaica WordPress site. The OUR is Jamaica's multi-sector economic
regulator covering electricity, telecommunications, water & sewerage, and
public passenger transport.

Strategy:
  1. Enumerate posts via the WordPress REST API for the regulatory-output
     categories (Determination Notices, Decisions and Regulations,
     Decisions & Regulations, Directives) — title + date + permalink.
  2. Fetch each post page and extract the linked PDF under
     /wp-content/uploads/.
  3. Download the PDF and extract text (opendataloader-pdf → pdfplumber →
     pypdf via common.pdf_extract).
  4. Skip records that yield no usable text (older notices are scanned
     image-only PDFs with no text layer).

Usage:
  python bootstrap.py bootstrap           # Full pull
  python bootstrap.py bootstrap --sample  # Sample records for validation
  python bootstrap.py test-api            # Quick connectivity test
"""

import re
import sys
import json
import html
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.JM.OUR-Decisions")

BASE_URL = "https://our.org.jm"
SOURCE_ID = "JM/OUR-Decisions"

# WordPress category id -> label used in the record. These are the OUR's
# binding regulatory outputs (as opposed to consultations/media releases).
CATEGORIES = {
    30: "Determination Notices",
    70: "Decisions and Regulations",
    80: "Decisions & Regulations",
    35: "Directives",
}

_TAG_RE = re.compile(r"<[^>]+>")
_PDF_RE = re.compile(
    r'href=["\']([^"\']*?wp-content/uploads[^"\']*?\.pdf)["\']', re.IGNORECASE
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; open legal data research)",
    "Accept": "application/json, text/html, application/pdf, */*",
}


def _strip_html(s: str) -> str:
    return html.unescape(_TAG_RE.sub("", s)).strip()


class OURDecisionsScraper(BaseScraper):

    def __init__(self):
        super().__init__(str(Path(__file__).parent))
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _get(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        try:
            return self.session.get(url, timeout=timeout)
        except Exception as e:
            logger.warning("GET failed %s: %s", url, e)
            return None

    def _api_posts(self, cat_id: int) -> Generator[dict, None, None]:
        """Yield WordPress post objects for a category, paginated."""
        page = 1
        while True:
            url = (
                f"{BASE_URL}/wp-json/wp/v2/document?categories={cat_id}"
                f"&per_page=100&page={page}&_fields=id,title,date,link"
            )
            resp = self._get(url)
            if resp is None or resp.status_code != 200:
                return  # 400 = page past the end
            try:
                posts = resp.json()
            except Exception:
                return
            if not posts:
                return
            for p in posts:
                yield p
            total_pages = int(resp.headers.get("X-WP-TotalPages", page))
            if page >= total_pages:
                return
            page += 1
            time.sleep(1)

    def _post_pdf_url(self, post_link: str) -> Optional[str]:
        """Fetch a post page and return its first uploaded-PDF URL."""
        resp = self._get(post_link)
        if resp is None or resp.status_code != 200:
            return None
        m = _PDF_RE.search(resp.text)
        return m.group(1) if m else None

    def _build_record(self, post: dict, category: str,
                      seen_pdfs: Optional[set] = None) -> Optional[dict]:
        title = _strip_html(post.get("title", {}).get("rendered", ""))
        if not title:
            title = f"OUR document {post.get('id')}"
        link = post.get("link", "")
        date = (post.get("date") or "")[:10] or None

        pdf_url = self._post_pdf_url(link)
        if not pdf_url:
            logger.warning("No PDF found on page: %s", title[:60])
            return None

        # Dedup by PDF: the OUR republishes one document under several posts.
        norm_pdf = pdf_url.split("?")[0]
        if seen_pdfs is not None:
            if norm_pdf in seen_pdfs:
                logger.debug("Skipping duplicate PDF: %s", norm_pdf)
                return None
            seen_pdfs.add(norm_pdf)

        text = extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=f"our-{post.get('id')}",
            pdf_url=pdf_url,
            table="doctrine",
        )
        if not text or len(text) < 200:
            # Older notices are scanned image-only PDFs with no text layer.
            logger.warning("No usable text (scanned?): %s", title[:60])
            return None

        return {
            "_id": f"our-{post.get('id')}",
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": pdf_url,
            "document_type": category.lower(),
            "category": category,
            "language": "en",
        }

    # ── BaseScraper interface ────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        seen_pdfs: set = set()
        for cat_id, label in CATEGORIES.items():
            logger.info("Fetching category: %s (id=%d)", label, cat_id)
            for post in self._api_posts(cat_id):
                record = self._build_record(post, label, seen_pdfs)
                if record:
                    yield record
                time.sleep(1)

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """Use the WP REST `after` filter when a date is provided."""
        seen_pdfs: set = set()
        since_str = None
        if since is not None:
            since_str = since if isinstance(since, str) else since.isoformat()
        for cat_id, label in CATEGORIES.items():
            page = 1
            while True:
                url = (
                    f"{BASE_URL}/wp-json/wp/v2/document?categories={cat_id}"
                    f"&per_page=100&page={page}&_fields=id,title,date,link"
                )
                if since_str:
                    url += f"&after={since_str}"
                resp = self._get(url)
                if resp is None or resp.status_code != 200:
                    break
                posts = resp.json()
                if not posts:
                    break
                for post in posts:
                    record = self._build_record(post, label, seen_pdfs)
                    if record:
                        yield record
                    time.sleep(1)
                if page >= int(resp.headers.get("X-WP-TotalPages", page)):
                    break
                page += 1

    def normalize(self, raw: dict) -> dict:
        return raw


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="JM/OUR-Decisions scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch only sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = OURDecisionsScraper()

    if args.command == "test-api":
        for cat_id, label in CATEGORIES.items():
            posts = list(scraper._api_posts(cat_id))
            logger.info("%s: %d posts", label, len(posts))
        return

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command in ("bootstrap", "update"):
        limit = 15 if args.sample else None
        count = 0
        for record in scraper.fetch_all():
            count += 1
            if args.sample or count <= 15:
                out_path = sample_dir / f"{count:04d}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(
                "[%d] %s — %d chars (%s)",
                count,
                record["title"][:55],
                len(record.get("text", "")),
                record.get("category"),
            )
            if limit and count >= limit:
                break

        logger.info("Done: %d records fetched", count)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
