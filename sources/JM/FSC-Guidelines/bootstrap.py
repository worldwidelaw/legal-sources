#!/usr/bin/env python3
"""
JM/FSC-Guidelines — Financial Services Commission (Jamaica)

Fetches guidelines, bulletins, and notices from the FSC Jamaica WordPress
site covering the securities, insurance, and private-pension industries.

Strategy:
  1. Enumerate posts via the WordPress REST API for the guidelines,
     bulletins, and notices categories (title + date + permalink).
  2. Fetch each post page and extract the linked PDF under
     /wp-content/uploads/.
  3. Download the PDF and extract text with pdfminer. If the PDF has no
     text layer, fall back to the post's inline HTML content.
  4. Skip records that yield no usable text (scanned, no inline content).

Usage:
  python bootstrap.py bootstrap           # Full pull
  python bootstrap.py bootstrap --sample  # Sample records for validation
  python bootstrap.py test-api            # Quick connectivity test
"""

import io
import re
import sys
import json
import html
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.JM.FSC-Guidelines")

BASE_URL = "https://www.fscjamaica.org"
SOURCE_ID = "JM/FSC-Guidelines"

# WordPress category id -> (label used in the record)
CATEGORIES = {
    78: "Guidelines",
    10: "Bulletins",
    11: "Notices",
}

_TAG_RE = re.compile(r"<[^>]+>")
_PDF_RE = re.compile(r'href="([^"]*wp-content/uploads[^"]*\.pdf[^"]*)"', re.IGNORECASE)


def _strip_html(s: str) -> str:
    return html.unescape(_TAG_RE.sub("", s)).strip()


def _clean_text(text: str) -> str:
    clean = text.strip().replace("\x0c", "")
    clean = re.sub(r"[ \t]+\n", "\n", clean)
    clean = re.sub(r"\n{3,}", "\n\n", clean)
    return clean


def _extract_pdf_text(pdf_bytes: bytes) -> Optional[str]:
    try:
        from pdfminer.high_level import extract_text as pdfminer_extract
        text = _clean_text(pdfminer_extract(io.BytesIO(pdf_bytes)))
        if len(text) > 200:
            return text
    except Exception as e:
        logger.warning("pdfminer extraction failed: %s", e)
    return None


class FSCGuidelinesScraper(BaseScraper):

    def __init__(self):
        super().__init__(str(Path(__file__).parent))
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; open legal data research)",
                "Accept": "application/json, text/html, application/pdf, */*",
            },
            timeout=60,
        )

    def _api_posts(self, cat_id: int) -> Generator[dict, None, None]:
        """Yield WordPress post objects for a category, paginated."""
        page = 1
        while True:
            url = f"{BASE_URL}/wp-json/wp/v2/posts?categories={cat_id}&per_page=100&page={page}"
            try:
                resp = self.http.get(url, timeout=60)
            except Exception as e:
                logger.warning("API page %d failed for cat %d: %s", page, cat_id, e)
                return
            if resp.status_code != 200:
                # 400 = page past the end
                return
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
        try:
            resp = self.http.get(post_link, timeout=60)
            if resp.status_code != 200:
                return None
        except Exception as e:
            logger.warning("Failed to fetch post page %s: %s", post_link, e)
            return None
        m = _PDF_RE.search(resp.text)
        return m.group(1) if m else None

    def _download_pdf(self, pdf_url: str) -> Optional[str]:
        try:
            resp = self.http.get(pdf_url, timeout=120)
            if resp.status_code != 200:
                logger.warning("PDF download failed (%d): %s", resp.status_code, pdf_url)
                return None
            return _extract_pdf_text(resp.content)
        except Exception as e:
            logger.warning("PDF download error %s: %s", pdf_url, e)
            return None

    def _classify(self, title: str, category: str) -> tuple[str, str]:
        t = title.lower()
        if "regulations" in t or re.search(r"\bact\b", t):
            return "legislation", "act_or_regulation"
        return "doctrine", category.lower().rstrip("s")

    def _build_record(self, post: dict, category: str,
                      seen_pdfs: Optional[set] = None) -> Optional[dict]:
        title = _strip_html(post.get("title", {}).get("rendered", ""))
        if not title:
            title = f"FSC document {post.get('id')}"
        link = post.get("link", "")
        date = (post.get("date") or "")[:10] or None

        pdf_url = self._post_pdf_url(link)
        # Dedup by PDF: FSC republishes one PDF under several posts (e.g. the
        # same "Management of Cyber Risks" guidance for each industry). Keep
        # only the first post pointing at a given PDF.
        if pdf_url and seen_pdfs is not None:
            norm_pdf = pdf_url.split("?")[0]
            if norm_pdf in seen_pdfs:
                logger.debug("Skipping duplicate PDF: %s", norm_pdf)
                return None
            seen_pdfs.add(norm_pdf)
        text = self._download_pdf(pdf_url) if pdf_url else None

        # Fallback to inline post content if the PDF had no text layer.
        if not text:
            inline = _strip_html(post.get("content", {}).get("rendered", ""))
            inline = re.sub(r"\s{3,}", "\n\n", inline)
            if len(inline) > 200:
                text = inline
            else:
                logger.warning("No usable text: %s", title[:60])
                return None

        _type, doc_type = self._classify(title, category)

        return {
            "_id": f"fsc-{post.get('id')}",
            "_source": SOURCE_ID,
            "_type": _type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": pdf_url or link,
            "document_type": doc_type,
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
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        seen_pdfs: set = set()
        for cat_id, label in CATEGORIES.items():
            page = 1
            while True:
                url = (f"{BASE_URL}/wp-json/wp/v2/posts?categories={cat_id}"
                       f"&per_page=100&page={page}")
                if since:
                    url += f"&after={since}"
                try:
                    resp = self.http.get(url, timeout=60)
                except Exception:
                    break
                if resp.status_code != 200:
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

    parser = argparse.ArgumentParser(description="JM/FSC-Guidelines scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch only sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = FSCGuidelinesScraper()

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
