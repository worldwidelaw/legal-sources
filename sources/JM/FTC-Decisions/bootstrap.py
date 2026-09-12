#!/usr/bin/env python3
"""
JM/FTC-Decisions — Jamaica Fair Trading Commission

Fetches case reports, judgments, consent agreements, market studies,
and guidelines from the Jamaica Fair Trading Commission website.

Strategy:
  1. Scrape section pages for PDF links and HTML post links
  2. For PDFs: download and extract text with pdfminer
  3. For HTML posts: scrape entry-content div
  4. Normalize into standard schema

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update              # Incremental (re-fetches all — no date API)
  python bootstrap.py test-api            # Quick connectivity test
"""

import html as html_mod
import io
import re
import sys
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import unquote, urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.JM.FTC-Decisions")

BASE_URL = "https://jftc.gov.jm"
SOURCE_ID = "JM/FTC-Decisions"

# Section pages to scrape — each may have PDF links and/or paginated posts
SECTIONS = [
    {
        "url": "/publications/case-reports/",
        "pages": 4,
        "doc_type": "case_report",
        "category": "Case Reports",
    },
    {
        "url": "/enforcement/judgments/",
        "pages": 2,
        "doc_type": "judgment",
        "category": "Judgments",
    },
    {
        "url": "/enforcement/consent-agreements/",
        "pages": 3,
        "doc_type": "consent_agreement",
        "category": "Consent Agreements",
    },
    {
        "url": "/publications/market-studies/",
        "pages": 1,
        "doc_type": "market_study",
        "category": "Market Studies",
    },
    {
        "url": "/publications/advisories/",
        "pages": 1,
        "doc_type": "advisory",
        "category": "Advisories",
    },
    {
        "url": "/enforcement/statements-of-non-objection/",
        "pages": 1,
        "doc_type": "non_objection",
        "category": "Statements of Non-Objection",
    },
    {
        "url": "/enforcement/legislation/",
        "pages": 1,
        "doc_type": "legislation",
        "category": "Legislation",
    },
]


def _extract_text_pdfminer(pdf_bytes: bytes) -> Optional[str]:
    """Extract text from PDF bytes using pdfminer."""
    try:
        from pdfminer.high_level import extract_text as pdfminer_extract
        text = pdfminer_extract(io.BytesIO(pdf_bytes))
        if text and len(text.strip()) > 50:
            return text.strip()
    except Exception as e:
        logger.warning("pdfminer extraction failed: %s", e)
    return None


def _clean_html(html: str) -> str:
    """Strip HTML tags and clean whitespace."""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?\s*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</?p[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'&nbsp;', ' ', text)
    text = re.sub(r'&amp;', '&', text)
    text = re.sub(r'&lt;', '<', text)
    text = re.sub(r'&gt;', '>', text)
    text = re.sub(r'&#8211;', '–', text)
    text = re.sub(r'&#8217;', "'", text)
    text = re.sub(r'&#8220;|&#8221;', '"', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class FTCDecisionsScraper(BaseScraper):

    def __init__(self):
        super().__init__(str(Path(__file__).parent))
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html, application/pdf, */*",
            },
            timeout=60,
        )
        self._seen_urls = set()

    def _scrape_section_page(self, page_url: str) -> tuple[list[dict], list[dict]]:
        """Scrape a page for PDF links and HTML post links.
        Returns (pdf_items, post_items).
        """
        logger.info("Scraping %s", page_url)
        resp = self.http.get(page_url, timeout=60)
        if resp.status_code != 200:
            logger.warning("Failed to fetch %s: HTTP %d", page_url, resp.status_code)
            return [], []

        html = resp.text
        pdfs = []
        posts = []

        # Find PDF links (handle both single and double quotes)
        pdf_pattern = r"""<a[^>]*href=["']([^"']*\.pdf(?:\?[^"']*)?)["'][^>]*>(.*?)</a>"""
        for href, label in re.findall(pdf_pattern, html, re.DOTALL | re.IGNORECASE):
            if href.startswith("/"):
                url = f"{BASE_URL}{href}"
            elif href.startswith("http"):
                url = href
            else:
                url = urljoin(page_url, href)

            norm = url.split("?")[0]
            if norm in self._seen_urls:
                continue
            self._seen_urls.add(norm)

            clean_label = html_mod.unescape(re.sub(r"<[^>]+>", "", label).strip())
            if not clean_label:
                clean_label = Path(unquote(norm)).stem.replace("-", " ").replace("_", " ")

            pdfs.append({"url": url, "norm_url": norm, "title": clean_label, "is_pdf": True})

        # Find HTML post links (case reports, enforcement posts)
        # These are individual WordPress post URLs within jftc.gov.jm
        post_pattern = r"""<a[^>]*href=["'](https?://jftc\.gov\.jm/(?!about|enforcement/(?:legislation|judgments|consent|statements|authorizations)|publications|tips|news|articles|wp-content|contact|privacy|i-would|page/)[^"']+/)["'][^>]*>(.*?)</a>"""
        for href, label in re.findall(post_pattern, html, re.DOTALL | re.IGNORECASE):
            if href in self._seen_urls:
                continue
            self._seen_urls.add(href)

            clean_label = html_mod.unescape(re.sub(r"<[^>]+>", "", label).strip())
            if not clean_label or len(clean_label) < 5:
                continue
            # Skip pagination and nav links
            if clean_label in ("Next »", "« Previous") or clean_label.isdigit():
                continue

            posts.append({"url": href, "norm_url": href.rstrip("/"), "title": clean_label, "is_pdf": False})

        return pdfs, posts

    def _download_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download PDF and extract text."""
        try:
            resp = self.http.get(pdf_url, timeout=120)
            if resp.status_code != 200:
                logger.warning("PDF download failed (%d): %s", resp.status_code, pdf_url)
                return None
            ct = resp.headers.get("Content-Type", "")
            if "html" in ct.lower():
                logger.warning("Got HTML instead of PDF: %s", pdf_url)
                return None
            return _extract_text_pdfminer(resp.content)
        except Exception as e:
            logger.warning("PDF download failed for %s: %s", pdf_url, e)
        return None

    def _fetch_post_text(self, post_url: str) -> Optional[str]:
        """Fetch an HTML post and extract entry-content text."""
        try:
            resp = self.http.get(post_url, timeout=60)
            if resp.status_code != 200:
                logger.warning("Post fetch failed (%d): %s", resp.status_code, post_url)
                return None
            html = resp.text

            # Try entry-content first
            m = re.search(
                r'<div[^>]*class="[^"]*entry-content[^"]*"[^>]*>(.*?)</div>\s*(?:<(?:div|footer|section))',
                html, re.DOTALL,
            )
            if m:
                text = _clean_html(m.group(1))
                if len(text) > 50:
                    return text

            # Fallback: try broader content area
            m = re.search(
                r'<article[^>]*>(.*?)</article>',
                html, re.DOTALL,
            )
            if m:
                text = _clean_html(m.group(1))
                if len(text) > 50:
                    return text
        except Exception as e:
            logger.warning("Post fetch failed for %s: %s", post_url, e)
        return None

    def _build_record(self, item: dict, section: dict) -> Optional[dict]:
        """Fetch content and build a normalized record."""
        if item["is_pdf"]:
            text = self._download_pdf_text(item["url"])
        else:
            text = self._fetch_post_text(item["url"])

        if not text:
            logger.warning("No text extracted: %s", item["title"])
            return None

        # Stable ID from URL path
        url_path = item["norm_url"].replace(BASE_URL, "").strip("/")
        if url_path.startswith("wp-content/"):
            doc_id = Path(unquote(url_path)).stem
        else:
            doc_id = url_path.replace("/", "-")

        # Map doc_type to _type
        if section["doc_type"] in ("legislation",):
            _type = "legislation"
        elif section["doc_type"] in ("judgment",):
            _type = "case_law"
        else:
            _type = "doctrine"

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": _type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": item["title"],
            "text": text,
            "date": None,
            "url": item["norm_url"],
            "document_type": section["doc_type"],
            "category": section["category"],
        }

    # ── BaseScraper interface ────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all FTC documents."""
        self._seen_urls = set()

        for section in SECTIONS:
            all_items = []
            max_pages = section.get("pages", 1)

            for page_num in range(1, max_pages + 1):
                if page_num == 1:
                    page_url = f"{BASE_URL}{section['url']}"
                else:
                    page_url = f"{BASE_URL}{section['url']}page/{page_num}/"

                pdfs, posts = self._scrape_section_page(page_url)
                all_items.extend(pdfs)
                all_items.extend(posts)
                time.sleep(1)

            logger.info(
                "%s: %d items found across %d pages",
                section["category"], len(all_items), max_pages,
            )

            for item in all_items:
                record = self._build_record(item, section)
                if record:
                    yield record
                time.sleep(1)

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        return raw


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="JM/FTC-Decisions scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch only sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = FTCDecisionsScraper()

    if args.command == "test-api":
        for section in SECTIONS:
            page_url = f"{BASE_URL}{section['url']}"
            pdfs, posts = scraper._scrape_section_page(page_url)
            logger.info(
                "%s: %d PDFs, %d posts found",
                section["category"], len(pdfs), len(posts),
            )
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
                "[%d] %s — %d chars",
                count,
                record["title"][:60],
                len(record.get("text", "")),
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
