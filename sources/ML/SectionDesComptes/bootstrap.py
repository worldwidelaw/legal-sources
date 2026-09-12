#!/usr/bin/env python3
"""
ML/SectionDesComptes -- Cour Suprême du Mali — Jurisprudence

Fetches court decisions (arrêts) from all chambers of Mali's Supreme Court
via WordPress REST API with PDF text extraction.

Strategy:
  - WP REST API pages contain lists of court decisions with PDF links
  - 6 pages: Jurisprudence, Chambres Civiles, Criminelle, Commerciale,
    Sociale, Section Administrative (~121 PDFs total)
  - PDFs downloaded and text extracted via pdfplumber
  - Also fetches 22 news posts with inline text

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import re
import time
import logging
import html
import tempfile
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import unquote

import requests
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ML.SectionDesComptes")

API_BASE = "https://www.coursupreme.ml/wp-json/wp/v2"
USER_AGENT = "LegalDataHunter/1.0 (research; https://github.com/ZachLaik/LegalDataHunter)"

MIN_TEXT_LENGTH = 150

# Pages containing court decisions with PDF links
DECISION_PAGES = [
    {"id": 2422, "name": "Jurisprudence", "chamber": "general"},
    {"id": 2983, "name": "Chambres Civiles", "chamber": "civile"},
    {"id": 2989, "name": "Chambre Criminelle", "chamber": "criminelle"},
    {"id": 2985, "name": "Chambre Commerciale", "chamber": "commerciale"},
    {"id": 2987, "name": "Chambre Sociale", "chamber": "sociale"},
    {"id": 3125, "name": "Section Administrative", "chamber": "administrative"},
]


def strip_html(raw_html: str) -> str:
    """Remove HTML tags, decode entities, collapse whitespace."""
    text = re.sub(r"<style[^>]*>.*?</style>", "", raw_html, flags=re.S | re.I)
    text = re.sub(r"<script[^>]*>.*?</script>", "", raw_html, flags=re.S | re.I)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"</?(p|div|h[1-6]|li|tr|td|th|blockquote)[^>]*>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    text = html.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def extract_pdf_urls(html_content: str) -> list:
    """Extract PDF URLs from HTML content."""
    return re.findall(r'href=["\']([^"\'\s]+\.pdf)["\']', html_content, re.I)


def title_from_filename(filename: str) -> str:
    """Extract a readable title from a PDF filename."""
    name = unquote(filename)
    name = re.sub(r"\.pdf$", "", name, flags=re.I)
    name = name.replace("CSM-", "").replace("_", " ").replace("-", " ")
    name = re.sub(r"\s+", " ", name).strip()
    return name


def download_pdf_text(url: str, session: requests.Session) -> Optional[str]:
    """Download a PDF and extract text using pdfplumber."""
    try:
        resp = session.get(url, timeout=60)
        resp.raise_for_status()
        if len(resp.content) > 50_000_000:
            logger.warning(f"PDF too large ({len(resp.content)} bytes): {url}")
            return None
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
            f.write(resp.content)
            tmp_path = f.name
        try:
            with pdfplumber.open(tmp_path) as pdf:
                pages_text = []
                for page in pdf.pages[:200]:
                    page_text = page.extract_text()
                    if page_text:
                        pages_text.append(page_text)
                return "\n\n".join(pages_text) if pages_text else None
        finally:
            os.unlink(tmp_path)
    except Exception as e:
        logger.warning(f"PDF extraction failed for {url}: {e}")
        return None


def wp_get(endpoint: str, params: dict = None, timeout: int = 30) -> requests.Response:
    """Make a GET request to the WordPress REST API."""
    url = f"{API_BASE}/{endpoint}" if not endpoint.startswith("http") else endpoint
    headers = {"User-Agent": USER_AGENT}
    resp = requests.get(url, params=params, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp


def paginate_wp(endpoint: str, params: dict = None, max_pages: int = 50) -> Generator[dict, None, None]:
    """Paginate through a WordPress REST API endpoint."""
    if params is None:
        params = {}
    params.setdefault("per_page", 100)
    page = 1
    while page <= max_pages:
        params["page"] = page
        try:
            resp = wp_get(endpoint, params=params)
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 400:
                break
            raise
        data = resp.json()
        if not data:
            break
        for item in data:
            yield item
        total_pages = int(resp.headers.get("X-WP-TotalPages", max_pages))
        if page >= total_pages:
            break
        page += 1
        time.sleep(1.0)


class SectionDesComptesScraper(BaseScraper):
    """
    Scraper for ML/SectionDesComptes — Cour Suprême du Mali.
    Country: ML
    URL: https://www.coursupreme.ml/

    Data types: case_law, doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self._session = requests.Session()
        self._session.headers["User-Agent"] = USER_AGENT

    def _normalize_decision(self, pdf_url: str, text: str, chamber: str, page_name: str) -> dict:
        """Normalize a court decision PDF into standard schema."""
        filename = pdf_url.split("/")[-1]
        title = title_from_filename(filename)

        # Try to extract date from filename (e.g., "du-14-juillet-2023" or "du-14-mai-2020")
        date_match = re.search(
            r"du[- ](\d{1,2})[- ](janvier|f[eé]vrier|mars|avril|mai|juin|juillet|ao[uû]t|septembre|octobre|novembre|d[eé]cembre)[- ](\d{4})",
            unquote(filename),
            re.I,
        )
        date_str = None
        if date_match:
            day, month_fr, year = date_match.groups()
            months = {
                "janvier": "01", "février": "02", "fevrier": "02", "mars": "03",
                "avril": "04", "mai": "05", "juin": "06", "juillet": "07",
                "août": "08", "aout": "08", "septembre": "09", "octobre": "10",
                "novembre": "11", "décembre": "12", "decembre": "12",
            }
            m = months.get(month_fr.lower(), "01")
            date_str = f"{year}-{m}-{int(day):02d}"

        doc_id = re.sub(r"[^a-zA-Z0-9]", "-", filename.lower()).strip("-")

        return {
            "_id": f"ml-csm-{doc_id}",
            "_source": "ML/SectionDesComptes",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str,
            "url": pdf_url,
            "chamber": chamber,
            "section": page_name,
        }

    def _normalize_post(self, post: dict) -> Optional[dict]:
        """Normalize a WordPress news post."""
        title = strip_html(post.get("title", {}).get("rendered", ""))
        content_html = post.get("content", {}).get("rendered", "")
        text = strip_html(content_html)
        if len(text) < MIN_TEXT_LENGTH:
            return None
        date_str = post.get("date", "")[:10] if post.get("date") else None
        post_id = post.get("id", 0)
        link = post.get("link", f"https://www.coursupreme.ml/?p={post_id}")
        return {
            "_id": f"ml-csm-post-{post_id}",
            "_source": "ML/SectionDesComptes",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date_str,
            "url": link,
            "chamber": None,
            "section": "news",
        }

    def normalize(self, raw: dict) -> dict:
        return raw

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all court decisions and news posts."""
        yielded = 0
        seen_pdfs = set()

        # 1. Fetch court decisions from decision pages
        for dp in DECISION_PAGES:
            logger.info(f"Fetching {dp['name']} (page {dp['id']})...")
            try:
                resp = wp_get(f"pages/{dp['id']}")
                content = resp.json()["content"]["rendered"]
                pdf_urls = extract_pdf_urls(content)
                logger.info(f"  Found {len(pdf_urls)} PDFs")

                for pdf_url in pdf_urls:
                    if pdf_url in seen_pdfs:
                        continue
                    seen_pdfs.add(pdf_url)

                    text = download_pdf_text(pdf_url, self._session)
                    if not text or len(text) < MIN_TEXT_LENGTH:
                        logger.debug(f"  Skipped (no text): {pdf_url.split('/')[-1][:50]}")
                        continue

                    record = self._normalize_decision(pdf_url, text, dp["chamber"], dp["name"])
                    yield record
                    yielded += 1
                    logger.info(f"  [{yielded}] {record['title'][:60]} ({len(text)} chars)")
                    time.sleep(1.0)
            except Exception as e:
                logger.error(f"  Error fetching {dp['name']}: {e}")

        # 2. Fetch news posts
        logger.info("Fetching news posts...")
        for post in paginate_wp("posts"):
            record = self._normalize_post(post)
            if record:
                yield record
                yielded += 1

        logger.info(f"fetch_all complete: {yielded} records")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Yield posts modified after 'since' date."""
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        params = {"after": f"{since}T00:00:00", "orderby": "modified"}
        for post in paginate_wp("posts", params=params):
            record = self._normalize_post(post)
            if record:
                yield record


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="ML/SectionDesComptes — Cour Suprême du Mali"
    )
    subparsers = parser.add_subparsers(dest="command")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    bp.add_argument("--sample-size", type=int, default=15, help="Sample size")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = SectionDesComptesScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            resp = wp_get("posts", {"per_page": 1})
            total = resp.headers.get("X-WP-Total", "?")
            logger.info(f"Posts: {total}")

            resp2 = wp_get("pages/2422")
            content = resp2.json()["content"]["rendered"]
            pdfs = extract_pdf_urls(content)
            logger.info(f"Jurisprudence page: {len(pdfs)} PDFs")

            logger.info("Connectivity test passed!")
        except Exception as e:
            logger.error(f"Connectivity test failed: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(
            sample_mode=args.sample,
            sample_size=args.sample_size,
        )
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
