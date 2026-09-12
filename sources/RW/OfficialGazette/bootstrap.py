#!/usr/bin/env python3
"""
RW/OfficialGazette -- Rwanda Legislation via RwandaLII (Laws.Africa)

Fetches ~500 Rwandan laws with full text from rwandalii.org.

Strategy:
  - Paginated listing at /legislation/?page=N (50 per page, ~10 pages)
  - Most law pages carry Akoma Ntoso (AKN) markup; extract text from akn-body
  - The rest were never marked up and only link the typeset original, so fall
    back to the attached {path}/source.pdf (issue #1596)

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap --full
  python bootstrap.py test
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.RW.OfficialGazette")

BASE_URL = "https://rwandalii.org"
LIST_URL = f"{BASE_URL}/legislation/"
MAX_PAGES = 15  # ~10 pages expected, extra margin


def strip_html(html_fragment: str) -> str:
    """Strip HTML tags and clean whitespace."""
    text = re.sub(r"<br\s*/?>", "\n", html_fragment)
    text = re.sub(r"</(p|div|li|tr|h[1-6]|article|section)>", "\n", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class RwandaLIIScraper(BaseScraper):
    """Scraper for RW/OfficialGazette -- Rwanda Legislation via RwandaLII."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })

    def _request(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(10)
        return None

    def _fetch_law_urls(self, page: int) -> List[str]:
        """Fetch law detail URLs from a listing page."""
        url = f"{LIST_URL}?page={page}"
        resp = self._request(url)
        if resp is None:
            return []
        # Extract /akn/rw/... links
        links = re.findall(r'href="(/akn/rw/[^"]+)"', resp.text)
        # Deduplicate while preserving order
        seen = set()
        unique = []
        for link in links:
            if link not in seen:
                seen.add(link)
                unique.append(link)
        return unique

    def _extract_law_content(self, html: str) -> Dict[str, str]:
        """Extract title, text, date, and metadata from a law page."""
        result = {"text": "", "title": "", "date": ""}

        # Extract title from h1
        m = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.DOTALL)
        if m:
            result["title"] = strip_html(m.group(1))

        # Extract date from meta or page content
        # Laws.Africa pages often have date in the URL or in metadata
        m = re.search(r'<time[^>]*datetime="([^"]+)"', html)
        if m:
            result["date"] = m.group(1)[:10]

        if not result["date"]:
            m = re.search(r'"datePublished"\s*:\s*"([^"]+)"', html)
            if m:
                result["date"] = m.group(1)[:10]

        # Extract full text from akn-body
        body_match = re.search(
            r'class="akn-body"[^>]*>(.*?)(?=<(?:footer|div\s+class="(?!akn-))|$)',
            html,
            re.DOTALL,
        )
        if body_match:
            result["text"] = strip_html(body_match.group(1))
        else:
            # Fallback: try akn-akomaNtoso container
            akn_match = re.search(
                r'class="akn-akomaNtoso"[^>]*>(.*?)</article>',
                html,
                re.DOTALL,
            )
            if akn_match:
                result["text"] = strip_html(akn_match.group(1))

        return result

    def _extract_source_pdf(self, path: str, frbr_uri: str) -> str:
        """Extract text from a law's attached source PDF.

        Roughly a third of RwandaLII's laws were never marked up in Akoma
        Ntoso — the page carries only metadata plus a link to the scanned/typeset
        original at {path}/source.pdf. Reading only akn-body silently dropped
        every one of them, which is a format boundary rather than a genuine
        absence of text.
        """
        resp = self._request(f"{BASE_URL}{path}/source.pdf")
        if resp is None:
            return ""
        if not resp.content.startswith(b"%PDF"):
            logger.warning(f"Attachment is not a PDF: {path}/source.pdf")
            return ""
        try:
            text = extract_pdf_markdown(
                "RW/OfficialGazette",
                frbr_uri,
                pdf_bytes=resp.content,
                table="legislation",
                force=True,
            )
        except Exception as e:
            logger.warning(f"PDF extraction failed for {path}: {e}")
            return ""
        return text or ""

    def _parse_date_from_url(self, url_path: str) -> str:
        """Try to extract a date from the AKN URL path."""
        # Pattern: /akn/rw/act/law/YYYY/NN/eng@YYYY-MM-DD
        m = re.search(r"eng@(\d{4}-\d{2}-\d{2})", url_path)
        if m:
            return m.group(1)
        # Fallback: extract year from path
        m = re.search(r"/(\d{4})/", url_path)
        if m:
            return f"{m.group(1)}-01-01"
        return ""

    def _classify_law_type(self, url_path: str) -> str:
        """Classify the law type from the AKN URL."""
        parts = url_path.strip("/").split("/")
        # /akn/rw/act/{type}/{year}/{number}/...
        if len(parts) >= 5:
            return parts[3]  # law, mo, reg, decree, etc.
        return "unknown"

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw.get("frbr_uri", ""),
            "_source": "RW/OfficialGazette",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "law_type": raw.get("law_type", ""),
            "url": raw.get("url", ""),
        }

    def fetch_all(
        self, max_records: int = None, skip_stored: bool = False
    ) -> Generator[Dict[str, Any], None, None]:
        """Walk the listing pages and yield each law with its full AKN text.

        skip_stored: skip the detail fetch for FRBR URIs already in the storage
        index. The listing is ~10 cheap requests while the details are ~340, so
        this turns a re-crawl into a near no-op — see fetch_updates().
        """
        count = 0
        skipped_stored = 0
        from_pdf = 0
        seen_uris = set()

        for page_num in range(1, MAX_PAGES + 1):
            law_paths = self._fetch_law_urls(page_num)
            if not law_paths:
                logger.info(f"No laws on page {page_num}, stopping pagination")
                break

            logger.info(f"Page {page_num}: {len(law_paths)} laws listed")

            for path in law_paths:
                if max_records and count >= max_records:
                    return

                # Extract FRBR URI (path without the point-in-time suffix)
                frbr_uri = re.sub(r"/eng@.*$", "", path)
                if frbr_uri in seen_uris:
                    continue
                seen_uris.add(frbr_uri)

                # The dedup key is _id, which is the FRBR URI, so the storage
                # index answers "have we already got this law?" without paying
                # for the detail page.
                if skip_stored and self.storage.exists(frbr_uri):
                    skipped_stored += 1
                    continue

                detail_url = f"{BASE_URL}{path}"
                resp = self._request(detail_url)
                if resp is None:
                    logger.warning(f"Failed to fetch: {path}")
                    continue

                extracted = self._extract_law_content(resp.text)
                if len(extracted["text"]) < 50:
                    # No AKN markup for this law — fall back to its source PDF.
                    extracted["text"] = self._extract_source_pdf(path, frbr_uri)
                    if len(extracted["text"]) >= 50:
                        from_pdf += 1

                if len(extracted["text"]) < 50:
                    logger.warning(
                        f"Insufficient text ({len(extracted.get('text', ''))} chars): "
                        f"{path} (no AKN body and no usable source PDF)"
                    )
                    continue

                date = extracted["date"] or self._parse_date_from_url(path)

                raw = {
                    "frbr_uri": frbr_uri,
                    "title": extracted["title"],
                    "text": extracted["text"],
                    "date": date,
                    "law_type": self._classify_law_type(path),
                    "url": detail_url,
                }
                count += 1
                yield raw

        if skipped_stored:
            logger.info(f"Skipped {skipped_stored} laws already in the storage index")
        logger.info(f"Completed: {count} laws fetched ({from_pdf} from source PDFs)")

    def fetch_updates(self, since=None) -> Generator[Dict[str, Any], None, None]:
        """Yield only laws we have not stored yet.

        `since` is deliberately unused: RwandaLII is a *consolidated* corpus, so
        a law's own enactment date says nothing about when Laws.Africa published
        it here (1959 agreements were added in 2023). The listing carries no
        published-at stamp either, so the only honest availability comparator is
        the seen-id checkpoint — which is also what append_only dedups on.
        """
        yield from self.fetch_all(skip_stored=True)

    def test(self) -> bool:
        law_paths = self._fetch_law_urls(1)
        if not law_paths:
            logger.error("Cannot fetch legislation listing from RwandaLII")
            return False

        logger.info(f"Listing OK: {len(law_paths)} laws on page 1")

        path = law_paths[0]
        resp = self._request(f"{BASE_URL}{path}")
        if resp:
            extracted = self._extract_law_content(resp.text)
            logger.info(
                f"Law OK: {path} "
                f"({len(extracted['text'])} chars, title={extracted['title'][:60]})"
            )
        else:
            logger.warning("Could not fetch sample law")

        return True


def main():
    parser = argparse.ArgumentParser(description="RW/OfficialGazette data fetcher (RwandaLII)")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = RwandaLIIScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        # Delegate to BaseScraper rather than writing records by hand: the
        # hand-rolled loop this replaces only ever wrote sample/*.json, so a
        # full crawl produced no data/records.jsonl for the fleet to ingest and
        # no storage index for dedup to stand on. See issue #1596.
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        # BaseScraper.update() derives `since` from status.yaml:last_run and
        # routes through fetch_updates(), writing like bootstrap does.
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
