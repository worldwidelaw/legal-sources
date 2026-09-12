#!/usr/bin/env python3
"""
ZM/NationalAssembly-Acts — Zambia National Assembly Acts of Parliament

Fetches official Acts of Parliament from parliament.gov.zm.

Strategy:
  - Paginate through acts listing (49 pages, ~20 acts per page)
  - For each act, visit the node page and find the PDF link
  - Download PDF and extract text via pdfminer
  - SSL cert may be problematic — use verify=False as fallback

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import time
import html as html_mod
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin

import requests
import urllib3

# Suppress SSL warnings for parliament.gov.zm
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ZM.NationalAssembly-Acts")

BASE_URL = "https://www.parliament.gov.zm"
MAX_PAGES = 60  # Safety cap


def strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    if not text:
        return ""
    text = re.sub(r'<div[^>]*class=["\']act-number-appended["\'][^>]*>', ' ', text)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</(?:p|div|h[1-6]|li|tr|blockquote)>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_mod.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfminer."""
    try:
        from pdfminer.high_level import extract_text
        text = extract_text(io.BytesIO(pdf_bytes))
        if text and len(text.strip()) > 50:
            return text.strip()
    except Exception as e:
        logger.debug(f"PDF extraction failed: {e}")
    return ""


class ZambiaNationalAssemblyActsScraper(BaseScraper):
    """Scraper for Zambia National Assembly Acts of Parliament."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.verify = False  # SSL cert issues
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (research; +https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml",
        })

    def _get_listing_page(self, page: int) -> list:
        """Parse a listing page and return list of (node_url, title, act_number, year)."""
        url = f"{BASE_URL}/acts-of-parliament"
        if page > 0:
            url += f"?page={page}"

        try:
            resp = self.session.get(url, timeout=15)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch listing page {page}: {e}")
            return []

        html = resp.text
        acts = []

        # Only parse within the views content div
        views_match = re.search(
            r'<div class="view-content">(.*?)(?:</div>\s*</div>\s*<div class="item-list"|$)',
            html, re.DOTALL
        )
        if not views_match:
            return []

        views_html = views_match.group(1)

        # Parse links within views rows — look for field-content spans
        row_pattern = re.compile(
            r'<span class="field-content"><a href="(/node/(\d+))">(.*?)</a></span>',
            re.DOTALL
        )

        for m in row_pattern.finditer(views_html):
            node_path = m.group(1)
            node_id = m.group(2)
            raw_title = m.group(3)

            # Extract act number from embedded div
            act_num_match = re.search(r'\(\s*Act\s+No\.\s*(\d+)\s+of\s+(\d{4})\s*\)', raw_title)
            act_number = None
            year = None
            if act_num_match:
                act_number = f"Act No. {act_num_match.group(1)} of {act_num_match.group(2)}"
                year = act_num_match.group(2)

            title = strip_html(raw_title).strip()
            # Clean up the act number suffix
            title = re.sub(r'\(\s*Act No\.\s*\d+\s+of\s+\d{4}\s*\)', '', title).strip()

            node_url = urljoin(BASE_URL, node_path)
            acts.append({
                "node_url": node_url,
                "node_id": node_id,
                "title": title,
                "act_number": act_number,
                "year": year,
            })

        return acts

    def _fetch_act_pdf(self, node_url: str) -> Optional[str]:
        """Visit node page, find PDF link, download and extract text."""
        try:
            resp = self.session.get(node_url, timeout=15)
            resp.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Failed to fetch {node_url}: {e}")
            return None

        html = resp.text

        # Find act PDF link (in /sites/default/files/documents/acts/ or similar)
        pdf_links = re.findall(
            r'href="([^"]*(?:documents/acts|acts)[^"]*\.pdf)"',
            html, re.IGNORECASE
        )

        if not pdf_links:
            # Try any PDF link that's not a general publication
            pdf_links = re.findall(r'href="([^"]*\.pdf)"', html, re.IGNORECASE)
            # Filter out known non-act PDFs
            pdf_links = [l for l in pdf_links
                         if 'publications/' not in l
                         and 'pfm_' not in l
                         and 'strategic-plan' not in l]

        if not pdf_links:
            logger.warning(f"No PDF found at {node_url}")
            return None

        pdf_url = urljoin(BASE_URL, pdf_links[0])

        try:
            pdf_resp = self.session.get(pdf_url, timeout=30)
            if pdf_resp.status_code == 200 and len(pdf_resp.content) > 100:
                text = extract_pdf_text(pdf_resp.content)
                if text:
                    return text
                else:
                    logger.warning(f"PDF text extraction yielded no text: {pdf_url}")
        except requests.RequestException as e:
            logger.warning(f"Failed to download PDF {pdf_url}: {e}")

        return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all acts from all listing pages."""
        seen_nodes = set()
        for page in range(MAX_PAGES):
            acts = self._get_listing_page(page)
            if not acts:
                logger.info(f"No acts on page {page}, stopping.")
                break
            logger.info(f"Page {page}: {len(acts)} acts")

            for act in acts:
                if act["node_id"] in seen_nodes:
                    continue
                seen_nodes.add(act["node_id"])

                time.sleep(1.0)
                text = self._fetch_act_pdf(act["node_url"])
                if text:
                    act["text"] = text
                    yield act
                else:
                    logger.warning(f"Skipping {act['title']} — no extractable text")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent acts (first few pages only)."""
        seen_nodes = set()
        for page in range(5):  # Check first 5 pages for recent acts
            acts = self._get_listing_page(page)
            if not acts:
                break
            for act in acts:
                if act["node_id"] in seen_nodes:
                    continue
                seen_nodes.add(act["node_id"])
                if act.get("year"):
                    try:
                        act_year = int(act["year"])
                        if act_year < since.year:
                            return
                    except ValueError:
                        pass
                time.sleep(1.0)
                text = self._fetch_act_pdf(act["node_url"])
                if text:
                    act["text"] = text
                    yield act

    def normalize(self, raw: dict) -> dict:
        """Transform a raw act into a standardized record."""
        text = raw.get("text", "")
        if not text or len(text.strip()) < 100:
            return None

        year = raw.get("year")
        date = f"{year}-01-01" if year else None

        title = raw["title"]
        if raw.get("act_number"):
            title = f"{title} ({raw['act_number']})"

        return {
            "_id": f"zm-parliament-act-{raw['node_id']}",
            "_source": "ZM/NationalAssembly-Acts",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": raw["node_url"],
            "act_number": raw.get("act_number"),
            "year": year,
            "language": "en",
            "jurisdiction": "Zambia",
        }


# ── CLI ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    import argparse

    parser = argparse.ArgumentParser(description="Zambia National Assembly Acts scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample records only")
    parser.add_argument("--full", action="store_true",
                        help="Full bootstrap")
    args = parser.parse_args()

    scraper = ZambiaNationalAssemblyActsScraper()

    if args.command == "test":
        print("Testing connectivity to parliament.gov.zm...")
        try:
            resp = scraper.session.get(f"{BASE_URL}/acts-of-parliament", timeout=15)
            print(f"OK: HTTP {resp.status_code}, {len(resp.text)} bytes")
            acts = scraper._get_listing_page(0)
            print(f"Found {len(acts)} acts on page 0")
            if acts:
                print(f"  First: {acts[0]['title']} ({acts[0]['act_number']})")
            print("Test PASSED")
        except Exception as e:
            print(f"Test FAILED: {e}")
            sys.exit(1)

    elif args.command == "bootstrap":
        if args.sample:
            logger.info("Running sample bootstrap...")
            records = []
            for raw in scraper.fetch_all():
                record = scraper.normalize(raw)
                if record and record.get("text"):
                    records.append(record)
                    logger.info(f"  [{len(records)}] {record['title'][:60]}... ({len(record['text'])} chars)")
                    if len(records) >= 15:
                        break

            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)
            for i, rec in enumerate(records):
                path = sample_dir / f"{i+1:03d}_{rec['_id']}.json"
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(rec, f, indent=2, ensure_ascii=False)
            logger.info(f"Saved {len(records)} sample records to {sample_dir}")
            for rec in records:
                text_len = len(rec.get("text", ""))
                print(f"  {rec['_id']}: {rec['title'][:70]} ({text_len} chars)")
        else:
            logger.info("Running full bootstrap...")
            stats = scraper.bootstrap(sample_mode=False)
            print(json.dumps(stats, indent=2))

    elif args.command == "update":
        logger.info("Running update...")
        stats = scraper.bootstrap(sample_mode=False)
        print(json.dumps(stats, indent=2))
