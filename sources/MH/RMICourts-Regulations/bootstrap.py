#!/usr/bin/env python3
"""
MH/RMICourts-Regulations -- Republic of the Marshall Islands Administrative Regulations

Fetches administrative regulations, court rules, standing orders, and procedural
guidance from the RMI Judiciary "Selected Regulations" page (rmicourts.org).
Documents are individual PDFs hosted on rmicourts.org/wp-content/uploads/.

Strategy:
  1. Fetch the Selected Regulations HTML listing
  2. Extract PDF links (title + URL) from the main content area, skipping nav menus
  3. Download each PDF and extract full text via the shared pdf_extract helper
  4. Guess publication date from the URL/filename where possible

Usage:
  python bootstrap.py bootstrap          # Full pull (~80 documents)
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import json
import time
import logging
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MH.RMICourts-Regulations")

USER_AGENT = "Mozilla/5.0 (compatible; LegalDataHunter/1.0; open-data research)"
LISTING_URL = "https://rmicourts.org/selected-regulations/"
REQUEST_DELAY = 1.5

# Date heuristics on the PDF URL / filename
_DATE_PATH_RE = re.compile(r"/(20\d{2})/(\d{2})/")          # /uploads/2023/03/
_DATE_PREFIX_RE = re.compile(r"/(\d{2})(\d{2})(\d{2})-")     # /250513-...  (YYMMDD)
_DATE_YEAR_RE = re.compile(r"((?:19|20)\d{2})")             # bare year in filename


def _guess_date(url: str) -> Optional[str]:
    """Best-effort publication date from URL path or filename."""
    m = _DATE_PATH_RE.search(url)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    m = _DATE_PREFIX_RE.search(url)
    if m:
        yy, mm, dd = m.group(1), m.group(2), m.group(3)
        if mm <= "12" and dd <= "31":
            return f"20{yy}-{mm}-{dd}"
    fname = url.rsplit("/", 1)[-1]
    m = _DATE_YEAR_RE.search(fname)
    if m:
        return m.group(1)
    return None


def _fetch_listing() -> list:
    """Fetch the Selected Regulations page and return [{url, title, date}].

    Only PDF links inside the <main> content area are returned; navigation
    menus are stripped first so menu entries don't pollute the result.
    """
    resp = requests.get(LISTING_URL, headers={"User-Agent": USER_AGENT}, timeout=40)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    # Drop navigation menus (they may contain stray PDF links).
    for nav in soup.find_all(
        class_=lambda c: c and ("navbar" in " ".join(c) or "menu" in " ".join(c))
    ):
        nav.decompose()

    main = soup.find("main") or soup

    docs = []
    seen = set()
    for a in main.find_all("a", href=True):
        href = a["href"]
        if not href.lower().endswith(".pdf"):
            continue
        full_url = urljoin(LISTING_URL, href)
        if full_url in seen:
            continue
        seen.add(full_url)
        title = re.sub(r"\s+", " ", a.get_text(strip=True))
        if not title:
            # Fall back to a humanised filename.
            title = full_url.rsplit("/", 1)[-1].rsplit(".", 1)[0].replace("_", " ").replace("-", " ")
        docs.append({"url": full_url, "title": title, "date": _guess_date(full_url)})

    return docs


class RMIRegulationsScraper(BaseScraper):
    """Scraper for RMI Judiciary Selected Regulations (PDF documents)."""

    def fetch_all(self) -> Generator[dict, None, None]:
        logger.info("Fetching Selected Regulations listing...")
        docs = _fetch_listing()
        logger.info("Found %d regulation PDFs", len(docs))
        for doc in docs:
            yield doc

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        cutoff = since.strftime("%Y-%m-%d")
        for doc in self.fetch_all():
            d = doc.get("date")
            # Only filter when we have a full ISO date; keep coarse/None dates.
            if d and len(d) == 10 and d < cutoff:
                continue
            yield doc

    def normalize(self, raw: dict) -> dict:
        url = raw["url"]
        title = raw["title"]
        doc_id = "rmireg-" + hashlib.sha1(url.encode()).hexdigest()[:16]

        logger.info("Processing: %s", title[:70])
        try:
            pdf_bytes = requests.get(
                url, headers={"User-Agent": USER_AGENT}, timeout=60
            ).content
        except Exception as e:
            logger.warning("Download failed for %s: %s", url, e)
            return None

        if not pdf_bytes[:5].startswith(b"%PDF"):
            logger.warning("Not a valid PDF (header %r): %s", pdf_bytes[:5], url)
            return None

        try:
            text = extract_pdf_markdown(
                source="MH/RMICourts-Regulations",
                source_id=doc_id,
                pdf_bytes=pdf_bytes,
                table="legislation",
            )
        except Exception as e:
            logger.warning("Extraction failed for %s: %s", url, e)
            return None

        if not text or len(text) < 50:
            logger.warning("Insufficient text (%d chars): %s", len(text or ""), title[:50])
            return None

        time.sleep(REQUEST_DELAY)

        return {
            "_id": doc_id,
            "_source": "MH/RMICourts-Regulations",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": url,
            "country": "MH",
            "language": "en",
            "issuing_body": "Republic of the Marshall Islands Judiciary",
        }


def main():
    source_dir = Path(__file__).parent
    scraper = RMIRegulationsScraper(source_dir)

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        logger.info("Testing connectivity to rmicourts.org...")
        try:
            docs = _fetch_listing()
            logger.info("Found %d regulation PDFs", len(docs))
            if docs:
                logger.info("First: %s -> %s", docs[0]["title"][:50], docs[0]["url"])
            print("OK")
        except Exception as e:
            logger.error("Connection test failed: %s", e)
            sys.exit(1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        result = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        logger.info("Bootstrap complete: %s", json.dumps(result, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
