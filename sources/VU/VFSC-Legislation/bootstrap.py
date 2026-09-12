#!/usr/bin/env python3
"""
VU/VFSC-Legislation -- Vanuatu Financial Services Commission Legislation

Fetches financial services legislation from the VFSC WordPress site.
Covers ~180+ PDF documents: consolidated acts, amendments, regulations,
and guidelines in English and French.

Strategy:
  1. Fetch the Legislation page via WP REST API (page ID 3208)
  2. Extract all PDF links from the rendered HTML content
  3. Also scrape the HTML page directly for any links not in the API
  4. Download each PDF and extract full text via common/pdf_extract
  5. Guess publication date from URL path or filename

Usage:
  python bootstrap.py bootstrap          # Full pull (~180 documents)
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
from typing import Generator, Optional, List, Dict, Any
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
logger = logging.getLogger("legal-data-hunter.VU.VFSC-Legislation")

USER_AGENT = "Mozilla/5.0 (compatible; LegalDataHunter/1.0; open-data research)"
LISTING_URL = "https://www.vfsc.vu/legislation/"
WP_API_URL = "https://www.vfsc.vu/wp-json/wp/v2/pages/3208"
REQUEST_DELAY = 1.5

# Skip non-legislation PDFs (forms, licensee lists, remittance instructions)
SKIP_PATTERNS = [
    "application-form", "application form", "personal-questionnaire",
    "licensee-list", "remittance-instructions", "current-register",
]

# Date heuristics
_DATE_PATH_RE = re.compile(r"/uploads/(20\d{2})/(\d{2})/")
_DATE_YEAR_RE = re.compile(r"((?:19|20)\d{2})")


def _guess_date(url: str) -> Optional[str]:
    """Best-effort publication date from URL path or filename."""
    m = _DATE_PATH_RE.search(url)
    if m:
        return "{}-{}".format(m.group(1), m.group(2))
    fname = url.rsplit("/", 1)[-1]
    m = _DATE_YEAR_RE.search(fname)
    if m:
        return m.group(1)
    return None


def _should_skip(url: str, title: str) -> bool:
    """Skip non-legislation PDFs like application forms and licensee lists."""
    combined = (url + " " + title).lower()
    return any(pat in combined for pat in SKIP_PATTERNS)


def _fetch_listing() -> List[Dict[str, Any]]:
    """Fetch the legislation page and extract all PDF links.

    Uses both the WP REST API and direct HTML scraping to ensure complete
    coverage of all PDF links.
    """
    headers = {"User-Agent": USER_AGENT}
    docs = []
    seen = set()

    # Method 1: WP REST API (gets rendered HTML content)
    try:
        resp = requests.get(WP_API_URL, headers=headers, timeout=40)
        resp.raise_for_status()
        page_data = resp.json()
        content_html = page_data.get("content", {}).get("rendered", "")
        if content_html:
            soup = BeautifulSoup(content_html, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if not href.lower().endswith(".pdf"):
                    continue
                full_url = urljoin(LISTING_URL, href)
                if full_url in seen:
                    continue
                title = re.sub(r"\s+", " ", a.get_text(strip=True))
                if not title:
                    title = full_url.rsplit("/", 1)[-1].rsplit(".", 1)[0].replace("-", " ").replace("_", " ")
                if _should_skip(full_url, title):
                    continue
                seen.add(full_url)
                docs.append({"url": full_url, "title": title, "date": _guess_date(full_url)})
            logger.info("WP API: found %d PDF links", len(docs))
    except Exception as e:
        logger.warning("WP API fetch failed, falling back to HTML: %s", e)

    # Method 2: Direct HTML scraping (catches anything the API missed)
    try:
        time.sleep(REQUEST_DELAY)
        resp = requests.get(LISTING_URL, headers=headers, timeout=40)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Remove navigation
        for nav in soup.find_all(class_=lambda c: c and ("navbar" in " ".join(c) or "menu" in " ".join(c))):
            nav.decompose()

        main = soup.find("main") or soup.find("article") or soup
        for a in main.find_all("a", href=True):
            href = a["href"]
            if not href.lower().endswith(".pdf"):
                continue
            full_url = urljoin(LISTING_URL, href)
            if full_url in seen:
                continue
            title = re.sub(r"\s+", " ", a.get_text(strip=True))
            if not title:
                title = full_url.rsplit("/", 1)[-1].rsplit(".", 1)[0].replace("-", " ").replace("_", " ")
            if _should_skip(full_url, title):
                continue
            seen.add(full_url)
            docs.append({"url": full_url, "title": title, "date": _guess_date(full_url)})
        logger.info("HTML scrape: total %d unique PDF links", len(docs))
    except Exception as e:
        logger.warning("HTML scrape failed: %s", e)

    return docs


class VFSCLegislationScraper(BaseScraper):
    """Scraper for VFSC Legislation (PDF documents)."""

    def fetch_all(self) -> Generator[dict, None, None]:
        logger.info("Fetching VFSC Legislation listing...")
        docs = _fetch_listing()
        logger.info("Found %d legislation PDFs", len(docs))
        for doc in docs:
            yield doc

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        cutoff = since.strftime("%Y-%m-%d")
        for doc in self.fetch_all():
            d = doc.get("date")
            if d and len(d) == 10 and d < cutoff:
                continue
            yield doc

    def normalize(self, raw: dict) -> Optional[dict]:
        url = raw["url"]
        title = raw["title"]
        doc_id = "vfsc-leg-" + hashlib.sha1(url.encode()).hexdigest()[:16]

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
                source="VU/VFSC-Legislation",
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

        # Detect language from title or URL
        language = "en"
        lower_title = title.lower()
        lower_url = url.lower()
        if any(kw in lower_title or kw in lower_url for kw in ["loi-", "francaise", "français", "version-francaise", "_societe", "_caisses", "_commerce"]):
            language = "fr"

        return {
            "_id": doc_id,
            "_source": "VU/VFSC-Legislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": url,
            "country": "VU",
            "language": language,
            "issuing_body": "Vanuatu Financial Services Commission",
        }


def main():
    source_dir = Path(__file__).parent
    scraper = VFSCLegislationScraper(source_dir)

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        logger.info("Testing connectivity to vfsc.vu...")
        try:
            docs = _fetch_listing()
            logger.info("Found %d legislation PDFs", len(docs))
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
        print("Unknown command: {}".format(command))
        sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
