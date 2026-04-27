#!/usr/bin/env python3
"""
AG/Legislation -- Antigua & Barbuda Laws

Fetches legislation from the official Ministry of Legal Affairs site at
laws.gov.ag. Laws are listed alphabetically and each links to a PDF.
Full text is extracted from PDFs via common.pdf_extract.

Endpoint:
  - Alphabetical listing: https://laws.gov.ag/laws/alphabetical/?letter={A-Z}
  - PDFs: https://laws.gov.ag/wp-content/uploads/.../*.pdf

Data:
  - ~1,100 acts
  - Full text extracted from PDFs
  - Language: English

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import ssl
import sys
import html as html_mod
import logging
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, List, Optional, Tuple
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AG.Legislation")

BASE_URL = "https://laws.gov.ag"
ALPHA_URL = f"{BASE_URL}/laws/alphabetical/"
LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

# Regex to extract links from HTML table rows pointing to PDFs
# Note: laws.gov.ag uses single quotes around href values
LINK_RE = re.compile(
    r"""<a[^>]+href=['"](https://laws\.gov\.ag/wp-content/uploads/[^'"]+\.pdf)['"][^>]*>"""
    r'(.*?)</a>',
    re.DOTALL | re.IGNORECASE,
)
TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


def strip_html(s: str) -> str:
    text = TAG_RE.sub(" ", s)
    text = html_mod.unescape(text)
    return WS_RE.sub(" ", text).strip()


def doc_id_from_url(pdf_url: str) -> str:
    """Derive a stable doc ID from the PDF filename."""
    fname = unquote(pdf_url.rsplit("/", 1)[-1])
    fname = fname.replace(".pdf", "").replace(".PDF", "")
    return fname


class AGLegislationScraper(BaseScraper):
    """Scraper for AG/Legislation -- Antigua & Barbuda Laws."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        # Create a requests Session with SSL verification disabled
        # (laws.gov.ag has certificate issues)
        import requests
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
        })
        # Suppress SSL warnings
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def _get(self, url: str, **kwargs) -> "requests.Response":
        self.rate_limiter.wait()
        resp = self.session.get(url, timeout=120, **kwargs)
        resp.raise_for_status()
        return resp

    def _list_laws(self) -> List[Tuple[str, str]]:
        """Scrape all (title, pdf_url) pairs from the alphabetical pages."""
        results = []
        seen_urls = set()
        for letter in LETTERS:
            url = f"{ALPHA_URL}?letter={letter}"
            logger.info(f"Listing letter {letter}...")
            try:
                resp = self._get(url)
            except Exception as e:
                logger.error(f"Failed to fetch letter {letter}: {e}")
                continue

            for match in LINK_RE.finditer(resp.text):
                pdf_url = match.group(1)
                title = strip_html(match.group(2))
                if pdf_url not in seen_urls and title:
                    seen_urls.add(pdf_url)
                    results.append((title, pdf_url))

        logger.info(f"Found {len(results)} unique laws across all letters")
        return results

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        doc_id = raw.get("doc_id", "")
        return {
            "_id": f"AG/Legislation/{doc_id}",
            "_source": "AG/Legislation",
            "_type": "legislation",
            "_fetched_at": now,
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": None,
            "url": raw.get("pdf_url", ""),
            "doc_id": doc_id,
            "pdf_url": raw.get("pdf_url", ""),
        }

    def _download_pdf(self, pdf_url: str) -> Optional[bytes]:
        """Download PDF bytes using our SSL-disabled session.

        The shared pdf_extract._fetch_pdf_bytes uses requests.get with default
        SSL verification, which fails on VPS where the CA store does not include
        the laws.gov.ag certificate chain.  By downloading here with
        self.session (verify=False) and passing pdf_bytes directly, we avoid
        that problem.
        """
        try:
            self.rate_limiter.wait()
            resp = self.session.get(pdf_url, timeout=120, stream=True)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"    PDF download failed: {e}")
            return None

        content = bytearray()
        max_size = 50_000_000  # 50 MB safety cap
        for chunk in resp.iter_content(chunk_size=65536):
            if not chunk:
                continue
            content.extend(chunk)
            if len(content) > max_size:
                logger.warning(f"    PDF exceeds 50 MB, skipping: {pdf_url}")
                return None
        return bytes(content)

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        limit = 15 if sample else None
        count = 0

        laws = self._list_laws()
        if sample:
            laws = laws[:20]  # fetch a few extra in case some fail

        for title, pdf_url in laws:
            if limit and count >= limit:
                break

            doc_id = doc_id_from_url(pdf_url)
            logger.info(f"  [{count+1}] {title} -> {doc_id}")

            # Download PDF ourselves using SSL-disabled session, then pass
            # raw bytes to extract_pdf_markdown so it skips its own download
            # (which would fail on VPS due to SSL verification).
            pdf_bytes = self._download_pdf(pdf_url)
            if pdf_bytes is None:
                logger.warning(f"    Skipping {doc_id} - PDF download failed")
                continue

            try:
                text = extract_pdf_markdown(
                    source="AG/Legislation",
                    source_id=doc_id,
                    pdf_bytes=pdf_bytes,
                    table="legislation",
                )
            except Exception as e:
                logger.warning(f"    PDF extraction failed for {doc_id}: {e}")
                text = None

            if not text or len(text.strip()) < 50:
                logger.warning(f"    Skipping {doc_id} - no/short text")
                continue

            record = self.normalize({
                "title": title,
                "text": text,
                "pdf_url": pdf_url,
                "doc_id": doc_id,
            })
            yield record
            count += 1
            logger.info(f"    OK ({len(text)} chars)")

        logger.info(f"Total records yielded: {count}")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """No update mechanism — the site has no modification dates."""
        logger.info("No incremental update support; use full refresh.")
        return
        yield


if __name__ == "__main__":
    scraper = AGLegislationScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        logger.info("Testing connectivity to laws.gov.ag...")
        try:
            resp = scraper._get(f"{ALPHA_URL}?letter=A")
            matches = LINK_RE.findall(resp.text)
            logger.info(f"Test OK: found {len(matches)} laws under letter A")
        except Exception as e:
            logger.error(f"Test FAILED: {e}")
            sys.exit(1)
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
