#!/usr/bin/env python3
"""
IT/AgenziaEntrate -- Italian Revenue Agency Tax Doctrine

Fetches tax doctrine documents from Agenzia delle Entrate:
  - Interpelli (tax rulings / risposte agli interpelli): ~300+ per year since 2018
  - Circolari (circulars): ~15 per year

Strategy:
  - Scrape Liferay asset-publisher paginated listing pages
  - Download PDF documents and extract full text via pdfplumber
  - Normalize to standard schema

URL patterns:
  - Interpelli listing: /portale/normativa-e-prassi/risposte-agli-interpelli/interpelli
    Paginated via Liferay asset publisher portlet (INSTANCE uG5PsLdoy927)
  - Circolari listing: /portale/web/guest/normativa-e-prassi/circolari
    Paginated via Liferay asset publisher portlet (INSTANCE mFmHL8QS3lq4)
  - PDFs: /portale/documents/{groupId}/{folderId}/{filename}.pdf/{uuid}

License: Open Government Data

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import io
import subprocess
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List, Dict
from html import unescape

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

# Optional PDF extraction
try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False
    logging.warning("pdfplumber not installed -- PDF text extraction unavailable")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IT.AgenziaEntrate")

BASE_URL = "https://www.agenziaentrate.gov.it"

# Liferay asset-publisher portlet configuration for each doc type
PORTLET_CONFIG = {
    "interpello": {
        "listing_path": "/portale/normativa-e-prassi/risposte-agli-interpelli/interpelli",
        "instance_id": "uG5PsLdoy927",
        "meta_re": re.compile(
            r"<strong>\s*(Risposta\s+n\.\s*(\d+)\s+del\s+([\d/]+))\s*</strong>",
            re.IGNORECASE,
        ),
        "prefix": "Risposta",
    },
    "circolare": {
        "listing_path": "/portale/web/guest/normativa-e-prassi/circolari",
        "instance_id": "mFmHL8QS3lq4",
        "meta_re": re.compile(
            r"<strong>\s*(Circolare\s+n\.\s*(\d+)\s+del\s+([\d/]+))\s*</strong>",
            re.IGNORECASE,
        ),
        "prefix": "Circolare",
    },
}

# Regex to grab <a href="...pdf...">title</a> after a meta <strong>
PDF_LINK_RE = re.compile(
    r'<a\s+href="(https://www\.agenziaentrate\.gov\.it/portale/documents/[^"]+\.pdf[^"]*)"[^>]*>'
    r"(.*?)</a>",
    re.IGNORECASE | re.DOTALL,
)

# Rate-limit delay between HTTP requests (seconds)
DELAY = 2.0


class AgenziaEntrateScraper(BaseScraper):
    """
    Scraper for IT/AgenziaEntrate -- Italian Revenue Agency tax doctrine.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    # ── HTTP helpers ──────────────────────────────────────────────────

    @staticmethod
    def _curl_get(url: str, timeout: int = 60) -> Optional[bytes]:
        """Fetch a URL via curl (avoids Python SSL / TLS hassles)."""
        try:
            result = subprocess.run(
                ["curl", "-sL", "--max-time", str(timeout), url],
                capture_output=True,
                timeout=timeout + 10,
            )
            if result.returncode == 0 and result.stdout:
                return result.stdout
        except Exception as e:
            logger.warning("curl failed for %s: %s", url[:100], e)
        return None

    def _fetch_html(self, url: str) -> Optional[str]:
        """Fetch an HTML page, respecting rate limit."""
        time.sleep(DELAY)
        data = self._curl_get(url, timeout=60)
        if data:
            return data.decode("utf-8", errors="replace")
        return None

    def _fetch_pdf_bytes(self, url: str) -> Optional[bytes]:
        """Download a PDF, return raw bytes or None."""
        time.sleep(DELAY)
        data = self._curl_get(url, timeout=120)
        if data and data[:4] == b"%PDF":
            return data
        return None

    # ── PDF text extraction ───────────────────────────────────────────

    @staticmethod
    def _extract_text_from_pdf(pdf_bytes: bytes) -> Optional[str]:
        if not HAS_PDFPLUMBER:
            return None
        try:
            parts: list[str] = []
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    t = page.extract_text()
                    if t:
                        parts.append(t)
            text = "\n\n".join(parts)
            # normalise whitespace
            text = re.sub(r"[ \t]+", " ", text)
            text = re.sub(r"\n\s*\n+", "\n\n", text)
            return text.strip() if len(text) > 100 else None
        except Exception as e:
            logger.warning("PDF extraction error: %s", e)
            return None

    # ── Listing page parsing ──────────────────────────────────────────

    def _build_paginated_url(self, doc_type: str, page_num: int, page_size: int = 20) -> str:
        """Build a Liferay asset-publisher paginated URL."""
        cfg = PORTLET_CONFIG[doc_type]
        iid = cfg["instance_id"]
        portlet = f"com_liferay_asset_publisher_web_portlet_AssetPublisherPortlet_INSTANCE_{iid}"
        base = f"{BASE_URL}{cfg['listing_path']}"
        params = (
            f"p_p_id={portlet}"
            f"&p_p_lifecycle=0&p_p_state=normal&p_p_mode=view"
            f"&_{portlet}_cur={page_num}"
            f"&_{portlet}_delta={page_size}"
        )
        return f"{base}?{params}"

    def _parse_listing_page(self, html: str, doc_type: str) -> List[Dict]:
        """
        Parse a single listing page and return document metadata dicts.

        Each entry has:
          meta_text  – e.g. "Risposta n. 84 del 25/03/2026"
          doc_number – e.g. "84"
          raw_date   – e.g. "25/03/2026"
          pdf_url    – full URL to the PDF
          title      – link text (subject description)
        """
        cfg = PORTLET_CONFIG[doc_type]
        meta_re = cfg["meta_re"]
        entries: List[Dict] = []

        # Split the HTML on each <strong>Risposta/Circolare ...</strong> block
        # and grab the next <a> with a PDF URL
        meta_matches = list(meta_re.finditer(html))
        for i, m in enumerate(meta_matches):
            doc_number = m.group(2)
            raw_date = m.group(3)
            # Slice html from this match to the next one (or end)
            end = meta_matches[i + 1].start() if i + 1 < len(meta_matches) else len(html)
            segment = html[m.end() : end]

            pdf_match = PDF_LINK_RE.search(segment)
            if not pdf_match:
                continue

            pdf_url = pdf_match.group(1).split("?")[0]  # strip query params
            title = pdf_match.group(2).strip()
            title = unescape(title).replace("\xa0", " ").replace("&nbsp;", " ")
            title = re.sub(r"\s+", " ", title).strip()

            entries.append(
                {
                    "doc_type": doc_type,
                    "doc_number": doc_number,
                    "raw_date": raw_date,
                    "pdf_url": pdf_url,
                    "title": title,
                }
            )

        return entries

    def _parse_date(self, raw: str) -> str:
        """Convert 'DD/MM/YYYY' or 'D/M/YYYY' to 'YYYY-MM-DD'."""
        parts = raw.strip().split("/")
        if len(parts) == 3:
            try:
                d, mo, y = int(parts[0]), int(parts[1]), int(parts[2])
                return f"{y:04d}-{mo:02d}-{d:02d}"
            except ValueError:
                pass
        return ""

    # ── Core fetch logic ──────────────────────────────────────────────

    def _iter_listing(self, doc_type: str, max_pages: int = 200) -> Generator[Dict, None, None]:
        """
        Iterate over all listing pages for a doc type,
        yielding raw entry dicts (without full text yet).
        """
        page_size = 20
        for page_num in range(1, max_pages + 1):
            url = self._build_paginated_url(doc_type, page_num, page_size)
            logger.info("Listing %s page %d ...", doc_type, page_num)
            html = self._fetch_html(url)
            if not html:
                logger.warning("Failed to fetch listing page %d for %s", page_num, doc_type)
                break

            entries = self._parse_listing_page(html, doc_type)
            if not entries:
                logger.info("No more entries on page %d for %s — done.", page_num, doc_type)
                break

            for entry in entries:
                yield entry

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents (interpelli + circolari) with full text."""
        count = 0
        for doc_type in ["interpello", "circolare"]:
            for entry in self._iter_listing(doc_type):
                # Download PDF and extract text
                pdf_bytes = self._fetch_pdf_bytes(entry["pdf_url"])
                if not pdf_bytes:
                    logger.warning("PDF download failed: %s", entry["pdf_url"][:80])
                    continue

                text = self._extract_text_from_pdf(pdf_bytes)
                if not text or len(text) < 200:
                    logger.warning("Text too short for %s n.%s", doc_type, entry["doc_number"])
                    continue

                entry["text"] = text
                yield entry
                count += 1

                if count % 50 == 0:
                    logger.info("Progress: %d documents fetched", count)

        logger.info("Fetch complete: %d total documents", count)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent documents only (first few pages of each type)."""
        for doc_type in ["interpello", "circolare"]:
            for entry in self._iter_listing(doc_type, max_pages=3):
                date_str = self._parse_date(entry["raw_date"])
                if date_str:
                    try:
                        pub = datetime.strptime(date_str, "%Y-%m-%d")
                        if pub < since.replace(tzinfo=None):
                            continue
                    except ValueError:
                        pass

                pdf_bytes = self._fetch_pdf_bytes(entry["pdf_url"])
                if not pdf_bytes:
                    continue
                text = self._extract_text_from_pdf(pdf_bytes)
                if not text or len(text) < 200:
                    continue
                entry["text"] = text
                yield entry

    # ── Normalisation ─────────────────────────────────────────────────

    def normalize(self, raw: dict) -> dict:
        doc_type = raw["doc_type"]
        doc_number = raw["doc_number"]
        date_str = self._parse_date(raw["raw_date"])
        year = int(date_str[:4]) if len(date_str) >= 4 else 0
        title = raw.get("title", "")

        type_label = {"interpello": "Interpello", "circolare": "Circolare"}.get(doc_type, doc_type)
        if not title or len(title) < 10:
            title = f"{type_label} n. {doc_number}/{year}"

        doc_id = f"IT:AE:{doc_type}:{doc_number}_{year}"

        pdf_url = raw["pdf_url"]
        if not pdf_url.startswith("http"):
            pdf_url = f"{BASE_URL}{pdf_url}"

        return {
            "_id": doc_id,
            "_source": "IT/AgenziaEntrate",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw["text"],
            "date": date_str,
            "url": pdf_url,
            "doc_id": doc_id,
            "doc_type": doc_type,
            "doc_number": doc_number,
            "year": year,
            "language": "it",
            "authority": "Agenzia delle Entrate",
            "country": "IT",
        }

    # ── Test ──────────────────────────────────────────────────────────

    def test_connection(self):
        print("Testing Agenzia delle Entrate endpoints...\n")

        for doc_type, cfg in PORTLET_CONFIG.items():
            url = self._build_paginated_url(doc_type, 1, 5)
            print(f"1. Listing {doc_type} page 1 ...")
            html = self._fetch_html(url)
            if not html:
                print("   FAILED to fetch listing page")
                continue
            entries = self._parse_listing_page(html, doc_type)
            print(f"   Found {len(entries)} entries")
            if entries:
                e = entries[0]
                print(f"   First: {e['doc_type']} n.{e['doc_number']} del {e['raw_date']}")
                print(f"   Title: {e['title'][:80]}")
                print(f"   PDF:   {e['pdf_url'][:100]}")

                # Download + extract first PDF
                print("\n2. Downloading PDF ...")
                pdf_bytes = self._fetch_pdf_bytes(e["pdf_url"])
                if pdf_bytes:
                    print(f"   OK — {len(pdf_bytes)} bytes")
                    text = self._extract_text_from_pdf(pdf_bytes)
                    if text:
                        print(f"   Extracted {len(text)} chars")
                        print(f"   Sample: {text[:200]}...")
                    else:
                        print("   FAILED to extract text")
                else:
                    print("   FAILED to download PDF")
            print()

        print("Test complete!")


# ── CLI entry-point ───────────────────────────────────────────────────

def main():
    scraper = AgenziaEntrateScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
