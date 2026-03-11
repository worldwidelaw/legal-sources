#!/usr/bin/env python3
"""
AT/Bundesgesetzblatt -- Austrian Federal Law Gazette Data Fetcher

Fetches authentic gazette publications from the RIS OGD API v2.6.
Since 2004, the electronic BGBl is legally binding (authentisch).

Strategy:
  - Bootstrap: Paginates through BgblAuth application in Bundesrecht endpoint.
  - Update: Uses ImRisSeit filter for recently published gazette entries.
  - Sample: Fetches 10+ records from all three parts (I, II, III).

API: https://data.bka.gv.at/ris/api/v2.6/
Docs: https://data.bka.gv.at/ris/ogd/v2.6/Documents/Dokumentation_OGD-RIS_API.pdf

Usage:
  python bootstrap.py bootstrap          # Full initial pull (~18K records)
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update             # Incremental update (last month)
  python bootstrap.py test-api           # Quick API connectivity test
"""

import sys
import json
import logging
import time
import re
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from xml.etree import ElementTree as ET

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AT.bundesgesetzblatt")

# RIS OGD API v2.6
API_BASE = "https://data.bka.gv.at/ris/api/v2.6"

# BGBl parts
BGBL_PARTS = {
    "Teil1": "BGBl I",   # Federal laws
    "Teil2": "BGBl II",  # Regulations
    "Teil3": "BGBl III", # International law
}


class BundesgesetzblattScraper(BaseScraper):
    """
    Scraper for AT/Bundesgesetzblatt -- Austrian Federal Law Gazette.
    Country: AT
    URL: https://www.ris.bka.gv.at/Bgbl-Auth/

    Data types: legislation
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=API_BASE,
            headers={"User-Agent": "WorldWideLaw/1.0 (Open Data Research)"},
            timeout=60,
        )

    # -- API helpers --------------------------------------------------------

    def _paginate(
        self,
        extra_params=None,
        max_pages=None,
    ):
        """
        Generator that paginates through BgblAuth records.

        Yields individual document references (raw dicts from the API).
        """
        page = 1
        total_hits = None

        while True:
            if max_pages and page > max_pages:
                logger.info(f"Reached max_pages={max_pages}, stopping pagination")
                return

            params = {
                "Applikation": "BgblAuth",
                "DokumenteProSeite": "OneHundred",
                "Seitennummer": str(page),
            }
            if extra_params:
                params.update(extra_params)

            self.rate_limiter.wait()

            try:
                resp = self.client.get("/Bundesrecht", params=params)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.error(f"API error on page {page}: {e}")
                # Retry once after a pause
                time.sleep(5)
                try:
                    resp = self.client.get("/Bundesrecht", params=params)
                    resp.raise_for_status()
                    data = resp.json()
                except Exception as e2:
                    logger.error(f"Retry failed: {e2}")
                    return

            search_result = data.get("OgdSearchResult", {})
            doc_results = search_result.get("OgdDocumentResults", {})

            # Parse total hits on first page
            if total_hits is None:
                hits_info = doc_results.get("Hits", {})
                try:
                    total_hits = int(hits_info.get("#text", "0"))
                except (ValueError, TypeError):
                    total_hits = 0
                logger.info(f"BgblAuth: {total_hits} total hits")
                if total_hits == 0:
                    return

            # Extract documents
            docs = doc_results.get("OgdDocumentReference", [])
            if not isinstance(docs, list):
                docs = [docs] if docs else []

            if not docs:
                logger.info(f"No more documents on page {page}")
                return

            for doc in docs:
                doc_data = doc.get("Data", {})
                if doc_data:
                    yield doc_data

            # Check if we've fetched all pages
            fetched_so_far = page * 100
            if fetched_so_far >= total_hits:
                logger.info(f"Fetched all {total_hits} BgblAuth records")
                return

            page += 1
            logger.info(f"  Page {page} ({fetched_so_far}/{total_hits} fetched)")

    # -- Document parsing ---------------------------------------------------

    def _parse_document(self, raw):
        """Parse a BgblAuth API response into a flat dict."""
        meta = raw.get("Metadaten", {})
        tech = meta.get("Technisch", {})
        allg = meta.get("Allgemein", {})
        br = meta.get("Bundesrecht", {})
        bgbl = br.get("BgblAuth", {})

        # Extract content URLs
        content_urls = self._extract_content_urls(raw)

        # Map Teil to human-readable part
        teil = bgbl.get("Teil", "")
        teil_name = BGBL_PARTS.get(teil, teil)

        return {
            "doc_id": tech.get("ID", ""),
            "organ": tech.get("Organ", ""),
            "title": br.get("Kurztitel", ""),
            "title_full": br.get("Titel", ""),
            "eli": br.get("Eli", ""),
            "document_url": allg.get("DokumentUrl", ""),
            "bgbl_number": bgbl.get("Bgblnummer", ""),
            "teil": teil,
            "teil_name": teil_name,
            "date_published": bgbl.get("Ausgabedatum", ""),
            "typ": bgbl.get("Typ", ""),
            "alte_dokumentnummer": bgbl.get("AlteDokumentnummer", ""),
            "content_urls": content_urls,
        }

    def _extract_content_urls(self, raw):
        """Extract document content URLs (XML/HTML/RTF/PDF) from API response."""
        urls = {}
        doc_liste = raw.get("Dokumentliste", {})
        content_refs = doc_liste.get("ContentReference", [])
        if not isinstance(content_refs, list):
            content_refs = [content_refs] if content_refs else []

        for cr in content_refs:
            content_urls = cr.get("Urls", {}).get("ContentUrl", [])
            if not isinstance(content_urls, list):
                content_urls = [content_urls] if content_urls else []
            for cu in content_urls:
                dtype = cu.get("DataType", "")
                url = cu.get("Url", "")
                if dtype and url:
                    urls[dtype.lower()] = url

        return urls

    def _download_full_text(self, content_urls):
        """
        Download and extract full text from content URLs.

        Prefers XML format for structured text extraction. Falls back to
        HTML if XML is not available. Returns cleaned plain text.
        """
        # Try XML first (cleanest for text extraction)
        xml_url = content_urls.get("xml")
        if xml_url:
            try:
                self.rate_limiter.wait()
                resp = self.client.get(xml_url)
                resp.raise_for_status()

                # Parse XML and extract text
                root = ET.fromstring(resp.content)
                text_parts = []

                # Extract text from all relevant elements
                # BGBl uses tags like: ueberschrift, absatz, titel, etc.
                text_tags = [
                    "ueberschrift", "absatz", "titel", "untertitel",
                    "text", "betreff", "anlage", "fussnote"
                ]

                for tag in text_tags:
                    for elem in root.iter(tag):
                        text = "".join(elem.itertext()).strip()
                        if text:
                            text_parts.append(text)

                # If no structured elements, get all text
                if not text_parts:
                    text_parts = [
                        "".join(elem.itertext()).strip()
                        for elem in root.iter()
                        if "".join(elem.itertext()).strip()
                    ]

                full_text = "\n\n".join(text_parts)

                # Clean up
                full_text = html.unescape(full_text)
                full_text = re.sub(r"\s+", " ", full_text)

                if full_text.strip():
                    return full_text.strip()

            except Exception as e:
                logger.warning(f"Failed to fetch XML from {xml_url}: {e}")

        # Try HTML as fallback
        html_url = content_urls.get("html")
        if html_url:
            try:
                self.rate_limiter.wait()
                resp = self.client.get(html_url)
                resp.raise_for_status()

                text = resp.text
                # Remove script and style
                text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL | re.IGNORECASE)
                text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)
                # Remove tags
                text = re.sub(r"<[^>]+>", " ", text)
                # Clean entities and whitespace
                text = html.unescape(text)
                text = re.sub(r"\s+", " ", text)
                text = text.strip()

                if text:
                    return text

            except Exception as e:
                logger.warning(f"Failed to fetch HTML from {html_url}: {e}")

        logger.warning(f"Could not fetch full text from: {content_urls}")
        return ""

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all authentic BGBl documents (since 2004).

        Total: ~18K+ records. Use --sample for testing.
        """
        logger.info("Fetching all BgblAuth (authentic Bundesgesetzblatt) records")
        for doc in self._paginate():
            yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield gazette entries published since the given date.

        Uses ImRisSeit parameter.
        """
        days_ago = (datetime.now(timezone.utc) - since).days

        # Map days to RIS filter values
        if days_ago <= 7:
            im_ris_seit = "EinerWoche"
        elif days_ago <= 14:
            im_ris_seit = "ZweiWochen"
        elif days_ago <= 30:
            im_ris_seit = "EinemMonat"
        elif days_ago <= 90:
            im_ris_seit = "DreiMonaten"
        elif days_ago <= 180:
            im_ris_seit = "SechsMonaten"
        else:
            im_ris_seit = "EinemJahr"

        logger.info(f"Fetching BgblAuth updates ({im_ris_seit})")
        extra = {"ImRisSeit": im_ris_seit}

        for doc in self._paginate(extra_params=extra):
            yield doc

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw API response into standard schema.

        CRITICAL: Downloads and includes FULL TEXT from content URLs.
        """
        parsed = self._parse_document(raw)

        # Download full text
        content_urls = parsed.get("content_urls", {})
        full_text = ""
        if content_urls:
            full_text = self._download_full_text(content_urls)

        return {
            # Required base fields
            "_id": parsed["doc_id"],
            "_source": "AT/Bundesgesetzblatt",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": parsed.get("title", ""),
            "title_full": parsed.get("title_full", ""),
            "text": full_text,  # MANDATORY FULL TEXT
            "date": parsed.get("date_published", ""),
            "url": parsed.get("document_url", "") or parsed.get("eli", ""),
            # BGBl-specific fields
            "bgbl_number": parsed.get("bgbl_number", ""),
            "eli": parsed.get("eli", ""),
            "teil": parsed.get("teil", ""),
            "teil_name": parsed.get("teil_name", ""),
            "typ": parsed.get("typ", ""),
            "organ": parsed.get("organ", ""),
            "alte_dokumentnummer": parsed.get("alte_dokumentnummer", ""),
            "content_urls": content_urls,
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity and record count test."""
        print("Testing RIS OGD API v2.6 for BgblAuth...")

        # Version check
        resp = self.client.get("/Version")
        data = resp.json()
        version = data.get("OgdSearchResult", {}).get("Version", "?")
        print(f"  API Version: {version}")

        # BgblAuth count
        resp = self.client.get(
            "/Bundesrecht",
            params={
                "Applikation": "BgblAuth",
                "DokumenteProSeite": "Ten",
                "Seitennummer": "1",
            },
        )
        data = resp.json()
        hits = (
            data.get("OgdSearchResult", {})
            .get("OgdDocumentResults", {})
            .get("Hits", {})
            .get("#text", "0")
        )
        print(f"  Authentic BGBl records (since 2004): {hits}")

        # Sample by Teil
        for teil, name in BGBL_PARTS.items():
            resp = self.client.get(
                "/Bundesrecht",
                params={
                    "Applikation": "BgblAuth",
                    "BgblTeil": teil,
                    "DokumenteProSeite": "Ten",
                    "Seitennummer": "1",
                },
            )
            data = resp.json()
            hits = (
                data.get("OgdSearchResult", {})
                .get("OgdDocumentResults", {})
                .get("Hits", {})
                .get("#text", "0")
            )
            print(f"    {name}: {hits} records")

        print("\nAPI test passed!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    scraper = BundesgesetzblattScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12  # Fetch slightly more than 10 to ensure good coverage
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test-api":
        scraper.test_api()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
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
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
