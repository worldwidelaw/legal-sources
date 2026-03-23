#!/usr/bin/env python3
"""
AT/VfGH -- Austrian Constitutional Court (Verfassungsgerichtshof) Case Law Fetcher

Fetches VfGH decisions from the RIS OGD API v2.6.

Strategy:
  - Bootstrap: Paginates through VfGH application using JSON REST API.
  - Update: Uses ImRisSeit filter for recent records.
  - Sample: Fetches 10+ records for validation.

API: https://data.bka.gv.at/ris/api/v2.6/Judikatur
Docs: https://data.bka.gv.at/ris/ogd/v2.6/Documents/Dokumentation_OGD-RIS_API.pdf

Usage:
  python bootstrap.py bootstrap          # Full initial pull (23K+ records)
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
from typing import Generator, Optional
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
logger = logging.getLogger("legal-data-hunter.AT.VfGH")

# RIS OGD API v2.6
API_BASE = "https://data.bka.gv.at/ris/api/v2.6"


class VfGHScraper(BaseScraper):
    """
    Scraper for AT/VfGH -- Austrian Constitutional Court.
    Country: AT
    URL: https://www.vfgh.gv.at

    Data types: case_law
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=API_BASE,
            headers={"User-Agent": "LegalDataHunter/1.0 (Open Data Research)"},
            timeout=60,
        )

    # -- API helpers --------------------------------------------------------

    def _paginate(
        self,
        extra_params=None,
        max_pages=None,
    ):
        """
        Generator that paginates through VfGH decisions.

        Yields individual document references (raw dicts from the API).
        """
        page = 1
        total_hits = None

        while True:
            if max_pages and page > max_pages:
                logger.info(f"Reached max_pages={max_pages}, stopping pagination")
                return

            params = {
                "Applikation": "Vfgh",
                "DokumenteProSeite": "OneHundred",
                "Seitennummer": str(page),
            }
            if extra_params:
                params.update(extra_params)

            self.rate_limiter.wait()

            try:
                resp = self.client.get("/Judikatur", params=params)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.error(f"API error on page {page}: {e}")
                # Retry once after a pause
                time.sleep(5)
                try:
                    resp = self.client.get("/Judikatur", params=params)
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
                logger.info(f"VfGH: {total_hits} total hits")
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
                logger.info(f"Fetched all {total_hits} VfGH records")
                return

            page += 1
            logger.info(f"  Page {page} ({fetched_so_far}/{total_hits} fetched)")

    # -- Parsing ------------------------------------------------------------

    def _parse_case_law(self, raw):
        """Parse a VfGH API response into a flat dict."""
        meta = raw.get("Metadaten", {})
        tech = meta.get("Technisch", {})
        allg = meta.get("Allgemein", {})
        jud = meta.get("Judikatur", {})
        vfgh = jud.get("Vfgh", {})

        content_urls = self._extract_content_urls(raw)

        # Get decision text link if available
        entscheidungstexte = vfgh.get("Entscheidungstexte", {})
        text_item = entscheidungstexte.get("item", {})
        if isinstance(text_item, list):
            text_item = text_item[0] if text_item else {}
        text_url = text_item.get("DokumentUrl", "")
        text_doc_num = text_item.get("Dokumentnummer", "")

        return {
            "doc_id": tech.get("ID", ""),
            "doc_type": "case_law",
            "applikation": "Vfgh",
            "organ": tech.get("Organ", ""),
            "dokumenttyp": jud.get("Dokumenttyp", ""),
            "geschaeftszahl": self._flatten_item(jud.get("Geschaeftszahl", {})),
            "normen": self._flatten_item(jud.get("Normen", {})),
            "title": self._flatten_item(jud.get("Geschaeftszahl", {})),
            "entscheidungsdatum": jud.get("Entscheidungsdatum"),
            "date_published": allg.get("Geaendert"),  # RIS uses Geaendert for publication
            "document_url": allg.get("DokumentUrl", ""),
            "ecli": jud.get("EuropeanCaseLawIdentifier", ""),
            "schlagworte": jud.get("Schlagworte", ""),
            # VfGH-specific fields
            "entscheidungsart": vfgh.get("Entscheidungsart", ""),
            "gericht": vfgh.get("Gericht", ""),
            "indizes": self._flatten_item(vfgh.get("Indizes", {})),
            "leitsatz": vfgh.get("Leitsatz", ""),
            "text_doc_url": text_url,
            "text_doc_num": text_doc_num,
            "content_urls": content_urls,
        }

    def _flatten_item(self, obj):
        """
        RIS API wraps single values as {"item": "value"} and
        multiple values as {"item": ["v1", "v2"]}. Normalize to a string.
        """
        if not obj:
            return ""
        if isinstance(obj, str):
            return obj
        item = obj.get("item", "")
        if isinstance(item, list):
            parts = []
            for i in item:
                if isinstance(i, dict):
                    parts.append(json.dumps(i, ensure_ascii=False))
                else:
                    parts.append(str(i))
            return " | ".join(parts)
        if isinstance(item, dict):
            return json.dumps(item, ensure_ascii=False)
        return str(item)

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

    def _download_full_text(self, parsed):
        """
        Download and extract full text from content URLs.

        For VfGH, first tries XML for cleanest extraction, then HTML.
        Also tries the linked decision text document if available.
        """
        content_urls = parsed.get("content_urls", {})

        # Try XML first (cleanest for text extraction)
        xml_url = content_urls.get("xml")
        if xml_url:
            text = self._fetch_xml_text(xml_url)
            if text:
                return text

        # Try HTML as fallback
        html_url = content_urls.get("html")
        if html_url:
            text = self._fetch_html_text(html_url)
            if text:
                return text

        # Try linked decision text document
        text_doc_num = parsed.get("text_doc_num", "")
        if text_doc_num:
            # Construct URLs for the decision text document
            text_xml_url = f"https://www.ris.bka.gv.at/Dokumente/Vfgh/{text_doc_num}/{text_doc_num}.xml"
            text = self._fetch_xml_text(text_xml_url)
            if text:
                return text

            text_html_url = f"https://www.ris.bka.gv.at/Dokumente/Vfgh/{text_doc_num}/{text_doc_num}.html"
            text = self._fetch_html_text(text_html_url)
            if text:
                return text

        # Last resort: use leitsatz (summary) if available
        leitsatz = parsed.get("leitsatz", "")
        if leitsatz:
            return leitsatz

        logger.warning(f"Could not fetch full text for {parsed.get('doc_id', 'unknown')}")
        return ""

    def _fetch_xml_text(self, url):
        """Fetch and parse XML content for text extraction."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()

            root = ET.fromstring(resp.content)
            text_parts = []

            # Extract text from relevant elements
            for tag in ["titel", "betreff", "spruch", "begruendung", "absatz", "text"]:
                for elem in root.iter(tag):
                    text = "".join(elem.itertext()).strip()
                    if text:
                        text_parts.append(text)

            # If specific tags not found, try all text content
            if not text_parts:
                text_parts = [root.text or ""]
                for elem in root.iter():
                    if elem.text:
                        text_parts.append(elem.text)
                    if elem.tail:
                        text_parts.append(elem.tail)

            full_text = "\n\n".join(text_parts)
            full_text = html.unescape(full_text)
            full_text = re.sub(r"\s+", " ", full_text)

            if full_text.strip():
                return full_text.strip()

        except Exception as e:
            logger.debug(f"Failed to fetch XML from {url}: {e}")

        return ""

    def _fetch_html_text(self, url):
        """Fetch and parse HTML content for text extraction."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()

            text = resp.text
            # Remove script and style
            text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL | re.IGNORECASE)
            text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)
            # Remove HTML tags
            text = re.sub(r"<[^>]+>", " ", text)
            # Clean up entities
            text = html.unescape(text)
            # Normalize whitespace
            text = re.sub(r"\s+", " ", text)
            text = text.strip()

            if text:
                return text

        except Exception as e:
            logger.debug(f"Failed to fetch HTML from {url}: {e}")

        return ""

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all VfGH decisions.

        Full fetch is 23K+ records.
        """
        logger.info("Fetching all VfGH decisions")
        for doc in self._paginate():
            yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield VfGH records modified since the given date.

        Uses ImRisSeit parameter for recent records.
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

        logger.info(f"Fetching VfGH updates ({im_ris_seit})")
        for doc in self._paginate(extra_params={"ImRisSeit": im_ris_seit}):
            yield doc

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw VfGH API response into standard schema.

        CRITICAL: Downloads and includes FULL TEXT from content URLs.
        """
        parsed = self._parse_case_law(raw)

        # Download full text
        full_text = self._download_full_text(parsed)

        url = parsed.get("document_url", "")
        date = parsed.get("entscheidungsdatum") or parsed.get("date_published") or ""

        return {
            # Required base fields
            "_id": parsed["doc_id"],
            "_source": "AT/VfGH",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": parsed.get("title", ""),
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date,
            "url": url,
            # VfGH-specific fields
            "ecli": parsed.get("ecli", ""),
            "geschaeftszahl": parsed.get("geschaeftszahl", ""),
            "entscheidungsart": parsed.get("entscheidungsart", ""),
            "gericht": parsed.get("gericht", ""),
            "normen": parsed.get("normen", ""),
            "schlagworte": parsed.get("schlagworte", ""),
            "indizes": parsed.get("indizes", ""),
            "leitsatz": parsed.get("leitsatz", ""),
            "dokumenttyp": parsed.get("dokumenttyp", ""),
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity and API test."""
        print("Testing VfGH via RIS OGD API v2.6...")

        # Version check
        resp = self.client.get("/Version")
        data = resp.json()
        version = data.get("OgdSearchResult", {}).get("Version", "?")
        print(f"  API Version: {version}")

        # VfGH count
        resp = self.client.get(
            "/Judikatur",
            params={
                "Applikation": "Vfgh",
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
        print(f"  VfGH decisions: {hits} records")

        print("\nAPI test passed!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    scraper = VfGHScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
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
