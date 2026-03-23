#!/usr/bin/env python3
"""
AT/BWB -- Austrian Competition Authority Decisions (Kartellgericht/Kartellobergericht)

Fetches cartel and competition law decisions from the RIS OGD API v2.6.
BWB (Bundeswettbewerbsbehörde) investigates, but decisions are made by:
  - Kartellgericht (OLG Wien) - first instance cartel court
  - Kartellobergericht (OGH) - appellate cartel court

Strategy:
  - Bootstrap: Search for decisions citing KartG (Cartel Act) and WettbG (Competition Act).
  - Update: Uses ImRisSeit filter for recent records.
  - Sample: Fetches 10+ records for validation.

API: https://data.bka.gv.at/ris/api/v2.6/Judikatur
Docs: https://data.bka.gv.at/ris/ogd/v2.6/Documents/Dokumentation_OGD-RIS_API.pdf

Usage:
  python bootstrap.py bootstrap          # Full initial pull
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
logger = logging.getLogger("legal-data-hunter.AT.BWB")

# RIS OGD API v2.6
API_BASE = "https://data.bka.gv.at/ris/api/v2.6"


class BWBScraper(BaseScraper):
    """
    Scraper for AT/BWB -- Austrian Competition Authority Decisions.
    Country: AT
    URL: https://www.bwb.gv.at

    Data types: regulatory_decisions, case_law
    Auth: none (Open Government Data)

    Note: BWB is the investigative authority. Actual decisions are issued by:
    - Kartellgericht (OLG Wien as Cartel Court)
    - Kartellobergericht (OGH as Supreme Cartel Court)

    We fetch decisions citing:
    - KartG (Kartellgesetz - Cartel Act)
    - WettbG (Wettbewerbsgesetz - Competition Act)
    - Kartellgericht/Kartellobergericht keywords
    """

    # Primary search term for cartel court decisions
    # Note: RIS API doesn't support OR queries well, so we search for Kartellgericht
    # which captures both first-instance and appellate court decisions
    PRIMARY_SEARCH_TERM = "Kartellgericht"

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
        Generator that paginates through cartel/competition decisions.

        Uses Justiz application with Suchworte filter for competition law terms.
        Yields individual document references (raw dicts from the API).
        """
        page = 1
        total_hits = None

        while True:
            if max_pages and page > max_pages:
                logger.info(f"Reached max_pages={max_pages}, stopping pagination")
                return

            # Search for Kartellgericht decisions
            params = {
                "Applikation": "Justiz",
                "Suchworte": self.PRIMARY_SEARCH_TERM,
                "Dokumenttyp": "Text",  # Only full decision texts, not just Rechtssatz
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
                logger.info(f"BWB/Kartell: {total_hits} total hits")
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
                logger.info(f"Fetched all {total_hits} BWB/Kartell records")
                return

            page += 1
            logger.info(f"  Page {page} ({fetched_so_far}/{total_hits} fetched)")

    # -- Parsing ------------------------------------------------------------

    def _parse_case_law(self, raw):
        """Parse an API response into a flat dict."""
        meta = raw.get("Metadaten", {})
        tech = meta.get("Technisch", {})
        allg = meta.get("Allgemein", {})
        jud = meta.get("Judikatur", {})
        justiz = jud.get("Justiz", {})

        content_urls = self._extract_content_urls(raw)

        # Get decision text link if available
        entscheidungstexte = justiz.get("Entscheidungstexte", {})
        text_item = entscheidungstexte.get("item", {})
        if isinstance(text_item, list):
            text_item = text_item[0] if text_item else {}
        text_url = text_item.get("DokumentUrl", "") if isinstance(text_item, dict) else ""
        text_doc_num = ""
        if isinstance(text_item, dict) and text_item.get("DokumentUrl"):
            # Extract document number from URL
            url_match = re.search(r"Dokumentnummer=([^&]+)", text_item.get("DokumentUrl", ""))
            if url_match:
                text_doc_num = url_match.group(1)

        # Handle geschaeftszahl which can be nested
        geschaeftszahl = self._flatten_item(jud.get("Geschaeftszahl", {}))
        if not geschaeftszahl and isinstance(text_item, dict):
            geschaeftszahl = text_item.get("Geschaeftszahl", "")

        # Determine if this is a cartel/competition case
        normen = self._flatten_item(jud.get("Normen", {}))
        is_kartell = any(term.lower() in normen.lower() for term in ["kartg", "wettbg", "kartell"])

        return {
            "doc_id": tech.get("ID", ""),
            "doc_type": "regulatory_decision",
            "applikation": "Justiz",
            "organ": tech.get("Organ", ""),
            "dokumenttyp": jud.get("Dokumenttyp", ""),
            "geschaeftszahl": geschaeftszahl,
            "normen": normen,
            "title": geschaeftszahl,  # Use case number as title
            "entscheidungsdatum": jud.get("Entscheidungsdatum"),
            "date_published": allg.get("Veroeffentlicht"),
            "date_changed": allg.get("Geaendert"),
            "document_url": allg.get("DokumentUrl", ""),
            "ecli": jud.get("EuropeanCaseLawIdentifier", ""),
            "schlagworte": jud.get("Schlagworte", ""),
            # Justiz-specific fields
            "gericht": justiz.get("Gericht", ""),
            "rechtsgebiete": self._flatten_item(justiz.get("Rechtsgebiete", {})),
            "rechtssatznummern": self._flatten_item(justiz.get("Rechtssatznummern", {})),
            "anmerkung": justiz.get("Anmerkung", ""),
            "entscheidungsart": text_item.get("Entscheidungsart", "") if isinstance(text_item, dict) else "",
            "text_doc_url": text_url,
            "text_doc_num": text_doc_num,
            "content_urls": content_urls,
            "is_kartell": is_kartell,
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

        Tries multiple sources:
        1. Main document XML/HTML content URLs
        2. Linked decision text document (Entscheidungstext)
        3. Anmerkung (annotation) as fallback
        """
        content_urls = parsed.get("content_urls", {})

        # Try XML first (cleanest for text extraction)
        xml_url = content_urls.get("xml")
        if xml_url:
            text = self._fetch_xml_text(xml_url)
            if text and len(text) > 100:
                return text

        # Try HTML as fallback
        html_url = content_urls.get("html")
        if html_url:
            text = self._fetch_html_text(html_url)
            if text and len(text) > 100:
                return text

        # Try linked decision text document
        text_doc_num = parsed.get("text_doc_num", "")
        if text_doc_num:
            # Construct URLs for the decision text document
            text_xml_url = f"https://www.ris.bka.gv.at/Dokumente/Justiz/{text_doc_num}/{text_doc_num}.xml"
            text = self._fetch_xml_text(text_xml_url)
            if text and len(text) > 100:
                return text

            text_html_url = f"https://www.ris.bka.gv.at/Dokumente/Justiz/{text_doc_num}/{text_doc_num}.html"
            text = self._fetch_html_text(text_html_url)
            if text and len(text) > 100:
                return text

        # Also try to construct URL from doc_id
        doc_id = parsed.get("doc_id", "")
        if doc_id and doc_id.startswith("JJT_"):
            text_xml_url = f"https://www.ris.bka.gv.at/Dokumente/Justiz/{doc_id}/{doc_id}.xml"
            text = self._fetch_xml_text(text_xml_url)
            if text and len(text) > 100:
                return text

        # Last resort: use anmerkung (annotation) if available
        anmerkung = parsed.get("anmerkung", "")
        if anmerkung:
            return anmerkung

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
            # RIS uses namespace, but we can iterate all elements
            for elem in root.iter():
                # Get local tag name without namespace
                tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
                tag_lower = tag.lower()

                # Include relevant sections
                if tag_lower in ["titel", "betreff", "spruch", "begruendung", "absatz",
                                "text", "rechtssatz", "kurztext", "inhalt",
                                "entscheidungsgruende", "kopf", "rechtlichebeurteilung"]:
                    text = "".join(elem.itertext()).strip()
                    if text and len(text) > 20:
                        text_parts.append(text)

            # If specific tags not found, try all text content
            if not text_parts:
                all_text = []
                for elem in root.iter():
                    if elem.text:
                        all_text.append(elem.text.strip())
                    if elem.tail:
                        all_text.append(elem.tail.strip())
                text_parts = [t for t in all_text if t and len(t) > 5]

            full_text = "\n\n".join(text_parts)
            full_text = html.unescape(full_text)
            # Clean up excessive whitespace but preserve some structure
            full_text = re.sub(r"[ \t]+", " ", full_text)
            full_text = re.sub(r"\n{3,}", "\n\n", full_text)

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
        Yield all cartel/competition decisions.
        """
        logger.info("Fetching all BWB/Kartell decisions")
        for doc in self._paginate():
            yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield records modified since the given date.

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

        logger.info(f"Fetching BWB/Kartell updates ({im_ris_seit})")
        for doc in self._paginate(extra_params={"ImRisSeit": im_ris_seit}):
            yield doc

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw API response into standard schema.

        CRITICAL: Downloads and includes FULL TEXT from content URLs.
        """
        parsed = self._parse_case_law(raw)

        # Download full text
        full_text = self._download_full_text(parsed)

        url = parsed.get("document_url", "")
        date = parsed.get("entscheidungsdatum") or parsed.get("date_published") or ""

        # All BWB/Kartellgericht decisions are case_law
        # Use standard type for validation; store detail in sub_type
        organ = parsed.get("organ", "")
        gericht = parsed.get("gericht", "")
        if "OGH" in organ or "Kartellobergericht" in gericht.lower():
            sub_type = "kartellobergericht"  # Supreme Cartel Court
        elif "OLG" in organ or "Kartellgericht" in gericht.lower():
            sub_type = "kartellgericht"  # First-instance Cartel Court
        else:
            sub_type = "competition_authority"

        return {
            # Required base fields
            "_id": parsed["doc_id"],
            "_source": "AT/BWB",
            "_type": "case_law",
            "_sub_type": sub_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": parsed.get("title", ""),
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date,
            "url": url,
            # Case-specific fields
            "ecli": parsed.get("ecli", ""),
            "geschaeftszahl": parsed.get("geschaeftszahl", ""),
            "entscheidungsart": parsed.get("entscheidungsart", ""),
            "gericht": parsed.get("gericht", ""),
            "organ": parsed.get("organ", ""),
            "normen": parsed.get("normen", ""),
            "schlagworte": parsed.get("schlagworte", ""),
            "rechtsgebiete": parsed.get("rechtsgebiete", ""),
            "rechtssatznummern": parsed.get("rechtssatznummern", ""),
            "anmerkung": parsed.get("anmerkung", ""),
            "dokumenttyp": parsed.get("dokumenttyp", ""),
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity and API test."""
        print("Testing BWB/Kartell via RIS OGD API v2.6...")

        # Version check
        resp = self.client.get("/Version")
        data = resp.json()
        version = data.get("OgdSearchResult", {}).get("Version", "?")
        print(f"  API Version: {version}")

        # Kartell count
        resp = self.client.get(
            "/Judikatur",
            params={
                "Applikation": "Justiz",
                "Suchworte": self.PRIMARY_SEARCH_TERM,
                "Dokumenttyp": "Text",
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
        print(f"  Kartell/Competition decisions: {hits} records")

        print("\nAPI test passed!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    scraper = BWBScraper()

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
