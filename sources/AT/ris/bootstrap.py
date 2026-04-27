#!/usr/bin/env python3
"""
AT/RIS -- Rechtsinformationssystem Data Fetcher

Fetches Austrian legislation and case law from the RIS OGD API v2.6.

Strategy:
  - Bootstrap: Paginates through BrKons (federal law) and Justiz (case law)
    using the JSON REST API. No auth required.
  - Update: Uses ImRisSeit filter to fetch only recently modified records.
  - Sample: Fetches 10+ records from legislation + case law for validation.

API: https://data.bka.gv.at/ris/api/v2.6/
Docs: https://data.bka.gv.at/ris/ogd/v2.6/Documents/Dokumentation_OGD-RIS_API.pdf

Usage:
  python bootstrap.py bootstrap            # Full initial pull (caution: 500K+ records)
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py bootstrap-fast       # Concurrent full-text download (recommended)
  python bootstrap.py bootstrap-fast --workers 8  # Custom thread count
  python bootstrap.py update               # Incremental update (last month)
  python bootstrap.py test-api             # Quick API connectivity test
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
logger = logging.getLogger("legal-data-hunter.AT.ris")

# RIS OGD API v2.6
API_BASE = "https://data.bka.gv.at/ris/api/v2.6"

# Applications to scrape
LEGISLATION_APPS = ["BrKons"]
CASE_LAW_APPS = ["Justiz", "Vfgh", "Vwgh", "Bvwg", "Lvwg"]

# Page size values accepted by the API
PAGE_SIZES = {10: "Ten", 20: "Twenty", 50: "Fifty", 100: "OneHundred"}


class RISScraper(BaseScraper):
    """
    Scraper for AT/RIS -- Austrian Legal Information System.
    Country: AT
    URL: https://www.ris.bka.gv.at

    Data types: legislation + case_law
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
        endpoint,
        applikation,
        extra_params=None,
        max_pages=None,
    ):
        """
        Generator that paginates through an RIS API endpoint.

        Yields individual document references (raw dicts from the API).
        """
        page = 1
        total_hits = None

        while True:
            if max_pages and page > max_pages:
                logger.info(f"Reached max_pages={max_pages}, stopping pagination")
                return

            params = {
                "Applikation": applikation,
                "DokumenteProSeite": "OneHundred",
                "Seitennummer": str(page),
            }
            if extra_params:
                params.update(extra_params)

            self.rate_limiter.wait()

            try:
                resp = self.client.get(endpoint, params=params)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.error(f"API error on {endpoint} page {page}: {e}")
                # Retry once after a pause
                time.sleep(5)
                try:
                    resp = self.client.get(endpoint, params=params)
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
                logger.info(
                    f"{endpoint} [{applikation}]: {total_hits} total hits"
                )
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
                    # Annotate with source metadata
                    doc_data["_applikation"] = applikation
                    doc_data["_endpoint"] = endpoint
                    yield doc_data

            # Check if we've fetched all pages
            fetched_so_far = page * 100
            if fetched_so_far >= total_hits:
                logger.info(
                    f"Fetched all {total_hits} records for {applikation}"
                )
                return

            page += 1
            logger.info(
                f"  Page {page} ({fetched_so_far}/{total_hits} fetched)"
            )

    # -- Legislation parsing ------------------------------------------------

    def _parse_legislation(self, raw):
        """Parse a Bundesrecht API response into a flat dict."""
        meta = raw.get("Metadaten", {})
        tech = meta.get("Technisch", {})
        allg = meta.get("Allgemein", {})
        br = meta.get("Bundesrecht", {})

        # BrKons-specific fields are nested
        br_kons = br.get("BrKons", {})

        # Extract content URLs
        content_urls = self._extract_content_urls(raw)

        return {
            "doc_id": tech.get("ID", ""),
            "doc_type": "legislation",
            "applikation": raw.get("_applikation", "BrKons"),
            "organ": tech.get("Organ", ""),
            "title": br.get("Kurztitel", ""),
            "title_full": br.get("Titel", ""),
            "eli": br.get("Eli", ""),
            "document_url": allg.get("DokumentUrl", ""),
            "date_published": allg.get("Veroeffentlicht"),
            "date_changed": allg.get("Geaendert"),
            "kundmachungsorgan": br_kons.get("Kundmachungsorgan", ""),
            "typ": br_kons.get("Typ", ""),
            "dokumenttyp": br_kons.get("Dokumenttyp", ""),
            "artikel_paragraph": br_kons.get("ArtikelParagraphAnlage", ""),
            "paragraphnummer": br_kons.get("Paragraphnummer", ""),
            "date_effective": br_kons.get("Inkrafttretensdatum"),
            "date_expired": br_kons.get("Ausserkrafttretensdatum"),
            "indizes": self._flatten_item(br_kons.get("Indizes", {})),
            "abkuerzung": br_kons.get("Abkuerzung", ""),
            "gesetzesnummer": br_kons.get("Gesetzesnummer", ""),
            "schlagworte": br_kons.get("Schlagworte", ""),
            "gesamte_rechtsvorschrift_url": br_kons.get(
                "GesamteRechtsvorschriftUrl", ""
            ),
            "content_urls": content_urls,
        }

    # -- Case law parsing ---------------------------------------------------

    def _parse_case_law(self, raw):
        """Parse a Judikatur API response into a flat dict."""
        meta = raw.get("Metadaten", {})
        tech = meta.get("Technisch", {})
        allg = meta.get("Allgemein", {})
        jud = meta.get("Judikatur", {})
        justiz = jud.get("Justiz", {})

        content_urls = self._extract_content_urls(raw)

        return {
            "doc_id": tech.get("ID", ""),
            "doc_type": "case_law",
            "applikation": raw.get("_applikation", "Justiz"),
            "organ": tech.get("Organ", ""),
            "dokumenttyp": jud.get("Dokumenttyp", ""),
            "geschaeftszahl": self._flatten_item(
                jud.get("Geschaeftszahl", {})
            ),
            "normen": self._flatten_item(jud.get("Normen", {})),
            "title": self._flatten_item(jud.get("Geschaeftszahl", {})),
            "entscheidungsdatum": jud.get("Entscheidungsdatum"),
            "date_published": allg.get("Veroeffentlicht"),
            "date_changed": allg.get("Geaendert"),
            "document_url": allg.get("DokumentUrl", ""),
            "ecli": jud.get("EuropeanCaseLawIdentifier", ""),
            "gericht": justiz.get("Gericht", ""),
            "rechtsgebiete": self._flatten_item(
                justiz.get("Rechtsgebiete", {})
            ),
            "rechtssatznummern": self._flatten_item(
                justiz.get("Rechtssatznummern", {})
            ),
            "anmerkung": justiz.get("Anmerkung", ""),
            "content_urls": content_urls,
        }

    # -- Helpers ------------------------------------------------------------

    def _flatten_item(self, obj):
        """
        RIS API wraps single values as {"item": "value"} and
        multiple values as {"item": ["v1", "v2"]}.  Normalize to a string.
        """
        if not obj:
            return ""
        if isinstance(obj, str):
            return obj
        item = obj.get("item", "")
        if isinstance(item, list):
            # Items can themselves be dicts in case law
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

    def _download_full_text(self, content_urls):
        """
        Download and extract full text from content URLs.

        Prefers XML format for structured text extraction. Falls back to
        HTML if XML is not available.

        Returns (full_text, section_heading) tuple. section_heading is
        extracted from <ueberschrift typ="para" ct="text"> in XML content
        (the paragraph/section heading within the law).
        """
        section_heading = ""

        # Try XML first (cleanest for text extraction)
        xml_url = content_urls.get("xml")
        if xml_url:
            try:
                self.rate_limiter.wait()
                resp = self.client.get(xml_url)
                resp.raise_for_status()

                # Parse XML and extract text from relevant elements
                root = ET.fromstring(resp.content)
                text_parts = []

                # Extract section heading from <ueberschrift typ="para" ct="text">
                ns = root.tag.split("}")[0] + "}" if "}" in root.tag else ""
                for ueber in root.iter(f"{ns}ueberschrift"):
                    if ueber.get("typ") == "para" and ueber.get("ct") == "text":
                        heading = "".join(ueber.itertext()).strip()
                        if heading:
                            section_heading = heading
                            break  # Use the first paragraph-level heading

                # Extract text from all <absatz> (paragraph) elements
                for absatz in root.iter(f"{ns}absatz" if ns else "absatz"):
                    text = absatz.text or ""
                    # Also get text from child elements
                    for child in absatz.iter():
                        if child.text:
                            text += " " + child.text
                        if child.tail:
                            text += " " + child.tail
                    text = text.strip()
                    if text:
                        text_parts.append(text)

                # If no paragraphs found with namespace, try without
                if not text_parts and ns:
                    for absatz in root.iter("absatz"):
                        text = absatz.text or ""
                        for child in absatz.iter():
                            if child.text:
                                text += " " + child.text
                            if child.tail:
                                text += " " + child.tail
                        text = text.strip()
                        if text:
                            text_parts.append(text)

                # If no paragraphs, try other text-containing elements
                if not text_parts:
                    for elem in root.iter():
                        tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
                        if tag in ["titel", "untertitel", "absatz", "text", "betreff"]:
                            text = "".join(elem.itertext()).strip()
                            if text:
                                text_parts.append(text)

                full_text = "\n\n".join(text_parts)

                # Clean up HTML entities (both named and numeric)
                full_text = html.unescape(full_text)
                # Normalize whitespace
                full_text = re.sub(r"\s+", " ", full_text)

                if full_text.strip():
                    return full_text.strip(), section_heading

            except Exception as e:
                logger.warning(f"Failed to fetch XML content from {xml_url}: {e}")

        # Try HTML as fallback
        html_url = content_urls.get("html")
        if html_url:
            try:
                self.rate_limiter.wait()
                resp = self.client.get(html_url)
                resp.raise_for_status()

                # Simple HTML tag stripping
                text = resp.text
                # Remove script and style tags
                text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL | re.IGNORECASE)
                text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)
                # Remove HTML tags
                text = re.sub(r"<[^>]+>", " ", text)
                # Clean up HTML entities (both named and numeric)
                text = html.unescape(text)
                # Normalize whitespace
                text = re.sub(r"\s+", " ", text)
                text = text.strip()

                if text:
                    return text, section_heading

            except Exception as e:
                logger.warning(f"Failed to fetch HTML content from {html_url}: {e}")

        # If all else fails, return empty string
        logger.warning(f"Could not fetch full text from any URL: {content_urls}")
        return "", section_heading

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents from RIS: federal legislation + case law.

        WARNING: Full fetch is 500K+ records. Use sample mode for testing.
        For full bootstrap, consider running off-hours and with --max-pages.
        """
        # Federal legislation (BrKons)
        for app in LEGISLATION_APPS:
            logger.info(f"Fetching legislation: {app}")
            for doc in self._paginate("/Bundesrecht", app):
                yield doc

        # Case law (Justiz, VfGH, VwGH, BVwG, LVwG)
        for app in CASE_LAW_APPS:
            logger.info(f"Fetching case law: {app}")
            for doc in self._paginate("/Judikatur", app):
                yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield records modified since the given date.

        Uses ImRisSeit parameter for recent records, falling back to
        date range filtering for older queries.
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

        extra = {"ImRisSeit": im_ris_seit}

        # Legislation updates
        for app in LEGISLATION_APPS:
            logger.info(f"Fetching legislation updates ({im_ris_seit}): {app}")
            for doc in self._paginate("/Bundesrecht", app, extra_params=extra):
                yield doc

        # Case law updates
        for app in CASE_LAW_APPS:
            logger.info(f"Fetching case law updates ({im_ris_seit}): {app}")
            for doc in self._paginate("/Judikatur", app, extra_params=extra):
                yield doc

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw RIS API response into standard schema.

        Detects whether the record is legislation or case law based on
        the endpoint and metadata, then delegates to the appropriate parser.

        CRITICAL: Downloads and includes FULL TEXT from content URLs.
        """
        meta = raw.get("Metadaten", {})
        endpoint = raw.get("_endpoint", "")

        # Determine record type
        if "Bundesrecht" in meta or "Bundesrecht" in endpoint:
            parsed = self._parse_legislation(raw)
        elif "Judikatur" in meta or "Judikatur" in endpoint:
            parsed = self._parse_case_law(raw)
        else:
            # Fallback: try legislation
            parsed = self._parse_legislation(raw)

        doc_type = parsed.get("doc_type", "legislation")

        # Download full text from content URLs
        content_urls = parsed.get("content_urls", {})
        full_text = ""
        section_heading = ""
        if content_urls:
            full_text, section_heading = self._download_full_text(content_urls)

        # Build section_path for legislation records
        # Format: "LawName > §X HeadingText" (matches FR/LegifranceCodes convention)
        section_path = ""
        if doc_type == "legislation":
            law_name = (parsed.get("abkuerzung") or parsed.get("title", "")).strip()
            art_para = parsed.get("artikel_paragraph", "").strip()
            if law_name and art_para and art_para != "§ 0":
                parts = [law_name]
                if section_heading:
                    parts.append(f"{art_para} {section_heading}")
                else:
                    parts.append(art_para)
                section_path = " > ".join(parts)
            elif law_name and section_heading:
                section_path = f"{law_name} > {section_heading}"

        # Use title as fallback if no full text is available
        url = parsed.get("document_url", "")
        date = parsed.get("date_published") or parsed.get("entscheidungsdatum") or parsed.get("date_changed") or ""

        return {
            # Required base fields
            "_id": parsed["doc_id"],
            "_source": "AT/RIS",
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": parsed.get("title", ""),
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date,
            "url": url,
            "section_path": section_path,
            # All other parsed fields
            **parsed,
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity and API version test."""
        print("Testing RIS OGD API v2.6...")

        # Version check
        resp = self.client.get("/Version")
        data = resp.json()
        version = data.get("OgdSearchResult", {}).get("Version", "?")
        print(f"  API Version: {version}")

        # Bundesrecht count
        resp = self.client.get(
            "/Bundesrecht",
            params={
                "Applikation": "BrKons",
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
        print(f"  Federal legislation (BrKons): {hits} records")

        # Judikatur count
        resp = self.client.get(
            "/Judikatur",
            params={
                "Applikation": "Justiz",
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
        print(f"  Case law (Justiz): {hits} records")

        print("\nAPI test passed!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    scraper = RISScraper()

    import argparse

    parser = argparse.ArgumentParser(description="AT/RIS Austrian Legal Data Fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (few records)")
    parser.add_argument("--sample-size", type=int, default=10, help="Number of sample records")
    parser.add_argument("--workers", type=int, default=None, help="Concurrent download threads (bootstrap-fast)")
    parser.add_argument("--batch-size", type=int, default=100, help="Records per batch write (bootstrap-fast)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == "test-api":
        scraper.test_api()

    elif args.command == "bootstrap":
        if args.sample:
            stats = scraper.run_sample(n=args.sample_size)
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

    elif args.command == "bootstrap-fast":
        print(f"Running fast bootstrap (workers={args.workers or 'auto'}, batch_size={args.batch_size})...")
        stats = scraper.bootstrap_fast(
            max_workers=args.workers,
            batch_size=args.batch_size,
        )
        print(f"\nFast bootstrap complete: {stats['records_new']} new, "
              f"{stats['records_updated']} updated, "
              f"{stats['errors']} errors")
        print(json.dumps(stats, indent=2))

    elif args.command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
