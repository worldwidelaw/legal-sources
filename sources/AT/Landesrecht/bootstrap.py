#!/usr/bin/env python3
"""
AT/Landesrecht -- Austrian State Legislation Data Fetcher

Fetches state legislation from all 9 Austrian Bundesländer via the RIS OGD API v2.6.

Strategy:
  - Bootstrap: Paginates through LrKons (consolidated state law) using the JSON REST API.
  - Update: Uses ImRisSeit filter to fetch only recently modified records.
  - Sample: Fetches 10+ records for validation.

API: https://data.bka.gv.at/ris/api/v2.6/Landesrecht
Applications:
  - LrKons: Consolidated state legislation (275K+ records)
  - LgblAuth: Authenticated state law gazette
  - Lgbl: State law gazette
  - LgblNO: Lower Austria gazette
  - Vbl: Announcements

Usage:
  python bootstrap.py bootstrap          # Full initial pull (275K+ records)
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
logger = logging.getLogger("legal-data-hunter.AT.Landesrecht")

# RIS OGD API v2.6
API_BASE = "https://data.bka.gv.at/ris/api/v2.6"

# Available Landesrecht applications and their record counts (approx)
LANDESRECHT_APPS = {
    "LrKons": "Consolidated state legislation",  # 275K records
}

# Austrian states (Bundesländer)
BUNDESLAENDER = {
    "Wien": "Vienna",
    "Niederösterreich": "Lower Austria",
    "Oberösterreich": "Upper Austria",
    "Steiermark": "Styria",
    "Tirol": "Tyrol",
    "Kärnten": "Carinthia",
    "Salzburg": "Salzburg",
    "Vorarlberg": "Vorarlberg",
    "Burgenland": "Burgenland",
}


class LandesrechtScraper(BaseScraper):
    """
    Scraper for AT/Landesrecht -- Austrian State Legislation.
    Country: AT
    URL: https://www.ris.bka.gv.at

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

    def _paginate(
        self,
        applikation,
        extra_params=None,
        max_pages=None,
    ):
        """
        Generator that paginates through the Landesrecht API endpoint.
        Yields individual document references (raw dicts from the API).
        """
        page = 1
        total_hits = None
        endpoint = "/Landesrecht"

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

            if total_hits is None:
                hits_info = doc_results.get("Hits", {})
                try:
                    total_hits = int(hits_info.get("#text", "0"))
                except (ValueError, TypeError):
                    total_hits = 0
                logger.info(f"{endpoint} [{applikation}]: {total_hits} total hits")
                if total_hits == 0:
                    return

            docs = doc_results.get("OgdDocumentReference", [])
            if not isinstance(docs, list):
                docs = [docs] if docs else []

            if not docs:
                logger.info(f"No more documents on page {page}")
                return

            for doc in docs:
                doc_data = doc.get("Data", {})
                if doc_data:
                    doc_data["_applikation"] = applikation
                    doc_data["_endpoint"] = endpoint
                    yield doc_data

            fetched_so_far = page * 100
            if fetched_so_far >= total_hits:
                logger.info(f"Fetched all {total_hits} records for {applikation}")
                return

            page += 1
            if page % 10 == 0:
                logger.info(f"  Page {page} ({fetched_so_far}/{total_hits} fetched)")

    def _parse_landesrecht(self, raw):
        """Parse a Landesrecht API response into a flat dict."""
        meta = raw.get("Metadaten", {})
        tech = meta.get("Technisch", {})
        allg = meta.get("Allgemein", {})
        lr = meta.get("Landesrecht", {})
        lr_kons = lr.get("LrKons", {})

        content_urls = self._extract_content_urls(raw)

        return {
            "doc_id": tech.get("ID", ""),
            "doc_type": "legislation",
            "applikation": raw.get("_applikation", "LrKons"),
            "organ": tech.get("Organ", ""),
            "title": lr.get("Kurztitel", ""),
            "title_full": lr.get("Titel", ""),
            "eli": lr_kons.get("Eli", "") or lr.get("Eli", ""),
            "document_url": allg.get("DokumentUrl", ""),
            "date_published": allg.get("Veroeffentlicht"),
            "date_changed": allg.get("Geaendert"),
            # LrKons-specific fields
            "land": lr.get("Bundesland", ""),  # Bundesland is at lr level, not lr_kons
            "kundmachungsorgan": lr_kons.get("Kundmachungsorgan", ""),
            "typ": lr_kons.get("Typ", ""),
            "dokumenttyp": lr_kons.get("Dokumenttyp", ""),
            "artikel_paragraph": lr_kons.get("ArtikelParagraphAnlage", ""),
            "paragraphnummer": lr_kons.get("Paragraphnummer", ""),
            "date_effective": lr_kons.get("Inkrafttretensdatum"),
            "date_expired": lr_kons.get("Ausserkrafttretensdatum"),
            "indizes": self._flatten_item(lr_kons.get("Indizes", {})),
            "gesetzesnummer": lr_kons.get("Gesetzesnummer", ""),
            "schlagworte": lr_kons.get("Schlagworte", ""),
            "stammfassung_url": lr_kons.get("StammfassungUrl", ""),
            "content_urls": content_urls,
        }

    def _flatten_item(self, obj):
        """Normalize RIS API nested values to a string."""
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

    def _download_full_text(self, content_urls):
        """
        Download and extract full text from content URLs.
        Prefers XML format for structured text extraction.
        """
        xml_url = content_urls.get("xml")
        if xml_url:
            try:
                self.rate_limiter.wait()
                resp = self.client.get(xml_url)
                resp.raise_for_status()

                root = ET.fromstring(resp.content)
                text_parts = []

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

                if not text_parts:
                    for elem in root.iter():
                        if elem.tag in ["titel", "untertitel", "absatz", "text", "betreff"]:
                            text = "".join(elem.itertext()).strip()
                            if text:
                                text_parts.append(text)

                full_text = "\n\n".join(text_parts)
                full_text = html.unescape(full_text)
                full_text = re.sub(r"\s+", " ", full_text)

                if full_text.strip():
                    return full_text.strip()

            except Exception as e:
                logger.warning(f"Failed to fetch XML content from {xml_url}: {e}")

        html_url = content_urls.get("html")
        if html_url:
            try:
                self.rate_limiter.wait()
                resp = self.client.get(html_url)
                resp.raise_for_status()

                text = resp.text
                text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL | re.IGNORECASE)
                text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)
                text = re.sub(r"<[^>]+>", " ", text)
                text = html.unescape(text)
                text = re.sub(r"\s+", " ", text)
                text = text.strip()

                if text:
                    return text

            except Exception as e:
                logger.warning(f"Failed to fetch HTML content from {html_url}: {e}")

        logger.warning(f"Could not fetch full text from any URL: {content_urls}")
        return ""

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all state legislation documents from RIS.
        WARNING: Full fetch is 275K+ records. Use sample mode for testing.
        """
        for app in LANDESRECHT_APPS:
            logger.info(f"Fetching state legislation: {app}")
            for doc in self._paginate(app):
                yield doc

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield records modified since the given date."""
        days_ago = (datetime.now(timezone.utc) - since).days

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

        for app in LANDESRECHT_APPS:
            logger.info(f"Fetching state legislation updates ({im_ris_seit}): {app}")
            for doc in self._paginate(app, extra_params=extra):
                yield doc

    def normalize(self, raw: dict) -> dict:
        """Transform raw RIS API response into standard schema."""
        parsed = self._parse_landesrecht(raw)

        content_urls = parsed.get("content_urls", {})
        full_text = ""
        if content_urls:
            full_text = self._download_full_text(content_urls)

        url = parsed.get("document_url", "")
        date = parsed.get("date_published") or parsed.get("date_changed") or ""

        return {
            "_id": parsed["doc_id"],
            "_source": "AT/Landesrecht",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": parsed.get("title", ""),
            "text": full_text,
            "date": date,
            "url": url,
            "land": parsed.get("land", ""),
            **parsed,
        }

    def test_api(self):
        """Quick connectivity and API version test."""
        print("Testing RIS OGD API v2.6 (Landesrecht)...")

        resp = self.client.get("/Version")
        data = resp.json()
        version = data.get("OgdSearchResult", {}).get("Version", "?")
        print(f"  API Version: {version}")

        for app, desc in LANDESRECHT_APPS.items():
            resp = self.client.get(
                "/Landesrecht",
                params={
                    "Applikation": app,
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
            print(f"  {app} ({desc}): {hits} records")

        print("\nAPI test passed!")


def main():
    scraper = LandesrechtScraper()

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
