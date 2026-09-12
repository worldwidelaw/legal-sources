#!/usr/bin/env python3
"""
DK/SKAT -- Danish Tax Authority Rulings (Skatterådet/Skattestyrelsen)

Fetches Danish tax rulings and binding answers (bindende svar) from Retsinformation.

Strategy:
  - Enumerate via Retsinformation's own search API
    GET /api/documentsearch?ps=100&dt=230&page=N
    (dt=230 is the "Afgørelse" / AFG document type; ~6,000 documents)
  - Keep the entries whose ressortName is a tax ministry
    ("Skatteministeriet" / "Skatte- og Vækstministeriet")
  - Download the LexDania XML per hit via its ELI link and extract full text

This replaces the previous brute-force scan of /eli/retsinfo/{year}/{number}
(2,500 numbers x 7 years = ~17,500 blind requests), which was the cause of
issue #1389: the scan wedged for ~29 minutes on a single year with nothing
written. The search API needs ~61 requests to enumerate the whole corpus and
covers 1954-present rather than 2020-present.

API endpoints:
  - Search:  https://www.retsinformation.dk/api/documentsearch?dt=230&ps=100&page=N
  - XML:     https://www.retsinformation.dk{retsinfoLink}/dan/xml

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py bootstrap-fast      # Full pull, concurrent + batched writes
  python bootstrap.py update              # Incremental update
  python bootstrap.py test-api            # Quick API connectivity test
"""

import sys
import json
import logging
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
logger = logging.getLogger("legal-data-hunter.DK.SKAT")

# API endpoint
RETSINFORMATION_BASE = "https://www.retsinformation.dk"

# Retsinformation search API: document type id for "Afgørelse" (ELI code AFG)
DOCUMENT_TYPE_AFG = 230

# Server caps page size at 100 regardless of a larger ps
PAGE_SIZE = 100

# Hard stop so a runaway pager can never spin forever (61 pages as of 2026-08)
MAX_PAGES = 400

# ressortName values that mark a document as tax doctrine. Retsinformation has
# used several names for the tax ministry over the decades.
TAX_RESSORT_MARKER = "skatte"


class SKATScraper(BaseScraper):
    """
    Scraper for DK/SKAT -- Danish Tax Authority Rulings.
    Country: DK
    URL: https://www.retsinformation.dk

    Data types: doctrine (tax rulings, binding answers)
    Auth: none (Open Data)
    """

    def __init__(self, source_dir=None):
        # source_dir is optional so the VPS bootstrap-fast wrapper can construct
        # SKATScraper() by introspection.
        super().__init__(Path(source_dir) if source_dir else Path(__file__).parent)

        # (connect, read) timeouts plus a wall-clock deadline: a host that
        # trickles bytes must never wedge the run the way #1389 did.
        self.client = HttpClient(
            base_url=RETSINFORMATION_BASE,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/json, application/xml;q=0.9, */*;q=0.8",
            },
            timeout=(10, 45),
            wall_timeout=180,
        )

    # -- Enumeration --------------------------------------------------------

    def _search_page(self, page: int) -> list:
        """Fetch one page of AFG search results. Returns [] on failure/end."""
        url = f"/api/documentsearch?ps={PAGE_SIZE}&dt={DOCUMENT_TYPE_AFG}&page={page}"

        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            logger.warning(f"Search page {page} failed: {e}")
            return []

        if data.get("isError"):
            logger.warning(f"Search page {page} returned isError")
            return []

        return data.get("documents") or []

    @staticmethod
    def _is_tax_document(entry: dict) -> bool:
        return TAX_RESSORT_MARKER in (entry.get("ressortName") or "").lower()

    @staticmethod
    def _parse_dk_date(value: str) -> Optional[str]:
        """'05/08/2026' (dd/mm/yyyy) -> '2026-08-05'."""
        if not value:
            return None
        m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", value.strip())
        if not m:
            return None
        day, month, year = m.groups()
        return f"{year}-{month}-{day}"

    def _iter_search_entries(self) -> Generator[dict, None, None]:
        """Page through the AFG result set, yielding tax-ministry entries."""
        page = 1
        seen = 0
        kept = 0

        while page <= MAX_PAGES:
            documents = self._search_page(page)
            if not documents:
                logger.info(f"Search exhausted at page {page}")
                break

            seen += len(documents)
            for entry in documents:
                if self._is_tax_document(entry):
                    kept += 1
                    yield entry

            if page % 10 == 0:
                logger.info(f"Search page {page}: {seen} scanned, {kept} tax rulings")

            page += 1

        logger.info(f"Enumeration complete: {seen} AFG scanned, {kept} tax rulings kept")

    # -- Full text ----------------------------------------------------------

    def _fetch_xml(self, retsinfo_link: str) -> Optional[ET.Element]:
        """Download and parse the LexDania XML for one ELI link."""
        url = f"{retsinfo_link.rstrip('/')}/dan/xml"

        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)

            if resp.status_code == 404:
                return None

            resp.raise_for_status()
            return ET.fromstring(resp.content)

        except ET.ParseError as e:
            logger.warning(f"XML parse error for {url}: {e}")
            return None
        except Exception as e:
            logger.warning(f"Error fetching {url}: {e}")
            return None

    def _extract_text_from_xml(self, root: ET.Element) -> str:
        """
        Extract full text content from Retsinformation XML structure.

        The XML uses elements like:
          - <Char> for text content
          - <Linea> for lines
          - <Resume>, <TekstGruppe> for content sections
        """
        text_parts = []

        # Extract title first
        for titel in root.iter("Titel"):
            title_text = "".join(titel.itertext()).strip()
            if title_text:
                text_parts.append(title_text)
                text_parts.append("")

        # Extract Resume (summary) section
        for resume in root.iter("Resume"):
            resume_text = "".join(resume.itertext()).strip()
            if resume_text:
                text_parts.append("RESUME:")
                text_parts.append(resume_text)
                text_parts.append("")

        # Extract main content from TekstGruppe
        for tekst in root.iter("TekstGruppe"):
            for elem in tekst.iter():
                if elem.tag == "Rubrica":
                    # Section header
                    header = "".join(elem.itertext()).strip()
                    if header:
                        text_parts.append("")
                        text_parts.append(header.upper())
                elif elem.tag == "Char":
                    text = elem.text or ""
                    text = text.strip()
                    if text:
                        text_parts.append(text)

        # If limited content found, also try direct Char extraction
        if len([p for p in text_parts if p]) < 10:
            for char_elem in root.iter("Char"):
                text = char_elem.text or ""
                text = text.strip()
                if text and text not in text_parts:
                    text_parts.append(text)

        # Join and clean up
        full_text = "\n".join(text_parts)

        # Clean up HTML entities
        full_text = html.unescape(full_text)

        # Normalize whitespace within lines
        lines = []
        for line in full_text.split("\n"):
            cleaned = re.sub(r"\s+", " ", line).strip()
            lines.append(cleaned)

        full_text = "\n".join(lines)

        # Remove excessive blank lines
        full_text = re.sub(r"\n{3,}", "\n\n", full_text)

        return full_text.strip()

    def _parse_meta(self, root: ET.Element) -> dict:
        """Extract metadata from XML <Meta> section."""
        meta = {}
        meta_elem = root.find(".//Meta")

        if meta_elem is None:
            return meta

        # Direct child elements
        field_map = {
            "DocumentType": "document_type_raw",
            "AccessionNumber": "accession_number",
            "DocumentId": "document_id",
            "UniqueDocumentId": "unique_document_id",
            "DocumentTitle": "title",
            "Year": "year",
            "Number": "number",
            "DiesSigni": "signature_date",
            "DiesEdicti": "publication_date",
            "Status": "status",
            "AnnouncedIn": "announced_in",
            "Ministry": "ministry",
            "AdministrativeAuthority": "administrative_authority",
            "JournalNumber": "journal_number",
            "Rank": "rank",
        }

        for xml_field, dict_field in field_map.items():
            elem = meta_elem.find(xml_field)
            if elem is not None and elem.text:
                meta[dict_field] = elem.text.strip()

        # Extract references to laws
        refs = []
        for ref in meta_elem.findall(".//Ref_Text"):
            if ref.text:
                refs.append(ref.text.strip())
        if refs:
            meta["references"] = refs

        return meta

    # -- Abstract method implementations ------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield search-result stubs for every Skatteministeriet AFG document.

        The XML download happens in normalize() so that bootstrap_fast's worker
        threads overlap the per-document fetches with the sequential paging.
        """
        yield from self._iter_search_entries()

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents published on or after `since`.

        Results are ordered newest-first, so stop as soon as a full page of
        older publications has gone by.
        """
        cutoff = since.date().isoformat()
        page = 1
        stale_pages = 0

        while page <= MAX_PAGES:
            documents = self._search_page(page)
            if not documents:
                break

            fresh_on_page = 0
            for entry in documents:
                pub = self._parse_dk_date(entry.get("offentliggoerelsesDato", ""))
                if pub and pub < cutoff:
                    continue
                fresh_on_page += 1
                if self._is_tax_document(entry):
                    yield entry

            if fresh_on_page == 0:
                stale_pages += 1
                # One fully-stale page is enough with a date-sorted feed; allow
                # a second in case of out-of-order publication dates.
                if stale_pages >= 2:
                    logger.info(f"Reached documents older than {cutoff} at page {page}")
                    break
            else:
                stale_pages = 0

            page += 1

    def normalize(self, raw: dict) -> Optional[dict]:
        """
        Download the document XML and transform it into the standard schema.

        CRITICAL: Extracts and includes FULL TEXT from XML content.
        Returns None when the document has no retrievable body.
        """
        eli_uri = (raw.get("retsinfoLink") or "").rstrip("/")
        if not eli_uri:
            return None

        root = self._fetch_xml(eli_uri)
        if root is None:
            return None

        meta = self._parse_meta(root)
        full_text = self._extract_text_from_xml(root)

        if not full_text:
            logger.debug(f"No text extracted for {eli_uri}")
            return None

        # Title: prefer the XML metadata, fall back to the search hit
        title = meta.get("title") or raw.get("title") or ""
        if not title:
            titel_elem = root.find(".//Titel")
            if titel_elem is not None:
                title = "".join(titel_elem.itertext()).strip()

        date = (
            meta.get("signature_date")
            or meta.get("publication_date")
            or self._parse_dk_date(raw.get("offentliggoerelsesDato", ""))
            or ""
        )

        # Year/number from the ELI path, e.g. /eli/retsinfo/2026/9740
        year = number = None
        m = re.search(r"/eli/retsinfo/(\d{4})/(\d+)", eli_uri)
        if m:
            year, number = int(m.group(1)), int(m.group(2))

        accession = meta.get("accession_number", "")
        doc_id = accession or f"DK-SKAT-{raw.get('id') or f'{year}-{number}'}"

        def _as_int(value, fallback):
            try:
                return int(value)
            except (TypeError, ValueError):
                return fallback

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "DK/SKAT",
            "_type": "doctrine",  # tax doctrine/rulings - not legislation or case_law
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date,
            "url": f"{RETSINFORMATION_BASE}{eli_uri}",
            # Source-specific fields
            "accession_number": accession,
            "document_id": meta.get("document_id", ""),
            "unique_document_id": meta.get("unique_document_id", str(raw.get("id", ""))),
            "year": _as_int(meta.get("year"), year),
            "number": _as_int(meta.get("number"), number),
            "short_name": raw.get("shortName", ""),
            "document_type": "AFG",
            "document_type_raw": meta.get("document_type_raw", ""),
            "eli_uri": eli_uri,
            "ministry": meta.get("ministry") or raw.get("ressortName", ""),
            "administrative_authority": meta.get("administrative_authority", ""),
            "status": meta.get("status", ""),
            "publication_date": meta.get("publication_date")
            or self._parse_dk_date(raw.get("offentliggoerelsesDato", ""))
            or "",
            "signature_date": meta.get("signature_date", ""),
            "journal_number": meta.get("journal_number", ""),
            "references": meta.get("references", []),
        }

    # -- Custom commands ----------------------------------------------------

    def test_api(self):
        """Quick connectivity and API test."""
        print("Testing Retsinformation search API for tax rulings...")

        print("\n1. Search API, page 1...")
        documents = self._search_page(1)
        print(f"   {len(documents)} AFG hits returned")
        tax = [d for d in documents if self._is_tax_document(d)]
        print(f"   {len(tax)} of them from a tax ministry")
        if not tax:
            print("   ERROR: no tax rulings on page 1")
            return

        print("\n2. Full-text download for the first hit...")
        record = self.normalize(tax[0])
        if not record:
            print(f"   ERROR: could not normalize {tax[0].get('retsinfoLink')}")
            return
        print(f"   Title: {record['title'][:70]}")
        print(f"   Date: {record['date']}  URL: {record['url']}")
        print(f"   Text length: {len(record['text'])} characters")
        print(f"   First 300 chars: {record['text'][:300]}...")

        print("\nAPI test complete!")


# -- CLI Entry Point -------------------------------------------------------


def main():
    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test-api] "
            "[--sample] [--full] [--sample-size N]"
        )
        sys.exit(1)

    scraper = SKATScraper()

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 15
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test-api":
        scraper.test_api()

    elif command in ("bootstrap", "bootstrap-fast"):
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        elif command == "bootstrap-fast":
            stats = scraper.bootstrap_fast()
            print(
                f"\nbootstrap_fast complete: {stats.get('records_fetched', 0)} fetched, "
                f"{stats.get('records_new', 0)} new, "
                f"{stats.get('records_updated', 0)} updated"
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
