#!/usr/bin/env python3
"""
EE/RiigiTeatajaLoomal -- Estonian Riigi Teataja (State Gazette) Fetcher

Fetches Estonian legislation from Riigi Teataja with full text content.

Strategy:
  - Bootstrap: Iterate through chronology pages to discover document IDs,
    then fetch XML for each document to get full text.
  - Update: Use chronology for recent dates since specified date.
  - Sample: Fetch recent documents for validation.

Data access method: HTML scraping for discovery + XML download for full text.
  - Chronology: /kronoloogia_tulemus.html?rtOsaId=&kpv=DD.MM.YYYY
  - Document XML: /akt/{id}.xml

The XML format contains structured legislation with:
  - <metaandmed>: Metadata (issuer, dates, type)
  - <aktinimi>: Document title
  - <sisu>: Full content with paragraphs and sections

Usage:
  python bootstrap.py bootstrap           # Full historical pull
  python bootstrap.py bootstrap --sample  # Fetch 10+ sample records
  python bootstrap.py update              # Incremental update (last week)
  python bootstrap.py test-api            # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Set
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
logger = logging.getLogger("legal-data-hunter.EE.RiigiTeatajaLoomal")

BASE_URL = "https://www.riigiteataja.ee"

# RT parts to scrape
RT_PARTS = {
    "2": "RT I",   # Laws and national regulations
    "3": "RT II",  # International agreements
    "4": "RT III", # Administrative acts
    "": "All",     # All parts
}


class RiigiTeatajaScraper(BaseScraper):
    """
    Scraper for EE/RiigiTeatajaLoomal -- Estonian State Gazette.
    Country: EE
    URL: https://www.riigiteataja.ee

    Data types: legislation
    Auth: none (Open Data - free public access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept": "text/html,application/xml,application/xhtml+xml",
                "Accept-Language": "et,en;q=0.9",
            },
            timeout=60,
        )

    # -- Document discovery via chronology pages ------------------------------

    def _get_documents_for_date(self, date: datetime, rt_part: str = "") -> Set[str]:
        """
        Fetch document IDs published on a specific date.

        Args:
            date: The date to fetch documents for
            rt_part: RT part filter ("2"=RT I, "3"=RT II, "4"=RT III, ""=all)

        Returns:
            Set of document IDs (e.g., "406022026048")
        """
        date_str = date.strftime("%d.%m.%Y")
        url = f"/kronoloogia_tulemus.html?rtOsaId={rt_part}&kpv={date_str}"

        self.rate_limiter.wait()

        try:
            resp = self.client.get(url)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch chronology for {date_str}: {e}")
            return set()

        # Extract document IDs from links like /akt/406022026048
        doc_ids = set()
        for match in re.finditer(r'href="[^"]*?/akt/(\d+)"', resp.text):
            doc_ids.add(match.group(1))

        logger.debug(f"Found {len(doc_ids)} documents for {date_str}")
        return doc_ids

    # -- XML Document fetching ------------------------------------------------

    def _fetch_document_xml(self, doc_id: str) -> Optional[dict]:
        """
        Fetch and parse the XML for a single document.

        Returns raw parsed data or None if fetch fails.
        """
        url = f"/akt/{doc_id}.xml"

        self.rate_limiter.wait()

        try:
            resp = self.client.get(url)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch XML for document {doc_id}: {e}")
            return None

        try:
            # Parse XML
            root = ET.fromstring(resp.content)
            return {
                "_doc_id": doc_id,
                "_xml_root": root,
                "_raw_xml": resp.content.decode("utf-8", errors="replace"),
            }
        except ET.ParseError as e:
            logger.warning(f"Failed to parse XML for document {doc_id}: {e}")
            return None

    def _extract_text_from_xml(self, root: ET.Element) -> str:
        """
        Extract full text content from the XML document.

        The XML structure has:
        - <sisu>: Main content container
          - <paragrahv>: Paragraph sections (legal paragraphs)
            - <loige>: Subsections
              - <sisuTekst>/<tavatekst>: Text content
        - <aktinimi>: Title info
        """
        text_parts = []

        # Define the namespace if present (Riigi Teataja uses a default namespace)
        # We'll try without namespace first, then with common patterns
        ns = {}
        if root.tag.startswith("{"):
            ns_uri = root.tag.split("}")[0][1:]
            ns = {"rt": ns_uri}

        def get_all_text(elem):
            """Recursively get all text content from an element."""
            text = ""
            if elem.text:
                text += elem.text
            for child in elem:
                text += get_all_text(child)
                if child.tail:
                    text += child.tail
            return text

        # Try to find sisu (content) element
        sisu = root.find(".//sisu") or root.find(".//{*}sisu")
        if sisu is not None:
            # Extract from paragraphs
            for para in sisu.iter():
                if para.tag.endswith("tavatekst") or para.tag == "tavatekst":
                    para_text = get_all_text(para)
                    if para_text.strip():
                        text_parts.append(para_text.strip())
                elif para.tag.endswith("sisuTekst") or para.tag == "sisuTekst":
                    para_text = get_all_text(para)
                    if para_text.strip():
                        text_parts.append(para_text.strip())

        # If no content found in sisu, try broader extraction
        if not text_parts:
            for elem in root.iter():
                tag_name = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
                if tag_name in ["tavatekst", "sisuTekst", "tekst", "pealkiri"]:
                    elem_text = get_all_text(elem)
                    if elem_text.strip():
                        text_parts.append(elem_text.strip())

        # Join all text parts
        full_text = "\n\n".join(text_parts)

        # Clean up the text
        full_text = html.unescape(full_text)  # Decode HTML entities
        full_text = re.sub(r"<[^>]+>", " ", full_text)  # Strip any remaining HTML
        full_text = re.sub(r"\s+", " ", full_text)  # Normalize whitespace
        full_text = full_text.strip()

        return full_text

    def _extract_metadata(self, root: ET.Element) -> dict:
        """Extract metadata fields from the XML document."""

        def get_text(elem):
            """Get text content of an element, handling None."""
            if elem is None:
                return ""
            return (elem.text or "").strip()

        def find_by_local_name(parent, local_name):
            """Find element by local name, ignoring namespace."""
            for elem in parent.iter():
                tag = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
                if tag == local_name:
                    return elem
            return None

        # Extract metadata section
        meta = find_by_local_name(root, "metaandmed")

        # Build metadata dict
        metadata = {
            "issuer": "",
            "document_type": "",
            "text_type": "",
            "abbreviation": "",
            "enacted_date": "",
            "effective_date": "",
            "expired_date": "",
            "rt_part": "",
            "rt_article": "",
            "global_id": "",
        }

        if meta is not None:
            metadata["issuer"] = get_text(find_by_local_name(meta, "valjaandja"))
            metadata["document_type"] = get_text(find_by_local_name(meta, "dokumentLiik"))
            metadata["text_type"] = get_text(find_by_local_name(meta, "tekstiliik"))
            metadata["abbreviation"] = get_text(find_by_local_name(meta, "lyhend"))
            metadata["global_id"] = get_text(find_by_local_name(meta, "globaalID"))

            # Kehtivus (validity) dates
            kehtivus = find_by_local_name(meta, "kehtivus")
            if kehtivus is not None:
                metadata["effective_date"] = get_text(find_by_local_name(kehtivus, "kehtivuseAlgus"))
                metadata["expired_date"] = get_text(find_by_local_name(kehtivus, "kehtivuseLopp"))

            # Vastuvoetud (enacted)
            vastuvoetud = find_by_local_name(meta, "vastuvoetud")
            if vastuvoetud is not None:
                metadata["enacted_date"] = get_text(find_by_local_name(vastuvoetud, "aktikuupaev"))

            # Avaldamismarge (publication info)
            avald = find_by_local_name(meta, "avaldamismarge")
            if avald is not None:
                metadata["rt_part"] = get_text(find_by_local_name(avald, "RTosa"))
                metadata["rt_article"] = get_text(find_by_local_name(avald, "RTartikkel"))

        # Extract title
        aktinimi = find_by_local_name(root, "aktinimi")
        title = ""
        if aktinimi is not None:
            pealkiri = find_by_local_name(aktinimi, "pealkiri")
            title = get_text(pealkiri)

        metadata["title"] = title

        return metadata

    # -- Abstract method implementations --------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all legislation documents from Riigi Teataja.

        Iterates through the chronology from 1990 to present, fetching
        document IDs and then downloading XML for each.
        """
        # Start from 1990 (when RT digital records begin)
        start_year = 1990
        end_date = datetime.now(timezone.utc)

        current = datetime(start_year, 1, 1, tzinfo=timezone.utc)

        while current <= end_date:
            logger.info(f"Fetching documents for {current.strftime('%Y-%m-%d')}")

            # Get document IDs for this date (RT I only for legislation)
            doc_ids = self._get_documents_for_date(current, rt_part="2")

            for doc_id in doc_ids:
                raw = self._fetch_document_xml(doc_id)
                if raw:
                    yield raw

            current += timedelta(days=1)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents published since the given date.
        """
        current = since
        end_date = datetime.now(timezone.utc)

        while current <= end_date:
            logger.info(f"Fetching updates for {current.strftime('%Y-%m-%d')}")

            doc_ids = self._get_documents_for_date(current, rt_part="")

            for doc_id in doc_ids:
                raw = self._fetch_document_xml(doc_id)
                if raw:
                    yield raw

            current += timedelta(days=1)

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw XML data into standard schema.

        CRITICAL: Extracts and includes FULL TEXT from the XML content.
        """
        doc_id = raw["_doc_id"]
        root = raw["_xml_root"]

        # Extract metadata
        metadata = self._extract_metadata(root)

        # Extract full text content
        full_text = self._extract_text_from_xml(root)

        # Determine the best date to use
        date = (
            metadata.get("enacted_date") or
            metadata.get("effective_date") or
            ""
        )

        # Clean date format if needed (should already be ISO 8601)
        if date and not re.match(r"\d{4}-\d{2}-\d{2}", date):
            # Try to parse and reformat
            for fmt in ["%d.%m.%Y", "%Y-%m-%d"]:
                try:
                    parsed = datetime.strptime(date.split("+")[0].strip(), fmt)
                    date = parsed.strftime("%Y-%m-%d")
                    break
                except ValueError:
                    continue

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "EE/RiigiTeatajaLoomal",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": metadata.get("title", ""),
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date,
            "url": f"{BASE_URL}/akt/{doc_id}",
            # Additional metadata
            "issuer": metadata.get("issuer", ""),
            "document_type": metadata.get("document_type", ""),
            "text_type": metadata.get("text_type", ""),
            "abbreviation": metadata.get("abbreviation", ""),
            "enacted_date": metadata.get("enacted_date", ""),
            "effective_date": metadata.get("effective_date", ""),
            "expired_date": metadata.get("expired_date", ""),
            "rt_part": metadata.get("rt_part", ""),
            "rt_article": metadata.get("rt_article", ""),
            "global_id": metadata.get("global_id", ""),
        }

    # -- Custom commands ------------------------------------------------------

    def test_api(self):
        """Quick connectivity test."""
        print("Testing Riigi Teataja connectivity...")

        # Test homepage
        try:
            resp = self.client.get("/")
            resp.raise_for_status()
            print(f"  Homepage: OK ({resp.status_code})")
        except Exception as e:
            print(f"  Homepage: FAILED ({e})")
            return

        # Test chronology page
        try:
            today = datetime.now().strftime("%d.%m.%Y")
            resp = self.client.get(f"/kronoloogia_tulemus.html?rtOsaId=&kpv={today}")
            resp.raise_for_status()
            doc_count = len(re.findall(r'href="[^"]*?/akt/(\d+)"', resp.text))
            print(f"  Chronology ({today}): {doc_count} documents")
        except Exception as e:
            print(f"  Chronology: FAILED ({e})")
            return

        # Test XML download
        try:
            # Try a well-known document (Riigi Teataja Act)
            resp = self.client.get("/akt/103032017023.xml")
            resp.raise_for_status()
            root = ET.fromstring(resp.content)
            title_elem = root.find(".//{*}pealkiri") or root.find(".//pealkiri")
            title = title_elem.text if title_elem is not None else "Unknown"
            print(f"  XML download: OK (Sample: {title[:50]}...)")
        except Exception as e:
            print(f"  XML download: FAILED ({e})")
            return

        print("\nConnectivity test passed!")

    def run_sample(self, n: int = 10) -> dict:
        """
        Fetch a sample of recent documents with full text.

        Overrides base class to ensure we get documents with actual content.
        """
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        checked = 0
        errors = []

        # Get documents from recent dates
        current = datetime.now(timezone.utc)
        dates_checked = 0

        while saved < n and dates_checked < 30:
            logger.info(f"Checking {current.strftime('%Y-%m-%d')} for documents...")

            doc_ids = self._get_documents_for_date(current, rt_part="2")  # RT I (laws)
            logger.info(f"  Found {len(doc_ids)} documents")

            for doc_id in list(doc_ids)[:min(5, n - saved)]:
                checked += 1
                raw = self._fetch_document_xml(doc_id)

                if not raw:
                    errors.append(f"Failed to fetch {doc_id}")
                    continue

                try:
                    normalized = self.normalize(raw)

                    # Validate the record
                    if not normalized.get("text"):
                        errors.append(f"{doc_id}: No text content")
                        logger.warning(f"Document {doc_id} has no text content")
                        continue

                    if len(normalized.get("text", "")) < 100:
                        errors.append(f"{doc_id}: Text too short ({len(normalized.get('text', ''))} chars)")
                        logger.warning(f"Document {doc_id} has very short text")
                        continue

                    # Save to sample directory
                    sample_path = sample_dir / f"{doc_id}.json"
                    with open(sample_path, "w", encoding="utf-8") as f:
                        json.dump(normalized, f, ensure_ascii=False, indent=2)

                    saved += 1
                    logger.info(
                        f"  Saved {doc_id}: {normalized.get('title', '')[:50]}... "
                        f"({len(normalized.get('text', ''))} chars)"
                    )

                except Exception as e:
                    errors.append(f"{doc_id}: {str(e)}")
                    logger.error(f"Error processing {doc_id}: {e}")

                if saved >= n:
                    break

            current -= timedelta(days=1)
            dates_checked += 1

        # Calculate statistics
        text_lengths = []
        for f in sample_dir.glob("*.json"):
            with open(f, "r", encoding="utf-8") as fp:
                data = json.load(fp)
                text_lengths.append(len(data.get("text", "")))

        stats = {
            "sample_records_saved": saved,
            "documents_checked": checked,
            "dates_checked": dates_checked,
            "errors": errors[:10],
            "avg_text_length": sum(text_lengths) / len(text_lengths) if text_lengths else 0,
            "min_text_length": min(text_lengths) if text_lengths else 0,
            "max_text_length": max(text_lengths) if text_lengths else 0,
        }

        return stats


# -- CLI Entry Point ----------------------------------------------------------


def main():
    scraper = RiigiTeatajaScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 10
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
            print(json.dumps(stats, indent=2))
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
