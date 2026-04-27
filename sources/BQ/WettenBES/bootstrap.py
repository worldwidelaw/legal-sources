#!/usr/bin/env python3
"""
BQ/WettenBES -- Caribbean Netherlands BES Legislation

Fetches consolidated BES (Bonaire, Sint Eustatius, Saba) legislation from
the Dutch Basis Wetten Bestand (BWB) using the SRU 1.2 API.

BES-specific document types:
  - wet-BES: Laws (~158)
  - AMvB-BES: Orders in Council (~184)
  - ministeriele-regeling-BES: Ministerial regulations (~264)

Strategy:
  1. SRU search with BES type filters to get metadata + XML URLs
  2. Download full-text XML from repository
  3. Extract plain text from XML structure

API Documentation:
  - SRU 1.2: https://zoekservice.overheid.nl/sru/Search?x-connection=BWB
  - Full text: https://repository.officiele-overheidspublicaties.nl/bwb/{id}/

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12+ sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import html as html_mod
import logging
import time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, List
from xml.etree import ElementTree as ET

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BQ.WettenBES")

SRU_ENDPOINT = "https://zoekservice.overheid.nl/sru/Search"
USER_AGENT = "LegalDataHunter/1.0 (Open Data Research)"

# BES document types
BES_TYPES = ["wet-BES", "AMvB-BES", "ministeriele-regeling-BES"]

# SRU XML namespaces
NS = {
    "srw": "http://www.loc.gov/zing/srw/",
    "gzd": "http://standaarden.overheid.nl/sru",
}


def _get_text_any_ns(parent: ET.Element, local_names: list) -> str:
    """Get text from an XML element, ignoring namespace prefix."""
    if parent is None:
        return ""
    for elem in parent.iter():
        tag_local = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
        if tag_local.lower() in [n.lower() for n in local_names]:
            if elem.text and elem.text.strip():
                return elem.text.strip()
    return ""


class WettenBESScraper(BaseScraper):
    """Scraper for BQ/WettenBES -- Caribbean Netherlands BES Legislation."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

    def _sru_search(self, query: str, start_record: int = 1,
                    maximum_records: int = 100) -> Optional[ET.Element]:
        """Execute an SRU searchRetrieve query against BWB."""
        params = {
            "operation": "searchRetrieve",
            "version": "1.2",
            "x-connection": "BWB",
            "query": query,
            "startRecord": str(start_record),
            "maximumRecords": str(maximum_records),
        }

        for attempt in range(5):
            self.rate_limiter.wait()
            try:
                resp = self.session.get(SRU_ENDPOINT, params=params, timeout=120)
                resp.raise_for_status()
                return ET.fromstring(resp.content)
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as e:
                wait = min(120, 10 * (2 ** attempt))
                logger.warning(f"Connection error (attempt {attempt+1}/5): {e}")
                time.sleep(wait)
                self.session = requests.Session()
                self.session.headers.update({"User-Agent": USER_AGENT})
            except requests.exceptions.HTTPError as e:
                if e.response is not None and e.response.status_code >= 500:
                    time.sleep(min(60, 10 * (2 ** attempt)))
                else:
                    logger.error(f"HTTP error: {e}")
                    return None
            except Exception as e:
                logger.error(f"SRU error: {e}")
                return None
        return None

    def _parse_record(self, record: ET.Element) -> Optional[dict]:
        """Parse SRU record into metadata dict."""
        try:
            record_data = record.find(".//srw:recordData", NS)
            if record_data is None:
                return None

            gzd = record_data.find(".//{http://standaarden.overheid.nl/sru}gzd")
            if gzd is None:
                return None

            original = gzd.find(".//{http://standaarden.overheid.nl/sru}originalData")
            enriched = gzd.find(".//{http://standaarden.overheid.nl/sru}enrichedData")
            if original is None:
                return None

            meta = original.find(".//{http://standaarden.overheid.nl/bwb/terms/}meta")
            if meta is None:
                return None

            doc_id = _get_text_any_ns(meta, ["identifier"])
            title = _get_text_any_ns(meta, ["title"])
            doc_type = _get_text_any_ns(meta, ["type"])
            authority = _get_text_any_ns(meta, ["authority"])
            modified = _get_text_any_ns(meta, ["modified"])
            geldig_start = _get_text_any_ns(meta, ["geldigheidsperiode_startdatum"])

            rechtsgebied = []
            for elem in meta.iter():
                if 'rechtsgebied' in elem.tag.lower() and elem.text:
                    rechtsgebied.append(elem.text.strip())

            xml_url = ""
            if enriched is not None:
                for elem in enriched.iter():
                    if 'locatie_toestand' in elem.tag.lower() and elem.text:
                        xml_url = elem.text.strip()
                        break

            return {
                "doc_id": doc_id,
                "title": title,
                "doc_type": doc_type,
                "authority": authority,
                "date_modified": modified,
                "geldig_start": geldig_start,
                "rechtsgebied": rechtsgebied,
                "xml_url": xml_url,
            }
        except Exception as e:
            logger.warning(f"Parse error: {e}")
            return None

    def _download_full_text(self, xml_url: str) -> str:
        """Download and extract plain text from BWB XML."""
        if not xml_url:
            return ""

        for attempt in range(3):
            try:
                self.rate_limiter.wait()
                resp = self.session.get(xml_url, timeout=60)
                resp.raise_for_status()
                root = ET.fromstring(resp.content)
                break
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout) as e:
                if attempt < 2:
                    time.sleep(10 * (2 ** attempt))
                else:
                    return ""
            except Exception as e:
                logger.warning(f"XML download error: {e}")
                return ""

        # Extract text from BWB XML elements
        text_tags = ['intitule', 'citeertitel', 'al', 'titel', 'wij',
                     'tussenkop', 'lidnr', 'li.nr']
        parts = []
        for elem in root.iter():
            tag = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
            if tag.lower() in [t.lower() for t in text_tags]:
                text = "".join(elem.itertext()).strip()
                if text:
                    parts.append(text)

        if not parts:
            for elem in root.iter():
                if elem.text and elem.text.strip():
                    parts.append(elem.text.strip())

        full_text = "\n\n".join(parts)
        full_text = html_mod.unescape(full_text)
        full_text = re.sub(r"[ \t]+", " ", full_text)
        full_text = re.sub(r"\n{3,}", "\n\n", full_text)
        return full_text.strip()

    def _build_bes_query(self) -> str:
        """Build CQL query for all BES document types, current versions only."""
        type_clauses = " OR ".join(f"dcterms.type=={t}" for t in BES_TYPES)
        today = datetime.now().strftime("%Y-%m-%d")
        return f"({type_clauses}) AND overheidbwb.geldigheidsdatum={today}"

    def _paginate_bes(self, query: str, max_pages: Optional[int] = None
                      ) -> Generator[dict, None, None]:
        """Paginate through BES records via SRU."""
        page = 1
        start = 1
        per_page = 100
        total = None

        while True:
            if max_pages and page > max_pages:
                return

            root = self._sru_search(query, start_record=start, maximum_records=per_page)
            if root is None:
                return

            if total is None:
                num_elem = root.find(".//srw:numberOfRecords", NS)
                total = int(num_elem.text) if num_elem is not None else 0
                logger.info(f"BES query: {total} total records")
                if total == 0:
                    return

            records = root.findall(".//srw:record", NS)
            if not records:
                return

            for rec in records:
                doc = self._parse_record(rec)
                if doc:
                    yield doc

            fetched = start + len(records) - 1
            if fetched >= total:
                return

            page += 1
            start = fetched + 1
            if page % 5 == 0:
                logger.info(f"  Page {page} ({fetched}/{total})")

    def normalize(self, raw: dict) -> dict:
        doc_id = raw.get("doc_id", "")

        # Use prefetched text if available, otherwise download
        full_text = raw.get("_prefetched_text", "")
        if not full_text:
            xml_url = raw.get("xml_url", "")
            full_text = self._download_full_text(xml_url) if xml_url else ""

        url = f"https://wetten.overheid.nl/{doc_id}" if doc_id else ""
        date = raw.get("geldig_start") or raw.get("date_modified") or ""

        return {
            "_id": f"BQ/WettenBES/{doc_id}",
            "_source": "BQ/WettenBES",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": full_text,
            "date": date,
            "url": url,
            "doc_id": doc_id,
            "doc_type": raw.get("doc_type", ""),
            "authority": raw.get("authority", ""),
            "rechtsgebied": raw.get("rechtsgebied", []),
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        query = self._build_bes_query()
        limit = 15 if sample else None
        count = 0

        max_pages = 3 if sample else None
        for raw in self._paginate_bes(query, max_pages=max_pages):
            if limit and count >= limit:
                break

            # Pre-download full text and attach to raw record so normalize()
            # doesn't need to re-download (BaseScraper calls normalize on yielded records)
            xml_url = raw.get("xml_url", "")
            text = self._download_full_text(xml_url) if xml_url else ""
            if not text or len(text) < 50:
                logger.warning(f"  Skipping {raw.get('doc_id', '?')} - no/short text")
                continue

            raw["_prefetched_text"] = text
            yield raw
            count += 1
            logger.info(f"  [{count}] {raw.get('title', '')[:60]} ({len(text)} chars)")

        logger.info(f"Total records yielded: {count}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        type_clauses = " OR ".join(f"dcterms.type=={t}" for t in BES_TYPES)
        query = f"({type_clauses}) AND dcterms.modified>={since}"
        for raw in self._paginate_bes(query):
            record = self.normalize(raw)
            if record["text"] and len(record["text"]) >= 50:
                yield record


if __name__ == "__main__":
    scraper = WettenBESScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        query = scraper._build_bes_query()
        root = scraper._sru_search(query, maximum_records=1)
        if root is None:
            print("Connection FAILED")
            sys.exit(1)
        num = root.find(".//srw:numberOfRecords", NS)
        print(f"Connection OK. BES records: {num.text if num is not None else '?'}")
        records = root.findall(".//srw:record", NS)
        if records:
            doc = scraper._parse_record(records[0])
            if doc:
                print(f"Sample: {doc['doc_id']} - {doc['title'][:80]}")
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
