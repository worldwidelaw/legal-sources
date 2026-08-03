#!/usr/bin/env python3
"""
NL/Tractatenblad -- Dutch Treaty Series (Tractatenblad van het Koninkrijk der Nederlanden)

Fetches the full-text corpus of the Tractatenblad -- the official Dutch treaty
gazette in which every treaty, convention and international agreement to which the
Kingdom of the Netherlands is a party (plus their Dutch translations, ratification
and entry-into-force notices) is published. ~15,970 publications since 1951.

This is DISTINCT from:
  - NL/Staatsblad (Bulletin of Acts & Decrees -- national laws and royal decrees)
  - NL/wetten.overheid.nl (consolidated national legislation)
  - NL/CVDR (decentralized regulations)
Treaties are binding legal instruments not covered by any existing source.

Strategy (official KOOP open data, no auth):
  1. SRU 2.0 searchRetrieve against the officiele-bekendmakingen repository,
     scoped to publicatienaam == Tractatenblad, to enumerate metadata + the
     manifestation URLs (xml / pdf) for every publication.
  2. Full text from the born-digital XML manifestation where present.
  3. Fall back to the born-digital PDF manifestation (PyMuPDF/fitz) when the XML
     is only a metadata stub -- this covers the older (pre-2010) publications,
     which are digitised born-digital PDFs with a real text layer.
  4. Skip the rare scanned-only publication with no text layer.

API Documentation:
  - SRU 2.0: https://repository.overheid.nl/sru (KOOP / overheid.nl)
  - Manifestations under https://repository.overheid.nl/frbr/officielepublicaties/trb/

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py bootstrap-fast     # Alias for full pull (runner entrypoint)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import re
import html as html_mod
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from xml.etree import ElementTree as ET

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NL.Tractatenblad")

SOURCE_ID = "NL/Tractatenblad"

SRU_ENDPOINT = "https://repository.overheid.nl/sru"
SRU_QUERY = "(w.publicatienaam==Tractatenblad)"
USER_AGENT = "LegalDataHunter/1.0 (Open Data Research)"

NS = {
    "sru": "http://docs.oasis-open.org/ns/search-ws/sruResponse",
    "gzd": "http://standaarden.overheid.nl/sru",
    "ow": "http://standaarden.overheid.nl/wetgeving/",
    "dcterms": "http://purl.org/dc/terms/",
    "c": "http://standaarden.overheid.nl/collectie/",
}

MIN_TEXT_LEN = 100


def _local(tag: str) -> str:
    return tag.split("}")[-1].lower() if "}" in tag else tag.lower()


def _first_text(parent: ET.Element, local_names) -> str:
    """First matching descendant's text, by local tag name (namespace-agnostic)."""
    if parent is None:
        return ""
    wanted = {n.lower() for n in local_names}
    for elem in parent.iter():
        if _local(elem.tag) in wanted and elem.text and elem.text.strip():
            return elem.text.strip()
    return ""


class NLTractatenbladScraper(BaseScraper):
    """Scraper for NL/Tractatenblad -- Dutch treaty series via KOOP SRU 2.0."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})

    # ---- SRU ---------------------------------------------------------------

    def _sru_search(self, start_record: int = 1,
                    maximum_records: int = 50) -> Optional[ET.Element]:
        params = {
            "operation": "searchRetrieve",
            "version": "2.0",
            "query": SRU_QUERY,
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
            except ET.ParseError as e:
                logger.warning(f"XML parse error (attempt {attempt+1}/5): {e}")
                time.sleep(min(60, 10 * (2 ** attempt)))
            except Exception as e:
                logger.error(f"SRU error: {e}")
                return None
        return None

    def _parse_record(self, record: ET.Element) -> Optional[dict]:
        try:
            gzd = record.find(".//{http://standaarden.overheid.nl/sru}gzd")
            if gzd is None:
                return None
            original = gzd.find("{http://standaarden.overheid.nl/sru}originalData")
            enriched = gzd.find("{http://standaarden.overheid.nl/sru}enrichedData")
            if original is None:
                return None

            doc_id = _first_text(original, ["identifier"])
            title = _first_text(original, ["title"])
            language = _first_text(original, ["language"]) or "nl"
            creator = _first_text(original, ["creator"])
            modified = _first_text(original, ["modified"])
            # available = publication date in the Tractatenblad;
            # date = date the treaty was concluded.
            available = _first_text(original, ["available"])
            concluded = _first_text(original, ["date"])
            subject = _first_text(original, ["subject"])

            # dcterms:type carries the treaty-document kind (Verdrag, Notawisseling...)
            doc_type = ""
            for elem in original.iter():
                if _local(elem.tag) == "type" and elem.get("scheme", "").endswith("Tractatenblad"):
                    if elem.text and elem.text.strip():
                        doc_type = elem.text.strip()
                        break

            xml_url = ""
            pdf_url = ""
            preferred_url = ""
            if enriched is not None:
                for item in enriched.iter():
                    lt = _local(item.tag)
                    if lt == "itemurl":
                        man = item.get("manifestation", "")
                        if man == "xml" and item.text:
                            xml_url = item.text.strip()
                        elif man == "pdf" and item.text:
                            pdf_url = item.text.strip()
                    elif lt == "preferredurl" and item.text:
                        preferred_url = item.text.strip()

            if not doc_id:
                return None

            return {
                "doc_id": doc_id,
                "title": title,
                "doc_type": doc_type,
                "language": language,
                "creator": creator,
                "date_available": available,
                "date_concluded": concluded,
                "date_modified": modified,
                "subject": subject,
                "xml_url": xml_url,
                "pdf_url": pdf_url,
                "preferred_url": preferred_url,
            }
        except Exception as e:
            logger.warning(f"Parse error: {e}")
            return None

    # ---- Full text ---------------------------------------------------------

    def _download(self, url: str, timeout: int = 90) -> Optional[bytes]:
        for attempt in range(3):
            try:
                self.rate_limiter.wait()
                resp = self.session.get(url, timeout=timeout)
                resp.raise_for_status()
                return resp.content
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout):
                if attempt < 2:
                    time.sleep(10 * (2 ** attempt))
            except Exception as e:
                logger.warning(f"Download error for {url}: {e}")
                return None
        return None

    def _text_from_xml(self, xml_url: str) -> str:
        """Extract the publication body text from the born-digital XML.

        The officiele-publicatie XML holds the body outside the <metadata>
        element. Older publications carry only a metadata stub (a pointer to an
        external metadata record) with no body -- those return empty here and
        fall through to the PDF path.
        """
        if not xml_url:
            return ""
        raw = self._download(xml_url, timeout=60)
        if not raw:
            return ""
        try:
            root = ET.fromstring(raw)
        except ET.ParseError:
            return ""

        parts = []
        for child in list(root):
            if _local(child.tag) == "metadata":
                continue
            text = "".join(child.itertext())
            if text and text.strip():
                parts.append(text)
        full_text = "\n".join(parts)
        full_text = html_mod.unescape(full_text)
        full_text = re.sub(r"[ \t]+", " ", full_text)
        full_text = re.sub(r"\n[ \t]*\n(\s*\n)+", "\n\n", full_text)
        full_text = re.sub(r"[ \t]*\n[ \t]*", "\n", full_text)
        return full_text.strip()

    def _text_from_pdf(self, pdf_url: str) -> str:
        """Extract text from the born-digital PDF manifestation via PyMuPDF."""
        if not pdf_url:
            return ""
        raw = self._download(pdf_url, timeout=120)
        if not raw:
            return ""
        try:
            import fitz  # PyMuPDF
        except Exception as e:
            logger.warning(f"PyMuPDF unavailable: {e}")
            return ""
        try:
            doc = fitz.open(stream=raw, filetype="pdf")
        except Exception as e:
            logger.warning(f"PDF open error for {pdf_url}: {e}")
            return ""
        try:
            parts = [page.get_text("text") for page in doc]
        except Exception as e:
            logger.warning(f"PDF text error for {pdf_url}: {e}")
            parts = []
        finally:
            doc.close()
        full_text = "\n".join(parts)
        full_text = re.sub(r"[ \t]+", " ", full_text)
        full_text = re.sub(r"\n{3,}", "\n\n", full_text)
        return full_text.strip()

    def _full_text(self, raw: dict) -> str:
        text = self._text_from_xml(raw.get("xml_url", ""))
        if len(text) >= MIN_TEXT_LEN:
            return text
        pdf_text = self._text_from_pdf(raw.get("pdf_url", ""))
        if len(pdf_text) > len(text):
            return pdf_text
        return text

    # ---- Pagination --------------------------------------------------------

    def _paginate(self, max_pages: Optional[int] = None
                  ) -> Generator[dict, None, None]:
        start = 1
        per_page = 50
        total = None
        page = 0

        while True:
            page += 1
            if max_pages and page > max_pages:
                return

            root = self._sru_search(start_record=start, maximum_records=per_page)
            if root is None:
                return

            if total is None:
                num_elem = root.find(".//{http://docs.oasis-open.org/ns/search-ws/sruResponse}numberOfRecords")
                total = int(num_elem.text) if num_elem is not None and num_elem.text else 0
                logger.info(f"Tractatenblad: {total} total publications")
                if total == 0:
                    return

            records = root.findall(".//{http://docs.oasis-open.org/ns/search-ws/sruResponse}record")
            if not records:
                return

            for rec in records:
                doc = self._parse_record(rec)
                if doc:
                    yield doc

            fetched = start + len(records) - 1
            if fetched >= total:
                return
            start = fetched + 1
            if page % 25 == 0:
                logger.info(f"  Page {page} ({fetched}/{total})")

    # ---- BaseScraper contract ---------------------------------------------

    def normalize(self, raw: dict) -> dict:
        doc_id = raw.get("doc_id", "")
        full_text = raw.get("_prefetched_text", "")
        if not full_text:
            full_text = self._full_text(raw)

        url = raw.get("preferred_url", "")
        if not url and doc_id:
            url = f"https://zoek.officielebekendmakingen.nl/{doc_id}.html"

        # Prefer the publication date; fall back to conclusion / modified.
        date = raw.get("date_available") or raw.get("date_concluded") or raw.get("date_modified") or ""

        return {
            "_id": f"{SOURCE_ID}/{doc_id}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": full_text,
            "date": date,
            "url": url,
            "doc_id": doc_id,
            "doc_type": raw.get("doc_type", ""),
            "subject": raw.get("subject", ""),
            "language": raw.get("language", "nl"),
            "creator": raw.get("creator", ""),
            "date_concluded": raw.get("date_concluded", ""),
        }

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        limit = 15 if sample else None
        count = 0
        max_pages = 4 if sample else None

        for raw in self._paginate(max_pages=max_pages):
            if limit and count >= limit:
                break

            text = self._full_text(raw)
            if not text or len(text) < MIN_TEXT_LEN:
                logger.warning(f"  Skipping {raw.get('doc_id', '?')} - no/short text")
                continue

            raw["_prefetched_text"] = text
            yield raw
            count += 1
            if count % 100 == 0 or sample:
                logger.info(f"  [{count}] {raw.get('title', '')[:60]} ({len(text)} chars)")

        logger.info(f"Total records yielded: {count}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        # SRU supports dt.modified filtering; enumerate and filter client-side by
        # modification date to keep the query simple and robust.
        for raw in self._paginate():
            mod = raw.get("date_modified", "")
            if mod and mod < since:
                continue
            record = self.normalize(raw)
            if record["text"] and len(record["text"]) >= MIN_TEXT_LEN:
                yield record


if __name__ == "__main__":
    scraper = NLTractatenbladScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        root = scraper._sru_search(maximum_records=1)
        if root is None:
            print("Connection FAILED")
            sys.exit(1)
        num = root.find(".//{http://docs.oasis-open.org/ns/search-ws/sruResponse}numberOfRecords")
        print(f"Connection OK. Tractatenblad publications: {num.text if num is not None else '?'}")
        records = root.findall(".//{http://docs.oasis-open.org/ns/search-ws/sruResponse}record")
        if records:
            doc = scraper._parse_record(records[0])
            if doc:
                print(f"Sample: {doc['doc_id']} - {doc['title'][:80]}")
                print(f"  xml={doc['xml_url'][:80]}")
                print(f"  pdf={doc['pdf_url'][:80]}")
    elif command in ("bootstrap", "bootstrap-fast"):
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
