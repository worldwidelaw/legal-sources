#!/usr/bin/env python3
"""
EE/RiigiTeatajaLoomal -- Estonian Riigi Teataja (State Gazette) Fetcher

Fetches Estonian legislation from Riigi Teataja with full text content.

Data access method: official JSON API (the one the riigiteataja.ee Angular
front-end itself calls) + per-act XML blob download for full text.

  - Chronology index: POST /api/v1/akt/kronoloogia
        body {"searchAfter": <offset>, "publicationDateStart": "YYYY-MM-DD",
              "publicationDateEnd": "YYYY-MM-DD"}
        -> {"kokku": <total>, "tulemused": [{id, title, publicationNotice,
                                             issuer, avaldamiseKp, ...}, ...]}
        Server page size is fixed at 10; `searchAfter` is a plain offset.
  - Full text:        GET  /api/v1/akt/{id}/blob-xml   (application/xml)

The XML format contains structured legislation with:
  - <metaandmed>: Metadata (issuer, dates, type)
  - <aktinimi>: Document title
  - <sisu>: Full content with paragraphs and sections

NOTE (2026-08-19, issue #1451): riigiteataja.ee was rebuilt as an Angular SPA.
The old server-rendered ``/kronoloogia_tulemus.html`` chronology and the
``/akt/{id}.xml`` document URL now both return the SPA HTML shell with HTTP 200,
so the previous scraper silently discovered 0 documents and wrote 0 records.
Discovery and full-text download now go through the JSON API above.

Usage:
  python bootstrap.py bootstrap           # Full historical pull
  python bootstrap.py bootstrap-fast      # Full pull, concurrent XML downloads
  python bootstrap.py bootstrap --sample  # Fetch 10+ sample records
  python bootstrap.py update              # Incremental update
  python bootstrap.py test-api            # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone, timedelta
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
logger = logging.getLogger("legal-data-hunter.EE.RiigiTeatajaLoomal")

BASE_URL = "https://www.riigiteataja.ee"
API_PREFIX = "/api/v1"

# Riigi Teataja digital chronology starts in 1990.
START_YEAR = 1990

# Server-side page size of POST /api/v1/akt/kronoloogia (not configurable).
PAGE_SIZE = 10


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
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "application/json, application/xml",
                "Accept-Language": "et,en;q=0.9",
            },
            timeout=60,
        )

        self._checkpoint_path = self.source_dir / "data" / "chronology_checkpoint.json"
        self._done_months = self._load_checkpoint()

    # -- Checkpoint / resume ---------------------------------------------------

    def _load_checkpoint(self) -> set:
        """Months (``YYYY-MM``) already fully enumerated by a previous run."""
        try:
            with open(self._checkpoint_path, "r", encoding="utf-8") as f:
                return set(json.load(f).get("completed_months", []))
        except (FileNotFoundError, ValueError, OSError):
            return set()

    def _save_checkpoint(self):
        self._checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._checkpoint_path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"completed_months": sorted(self._done_months)}, f)
        tmp.replace(self._checkpoint_path)

    # -- Document discovery via the chronology API -----------------------------

    @staticmethod
    def _month_bounds(year: int, month: int) -> tuple:
        start = datetime(year, month, 1)
        end = datetime(year + (month // 12), (month % 12) + 1, 1) - timedelta(days=1)
        return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")

    def _chronology_page(self, start: str, end: str, offset: int) -> dict:
        """One page of the chronology API. Raises on transport/HTTP error."""
        self.rate_limiter.wait()
        resp = self.client.post(
            f"{API_PREFIX}/akt/kronoloogia",
            json_data={
                "searchAfter": offset,
                "publicationDateStart": start,
                "publicationDateEnd": end,
            },
            headers={"Content-Type": "application/json"},
        )
        resp.raise_for_status()
        return resp.json()

    def _iter_month(self, year: int, month: int) -> Generator[dict, None, None]:
        """
        Yield every chronology entry published in the given month.

        Entries are the raw API rows; the full text is downloaded later in
        normalize() so that bootstrap-fast can overlap the XML downloads.
        """
        start, end = self._month_bounds(year, month)
        offset = 0
        total = None
        seen = set()

        while True:
            try:
                payload = self._chronology_page(start, end, offset)
            except Exception as e:
                logger.error(f"Chronology {start}..{end} offset {offset} failed: {e}")
                self.record_coverage_gap(
                    f"{year}-{month:02d}", "chronology_page_failed", offset=offset
                )
                return

            if total is None:
                total = payload.get("kokku") or 0
                if total:
                    logger.info(f"{year}-{month:02d}: {total} acts")

            rows = payload.get("tulemused") or []
            if not rows:
                break

            for row in rows:
                doc_id = row.get("id")
                if doc_id is None or doc_id in seen:
                    continue
                seen.add(doc_id)
                yield {"_doc_id": str(doc_id), "_index": row}

            offset += PAGE_SIZE
            if offset >= total:
                break

        if total and len(seen) < total:
            logger.warning(
                f"{year}-{month:02d}: enumerated {len(seen)} of {total} acts"
            )
            self.record_coverage_gap(
                f"{year}-{month:02d}", "incomplete_enumeration",
                enumerated=len(seen), expected=total,
            )
        else:
            self.clear_coverage_gap(f"{year}-{month:02d}")

    # -- XML Document fetching -------------------------------------------------

    def _fetch_document_xml(self, doc_id: str) -> Optional[ET.Element]:
        """Download and parse the act XML. Returns None if unavailable."""
        url = f"{API_PREFIX}/akt/{doc_id}/blob-xml"

        self.rate_limiter.wait()

        try:
            resp = self.client.get(url)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch XML for document {doc_id}: {e}")
            return None

        try:
            return ET.fromstring(resp.content)
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
        sisu = root.find(".//sisu")
        if sisu is None:
            sisu = root.find(".//{*}sisu")
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

    def _iter_range(self, first: tuple, last: tuple, use_checkpoint: bool):
        """Yield chronology entries month by month from `first` to `last`."""
        year, month = first
        end_year, end_month = last

        while (year, month) <= (end_year, end_month):
            key = f"{year}-{month:02d}"
            if use_checkpoint and key in self._done_months:
                logger.debug(f"Skipping {key} (checkpointed)")
            else:
                logger.info(f"Fetching chronology for {key}")
                yield from self._iter_month(year, month)
                if use_checkpoint:
                    self._done_months.add(key)
                    self._save_checkpoint()

            month += 1
            if month > 12:
                month, year = 1, year + 1

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield every act in the Riigi Teataja chronology, oldest month first.

        Completed months are checkpointed to data/chronology_checkpoint.json so
        a re-launched run resumes without re-walking the whole archive.
        """
        now = datetime.now(timezone.utc)
        yield from self._iter_range(
            (START_YEAR, 1), (now.year, now.month), use_checkpoint=True
        )

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield acts published since the given date (month granularity)."""
        now = datetime.now(timezone.utc)
        yield from self._iter_range(
            (since.year, since.month), (now.year, now.month), use_checkpoint=False
        )

    def normalize(self, raw: dict) -> Optional[dict]:
        """
        Transform a chronology entry into the standard schema, downloading the
        act XML for the full text.

        CRITICAL: Extracts and includes FULL TEXT from the XML content.
        """
        doc_id = raw["_doc_id"]
        index = raw.get("_index") or {}

        root = raw.get("_xml_root")
        if root is None:
            root = self._fetch_document_xml(doc_id)
        if root is None:
            return None

        # Extract metadata
        metadata = self._extract_metadata(root)

        # Extract full text content
        full_text = self._extract_text_from_xml(root)
        if not full_text:
            logger.warning(f"Document {doc_id} has no extractable text")
            return None

        # Determine the best date to use
        date = (
            metadata.get("enacted_date")
            or metadata.get("effective_date")
            or (index.get("avaldamiseKp") or "")[:10]
            or ""
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
            "title": metadata.get("title") or index.get("title") or "",
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date,
            "url": f"{BASE_URL}/akt/{doc_id}",
            # Additional metadata
            "issuer": metadata.get("issuer") or index.get("issuer") or "",
            "document_type": metadata.get("document_type") or index.get("reportType") or "",
            "text_type": metadata.get("text_type") or index.get("tekstiliik") or "",
            "abbreviation": metadata.get("abbreviation", ""),
            "enacted_date": metadata.get("enacted_date", ""),
            "effective_date": metadata.get("effective_date", ""),
            "expired_date": metadata.get("expired_date", ""),
            "rt_part": metadata.get("rt_part", ""),
            "rt_article": metadata.get("rt_article", ""),
            "global_id": metadata.get("global_id", ""),
            "publication_notice": index.get("publicationNotice", ""),
        }

    # -- Custom commands ------------------------------------------------------

    def test_api(self):
        """Quick connectivity test."""
        print("Testing Riigi Teataja API connectivity...")

        # Test a static reference endpoint
        try:
            resp = self.client.get(f"{API_PREFIX}/avalik/rtOsad")
            resp.raise_for_status()
            parts = resp.json()
            print(f"  RT parts endpoint: OK ({len(parts)} parts)")
        except Exception as e:
            print(f"  RT parts endpoint: FAILED ({e})")
            return

        # Test chronology
        try:
            entries = list(self._iter_month(2024, 12))
            print(f"  Chronology 2024-12: {len(entries)} acts")
            if not entries:
                print("  Chronology returned nothing — API contract may have changed")
                return
        except Exception as e:
            print(f"  Chronology: FAILED ({e})")
            return

        # Test XML download + extraction
        try:
            root = self._fetch_document_xml(entries[0]["_doc_id"])
            if root is None:
                print("  XML download: FAILED (no XML returned)")
                return
            text = self._extract_text_from_xml(root)
            title = self._extract_metadata(root).get("title", "")
            print(f"  XML download: OK ({len(text)} chars) — {title[:60]}...")
        except Exception as e:
            print(f"  XML download: FAILED ({e})")
            return

        print("\nConnectivity test passed!")

    def run_sample(self, n: int = 10) -> dict:
        """Fetch a sample of recent acts with full text."""
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        checked = 0
        errors = []

        now = datetime.now(timezone.utc)
        year, month = now.year, now.month
        months_checked = 0

        while saved < n and months_checked < 12:
            for raw in self._iter_month(year, month):
                checked += 1
                try:
                    normalized = self.normalize(raw)
                except Exception as e:
                    errors.append(f"{raw['_doc_id']}: {e}")
                    logger.error(f"Error processing {raw['_doc_id']}: {e}")
                    continue

                if not normalized:
                    errors.append(f"{raw['_doc_id']}: No text content")
                    continue

                if len(normalized["text"]) < 100:
                    errors.append(
                        f"{raw['_doc_id']}: Text too short ({len(normalized['text'])} chars)"
                    )
                    continue

                sample_path = sample_dir / f"{normalized['_id']}.json"
                with open(sample_path, "w", encoding="utf-8") as f:
                    json.dump(normalized, f, ensure_ascii=False, indent=2)

                saved += 1
                logger.info(
                    f"  Saved {normalized['_id']}: {normalized['title'][:50]}... "
                    f"({len(normalized['text'])} chars)"
                )
                if saved >= n:
                    break

            month -= 1
            if month < 1:
                month, year = 12, year - 1
            months_checked += 1

        text_lengths = []
        for f in sample_dir.glob("*.json"):
            with open(f, "r", encoding="utf-8") as fp:
                text_lengths.append(len(json.load(fp).get("text", "")))

        return {
            "sample_records_saved": saved,
            "documents_checked": checked,
            "months_checked": months_checked,
            "errors": errors[:10],
            "avg_text_length": sum(text_lengths) / len(text_lengths) if text_lengths else 0,
            "min_text_length": min(text_lengths) if text_lengths else 0,
            "max_text_length": max(text_lengths) if text_lengths else 0,
        }


# -- CLI Entry Point ----------------------------------------------------------


def main():
    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py "
            "[bootstrap|bootstrap-fast|update|test-api] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    scraper = RiigiTeatajaScraper()

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 10
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
            print(json.dumps(stats, indent=2))
        elif command == "bootstrap-fast":
            stats = scraper.bootstrap_fast()
            print(
                f"\nBootstrap-fast complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
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
