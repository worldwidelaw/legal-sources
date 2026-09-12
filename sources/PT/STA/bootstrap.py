#!/usr/bin/env python3
"""
PT/STA -- Portuguese Supreme Administrative Court Case Law Fetcher

Fetches Supreme Administrative Court (Supremo Tribunal Administrativo - STA) decisions
from the DGSI jurisprudence database at dgsi.pt/jsta.nsf.

Strategy:
  - Enumeration: the Domino view API
    /jsta.nsf/Por+Ano?ReadViewEntries&Start=N&Count=M&OutputFormat=JSON
    returns structured entries (UNID + ISO decision date + case number + rapporteur)
    plus the authoritative "@toplevelentries" total (90,047 as of 2026-08).
    This replaces scraping /jsta.nsf?OpenDatabase&Start=N, whose page size is not
    fixed (108 view positions on page 1, 99 afterwards) — the old hardcoded
    PAGE_SIZE=121 silently skipped ~20 entries per page.
  - Document detail: /jsta.nsf/{view_id}/{doc_id}?OpenDocument&ExpandSection=1 gives full text
  - The view_id is constant: 35fbbbf22e1bb1e680256f8e003ea931

Data:
  - Case types: Administrative contentious, Tax/customs contentious
  - Coverage: Administrative since 1950, Tax since 1963, full text from 2002
  - Sections: Secção do Contencioso Administrativo, Secção do Contencioso Tributário
  - License: Public (open government data)
  - Full text: HTML content with legal arguments, facts, and decision

Note: the view is sorted by decision date descending, and full text stops abruptly
at 2002-01-09 (view position 35,005). Positions 35,006-90,047 are 2001-12-24 and
older and carry no "Texto Integral" at all, so walking them costs hours and writes
nothing. fetch_all() therefore stops once NO_TEXT_TOLERANCE consecutive documents
come back without full text. Usable corpus: ~35,000 decisions (2002-present).

Usage:
  python bootstrap.py bootstrap          # Full initial pull (resumes from checkpoint)
  python bootstrap.py bootstrap-fast     # Same, the VPS fleet entrypoint
  python bootstrap.py bootstrap --sample # Fetch 12 sample records for validation
  python bootstrap.py bootstrap --restart # Ignore the checkpoint and walk from the top
  python bootstrap.py update             # Incremental update (recent decisions)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from urllib.parse import urljoin, quote
from concurrent.futures import ThreadPoolExecutor

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PT.STA")

# Base URL for the DGSI STA database
BASE_URL = "http://www.dgsi.pt"

# The STA database path
JSTA_DB = "/jsta.nsf"

# Constant view ID for document links
VIEW_ID = "35fbbbf22e1bb1e680256f8e003ea931"

# Domino view queried through the ReadViewEntries API (sorted by date descending)
VIEW_NAME = "Por+Ano"

# View entries requested per ReadViewEntries call
VIEW_PAGE = 200

# Concurrent detail fetches per view page
DETAIL_WORKERS = 5

# Consecutive documents without "Texto Integral" that end the walk. The full-text
# era ends sharply at 2002-01-09 (view position 35,005); everything older is
# metadata-only, so continuing past the boundary writes nothing for hours.
NO_TEXT_TOLERANCE = 400

# Minimum characters for a document to count as having full text
MIN_TEXT_CHARS = 100


class STAScraper(BaseScraper):
    """
    Scraper for PT/STA -- Portuguese Supreme Administrative Court.
    Country: PT
    URL: https://www.dgsi.pt/jsta.nsf

    Data types: case_law
    Auth: none (Public government data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        # Sample runs stop after a dozen records; they must not leave a
        # checkpoint that makes the next full crawl skip the whole corpus.
        self.use_checkpoint = True

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "pt-PT,pt;q=0.9,en;q=0.8",
                "Accept-Charset": "utf-8,ISO-8859-1;q=0.7,*;q=0.3",
            },
            timeout=60,
        )

    # ------------------------------------------------------------------
    # Enumeration via the Domino ReadViewEntries JSON API
    # ------------------------------------------------------------------

    def _fetch_view_page(self, start: int, count: int = VIEW_PAGE) -> Optional[dict]:
        """
        Read a slice of the "Por Ano" view as JSON.

        Returns the decoded payload, or None if the request failed.
        """
        url = (
            f"{JSTA_DB}/{VIEW_NAME}?ReadViewEntries"
            f"&Start={start}&Count={count}&OutputFormat=JSON"
        )

        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)

            if resp.status_code != 200:
                logger.warning(f"View fetch failed for start={start}: HTTP {resp.status_code}")
                return None

            return json.loads(resp.content.decode("utf-8", errors="replace"))

        except Exception as e:
            logger.warning(f"Error fetching view page start={start}: {e}")
            return None

    @staticmethod
    def _parse_view_page(payload: dict) -> List[Dict[str, str]]:
        """
        Turn a ReadViewEntries payload into listing entries.

        Each entry carries: position, doc_id (UNID), session_date (ISO),
        case_number, rapporteur.
        """
        entries = payload.get("viewentry") or []
        if isinstance(entries, dict):  # Domino collapses a single entry to an object
            entries = [entries]

        results = []
        for entry in entries:
            unid = entry.get("@unid")
            if not unid:
                continue

            columns = {}
            for col in entry.get("entrydata") or []:
                name = col.get("@name")
                if not name:
                    continue
                for kind in ("text", "datetime", "number"):
                    if kind in col:
                        columns[name] = str(col[kind].get("0", "")).strip()
                        break

            # DATAAC arrives as a Domino datetime string, e.g. "20260714" or
            # "20260714T000000,00Z"
            raw_date = columns.get("DATAAC", "")
            session_date = ""
            if len(raw_date) >= 8 and raw_date[:8].isdigit():
                session_date = f"{raw_date[0:4]}-{raw_date[4:6]}-{raw_date[6:8]}"

            results.append({
                "position": entry.get("@position", ""),
                "doc_id": unid.lower(),
                "session_date": session_date,
                "case_number": columns.get("PROCESSO", ""),
                "rapporteur": columns.get("RELATOR", ""),
                "descriptors": "",
            })

        return results

    def _iter_view_entries(self, start: int = 1) -> Generator[Dict[str, str], None, None]:
        """
        Yield every view entry from `start` to the end of the view.

        Pagination advances to (last returned position + 1) rather than a
        hardcoded page size: the view interleaves category rows, so positions are
        not contiguous with the number of documents returned.
        """
        total = None
        while True:
            payload = self._fetch_view_page(start)
            if payload is None:
                raise RuntimeError(
                    f"dgsi.pt ReadViewEntries returned no usable payload at Start={start} "
                    "— refusing to report a silently truncated corpus"
                )

            if total is None:
                total = payload.get("@toplevelentries")
                logger.info(f"View reports {total} total entries")

            entries = self._parse_view_page(payload)
            if not entries:
                logger.info(f"End of view reached at Start={start}")
                return

            for entry in entries:
                yield entry

            try:
                last_position = int(str(entries[-1]["position"]).split(".")[0])
            except (ValueError, KeyError):
                last_position = start + len(entries) - 1

            next_start = max(last_position + 1, start + 1)
            if next_start <= start:
                return
            start = next_start

    # ------------------------------------------------------------------
    # Checkpointing
    # ------------------------------------------------------------------

    def _checkpoint_path(self) -> Path:
        path = Path(self.source_dir) / "data" / "sta_checkpoint.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def _load_checkpoint(self) -> dict:
        path = self._checkpoint_path()
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning(f"Ignoring unreadable checkpoint {path}: {e}")
            return {}

    def _save_checkpoint(self, **fields):
        if not self.use_checkpoint:
            return
        path = self._checkpoint_path()
        try:
            path.write_text(json.dumps(fields, indent=2), encoding="utf-8")
        except Exception as e:
            logger.warning(f"Could not write checkpoint {path}: {e}")

    def _fetch_document(self, doc_id: str) -> Optional[Dict[str, Any]]:
        """
        Fetch a full document with all metadata and full text.

        Args:
            doc_id: The Lotus Notes document ID (hex string)

        Returns:
            Dict with all parsed fields, or None if fetch failed
        """
        url = f"{JSTA_DB}/{VIEW_ID}/{doc_id}?OpenDocument&ExpandSection=1"

        try:
            self.rate_limiter.wait()
            resp = self.client.get(url)

            if resp.status_code != 200:
                logger.debug(f"Document fetch failed: {doc_id} -> HTTP {resp.status_code}")
                return None

            # Decode content
            try:
                html_content = resp.content.decode("iso-8859-1")
            except:
                html_content = resp.content.decode("utf-8", errors="replace")

            return self._parse_document(html_content, doc_id)

        except Exception as e:
            logger.warning(f"Error fetching document {doc_id}: {e}")
            return None

    def _parse_document(self, html_content: str, doc_id: str) -> Optional[Dict[str, Any]]:
        """
        Parse a document page to extract all fields.
        """
        result = {"doc_id": doc_id}

        # Helper function to extract a field value from the two-column table structure
        # Format: <td>...<font ...>Label:</font>...</td>[stray tags]<td>...VALUE...</td>
        #
        # DGSI emits a stray <font> between the label cell and the value cell for
        # some fields (notably Sumário), which is why a rigid `</td><td>` pattern
        # returned nothing and every record shipped an empty summary. Tolerate any
        # run of tags between the two cells and clean whatever markup wraps the value.
        def extract_field(label_pattern: str) -> Optional[str]:
            pattern = (
                label_pattern + r'</font></b></td>'
                r'(?:\s*<(?!td[\s>])[^>]*>)*'
                r'\s*<td[^>]*>(.*?)</td>'
            )
            match = re.search(pattern, html_content, re.DOTALL | re.IGNORECASE)
            if not match:
                return None
            value = self._clean_html(match.group(1)).strip()
            return value or None

        # Extract basic fields
        result["case_number"] = extract_field(r'Processo:')
        result["date"] = extract_field(r'Data do Acord[^<]*:')
        result["section"] = extract_field(r'Tribunal:')
        result["rapporteur"] = extract_field(r'Relator:')
        result["conventional_number"] = extract_field(r'N[^<]*Convencional:')
        result["document_number"] = extract_field(r'N[^<]*do Documento:')
        result["appellant"] = extract_field(r'Recorrente:')
        result["appellee"] = extract_field(r'Recorrido[^<]*:')
        result["voting"] = extract_field(r'Vota[^<]*o:')

        # Extract summary (may have more complex content)
        result["summary"] = extract_field(r'Sum[^<]*rio:') or ""

        # Extract descriptors (one per line, separated by <br> in the source)
        descriptors_text = extract_field(r'Descritores:') or ""
        result["descriptors"] = [d.strip() for d in descriptors_text.split("\n") if d.strip()]

        # Extract full text (Texto Integral)
        # The full text is in a table row after the "Texto Integral" section
        texto_match = re.search(
            r'Texto Integral:</font></b></td>\s*<td[^>]*>(.*?)</td>\s*</tr>',
            html_content,
            re.DOTALL | re.IGNORECASE
        )
        if texto_match:
            texto_html = texto_match.group(1)
            result["full_text"] = self._clean_html(texto_html)
        else:
            # Try alternative pattern
            texto_match2 = re.search(
                r'<b><font[^>]*>Texto Integral</font></b>.*?<td[^>]*>(.*?)</td>\s*</tr>',
                html_content,
                re.DOTALL | re.IGNORECASE
            )
            if texto_match2:
                texto_html = texto_match2.group(1)
                result["full_text"] = self._clean_html(texto_html)

        return result

    def _clean_html(self, html_text: str) -> str:
        """Strip HTML tags and clean text."""
        if not html_text:
            return ""

        # Remove style and script tags
        text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)

        # Convert br/p/div to newlines
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)

        # Remove all remaining tags
        text = re.sub(r'<[^>]+>', '', text)

        # Decode HTML entities
        text = html.unescape(text)

        # Clean up whitespace
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' +', ' ', text)

        # Strip lines
        lines = [line.strip() for line in text.split('\n')]
        text = '\n'.join(lines)

        return text.strip()

    def _parse_date(self, date_str: str) -> Optional[str]:
        """
        Normalize a DGSI date to ISO YYYY-MM-DD.

        DGSI's Domino front end renders the detail-page date according to the
        request's Accept-Language: "14-07-2026" (DD-MM-YYYY) for pt-PT, but
        "07/14/2026" (MM/DD-YYYY) for the default locale. Neither is the
        DD/MM/YYYY this scraper previously assumed, which produced impossible
        months like "2026-14-07" and left `date` null for every record.

        Accepts an already-ISO string, YYYYMMDD, dash-separated DD-MM-YYYY and
        slash-separated MM/DD/YYYY, disambiguating whenever a component exceeds 12.
        """
        if not date_str:
            return None

        date_str = str(date_str).strip()

        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_str):
            iso = date_str
        elif re.fullmatch(r"\d{8}", date_str):
            iso = f"{date_str[0:4]}-{date_str[4:6]}-{date_str[6:8]}"
        else:
            match = re.fullmatch(r"(\d{1,2})([-/])(\d{1,2})\2(\d{4})", date_str)
            if not match:
                return None
            first, separator, second, year = match.groups()
            first, second, year = int(first), int(second), int(year)
            if separator == "-":
                day, month = first, second  # pt-PT rendering
            else:
                month, day = first, second  # default (US) rendering
            if month > 12 >= day:  # trust the component that can only be a day
                month, day = day, month
            iso = f"{year:04d}-{month:02d}-{day:02d}"

        try:
            parsed = datetime.strptime(iso, "%Y-%m-%d")
        except ValueError:
            logger.debug(f"Unparseable date: {date_str!r}")
            return None

        # The view carries a handful of corrupt years (e.g. "0223")
        if not (1900 <= parsed.year <= datetime.now(timezone.utc).year + 1):
            return None

        return iso

    def fetch_all(self, restart: bool = False) -> Generator[dict, None, None]:
        """
        Yield all full-text decisions of the Portuguese Supreme Administrative Court.

        Walks the "Por Ano" view newest-to-oldest, detail-fetching each entry, and
        stops once NO_TEXT_TOLERANCE consecutive documents come back without a
        "Texto Integral" section — the pre-2002 tail is metadata-only and walking
        it produced hours of writing nothing (issue #1461).

        Progress is checkpointed to data/sta_checkpoint.json so a torn-down fleet
        slot resumes where it stopped instead of re-walking from the top.
        """
        checkpoint = self._load_checkpoint() if (self.use_checkpoint and not restart) else {}
        start = int(checkpoint.get("next_position") or 1)
        if start > 1:
            logger.info(f"Resuming from checkpoint at view position {start}")

        processed = 0
        consecutive_no_text = 0
        buffer: List[Dict[str, str]] = []
        stop = False

        def flush(entries: List[Dict[str, str]]):
            """Detail-fetch a slice of entries concurrently, preserving view order."""
            with ThreadPoolExecutor(max_workers=DETAIL_WORKERS) as pool:
                return list(pool.map(lambda e: self._fetch_document(e["doc_id"]), entries))

        for entry in self._iter_view_entries(start):
            buffer.append(entry)
            if len(buffer) < DETAIL_WORKERS * 4:
                continue

            for entry_, doc in zip(buffer, flush(buffer)):
                processed += 1
                if doc is None:
                    continue

                doc["listing_data"] = entry_
                if len((doc.get("full_text") or "").strip()) >= MIN_TEXT_CHARS:
                    consecutive_no_text = 0
                    yield doc
                else:
                    consecutive_no_text += 1
                    if consecutive_no_text >= NO_TEXT_TOLERANCE:
                        stop = True
                        break

            last_position = buffer[-1]["position"]
            buffer = []

            if processed % 1000 == 0 or stop:
                logger.info(f"Processed {processed} view entries (position {last_position})")

            if stop:
                logger.info(
                    f"Stopping at view position {last_position}: "
                    f"{NO_TEXT_TOLERANCE} consecutive documents with no Texto Integral "
                    "(pre-2002 metadata-only tail)"
                )
                self._save_checkpoint(
                    next_position=1,
                    completed=True,
                    completed_at=datetime.now(timezone.utc).isoformat(),
                    last_position=last_position,
                    entries_processed=processed,
                )
                return

            try:
                self._save_checkpoint(
                    next_position=int(str(last_position).split(".")[0]) + 1,
                    completed=False,
                    entries_processed=processed,
                )
            except ValueError:
                pass

        # View exhausted without ever hitting the no-text tail.
        if buffer:
            for entry_, doc in zip(buffer, flush(buffer)):
                processed += 1
                if doc is None:
                    continue
                doc["listing_data"] = entry_
                if len((doc.get("full_text") or "").strip()) >= MIN_TEXT_CHARS:
                    yield doc

        self._save_checkpoint(
            next_position=1,
            completed=True,
            completed_at=datetime.now(timezone.utc).isoformat(),
            entries_processed=processed,
        )

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield decisions newer than `since`.

        The view is sorted by decision date descending, so walking from the top
        and stopping at the first older entry covers every new decision.
        """
        cutoff = since.replace(tzinfo=None)
        max_entries = 5000  # guard against a re-sorted view walking the whole corpus

        for index, entry in enumerate(self._iter_view_entries(1)):
            if index >= max_entries:
                logger.warning(
                    f"fetch_updates scanned {max_entries} entries without reaching "
                    f"{cutoff.date()} — stopping; run a full bootstrap instead"
                )
                return

            iso_date = self._parse_date(entry.get("session_date", ""))
            if iso_date and datetime.fromisoformat(iso_date) < cutoff:
                return

            doc = self._fetch_document(entry["doc_id"])
            if doc:
                doc["listing_data"] = entry
                yield doc

    def normalize(self, raw: dict) -> Optional[dict]:
        """
        Transform raw decision data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        Returns None if full text is not available (pre-2002 decisions).
        """
        listing = raw.get("listing_data", {})

        # Get full text first - skip records without full text
        full_text = raw.get("full_text", "")
        if not full_text or len(full_text.strip()) < 100:
            # Pre-2002 decisions don't have full text available in DGSI
            # Skip these records rather than ingesting empty text
            return None

        # Get identifiers. Note `or` rather than a dict.get() default: the detail
        # parser stores an explicit None when a field is absent, so a default
        # would never fire.
        case_number = raw.get("case_number") or listing.get("case_number", "")
        doc_id = raw.get("doc_id", "")
        conv_num = raw.get("conventional_number") or ""

        # Build document ID
        if conv_num:
            doc_id_str = f"STA-{conv_num}"
        elif case_number:
            doc_id_str = f"STA-{case_number.replace('/', '-').replace(' ', '')}"
        else:
            doc_id_str = f"STA-{doc_id[:16]}"

        # Get date. The view's DATAAC column is already ISO and authoritative;
        # the detail page's MM/DD/YYYY rendering is the fallback.
        iso_date = (
            self._parse_date(listing.get("session_date", ""))
            or self._parse_date(raw.get("date") or "")
        )

        # Get summary
        summary = raw.get("summary") or ""

        # Get section
        section = raw.get("section") or ""

        # Build title
        title = f"Acórdão STA {case_number}"
        if section:
            # Abbreviate section name
            if "ADMINISTRATIVO" in section.upper():
                title = f"{title} (CA)"
            elif "TRIBUTÁRIO" in section.upper() or "TRIBUT" in section.upper():
                title = f"{title} (CT)"

        # Get descriptors
        descriptors = raw.get("descriptors", [])
        if isinstance(descriptors, str):
            descriptors = [d.strip() for d in descriptors.split("\n") if d.strip()]

        # Build URL
        url = f"{BASE_URL}{JSTA_DB}/{VIEW_ID}/{doc_id}?OpenDocument"

        return {
            # Required base fields
            "_id": doc_id_str,
            "_source": "PT/STA",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "summary": summary,
            "date": iso_date,
            "url": url,
            # Case law specific fields
            "case_number": case_number,
            "conventional_number": conv_num,
            "document_number": raw.get("document_number") or "",
            "rapporteur": raw.get("rapporteur") or listing.get("rapporteur", ""),
            "section": section,
            "descriptors": descriptors,
            "appellant": raw.get("appellant") or "",
            "appellee": raw.get("appellee") or "",
            "voting": raw.get("voting") or "",
            # Source info
            "court": "Supremo Tribunal Administrativo",
            "jurisdiction": "PT",
            "language": "pt",
            "doc_id": doc_id,
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Portuguese Supreme Administrative Court (STA) endpoints...")

        # Test 1: view enumeration
        print("\n1. Testing ReadViewEntries view API...")
        payload = self._fetch_view_page(start=1, count=20)
        entries = []
        if payload:
            print(f"   Total view entries: {payload.get('@toplevelentries')}")
            entries = self._parse_view_page(payload)
            print(f"   Found {len(entries)} entries")

            if entries:
                sample = entries[0]
                print(f"   Sample entry:")
                print(f"     Case: {sample.get('case_number')}")
                print(f"     Date: {sample.get('session_date')}")
                print(f"     Rapporteur: {sample.get('rapporteur')}")
                print(f"     Doc ID: {sample.get('doc_id')}")
        else:
            print("   Failed to fetch view page")

        # Test 2: Document fetch with full text
        print("\n2. Testing document fetch (full text)...")
        if entries:
            doc_id = entries[0]["doc_id"]
            doc = self._fetch_document(doc_id)
            if doc:
                print(f"   Document fetched successfully")
                print(f"   Case number: {doc.get('case_number')}")
                print(f"   Date: {doc.get('date')}")
                print(f"   Section: {doc.get('section')}")
                print(f"   Rapporteur: {doc.get('rapporteur')}")
                print(f"   Conventional number: {doc.get('conventional_number')}")

                full_text = doc.get("full_text", "")
                print(f"   Full text length: {len(full_text)} characters")
                if full_text:
                    print(f"   Full text preview: {full_text[:400]}...")

                summary = doc.get("summary", "")
                print(f"   Summary length: {len(summary)} characters")

                descriptors = doc.get("descriptors", [])
                print(f"   Descriptors: {descriptors}")
            else:
                print("   Failed to fetch document")

        # Test 3: pagination advances without gaps
        print("\n3. Testing pagination...")
        if entries:
            next_start = int(str(entries[-1]["position"]).split(".")[0]) + 1
            payload2 = self._fetch_view_page(start=next_start, count=5)
            entries2 = self._parse_view_page(payload2) if payload2 else []
            print(f"   Next Start={next_start} -> {len(entries2)} entries")
            if entries2:
                print(f"   First entry: {entries2[0].get('case_number')} "
                      f"({entries2[0].get('session_date')})")
                overlap = {e["doc_id"] for e in entries} & {e["doc_id"] for e in entries2}
                print(f"   Overlap with page 1: {len(overlap)} (expected 0)")

        print("\nTest complete!")


def main():
    scraper = STAScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] "
            "[--sample] [--sample-size N] [--restart]"
        )
        sys.exit(1)

    command = sys.argv[1]
    # bootstrap-fast is the VPS fleet entrypoint; treat it as a full bootstrap.
    # Without this alias argparse-style dispatch fell through and the pipeline
    # re-ingested sample/ instead of crawling (issue #1461).
    sample_mode = "--sample" in sys.argv and command != "bootstrap-fast"
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if "--restart" in sys.argv:
        scraper._save_checkpoint(next_position=1, completed=False)
        print("Checkpoint reset — walking the view from the top.")

    if command == "test":
        scraper.test_connection()

    elif command in ("bootstrap", "bootstrap-fast", "--full"):
        if sample_mode:
            scraper.use_checkpoint = False
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
