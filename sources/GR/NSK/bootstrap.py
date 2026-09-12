#!/usr/bin/env python3
"""
GR/NSK -- Greek Legal Council of the State (Νομικό Συμβούλιο του Κράτους)

Fetches legal opinions (γνωμοδοτήσεις) from NSK, the official legal advisory body
that provides binding legal advice to the Greek government since 1951.

Access strategy (no API exists — Liferay portlet form POST is the only endpoint):
  - POST the search portlet with ΕΤΟΣ (year) to list a year's opinions.
    The result listing already carries the full record: number, year, title
    (= the question put to NSK), Λήμματα, Διατάξεις, Πρόεδρος, Εισηγητής,
    Κατάσταση and the Περίληψη body (= the reasoned answer). No detail-page
    round trip is needed.
  - The server caps every query at the FIRST 500 results
    ("ΕΜΦΑΝΙΖΟΝΤΑΙ ΤΑ ΠΡΩΤΑ 500 ΑΠΟΤΕΛΕΣΜΑΤΑ"). Busy years (most of 1951-2010)
    exceed that, so a capped year is re-walked per opinion number
    (ΑΡΙΘΜΟΣ ΓΝΩΜΟΔΟΤΗΣΗΣ) until the number space runs dry.
  - Opinions from ~2021 onwards ship a born-digital PDF via the ΛΗΨΗ ΑΡΧΕΙΟΥ
    resource URL; that is downloaded and its text becomes the record body.
    Older PDFs are scanned images (0 chars without OCR) so they are not
    downloaded at all — the listing's question + Περίληψη is the text.

Data types: doctrine (official government legal opinions)
Auth: none (open data)
License: Public domain (official government acts)

Usage:
  python bootstrap.py bootstrap           # Full pull (all years, streams to data/)
  python bootstrap.py bootstrap-fast      # Same, concurrent normalize (fleet entry point)
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update              # Incremental update (recent years)
  python bootstrap.py test                # Quick connectivity test
"""

import sys
import os
import json
import logging
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from html import unescape

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GR.NSK")

# API configuration
BASE_URL = "https://www.nsk.gr"
SEARCH_URL = f"{BASE_URL}/web/nsk/anazitisi-gnomodoteseon"
PORTLET_ID = "nskconsulatories_WAR_nskplatformportlet"

FIRST_YEAR = 1951
# Server-side ceiling on results per query — a year hitting this is truncated.
RESULT_CAP = 500
# Consecutive empty opinion numbers before a capped year's sweep gives up.
NUMBER_GAP_TOLERANCE = 40
MAX_OPINION_NUMBER = 5000

# Opinions published from this year on carry born-digital PDFs whose text
# extracts cleanly. Everything earlier is a scanned image (0 chars, needs OCR),
# so downloading ~0.5 MB per record would buy nothing.
PDF_TEXT_FROM_YEAR = int(os.environ.get("NSK_PDF_FROM_YEAR", "2021"))

# Status codes (ΚΑΤΑΣΤΑΣΗ select options)
STATUS_MAP = {
    "1": "Αποδεκτή",          # Accepted
    "0": "Μη αποδεκτή",       # Not accepted
    "2": "Εν μέρει αποδεκτή",  # Partially accepted
    "-1": "Εκκρεμεί αποδοχή",  # Pending
    "3": "Ανακλήθηκε το ερώτημα",  # Withdrawn
    "4": "Για την αποδοχή ή μη επικοινωνήστε με τον Σχ. Επιστ. Δραστηριοτήτων κ Δημοσίων Σχέσεων",
}

# --- listing parsing -------------------------------------------------------

ROW_SPLIT = re.compile(r'<div class="article_text_inner2 consultatory"')
NUM_YEAR_RE = re.compile(
    r'<div class="gray">\s*(\d+)\s*</div>\s*<div class="blue">\s*(\d{4})\s*</div>'
)
LINK_RE = re.compile(r"consultId=(\d+)'>(.*?)</a>", re.DOTALL)
FIELD_RE = {
    "keywords": re.compile(r"<strong>\s*Λήμματα\s*:\s*</strong>(.*?)(?:<br|</p>)", re.DOTALL),
    "provisions": re.compile(r"<strong>\s*Διατάξεις\s*:\s*</strong>(.*?)(?:<br|</p>)", re.DOTALL),
    "president": re.compile(
        r"<strong>\s*Πρόεδρος/Προεδρεύων\s*:\s*</strong>(.*?)(?:<br|</p>)", re.DOTALL
    ),
    "rapporteur": re.compile(
        r"<strong>\s*Εισηγητής/Γνωμοδοτών\s*:\s*</strong>(.*?)(?:<br|</p>)", re.DOTALL
    ),
    "status": re.compile(r"<strong>\s*Κατάσταση\s*:\s*</strong>(.*?)(?:<br|</p>)", re.DOTALL),
}
# The Περίληψη body sits between the metadata </p> and the ΛΗΨΗ ΑΡΧΕΙΟΥ button.
SUMMARY_RE = re.compile(r"</p>\s*(.*?)\s*<div class=\"row\"", re.DOTALL)


# Words that appear in essentially every NSK opinion. If none survive
# extraction the PDF's font cmap is broken and the text is transliterated junk.
GREEK_SANITY_MARKERS = (
    "ΝΟΜΙΚΟ ΣΥΜΒΟΥΛΙΟ",
    "γνωμοδότησ",
    "Γνωμοδότησ",
    "ΓΝΩΜΟΔΟΤΗΣ",
    "ΕΛΛΗΝΙΚΗ ΔΗΜΟΚΡΑΤΙΑ",
    "Τμήμα",
)


def _greek_text_looks_sane(text: str) -> bool:
    """True if extracted PDF text still reads as Greek rather than mojibake."""
    return any(marker in text for marker in GREEK_SANITY_MARKERS)


def strip_html(html: str) -> str:
    """Remove HTML tags and decode entities."""
    if not html:
        return ""
    text = re.sub(r"<(script|style)\b.*?</\1>", " ", html, flags=re.DOTALL | re.I)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"[ \t\xa0]+", " ", text)
    text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
    return text.strip()


class NSKScraper(BaseScraper):
    """
    Scraper for GR/NSK -- Greek Legal Council of the State.
    Country: GR
    URL: https://www.nsk.gr

    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        # www.nsk.gr sends only its leaf certificate and omits the
        # "GeoTrust TLS RSA CA G1" intermediate, so certifi cannot complete the
        # chain and every request raises CERTIFICATE_VERIFY_FAILED "unable to
        # get local issuer certificate" (issue #1513). HttpClient repairs that
        # by fetching the intermediate from the leaf's AIA caIssuers URL and
        # retrying with an augmented CA bundle, keeping verification on.
        self.client = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "el-GR,el;q=0.9,en;q=0.8",
            },
            timeout=120,
            wall_timeout=600,
        )

        self.checkpoint_path = Path(__file__).parent / "data" / "nsk_checkpoint.json"
        self._checkpoint = self._load_checkpoint()

    # --- checkpoint --------------------------------------------------------

    def _load_checkpoint(self) -> Dict[str, Any]:
        try:
            with open(self.checkpoint_path, encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and isinstance(data.get("completed_years"), list):
                return {"completed_years": [int(y) for y in data["completed_years"]]}
        except (FileNotFoundError, ValueError, TypeError):
            pass
        return {"completed_years": []}

    def _mark_year_done(self, year: int):
        done = set(self._checkpoint["completed_years"])
        done.add(year)
        self._checkpoint["completed_years"] = sorted(done)
        try:
            self.checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self.checkpoint_path, "w", encoding="utf-8") as f:
                json.dump(self._checkpoint, f)
        except OSError as e:
            logger.warning(f"Could not persist checkpoint: {e}")

    # --- search ------------------------------------------------------------

    def _search(
        self,
        year: int,
        number: Optional[int] = None,
        status: str = "null",
    ) -> List[Dict[str, Any]]:
        """
        POST the search portlet and parse the result listing.

        The listing carries every field we need, so this is the only network
        call per batch of opinions.
        """
        self.rate_limiter.wait()

        url = (
            f"{SEARCH_URL}?p_p_id={PORTLET_ID}&p_p_lifecycle=0&p_p_state=normal"
            f"&p_p_mode=view&p_p_col_id=column-4&p_p_col_pos=2&p_p_col_count=3"
        )
        data = {
            f"_{PORTLET_ID}_isSearch": "1",
            f"_{PORTLET_ID}_inputDatefrom": str(year),
            f"_{PORTLET_ID}_consulState": status,
            f"_{PORTLET_ID}_inputKeywords": "",
            f"_{PORTLET_ID}_inputRelated": "",
            f"_{PORTLET_ID}_inputSuggestionNo": "" if number is None else str(number),
        }

        last_error = None
        for attempt in range(4):
            try:
                resp = self.client.post(url, data=data, timeout=120)
                resp.raise_for_status()
                return self._parse_listing(resp.text, year)
            except Exception as e:  # noqa: BLE001 — retry any transport/HTTP fault
                last_error = e
                logger.warning(
                    f"Search {year}/{number or 'all'} attempt {attempt + 1} failed: {e}"
                )

        raise RuntimeError(
            f"nsk.gr search failed for year={year} number={number}: {last_error}"
        )

    def _parse_listing(self, html: str, year: int) -> List[Dict[str, Any]]:
        """Parse the search result listing into raw opinion dicts."""
        chunks = ROW_SPLIT.split(html)[1:]
        out = []
        for chunk in chunks:
            link = LINK_RE.search(chunk)
            if not link:
                continue
            consult_id = int(link.group(1))

            num_year = NUM_YEAR_RE.search(chunk)
            number = int(num_year.group(1)) if num_year else None
            row_year = int(num_year.group(2)) if num_year else year

            record = {
                "consult_id": consult_id,
                "number": number,
                "year": row_year,
                "title": strip_html(link.group(2)),
            }
            for key, pattern in FIELD_RE.items():
                m = pattern.search(chunk)
                record[key] = strip_html(m.group(1)) if m else None

            summary = SUMMARY_RE.search(chunk)
            record["summary"] = strip_html(summary.group(1)) if summary else None
            out.append(record)
        return out

    # --- PDF full text -----------------------------------------------------

    def _pdf_url(self, consult_id: int) -> str:
        return (
            f"{SEARCH_URL}?p_p_id={PORTLET_ID}&p_p_lifecycle=2&p_p_state=normal"
            f"&p_p_mode=view&p_p_cacheability=cacheLevelPage"
            f"&_{PORTLET_ID}_consultId={consult_id}"
            f"&_{PORTLET_ID}_jspPage=%2Fjsps%2Fconsulatories%2Fview-consultatory.jsp"
        )

    def _fetch_pdf_text(self, consult_id: int) -> Optional[str]:
        """Download the ΛΗΨΗ ΑΡΧΕΙΟΥ PDF and extract its text, or None."""
        try:
            from common.pdf_extract import extract_pdf_markdown
        except ImportError:
            return None

        try:
            self.rate_limiter.wait()
            resp = self.client.get(self._pdf_url(consult_id), timeout=180)
            resp.raise_for_status()
            if "pdf" not in resp.headers.get("Content-Type", "").lower():
                return None
            text = extract_pdf_markdown(
                "GR/NSK",
                str(consult_id),
                pdf_bytes=resp.content,
                table="doctrine",
                force=True,
            )
            if not text:
                return None
            if not _greek_text_looks_sane(text):
                # A few NSK PDFs embed a non-standard Greek cmap; pdfplumber then
                # emits transliterated mojibake ("ΓΗΜΟΚΡΑΣΙΑ" for "ΔΗΜΟΚΡΑΤΙΑ").
                # Better to keep the clean Περίληψη than to store garbage.
                logger.warning(
                    f"PDF text for {consult_id} failed the Greek sanity check "
                    f"(broken font cmap) — falling back to the listing summary"
                )
                return None
            return text
        except Exception as e:  # noqa: BLE001 — PDF is an enrichment, never fatal
            logger.debug(f"PDF text unavailable for {consult_id}: {e}")
            return None

    # --- crawl -------------------------------------------------------------

    def _fetch_year(self, year: int) -> Generator[dict, None, None]:
        """
        Yield every opinion of a year, working around the 500-result cap.

        A year under the cap comes back in one request. A capped year is
        complete only up to the last fully-listed opinion number, so the rest
        of the number space is walked one ΑΡΙΘΜΟΣ ΓΝΩΜΟΔΟΤΗΣΗΣ at a time.
        """
        rows = self._search(year)
        if not rows:
            return

        if len(rows) < RESULT_CAP:
            for row in rows:
                yield row
            return

        # Truncated: the highest number listed may itself be cut in half, so
        # trust everything below it and re-query from that number on.
        numbers = [r["number"] for r in rows if r["number"] is not None]
        resume_at = max(numbers) if numbers else 1
        logger.info(
            f"Year {year} hit the {RESULT_CAP}-result cap at opinion no. {resume_at} "
            f"— sweeping by opinion number from there"
        )
        for row in rows:
            if row["number"] is not None and row["number"] < resume_at:
                yield row

        misses = 0
        number = resume_at
        while misses < NUMBER_GAP_TOLERANCE and number <= MAX_OPINION_NUMBER:
            batch = self._search(year, number=number)
            # inputSuggestionNo is an exact match on ΑΡΙΘΜΟΣ; keep only this year.
            batch = [r for r in batch if r["year"] == year and r["number"] == number]
            if batch:
                misses = 0
                for row in batch:
                    yield row
            else:
                misses += 1
            number += 1

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all NSK opinions from 1951 to the current year."""
        current_year = datetime.now().year
        done = set(self._checkpoint["completed_years"])

        for year in range(current_year, FIRST_YEAR - 1, -1):
            if year in done:
                logger.info(f"Year {year} already completed — skipping (checkpoint)")
                continue

            logger.info(f"Fetching opinions for year {year}...")
            count = 0
            for row in self._fetch_year(year):
                count += 1
                yield row
            logger.info(f"Year {year}: {count} opinions")
            self._mark_year_done(year)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield opinions from the years touched since the given date."""
        current_year = datetime.now().year
        for year in range(current_year, max(since.year, FIRST_YEAR) - 1, -1):
            logger.info(f"Checking updates for year {year}...")
            yield from self._fetch_year(year)

    # --- normalize ---------------------------------------------------------

    def normalize(self, raw: dict) -> dict:
        """Transform a raw NSK listing row into the standard schema."""
        consult_id = raw["consult_id"]
        number = raw.get("number")
        year = raw.get("year")

        title = raw.get("title") or ""
        display_title = title
        if not display_title:
            display_title = (
                f"Γνωμοδότηση ΝΣΚ {number}/{year}" if number and year
                else f"Γνωμοδότηση {consult_id}"
            )
        elif len(display_title) > 200:
            display_title = display_title[:200] + "..."

        # Body: the born-digital PDF is the real opinion; before ~2021 the PDFs
        # are scans, so the listing's question + Περίληψη is what we have.
        full_text = None
        if year and year >= PDF_TEXT_FROM_YEAR:
            full_text = self._fetch_pdf_text(consult_id)

        if not full_text:
            parts = []
            if title:
                parts.append(f"ΕΡΩΤΗΜΑ:\n{title}")
            if raw.get("summary"):
                parts.append(f"ΑΠΑΝΤΗΣΗ:\n{raw['summary']}")
            if raw.get("provisions"):
                parts.append(f"ΔΙΑΤΑΞΕΙΣ:\n{raw['provisions']}")
            full_text = "\n\n".join(parts)

        if not full_text.strip():
            return None

        keywords = []
        if raw.get("keywords"):
            keywords = [k.strip() for k in raw["keywords"].split(",") if k.strip()]

        date = None
        if year:
            try:
                date = datetime(int(year), 1, 1, tzinfo=timezone.utc).isoformat()
            except (ValueError, TypeError):
                pass

        url = (
            f"{SEARCH_URL}?p_p_id={PORTLET_ID}&p_p_lifecycle=0&p_p_state=normal"
            f"&p_p_mode=view"
            f"&_{PORTLET_ID}_jspPage=%2Fjsps%2Fconsulatories%2Fview-consultatory.jsp"
            f"&_{PORTLET_ID}_consultId={consult_id}"
        )

        return {
            "_id": f"NSK-{consult_id}",
            "_source": "GR/NSK",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": display_title,
            "text": full_text,
            "date": date,
            "url": url,
            "pdf_url": self._pdf_url(consult_id),
            "consult_id": consult_id,
            "opinion_number": number,
            "year": year,
            "president": raw.get("president") or None,
            "rapporteur": raw.get("rapporteur") or None,
            "provisions": raw.get("provisions") or None,
            "keywords": keywords,
            "status": raw.get("status") or None,
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="GR/NSK Data Fetcher - Greek Legal Council of the State"
    )
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true",
                        help="Only fetch sample records for validation")
    parser.add_argument("--full", action="store_true",
                        help="Fetch all records (default for bootstrap)")
    parser.add_argument("--sample-size", type=int, default=12)
    args = parser.parse_args()

    scraper = NSKScraper()

    if args.command == "test":
        print("Testing GR/NSK search portlet...")
        rows = scraper._search(2024)
        print(f"Year 2024: {len(rows)} opinions listed")
        if not rows:
            print("FAILED: no results for 2024")
            sys.exit(1)

        row = rows[0]
        print(f"  First: no. {row['number']}/{row['year']} (consultId {row['consult_id']})")
        print(f"  Title: {(row['title'] or '')[:80]}...")
        print(f"  Περίληψη: {len(row.get('summary') or '')} chars")

        record = scraper.normalize(row)
        if not record:
            print("FAILED: normalize produced no text")
            sys.exit(1)
        print(f"\nNormalized: {record['_id']} — {len(record['text'])} chars of text")

        capped = scraper._search(1990)
        print(f"Year 1990: {len(capped)} listed (cap is {RESULT_CAP})")

    elif args.command == "bootstrap":
        if args.sample:
            print("Fetching sample records from GR/NSK...")
            stats = scraper.bootstrap(sample_mode=True, sample_size=args.sample_size)
        else:
            print("Bootstrapping GR/NSK (all years 1951-present)...")
            stats = scraper.bootstrap()
        print(json.dumps(stats, indent=2, default=str))

    elif args.command == "bootstrap-fast":
        print("Bootstrapping GR/NSK (fast mode, all years 1951-present)...")
        stats = scraper.bootstrap_fast()
        print(json.dumps(stats, indent=2, default=str))

    elif args.command == "update":
        stats = scraper.update()
        print(json.dumps(stats, indent=2, default=str))


if __name__ == "__main__":
    main()
