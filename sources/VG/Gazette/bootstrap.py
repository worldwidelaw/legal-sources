#!/usr/bin/env python3
"""
VG/Gazette -- The Virgin Islands Official Gazette (full text)

The Official Gazette is the official publication of the Government of the
British Virgin Islands, published weekly on Thursdays (plus Extraordinary
editions) by the Gazette Unit of the Cabinet Office. Each edition carries
Government and Statutory Notices -- statutory instruments, proclamations,
commencement notices, appointments, land/court/election notices -- and the
Acts, Bills and Statutory Instruments circulated with it as attachments.

Access model (this is the important part):
  eservices.gov.vg/gazette is a Drupal 7 site whose *browse and search pages
  are login-gated*: /gazette/content/recent-gazettes returns HTTP 403 to an
  anonymous client, taxonomy term pages report "no content classified with
  this term", and the gazette nodes themselves 403. Registration is free but
  this project only ingests genuinely open data, so we do not authenticate.

  The site's own Help page states the access tiers: Gazettes are free in their
  entirety for two months after publication, after which "only Government and
  Statutory Notices and Attachments are available free of charge"; the
  Liquidation and Other Notices (Part 2) archive needs a $500/year enhanced
  subscription.

  The free tiers are served as static files out of the public Drupal file
  system and need no session at all:

      /sites/eservices.gov.vg.gazette/files/governmentandstatutorynotices/  (Part 1)
      /sites/eservices.gov.vg.gazette/files/archiveattachments/             (Acts / SIs)

  Part 1 of the archive is *enumerable*: editions are numbered G00001 upwards,
  one file per edition, verified live from G00001 (9 November 2006) through
  G00767 (late 2015). Post-2015 editions moved to a free-form
  "#<issue> <date> Part 1.pdf" name that cannot be derived, so those are
  carried as a seed list harvested from the Internet Archive's index of the
  same public directory.

  We deliberately do NOT fetch /files/LiquidationandOtherNotices/ -- that is
  the subscription-gated Part 2, and downloading it would be circumventing a
  paywall even though the files answer.

Data:
  - ~760 enumerable Government & Statutory Notices editions, Nov 2006 - 2015
  - a seed list of later Part 1 editions (2016-2023) and free attachments
    (Acts, Statutory Instruments, Orders) discovered via the public file index
  - Full text extracted from the official born-digital PDFs. Language: English.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records
  python bootstrap.py bootstrap-fast     # Concurrent full pull (VPS pipeline)
  python bootstrap.py test               # Connectivity test
"""

import re
import sys
import logging
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.VG.Gazette")

SOURCE_ID = "VG/Gazette"
BASE_URL = "https://eservices.gov.vg/gazette"
FILES_URL = f"{BASE_URL}/sites/eservices.gov.vg.gazette/files"

# Free tiers only. LiquidationandOtherNotices/ is the $500/yr Part 2 archive
# and is intentionally absent.
NOTICES_DIR = f"{FILES_URL}/governmentandstatutorynotices"
ATTACH_DIR = f"{FILES_URL}/archiveattachments"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research; github.com/ZachLaik/LegalDataHunter)",
    "Accept": "application/pdf,*/*",
}

# Sequential Part 1 range. G00001 = 9 November 2006 (the start of the online
# archive, which the site's FAQ dates to October 2006); the sequence stops at
# G00767 when the Gazette Unit switched to date-based file names. A handful of
# numbers inside the range 404 -- those are genuine gaps, not a block.
FIRST_EDITION = 1
LAST_EDITION = 767

# Post-2015 Part 1 editions. The Gazette Unit stopped using the G-number scheme
# and these names are not derivable (the issue number drifts against the
# publication date because Extraordinary editions consume numbers), so the list
# is seeded from the Internet Archive's index of the same public directory.
# Extend it whenever new names surface.
SEED_NOTICES = [
    "#6 28th January, 2016 Part 1.pdf",
    "#20 14th April, 2016 Part 1.pdf",
    "#27 19th May, 2016 Part 1.pdf",
    "#18 2nd March, 2017 Part 1.pdf",
    "#53 12th July, 2017 EXTRA Part 1.pdf",
    "#83 16th November, 2017 Part 1_0.pdf",
    "#81 18th October, 2018 Part 1.pdf",
    "#102 11th December, 2018 EXTRA.pdf",
    "#35 2nd April, 2020.pdf",
    "#86 2nd July, 2020 EXTRA Part 1.pdf",
    "#67 6th July, 2021 EXTRA Part 1.pdf",
    "#83 10th August, 2021 EXTRA Part 1.pdf",
    "#101 1st October, 2021 EXTRA Part 1.pdf",
    "#103 7th October, 2021 Part 1.pdf",
    "#104 12th October, 2021 EXTRA Part 1.pdf",
    "#118 19th November, 2021 EXTRA Part 1.pdf",
    "#105 18th November, 2022 EXTRA.pdf",
    "#3 12th January, 2023 Part 1.pdf",
    "#23 27th February, 2023 EXTRA Part 1.pdf",
]

# Free attachments circulated with the Gazette -- the Acts and Statutory
# Instruments themselves. Same seeding caveat as SEED_NOTICES; the file name
# carries the edition number it shipped with.
SEED_ATTACHMENTS = [
    "G00088_SI NO 59 of 2007 ~ Fireworks (Exemption) (No 3) Order, 2007.pdf",
    "G00088_SI NO 60 of 2007 ~  Labour Code (Work Permit Exemption) (No 17) Order, 2007.pdf",
    "G00088_SI NO 61 of 2007 ~ Deportation Order for Gladston Gawen Wallace.pdf",
    "G00136_Financing and Money Services Act, 2008.pdf",
    "G00222_Jury  Act, 2009.pdf",
    "G00383_The Virgin Islands Comprehensive Disaster Management Policy.pdf",
    "G00403_The Syria (Restrictive Measures) (Overseas Territories) (Amendment) Notice, 2011.pdf",
    "G00465_2012 No 1389 - The Iran (Restrictive Measures) (Overseas Territories) Order 2012.pdf",
    "G00580_2013 No 1718 - The Democratic Peoples Republic of Korea (Sanctions) (OT) (Amendment) Order, 2013.pdf",
    "G00591_Computer Misuse and Cybercrime Act, 2014.pdf",
    "G00591_Criminal Code (Amendment) Act, 2014.pdf",
    "G00610_Consolidated List of Financial Sanctions Targets in the UK.pdf",
    "G00616_No 6 of 2014 - Status of Children Act,  2014.pdf",
    "G00635_No 9 - Computer Misuse and Cybercrime Act, 2014.pdf",
    "G00635_SI No 60 of 2014 - A Proclamation for the Computer Misuse and Cybercrime Act, 2014.pdf",
    "G00656_SI No 78 of 2014 - Notice made by the Minister of H&SD Bringing the Status of Children Act, 2014 into Force.pdf",
    "G00658_SI No 83 of 2014 - Status of Children Act (Parentage Testing Procedure) Regulations, 2014.pdf",
]

MONTHS = {
    "JANUARY": 1, "FEBRUARY": 2, "MARCH": 3, "APRIL": 4, "MAY": 5, "JUNE": 6,
    "JULY": 7, "AUGUST": 8, "SEPTEMBER": 9, "OCTOBER": 10, "NOVEMBER": 11,
    "DECEMBER": 12,
}

# "9TH NOVEMBER, 2006" / "16 APRIL 2009" / "12 January, 2023"
DATE_RE = re.compile(
    r"(\d{1,4})\s*(?:ST|ND|RD|TH)?[\s,.]+([A-Z]{3,14})[\s,.]+(\d{4})",
    re.IGNORECASE,
)

# Some older editions were typeset with overlapping glyphs, so pdfplumber reads
# the masthead as "THHURSDAY 110 JANUARRY 2013". No English month name contains
# a doubled letter, so collapsing runs is a safe repair for the month token.
_DOUBLED = re.compile(r"(.)\1+")


def _collapse(token: str) -> str:
    return _DOUBLED.sub(r"\1", token)


def parse_masthead_date(text: str) -> Optional[str]:
    """Return the edition's publication date as ISO 8601, or None.

    The date lives in the masthead ("... ROAD TOWN, TORTOLA THURSDAY 16 APRIL
    2009"), so only the head of the document is searched -- notice bodies are
    full of unrelated dates.
    """
    head = text[:4000]
    for raw_day, raw_month, raw_year in DATE_RE.findall(head):
        month = MONTHS.get(_collapse(raw_month.upper()))
        if not month:
            continue
        year = int(raw_year)
        if not 1950 <= year <= 2100:
            continue
        day = int(raw_day)
        if day > 31:
            # Glyph doubling again: "110" is really "10".
            day = int(_collapse(raw_day) or 0)
        if not 1 <= day <= 31:
            continue
        try:
            return datetime(year, month, day).date().isoformat()
        except ValueError:
            continue
    return None


# "#83 16th November, 2017 Part 1_0.pdf" -> 2017-11-16
FILENAME_DATE_RE = re.compile(
    r"(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]{3,14})[,.]?\s+(\d{4})"
)


def parse_filename_date(filename: str) -> Optional[str]:
    m = FILENAME_DATE_RE.search(filename)
    if not m:
        return None
    month = MONTHS.get(m.group(2).upper())
    if not month:
        return None
    try:
        return datetime(int(m.group(3)), month, int(m.group(1))).date().isoformat()
    except ValueError:
        return None


def parse_title_year(filename: str) -> Optional[str]:
    """Last-resort date for attachments: the year in the instrument's title."""
    years = re.findall(r"\b(19\d{2}|20\d{2})\b", filename)
    if not years:
        return None
    return f"{years[-1]}-01-01"


class SourceBlockedError(RuntimeError):
    """Raised when the public file directory stops answering for this vantage."""


class VGGazetteScraper(BaseScraper):
    """Scraper for VG/Gazette -- BVI Official Gazette, free tiers only."""

    # Individual editions do 404 inside the G-number range, so a few misses are
    # normal. A long unbroken run of failures is not: it means eservices.gov.vg
    # is refusing this vantage, and writing the partial corpus would look like a
    # successful shrinking crawl. Sized well above the largest gap observed in
    # the archive (a handful of consecutive numbers).
    MAX_CONSECUTIVE_FAILURES = 40

    def __init__(self):
        super().__init__(Path(__file__).parent)
        self._consecutive_failures = 0

    # ---------------------------------------------------------------- helpers

    def _note_failure(self, doc_id: str, reason: str) -> None:
        self._consecutive_failures += 1
        logger.warning(
            f"  {reason} for {doc_id} "
            f"({self._consecutive_failures}/{self.MAX_CONSECUTIVE_FAILURES})"
        )
        if self._consecutive_failures >= self.MAX_CONSECUTIVE_FAILURES:
            raise SourceBlockedError(
                f"{self._consecutive_failures} consecutive failures fetching "
                f"{NOTICES_DIR}/*.pdf (last: {doc_id} -- {reason}). The archive "
                f"has isolated gaps but never a run this long, so the public "
                f"file directory is refusing this vantage. Refusing to write a "
                f"partial corpus."
            )

    def _download(self, url: str) -> Optional[bytes]:
        resp = requests.get(url, headers=HEADERS, timeout=(15, 120))
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        if "pdf" not in resp.headers.get("Content-Type", "").lower():
            raise RuntimeError(
                f"expected application/pdf, got {resp.headers.get('Content-Type')}"
            )
        return resp.content

    # -------------------------------------------------------------- interface

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        doc_id = raw["doc_id"]
        filename = raw["filename"]
        kind = raw["kind"]
        url = f"{raw['directory']}/{urllib.parse.quote(filename)}"

        try:
            pdf_bytes = self._download(url)
        except Exception as e:
            self._note_failure(doc_id, f"Fetch failed ({e})")
            return None
        if pdf_bytes is None:
            # A 404 is an ordinary hole in the numbering, not a vantage problem,
            # but a solid wall of them still trips the blocked guard above.
            self._note_failure(doc_id, "Not published (404)")
            return None

        text = extract_pdf_markdown(
            SOURCE_ID, doc_id, pdf_bytes=pdf_bytes, table="legislation", force=True
        )
        if not text or len(text.strip()) < 500:
            self._note_failure(doc_id, f"Insufficient text ({len(text or '')} chars)")
            return None

        self._consecutive_failures = 0
        text = text.strip()

        if kind == "attachment":
            # "G00591_Criminal Code (Amendment) Act, 2014.pdf"
            title = filename.rsplit(".pdf", 1)[0].split("_", 1)[-1]
            title = re.sub(r"\s+", " ", title).strip()
            date = parse_masthead_date(text) or parse_title_year(filename)
        else:
            date = parse_filename_date(filename) or parse_masthead_date(text)
            if date:
                pretty = datetime.fromisoformat(date).strftime("%d %B %Y")
                title = f"Virgin Islands Official Gazette, {pretty} (Government and Statutory Notices)"
            else:
                title = (
                    f"Virgin Islands Official Gazette {doc_id} "
                    f"(Government and Statutory Notices)"
                )

        return {
            "_id": f"{SOURCE_ID}/{doc_id}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "doc_id": doc_id,
            "gazette_part": "Government and Statutory Notices",
            "document_kind": kind,
            "jurisdiction": "VG",
            "publisher": "Gazette Unit, Cabinet Office, Government of the Virgin Islands",
            "language": "en",
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        def seeded(filenames, directory, kind):
            # Newest-first, so a truncated fleet run lands the most recent law
            # rather than 2006 land notices.
            for filename in filenames[::-1]:
                yield {
                    "doc_id": Path(filename).stem,
                    "filename": filename,
                    "directory": directory,
                    "kind": kind,
                }

        def numbered():
            for number in range(LAST_EDITION, FIRST_EDITION - 1, -1):
                doc_id = f"G{number:05d}"
                yield {
                    "doc_id": doc_id,
                    "filename": f"{doc_id}.pdf",
                    "directory": NOTICES_DIR,
                    "kind": "notices",
                }

        # Round-robin the three families rather than draining one at a time, so
        # a sample run (which stops at the first 10 records) exercises all three
        # fetch paths instead of only the first list.
        queues = [
            seeded(SEED_NOTICES, NOTICES_DIR, "notices"),
            seeded(SEED_ATTACHMENTS, ATTACH_DIR, "attachment"),
            numbered(),
        ]
        while queues:
            for queue in list(queues):
                try:
                    yield next(queue)
                except StopIteration:
                    queues.remove(queue)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        # The archive is append-only and editions are immutable once published,
        # but new file names are not derivable, so a full re-pull (deduped on
        # doc_id by the loader) is the only correct refresh.
        yield from self.fetch_all()

    def test_connection(self) -> bool:
        url = f"{NOTICES_DIR}/G{FIRST_EDITION:05d}.pdf"
        try:
            r = requests.get(url, headers=HEADERS, timeout=60)
            ctype = r.headers.get("Content-Type", "")
            ok = r.status_code == 200 and "pdf" in ctype.lower()
            print(f"GET {url} -> {r.status_code} ({ctype}, {len(r.content)} bytes)")
            print("OK" if ok else "FAILED")
            return ok
        except Exception as e:
            print(f"FAILED: {e}")
            return False


if __name__ == "__main__":
    scraper = VGGazetteScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        sys.exit(0 if scraper.test_connection() else 1)
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "bootstrap-fast":
        scraper.bootstrap_fast()
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
