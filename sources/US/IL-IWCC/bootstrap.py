#!/usr/bin/env python3
"""
US/IL-IWCC -- Illinois Workers' Compensation Commission, Commission-level decisions.

The Commission reviews arbitrator awards in Illinois workers' compensation claims.
Its decisions are posted at

    https://iwcc.illinois.gov/resources/resources-for/decisions.html

as MONTHLY ROLL-UP PDFs — one file per month holding every decision issued that
month, ~50 decisions / ~900 pages each. Individual decisions are recovered by
splitting on the "DECISION SIGNATURE PAGE" cover sheet that CompFile stamps in
front of each one; that sheet also carries the authoritative metadata (WC case
number, case name, proceeding type, Commission decision number, issuing
commissioner, both attorneys, filing date).

A commission decision is normally followed by the arbitrator's award it affirms,
which has its own signature page marked "Arbitration Decision". Those are part of
the decision they follow, not separate records, so only non-arbitration signature
pages open a new record.

COVERAGE: the CompFile era, June 2021-present. Those roll-ups sit in two DAM
folders — /iwcc/documents/monthly-decisions/{YEAR}/{month}/ for 2023 onward and
the flat /iwcc/resources/documents/ for 2021-2022 — so roll-ups are selected by
the "CompFile" marker in the filename as well as by the dated path.

The 2014-2021 roll-ups under /monthly-decisions/archived/ are NOT ingestible
from here: 2015 through early 2021 are pure image scans with no text layer at
all (verified: 0 of 478 pages carry text in April 2019, 0 of 856 in August 2020)
and 2014 has only a poor OCR layer with no signature pages, in 210 MB-per-month
files. Those need OCR — see README.

Plain GET, no auth/JS/CAPTCHA.

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap          # sequential full pull
  python bootstrap.py bootstrap-fast     # concurrent full pull (VPS wrapper)
"""

import sys
import re
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin, unquote, urlsplit

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.IL-IWCC")

BASE_URL = "https://iwcc.illinois.gov"
INDEX_URL = BASE_URL + "/resources/resources-for/decisions.html"

HREF_RE = re.compile(r'href="([^"]+)"', re.I)
# 2023-present roll-ups live under /monthly-decisions/{YEAR}/{month}/ ; the
# /archived/ tree is the image-only 2014-2021 legacy described above.
COMPFILE_PATH_RE = re.compile(r"/monthly-decisions/(20\d\d)/", re.I)
# 2021-2022 roll-ups predate that tree and sit in a flat folder, identifiable
# only by the "CompFile" marker their filenames carry.
COMPFILE_NAME_RE = re.compile(r"compfile", re.I)
# "Index for Posting ..." / "... Index CompFile ..." files are per-month tables
# of contents, not decisions. No decision file carries the word at all.
INDEX_FILE_RE = re.compile(r"\bindex\b", re.I)
MONTH_NAME_RE = re.compile(
    r"\b(january|february|febuary|march|april|may|june|july|august|september|"
    r"october|november|december)\b", re.I)
YEAR_RE = re.compile(r"\b(20\d{2})\b")
MONTH_NUMBERS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july", "august",
     "september", "october", "november", "december"], start=1)}
MONTH_NUMBERS["febuary"] = 2  # the site's own recurring misspelling
DECISION_NO_RE = re.compile(r"\b(\d{2}IWCC\d{3,5})\b")
CASE_NO_RE = re.compile(r"\b(\d{2}WC\d{4,6})\b")
SIGNATURE_MARK = "DECISION SIGNATURE PAGE"
DATE_FILED_RE = re.compile(r"DATE FILED:\s*(\d{1,2})/(\d{1,2})/(\d{4})")
# The cover sheet ends with the filing date and the commissioner's /s/ block;
# without this the trailing label (Respondent Attorney) swallows them.
SIG_FOOTER_RE = re.compile(r"^(DATE FILED\b|/s/|Signature\s*$)", re.I)

SIG_LABELS = [
    "Case Number",
    "Case Name",
    "Consolidated Cases",
    "Proceeding Type",
    "Decision Type",
    "Commission Decision Number",
    "Number of Pages of Decision",
    "Decision Issued By",
    "Petitioner Attorney",
    "Respondent Attorney",
]


def parse_signature_page(text: str) -> dict:
    """Read the CompFile cover sheet, whose labels and values alternate as lines."""
    fields, current = {}, None
    for line in (l.strip() for l in text.split("\n")):
        if not line:
            continue
        if line in SIG_LABELS:
            current = line
            fields.setdefault(current, [])
            continue
        # The signature block closes the last field; without this the trailing
        # label (Respondent Attorney) swallows the filing date and signature.
        if SIG_FOOTER_RE.match(line):
            current = None
            continue
        if current:
            fields[current].append(line)

    parsed = {k: " ".join(v).strip() for k, v in fields.items()}

    date = None
    m = DATE_FILED_RE.search(text)
    if m:
        month, day, year = (int(x) for x in m.groups())
        date = f"{year:04d}-{month:02d}-{day:02d}"
    parsed["date_filed"] = date

    # The decision number is a field on commission decisions and a bare stamp at
    # the foot of the page on the arbitration awards attached to them.
    number = parsed.get("Commission Decision Number") or ""
    if not DECISION_NO_RE.fullmatch(number.strip()):
        m = DECISION_NO_RE.search(text)
        number = m.group(1) if m else ""
    parsed["decision_number"] = number.strip() or None
    return parsed


def _month_key(name: str) -> tuple:
    """(year, month) a roll-up covers, read off its filename.

    Filenames repeat the month and year ("...issued August 2022 22IWCC0281..."),
    and the corrected-decision files that carry only a `NNIWCCNNNN` stamp still
    date themselves through its two-digit year.
    """
    year_match = YEAR_RE.search(name)
    year = int(year_match.group(1)) if year_match else 0
    if not year:
        number = DECISION_NO_RE.search(name.upper())
        if number:
            year = 2000 + int(number.group(1)[:2])
    month_match = MONTH_NAME_RE.search(name)
    month = MONTH_NUMBERS[month_match.group(1).lower()] if month_match else 0
    return (year, month)


def is_attached_award(fields: dict) -> bool:
    """True for the arbitrator's award attached to the decision that precedes it."""
    return "arbitration" in fields.get("Decision Type", "").lower()


class ILIWCCScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
        })

    def _get(self, url: str) -> Optional[requests.Response]:
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=180)
                # A handful of roll-ups linked from the page were never uploaded;
                # retrying a 404 only costs three round trips.
                if resp.status_code == 404:
                    logger.warning(f"GET {url} -> 404 (link points at no file)")
                    return None
                resp.raise_for_status()
                return resp
            except Exception as e:
                logger.warning(f"GET {url} failed (attempt {attempt + 1}/3): {e}")
        return None

    def list_rollups(self) -> list:
        """Every CompFile monthly roll-up PDF, newest month first."""
        resp = self._get(INDEX_URL)
        if resp is None:
            raise RuntimeError(
                f"Could not fetch {INDEX_URL}. iwcc.illinois.gov answers from "
                f"residential vantages — a hard failure here usually means the "
                f"egress IP is blocked."
            )

        rollups = {}
        for href in HREF_RE.findall(resp.text):
            if not href.lower().endswith(".pdf"):
                continue
            name = unquote(href.rsplit("/", 1)[-1])
            if not (COMPFILE_PATH_RE.search(href) or COMPFILE_NAME_RE.search(name)):
                continue
            if INDEX_FILE_RE.search(name):
                continue
            url = urljoin(INDEX_URL, href)
            rollups[urlsplit(url).path.lower()] = (url, name)

        if not rollups:
            raise RuntimeError(
                "The IWCC decisions page returned 200 but listed no CompFile "
                "roll-up PDFs — the page layout or the /monthly-decisions/ path "
                "scheme changed."
            )
        # Newest first. Sorting on the URL would order by DAM folder instead of
        # by date, putting the 2021-2022 flat folder ahead of 2026.
        return sorted(rollups.values(), key=lambda x: _month_key(x[1]), reverse=True)

    def _split_rollup(self, pdf_bytes: bytes, url: str, name: str) -> list:
        """Cut one monthly roll-up into its individual Commission decisions."""
        import fitz

        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        try:
            pages = [doc[i].get_text() for i in range(doc.page_count)]
        finally:
            doc.close()

        # Locate every cover sheet, then keep those that open a new decision.
        sig_pages = [i for i, t in enumerate(pages) if SIGNATURE_MARK in t]
        if not sig_pages:
            logger.warning(
                f"No signature pages in {name} — image-only scan or a new layout"
            )
            return []

        starts = []
        for i in sig_pages:
            fields = parse_signature_page(pages[i])
            if not is_attached_award(fields):
                starts.append((i, fields))

        decisions = []
        for n, (page_idx, fields) in enumerate(starts):
            end = starts[n + 1][0] if n + 1 < len(starts) else len(pages)
            text = "\n".join(pages[page_idx:end]).strip()
            if len(text) < 500:
                continue
            decisions.append({
                "text": text,
                "fields": fields,
                "source_pdf": url,
                "source_pdf_name": name,
                "page_start": page_idx + 1,
                "page_end": end,
            })
        return decisions

    def test_api(self) -> bool:
        logger.info("Testing IWCC decisions page...")
        try:
            rollups = self.list_rollups()
            logger.info(f"  {len(rollups)} CompFile monthly roll-ups listed")
            url, name = rollups[0]
            resp = self._get(url)
            if resp is None:
                raise RuntimeError(f"could not download {name}")
            decisions = self._split_rollup(resp.content, url, name)
            logger.info(f"  {name}: {len(decisions)} decisions split out")
            if not decisions:
                raise RuntimeError(f"{name} yielded no decisions")
            logger.info("Connectivity test PASSED")
            return True
        except Exception as e:
            logger.error(f"Connectivity test FAILED: {e}")
            return False

    def fetch_all(self) -> Generator[dict, None, None]:
        rollups = self.list_rollups()
        logger.info(f"{len(rollups)} CompFile monthly roll-ups to process")
        for n, (url, name) in enumerate(rollups, 1):
            resp = self._get(url)
            if resp is None:
                logger.error(f"Skipping unreachable roll-up {name}")
                continue
            try:
                decisions = self._split_rollup(resp.content, url, name)
            except Exception as e:
                logger.error(f"Could not split {name}: {e}")
                continue
            logger.info(f"[{n}/{len(rollups)}] {name}: {len(decisions)} decisions")
            yield from decisions

    def fetch_updates(self, since=None) -> Generator[dict, None, None]:
        """Roll-ups are listed newest-first; stop once a month predates `since`."""
        for raw in self.fetch_all():
            date = raw["fields"].get("date_filed")
            if since and date and date < str(since)[:10]:
                continue
            yield raw

    def normalize(self, raw: dict) -> Optional[dict]:
        fields = raw["fields"]
        text = raw["text"].strip()
        if len(text) < 500:
            return None

        number = fields.get("decision_number")
        case_no = (fields.get("Case Number") or "").strip()
        if not case_no:
            m = CASE_NO_RE.search(text)
            case_no = m.group(1) if m else ""

        if number:
            doc_id = f"US-IL-IWCC-{number}"
        elif case_no and fields.get("date_filed"):
            doc_id = f"US-IL-IWCC-{case_no}-{fields['date_filed']}"
        else:
            logger.warning(f"Unidentifiable decision in {raw['source_pdf_name']}")
            return None

        case_name = (fields.get("Case Name") or "").strip()
        decision_type = (fields.get("Decision Type") or "Commission Decision").strip()
        title = case_name or case_no or number
        title = f"{title} — IWCC {decision_type}"
        if number:
            title = f"{title} ({number})"

        return {
            "_id": doc_id,
            "_source": "US/IL-IWCC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": fields.get("date_filed"),
            "url": raw["source_pdf"],
            "court": "Illinois Workers' Compensation Commission",
            "jurisdiction": "US-IL",
            "decision_number": number,
            "case_number": case_no or None,
            "parties": case_name or None,
            "decision_type": decision_type,
            "proceeding_type": (fields.get("Proceeding Type") or "").strip() or None,
            "consolidated_cases": (fields.get("Consolidated Cases") or "").strip() or None,
            "decided_by": (fields.get("Decision Issued By") or "").strip() or None,
            "petitioner_attorney": (fields.get("Petitioner Attorney") or "").strip() or None,
            "respondent_attorney": (fields.get("Respondent Attorney") or "").strip() or None,
            "source_pdf": raw["source_pdf_name"],
            "pages": f"{raw['page_start']}-{raw['page_end']}",
            "language": "en",
        }

    def run_curated_sample(self, size: int = 15) -> int:
        logger.info(f"=== SAMPLE MODE: {size} decisions ===")
        records, seen = [], set()
        for url, name in self.list_rollups():
            resp = self._get(url)
            if resp is None:
                continue
            for raw in self._split_rollup(resp.content, url, name):
                record = self.normalize(raw)
                if not record or record["_id"] in seen:
                    continue
                seen.add(record["_id"])
                records.append(record)
                logger.info(f"  {record['_id']}: {len(record['text'])} chars")
                if len(records) >= size:
                    break
            if len(records) >= size:
                break

        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        for record in records:
            path = sample_dir / f"{record['_id']}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
        logger.info(f"=== Sample complete: {len(records)} records ===")
        return len(records)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="US/IL-IWCC bootstrap")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "test-api"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    scraper = ILIWCCScraper()
    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)
    if args.sample:
        sys.exit(0 if scraper.run_curated_sample() > 0 else 1)

    if args.command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
    else:
        stats = scraper.bootstrap()
    logger.info(
        f"{args.command} complete: {stats.get('records_fetched', 0)} fetched, "
        f"{stats.get('records_new', 0)} new, {stats.get('errors', 0)} errors"
    )
    sys.exit(0 if stats.get("records_fetched", 0) > 0 else 1)


if __name__ == "__main__":
    main()
