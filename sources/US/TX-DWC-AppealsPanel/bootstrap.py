#!/usr/bin/env python3
"""
US/TX-DWC-AppealsPanel -- Texas DWC Workers' Compensation Appeals Panel decisions.

The Appeals Panel of the Texas Department of Insurance, Division of Workers'
Compensation, reviews the decisions of administrative law judges (called
hearing officers before 2020) in contested case hearings under the Texas
Workers' Compensation Act, Tex. Lab. Code Ann. § 401.001 et seq. Its decisions
are Texas's workers' compensation case law and are cited by appeal number
("APD 220175-s").

Every decision since 1991 is a born-digital PDF at

    https://www.tdi.texas.gov/appeals/{year}cases/{appeal}r.pdf

with the ones the panel designates significant republished under
`/appeals/sig_cases/`. `https://www.tdi.texas.gov/appeals/` itself is 403, so
there is no directory listing to walk.

The index comes from the public search tool at
`/inter/perlroot/wc/appeals/index.html`, which is a DataTables front end. Its
own AJAX source is an anonymous SAS broker endpoint:

    /inter/perlroot/sasweb9/cgi-bin/broker.exe
        ?_service=wcExt&_program=progExt.Appeal_API.sas&opt=GET

That one call returns the entire index (~21,800 rows, ~5 MB) as JSON -- year,
appeal number, issues, decision file date, disposition, significant-case flag
and the PDF URL -- so it is the authoritative list rather than a page of one.
The index is fetched once per run and cached on the instance; the crawl is then
partitioned by year (oldest first) with completed years checkpointed to
`data/tx_dwc_apd_checkpoint.json`, so a killed run resumes instead of
re-downloading the years it already wrote.

`wwwapps.tdi.texas.gov` does not answer datacenter vantages (issue #1402), and
the broker is the only enumeration there is, so a fleet run from a blocked IP
could not reach a single PDF even though `www.tdi.texas.gov` serves them. The
index is therefore also carried in the repo, gzipped, at `index_snapshot.json.gz`
and used when the live call fails: a blocked run still writes the whole corpus
as of the snapshot's capture date instead of nothing. Refresh it from an
unblocked vantage with `bootstrap.py refresh-index`.

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap          # full pull
  python bootstrap.py bootstrap-fast     # full pull (VPS wrapper alias)
  python bootstrap.py refresh-index      # re-capture index_snapshot.json.gz
"""

import sys
import re
import gzip
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.TX-DWC-AppealsPanel")

APPS_BASE = "https://wwwapps.tdi.texas.gov"
INDEX_PAGE = APPS_BASE + "/inter/perlroot/wc/appeals/index.html"
INDEX_URL = (
    APPS_BASE + "/inter/perlroot/sasweb9/cgi-bin/broker.exe"
    "?_service=wcExt&_program=progExt.Appeal_API.sas&opt=GET"
)

# The committed fallback copy of the broker's payload, for vantages the broker
# refuses. Only the fields `normalize` reads are kept, so it gzips to ~250 KB.
INDEX_SNAPSHOT = Path(__file__).parent / "index_snapshot.json.gz"
SNAPSHOT_FIELDS = ("yr", "appealNum", "URL", "decisionDate", "orderDesc", "Issues", "sigCase")

MAX_ATTEMPTS = 5
# The oldest decisions the index carries; used only to sanity-check the payload.
FIRST_YEAR = 1991
# Shortest plausible decision. The panel's terser dispositions ("the decision
# and order of the ALJ is affirmed") still run past this once the standing
# recitation of the Act and the signature block are counted.
MIN_TEXT_CHARS = 400

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# "FILED MARCH 24, 2022" -- present from roughly 2005 on, absent before it.
FILED_RE = re.compile(
    r"\bFILED\s+([A-Z][A-Za-z]+)\s+(\d{1,2}),?\s+((?:19|20)\d{2})", re.I
)
# The panel's own caption, always the first line of the decision.
APPEAL_NO_RE = re.compile(r"\bAPPEAL\s+NO\.?\s*([0-9]{5,6}(?:-[A-Za-z]{1,2})?)", re.I)
# Contested case hearing date, the only date the oldest decisions state. The
# panel writes the sentence both ways round -- "a contested case hearing was
# held on November 14, 2000" and "On November 14, 2000, a contested case
# hearing was held" -- so both orders are matched.
# The PDF's line breaks fall anywhere in the sentence, so every inter-word gap
# has to be \s+ rather than a literal space.
CCH_RE = re.compile(
    r"contested\s+case\s+hearing\s+(?:was\s+)?(?:held|conducted)[^.]{0,80}?\bon\s+"
    r"([A-Z][A-Za-z]+)\s+(\d{1,2}),?\s+((?:19|20)\d{2})",
    re.I,
)
CCH_LEADING_RE = re.compile(
    r"\bOn\s+([A-Z][A-Za-z]+)\s+(\d{1,2}),?\s+((?:19|20)\d{2}),?\s+"
    r"[^.]{0,60}?contested\s+case\s+hearing",
    re.I,
)

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}

# The 403 on /appeals/ is a directory-listing block; a missing decision file is
# served as a 404 HTML page, and TDI's 404 page is also rendered as a PDF for
# .pdf requests, so the extracted text has to be screened for it too.
NOT_FOUND_MARKERS = (
    "Sorry, we can’t find that page",
    "Sorry, we can't find that page",
)


def iso_date(value: Optional[str]) -> Optional[str]:
    """`MM/DD/YYYY` (the index's format) as ISO 8601."""
    match = re.match(r"\s*(\d{1,2})/(\d{1,2})/((?:19|20)\d{2})\s*$", value or "")
    if not match:
        return None
    month, day, year = (int(part) for part in match.groups())
    if not (1 <= month <= 12 and 1 <= day <= 31):
        return None
    return f"{year:04d}-{month:02d}-{day:02d}"


def _month_day_year(month_name: str, day: str, year: str) -> Optional[str]:
    month = MONTHS.get(month_name.lower())
    if not month:
        return None
    day_number = int(day)
    if not 1 <= day_number <= 31:
        return None
    return f"{int(year):04d}-{month:02d}-{day_number:02d}"


def filed_date(text: str) -> Optional[str]:
    match = FILED_RE.search(text[:1200])
    return _month_day_year(*match.groups()) if match else None


def hearing_date(text: str) -> Optional[str]:
    head = text[:2500]
    match = CCH_RE.search(head) or CCH_LEADING_RE.search(head)
    return _month_day_year(*match.groups()) if match else None


def url_stem(url: str) -> str:
    """The PDF's file name without its extension."""
    return re.sub(r"\.pdf$", "", url.rsplit("/", 1)[-1], flags=re.I)


def doc_id(row: dict) -> str:
    """A stable id per decision.

    Keyed on the appeal number, which is what the panel cites and what
    identifies the decision no matter which folder serves it -- the same
    decision is published under two paths often enough (see `merge_key`) that
    a path-derived id would split it in two.
    """
    key = (row.get("appealNum") or "").strip() or url_stem(row.get("URL") or "")
    return "TX-DWC-APD-" + re.sub(r"[^A-Za-z0-9]+", "-", key.upper()).strip("-")


def merge_key(row: dict) -> str:
    return (row.get("appealNum") or "").strip().upper() or (row.get("URL") or "")


def is_significant(row: dict) -> bool:
    return (row.get("sigCase") or "").strip().lower() == "yes" \
        or "/sig_cases/" in (row.get("URL") or "").lower()


def path_rank(row: dict) -> tuple:
    """How canonical a row's URL is, lowest first.

    A decision the panel designates significant is served twice: once from its
    year folder and once, byte-identical, from `/appeals/sig_cases/`. A handful
    of decisions are likewise served from two year folders when the filing year
    and the publication year differ ("221683r.pdf" under both /2022cases/ and
    /2023cases/). The year folder wins over sig_cases, and the folder matching
    the row's own year wins over the other one.
    """
    url = (row.get("URL") or "").lower()
    folder = re.search(r"/appeals/((?:19|20)\d{2})cases/", url)
    return (
        1 if "/sig_cases/" in url else 0,
        0 if folder and folder.group(1) == (row.get("yr") or "").strip() else 1,
    )


def split_issues(value: Optional[str]) -> list:
    """The index's comma-separated issue list, minus its "Unavailable" filler."""
    issues = []
    for issue in re.split(r"\s*,\s*", (value or "").strip()):
        issue = issue.strip()
        if issue and issue.lower() != "unavailable" and issue not in issues:
            issues.append(issue)
    return issues


class TXDWCAppealsPanelScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.data_dir = Path(__file__).parent / "data"
        self.checkpoint_path = self.data_dir / "tx_dwc_apd_checkpoint.json"
        self.rate_limit_delay = float(
            self.config.get("fetch", {}).get("rate_limit_delay", 1)
        )
        self._index = None

    # ------------------------------------------------------------------ HTTP

    def _get(self, url: str, attempts: int = MAX_ATTEMPTS, timeout: int = 90):
        delay = 3.0
        for attempt in range(attempts):
            try:
                resp = self.session.get(url, timeout=timeout)
                resp.raise_for_status()
                return resp
            except Exception as exc:
                logger.warning(
                    f"GET {url} failed (attempt {attempt + 1}/{attempts}): {exc}"
                )
                if attempt < attempts - 1:
                    time.sleep(delay)
                    delay = min(delay * 2, 60.0)
        return None

    # -------------------------------------------------------------- indexing

    def _fetch_index_rows(self) -> list:
        """The broker's raw rows, or [] when the vantage cannot reach it."""
        resp = self._get(INDEX_URL, timeout=180)
        if resp is None:
            logger.warning(
                f"The Appeals Panel index at {INDEX_URL} did not answer "
                f"(wwwapps.tdi.texas.gov refuses datacenter vantages, #1402)."
            )
            return []
        try:
            payload = resp.json()
        except ValueError as exc:
            logger.warning(
                f"The Appeals Panel index at {INDEX_URL} returned "
                f"{resp.headers.get('Content-Type')} instead of JSON: {exc}"
            )
            return []
        return payload.get("data") or []

    def _snapshot_rows(self) -> list:
        """The committed copy of the index, or [] when it is absent/unreadable."""
        try:
            with gzip.open(INDEX_SNAPSHOT, "rt", encoding="utf-8") as f:
                payload = json.load(f)
        except FileNotFoundError:
            return []
        except (OSError, ValueError) as exc:
            logger.warning(f"Could not read {INDEX_SNAPSHOT.name}: {exc}")
            return []
        rows = payload.get("data") or []
        if rows:
            logger.warning(
                f"Falling back to the committed index snapshot "
                f"({INDEX_SNAPSHOT.name}, captured {payload.get('captured')}, "
                f"{len(rows)} rows). Decisions published after that date are "
                f"NOT in this run — refresh it with `bootstrap.py refresh-index` "
                f"from a vantage the broker answers."
            )
        return rows

    def index(self) -> list:
        """The whole decision index, resolved once per run.

        The live broker first, then the committed snapshot. Raises rather than
        returning an empty list if neither answers: without an enumeration
        there is no way to reach the PDFs (the /appeals/ directory is 403), so
        that has to fail loudly instead of writing a zero-record corpus.
        """
        if self._index is not None:
            return self._index

        raw_rows = self._fetch_index_rows() or self._snapshot_rows()
        if not raw_rows:
            raise RuntimeError(
                f"The Appeals Panel index at {INDEX_URL} did not answer and "
                f"there is no usable {INDEX_SNAPSHOT.name} to fall back on. The "
                f"broker is the AJAX source of the search tool at {INDEX_PAGE} "
                f"and the only enumeration of the decision PDFs "
                f"(https://www.tdi.texas.gov/appeals/ is 403)."
            )

        # One row per decision: the index lists a significant decision under
        # both its year folder and /sig_cases/, and the significance flag has
        # to survive the row that loses (the year-folder row carries an empty
        # sigCase even when the /sig_cases/ twin says "Yes").
        merged = {}
        for row in raw_rows:
            if not (row.get("URL") or "").strip():
                continue
            key = merge_key(row)
            row = dict(row, significant=is_significant(row))
            winner = merged.get(key)
            if winner is None:
                merged[key] = row
                continue
            if path_rank(row) < path_rank(winner):
                row["significant"] = row["significant"] or winner["significant"]
                merged[key] = row
            else:
                winner["significant"] = winner["significant"] or row["significant"]
        rows = list(merged.values())
        if not rows:
            raise RuntimeError(
                f"The Appeals Panel index carried {len(raw_rows)} rows but none "
                f"of them named a PDF, so there is nothing to crawl."
            )
        logger.info(f"index: {len(rows)} decisions listed")
        self._index = rows
        return rows

    def refresh_snapshot(self) -> int:
        """Re-capture `index_snapshot.json.gz` from the live broker.

        Run from a vantage the broker answers; the fleet's does not.
        """
        raw_rows = self._fetch_index_rows()
        if len(raw_rows) < 1000:
            raise RuntimeError(
                f"The live broker returned {len(raw_rows)} rows, too few to "
                f"overwrite a {len(self._snapshot_rows())}-row snapshot with."
            )
        payload = {
            "captured": datetime.now(timezone.utc).date().isoformat(),
            "source_url": INDEX_URL,
            "data": [{k: row.get(k, "") for k in SNAPSHOT_FIELDS} for row in raw_rows],
        }
        tmp = INDEX_SNAPSHOT.with_suffix(".tmp")
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        # mtime=0 so an unchanged index re-captures to identical bytes and does
        # not show up as a diff.
        with open(tmp, "wb") as raw:
            with gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=9, mtime=0) as f:
                f.write(body)
        tmp.replace(INDEX_SNAPSHOT)
        logger.info(
            f"{INDEX_SNAPSHOT.name}: {len(raw_rows)} rows, "
            f"{INDEX_SNAPSHOT.stat().st_size / 1024:.0f} KB"
        )
        return len(raw_rows)

    def years(self) -> list:
        years = set()
        for row in self.index():
            year = (row.get("yr") or "").strip()
            if re.fullmatch(r"(19|20)\d{2}", year):
                years.add(int(year))
        return sorted(years)

    def year_rows(self, year: int) -> list:
        return [row for row in self.index() if (row.get("yr") or "").strip() == str(year)]

    # ------------------------------------------------------------ checkpoint

    def _load_checkpoint(self) -> list:
        try:
            with open(self.checkpoint_path, encoding="utf-8") as f:
                done = json.load(f).get("years_done", [])
            return [int(y) for y in done]
        except (FileNotFoundError, ValueError, AttributeError) as exc:
            if not isinstance(exc, FileNotFoundError):
                logger.warning(f"Could not read {self.checkpoint_path}: {exc}")
            return []

    def _mark_year_done(self, year: int) -> None:
        done = self._load_checkpoint()
        if year in done:
            return
        done.append(year)
        self.data_dir.mkdir(exist_ok=True)
        tmp = self.checkpoint_path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"years_done": sorted(done)}, f)
        tmp.replace(self.checkpoint_path)

    def sample_rows(self, size: int) -> list:
        """A spread of decisions across the whole 1991-present range."""
        years = self.years()
        step = max(1, len(years) / size)
        per_year = []
        for index in range(size):
            year = years[min(int(index * step), len(years) - 1)]
            per_year.append(self.year_rows(year)[:3])
        # One decision from every sampled year first, then the spares, so a run
        # that loses a few PDFs to extraction still spans the full range.
        picks = []
        for rank in range(3):
            picks.extend(rows[rank] for rows in per_year if len(rows) > rank)
        # Designated-significant decisions are ~1% of the index, so a plain
        # year spread misses them and the sample never exercises the flag.
        if not any(row.get("significant") for row in picks[:size]):
            significant = [row for row in self.index() if row.get("significant")]
            if significant:
                picks.insert(1, significant[0])
        return picks

    # -------------------------------------------------------------- scraping

    def fetch_all(self) -> Generator[dict, None, None]:
        done = set(self._load_checkpoint())
        for year in self.years():
            if year in done:
                logger.info(f"{year}: already written (checkpoint), skipping")
                continue
            rows = self.year_rows(year)
            logger.info(f"{year}: {len(rows)} decisions")
            for row in rows:
                yield row
            self._mark_year_done(year)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Decisions filed on or after `since`.

        Rows whose file date the index left blank (mostly the 1991 series) are
        kept when their year is at or after the cutoff year, so an undated
        recent decision is not silently dropped from an update run.
        """
        cutoff = since.strftime("%Y-%m-%d")
        for row in self.index():
            date = iso_date(row.get("decisionDate"))
            if date:
                if date >= cutoff:
                    yield row
                continue
            year = (row.get("yr") or "").strip()
            if re.fullmatch(r"(19|20)\d{2}", year) and int(year) >= since.year:
                yield row

    def normalize(self, raw: dict) -> Optional[dict]:
        url = (raw.get("URL") or "").strip()
        if not url:
            return None
        record_id = doc_id(raw)

        resp = self._get(url)
        if resp is None:
            logger.warning(f"Skipping unreachable decision: {url}")
            return None

        text = extract_pdf_markdown(
            "US/TX-DWC-AppealsPanel", record_id, pdf_bytes=resp.content
        ) or ""
        if any(marker in text for marker in NOT_FOUND_MARKERS):
            logger.warning(f"Skipping decision served as the TDI 404 page: {url}")
            return None
        if len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text ({len(text)} chars): {url}")
            return None

        # The index's appeal number is the citation the panel itself uses; the
        # caption is the fallback for the handful of rows it leaves blank, and
        # it also carries the "-s" significant-decision suffix that some index
        # rows drop.
        appeal_number = (raw.get("appealNum") or "").strip()
        caption = APPEAL_NO_RE.search(text[:400])
        if caption:
            captioned = caption.group(1)
            if not appeal_number or captioned.lower().startswith(appeal_number.lower()):
                appeal_number = captioned
        if not appeal_number:
            logger.warning(f"No appeal number for {url}")
            return None

        date = (
            iso_date(raw.get("decisionDate"))
            or filed_date(text)
            or hearing_date(text)
        )
        year = None
        if date:
            year = int(date[:4])
        elif re.fullmatch(r"(19|20)\d{2}", (raw.get("yr") or "").strip()):
            year = int(raw["yr"])

        disposition = re.sub(r"\s+", " ", raw.get("orderDesc") or "").strip() or None
        issues = split_issues(raw.get("Issues"))
        significant = bool(raw.get("significant")) or is_significant(raw)

        title = f"Appeals Panel Decision No. {appeal_number}"
        if disposition:
            title += f" — {disposition}"

        return {
            "_id": record_id,
            "_source": "US/TX-DWC-AppealsPanel",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "court": (
                "Texas Department of Insurance, Division of Workers' "
                "Compensation — Appeals Panel"
            ),
            "jurisdiction": "US-TX",
            "docket_number": appeal_number,
            "citation": f"APD {appeal_number}",
            "disposition": disposition,
            "issues": issues or None,
            "significant": significant,
            "year": year,
            "source_format": "pdf",
            "language": "en",
        }

    # ------------------------------------------------------------------ CLI

    def test_api(self) -> bool:
        try:
            rows = self.index()
        except Exception as exc:
            logger.error(f"Decision index unreachable: {exc}")
            return False
        years = self.years()
        if not years or years[0] > FIRST_YEAR or len(rows) < 1000:
            logger.error(
                f"Index looks truncated: {len(rows)} rows spanning "
                f"{years[:1]}-{years[-1:]}"
            )
            return False
        record = self.normalize(rows[0])
        if not record or not record.get("text"):
            logger.error(f"No full text from probe decision {rows[0].get('URL')}")
            return False
        logger.info(
            f"OK: {len(rows)} decisions listed for {years[0]}-{years[-1]}; probe "
            f"{record['_id']} ({record['date']}) {len(record['text'])} chars "
            f"— {record['title']}"
        )
        return True

    def run_curated_sample(self, size: int = 15) -> int:
        logger.info(f"=== SAMPLE MODE: {size} decisions ===")
        records = []
        for row in self.sample_rows(size):
            if len(records) >= size:
                break
            try:
                record = self.normalize(row)
            except Exception as exc:
                logger.warning(f"normalize failed for {row.get('URL')}: {exc}")
                continue
            if record:
                records.append(record)
                logger.info(f"  {record['_id']}: {len(record['text'])} chars")
            time.sleep(self.rate_limit_delay)

        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        for record in records:
            with open(sample_dir / f"{record['_id']}.json", "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
        logger.info(f"=== Sample complete: {len(records)} records ===")
        return len(records)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="US/TX-DWC-AppealsPanel bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api", "refresh-index"],
    )
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    scraper = TXDWCAppealsPanelScraper()
    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)
    if args.command == "refresh-index":
        try:
            scraper.refresh_snapshot()
        except Exception as exc:
            logger.error(f"Could not refresh the index snapshot: {exc}")
            sys.exit(1)
        sys.exit(0)
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
