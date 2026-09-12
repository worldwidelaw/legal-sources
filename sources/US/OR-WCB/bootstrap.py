#!/usr/bin/env python3
"""
US/OR-WCB -- Oregon Workers' Compensation Board, Board Orders.

The Board reviews Administrative Law Judge orders in Oregon workers'
compensation claims (ORS chapter 656). Its orders -- Orders on Review, Orders
on Reconsideration, Own Motion orders, Claim Disposition Agreement orders,
Third Party Distribution orders and miscellaneous orders -- are the state's
workers' compensation case law and are published in the Van Natta reporter
("77 Van Natta 579 (2025)").

Every order since 1996 is a static file under
`https://www.oregon.gov/wcb/Orders/{year}/{type}/{month}/{case}.pdf`
(born-digital PDFs; the earliest years are `<PRE>`-formatted `.htm`). There is
no directory listing, and `/wcb/Orders/Forms/AllItems.aspx` is 401.

Enumeration goes through the SharePoint list that backs the public order
finder. The finder page's own JavaScript reads the list over SOAP
(`_vti_bin/Lists.asmx`), which answers anonymous callers `401 NTLM`, but the
REST endpoint on the same list is anonymous-readable:

    /wcb/_api/web/lists/getbytitle('Orders')/items
        ?$select=Id,Title,FileRef,WCBYear,WCBOrderType,WCBClaimantName,...
        &$filter=WCBYear eq {year}&$top=1000

That is the authoritative index — one row per order, carrying `FileRef` (the
document's server-relative path) plus the claimant name, WCB case number, issue
date, order type and Van Natta volume/page the finder displays. Folder rows
have a null `WCBYear`, so filtering by year yields file rows only.

The crawl is partitioned by year (1996-present, the range the finder's own year
selector offers) and the completed years are checkpointed to
`data/or_wcb_checkpoint.json`, so a killed run resumes instead of re-walking
the years it already wrote.

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap          # full pull
  python bootstrap.py bootstrap-fast     # full pull (VPS wrapper alias)
"""

import sys
import re
import json
import time
import html as html_lib
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import quote

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.OR-WCB")

BASE_URL = "https://www.oregon.gov"
LIST_URL = BASE_URL + "/wcb/_api/web/lists/getbytitle('Orders')/items"
ORDERS_PREFIX = BASE_URL + "/wcb/Orders/"

# The finder's year selector runs 1996..current year (LoadYearSelector in
# /wcb/board-orders/Pages/board-review.aspx); the list holds nothing older.
FIRST_YEAR = 1996
PAGE_SIZE = 1000
MAX_ATTEMPTS = 5

SELECT_FIELDS = (
    "Id,Title,FileRef,FileLeafRef,WCBYear,WCBOrderType,WCBClaimantName,"
    "WCBCase,WCBDateOrderIssued,WCBVanNattaVolume,WCBVanNattaPage,Modified"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}
# odata=nometadata keeps the list payload to the selected fields only.
JSON_HEADERS = {"Accept": "application/json;odata=nometadata"}

DOC_EXT_RE = re.compile(r"\.(pdf|html?|rtf|docx?)$", re.I)

# The list's WCBOrderType vocabulary, mapped to the order's own caption wording.
LIST_TYPE_NAMES = {
    "review": "Order on Review",
    "reconsideration": "Order on Reconsideration",
    "remand": "Order on Remand",
    "own motion": "Own Motion Order",
    "claim disposition agreement": "Claim Disposition Agreement Order",
    "third party": "Third Party Distribution Order",
    "crime victim": "Crime Victim Order",
    "osha": "OSHA Order",
    "miscellaneous": "Miscellaneous Order",
}

CITE_RE = re.compile(r"(\d{1,3})\s+Van\s+Natta\s+(\d{1,5})\s*\((\d{4})\)")
# Modern orders caption the claimant right after "In the Matter of the
# Compensation of"; pre-2002 orders lay the caption out as a two-column table,
# so the name lands on its own line ahead of the ") ORDER ON REVIEW" cell.
CLAIMANT_RE = re.compile(
    r"In the Matter of the Compensation\s*(?:\)?\s*)?(?:of\s*)?[\s\)]*"
    r"([A-Z][A-Z\.\,\'\-\s]{3,60}?),?\s*Claimant",
    re.S,
)
CLAIMANT_LINE_RE = re.compile(
    r"^[ \t\)]*([A-Z][A-Z\.\'\-]*(?:[ \t]+[A-Z][A-Z\.\'\-]*){1,5}),[ \t]*Claimant\b",
    re.M,
)
CASE_NO_RE = re.compile(
    r"(?:WCB\s+Case\s+No[:.\s]*|Own\s+Motion\s+No[:.\s]*)\s*"
    r"([A-Z]{0,3}-?[0-9]{2}-?[0-9]{3,6}[A-Za-z]{0,3})",
    re.I,
)
ENTERED_RE = re.compile(
    r"Entered\s+at\s+Salem,?\s+Oregon\s+on\s+"
    r"([A-Z][a-z]+)\s+(\d{1,2}),?\s+(\d{4})",
    re.I,
)
ORDER_TYPE_RE = re.compile(
    r"^\s*((?:OWN\s+MOTION\s+|THIRD\s+PARTY\s+|ORDER\s+|AMENDED\s+|SUPPLEMENTAL\s+)"
    r"[A-Z][A-Z\'\s,\-\.]{4,90})\s*$",
    re.M,
)

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}
MONTH_DIRS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "sept": 9, "oct": 10, "nov": 11, "dec": 12,
}

# Directory segment -> the kind of proceeding it holds. Used as a fallback when
# the order's own caption line cannot be parsed.
CATEGORY_NAMES = {
    "review": "Order on Review",
    "recon": "Order on Reconsideration",
    "remand": "Order on Remand",
    "omo": "Own Motion Order",
    "cda": "Claim Disposition Agreement Order",
    "tpo": "Third Party Distribution Order",
    "miscellaneous": "Miscellaneous Order",
    "misc": "Miscellaneous Order",
    "cv": "Civil Penalty Order",
    "osha": "OSHA Order",
    "dismissal": "Order of Dismissal",
}


def html_to_text(page: str) -> str:
    # The SharePoint migration rewrote every legacy .htm order with an
    # `<!--[if gte mso 9]><xml><mso:CustomDocumentProperties>` block in the
    # head. Tag-stripping alone leaves its values (claimant, month, order type,
    # "Oregon-Gov-SharepointMigrationBot") sitting in front of the order text,
    # so the comment and the head go before anything else does.
    body = re.sub(r"<!--.*?-->", " ", page, flags=re.S)
    body = re.sub(r"<head\b.*?</head>", " ", body, flags=re.S | re.I)
    body = re.sub(r"<xml\b.*?</xml>", " ", body, flags=re.S | re.I)
    body = re.sub(r"<(script|style)\b.*?</\1>", " ", body, flags=re.S | re.I)
    body = re.sub(r"<br\s*/?>", "\n", body, flags=re.I)
    body = re.sub(r"</(p|div|tr|h[1-6]|li)>", "\n", body, flags=re.I)
    text = html_lib.unescape(re.sub(r"<[^>]+>", " ", body))
    text = text.replace("\xa0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
    return text.strip()


def parse_entered_date(text: str) -> Optional[str]:
    match = ENTERED_RE.search(text)
    if not match:
        return None
    month = MONTHS.get(match.group(1).lower())
    if not month:
        return None
    return f"{int(match.group(3)):04d}-{month:02d}-{int(match.group(2)):02d}"


def list_date(value: Optional[str]) -> Optional[str]:
    """The date half of a SharePoint ISO timestamp."""
    match = re.match(r"(\d{4})-(\d{2})-(\d{2})", value or "")
    return match.group(0) if match else None


def path_parts(url: str) -> list:
    """The `{year}/{type}/{month}/{file}` tail of an order URL, lowercased."""
    tail = url.split("/wcb/Orders/", 1)[-1]
    return [p for p in tail.split("/") if p]


def url_date(url: str) -> Optional[str]:
    """Issue date from the directory path when the order text has none.

    The path carries year and (for most categories) month but never a day, so
    this returns the first of the month; `parse_entered_date` is always tried
    first.
    """
    parts = path_parts(url)
    if not parts or not re.fullmatch(r"(19|20)\d{2}", parts[0]):
        return None
    year = int(parts[0])
    month = 1
    for part in parts[1:-1]:
        if part.lower() in MONTH_DIRS:
            month = MONTH_DIRS[part.lower()]
            break
    return f"{year:04d}-{month:02d}-01"


def order_id(url: str) -> str:
    stem = DOC_EXT_RE.sub("", "/".join(path_parts(url)))
    return "OR-WCB-" + re.sub(r"[^A-Za-z0-9]+", "-", stem).strip("-")


def canonical(url: str) -> str:
    """One spelling per document: https, www, no fragment."""
    url = url.split("#")[0].strip()
    url = re.sub(r"^http://", "https://", url, flags=re.I)
    url = re.sub(r"^https://oregon\.gov/", "https://www.oregon.gov/", url, flags=re.I)
    return url


class ORWCBScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self.data_dir = Path(__file__).parent / "data"
        self.checkpoint_path = self.data_dir / "or_wcb_checkpoint.json"
        self.rate_limit_delay = float(
            self.config.get("fetch", {}).get("rate_limit_delay", 1)
        )

    # ------------------------------------------------------------------ HTTP

    def _get(self, url: str, params: dict = None, attempts: int = MAX_ATTEMPTS,
             headers: dict = None):
        delay = 3.0
        for attempt in range(attempts):
            try:
                resp = self.session.get(
                    url, params=params, headers=headers, timeout=90
                )
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

    # -------------------------------------------------------- list indexing

    def _list_page(self, url: str) -> dict:
        """One page of the SharePoint list. Raises if the endpoint is gone, so
        a block on the index fails loudly instead of yielding an empty corpus."""
        resp = self._get(url, headers=JSON_HEADERS)
        if resp is None:
            raise RuntimeError(
                f"The SharePoint order list at {url} did not answer. Without it "
                f"there is no index of the order files (the SOAP endpoint at "
                f"/wcb/_vti_bin/Lists.asmx answers anonymous callers 401 NTLM)."
            )
        try:
            return resp.json()
        except ValueError as exc:
            raise RuntimeError(
                f"The SharePoint order list at {url} returned "
                f"{resp.headers.get('Content-Type')} instead of JSON: {exc}"
            )

    def year_items(self, year: int) -> Generator[dict, None, None]:
        """Every order row the list holds for one year, following nextLink."""
        url = (
            f"{LIST_URL}?$select={quote(SELECT_FIELDS, safe=',')}"
            f"&$filter=WCBYear%20eq%20{year}&$top={PAGE_SIZE}"
        )
        seen = set()
        while url:
            payload = self._list_page(url)
            for row in payload.get("value") or []:
                file_ref = row.get("FileRef")
                if not file_ref or file_ref in seen:
                    continue
                seen.add(file_ref)
                row["url"] = BASE_URL + quote(file_ref)
                yield row
            url = payload.get("odata.nextLink")
            if url:
                time.sleep(self.rate_limit_delay)
        logger.info(f"{year}: {len(seen)} orders")

    def years(self) -> list:
        return list(range(FIRST_YEAR, datetime.now(timezone.utc).year + 1))

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
        """A spread of orders across the whole 1996-present range.

        One page per sampled year is enough: the list is cheap, the documents
        are not, and a short sample should still span both the HTML era and the
        PDF era rather than bunching at the start of the range.
        """
        years = self.years()
        step = max(1, len(years) / size)
        per_year = []
        for index in range(size):
            year = years[min(int(index * step), len(years) - 1)]
            rows = []
            for row in self.year_items(year):
                rows.append(row)
                if len(rows) >= 3:
                    break
            per_year.append(rows)
            time.sleep(self.rate_limit_delay)
        # One order from every sampled year first, then the spares -- so a run
        # that loses a few documents to extraction still spans the full range
        # instead of stopping part-way through it.
        picks = []
        for rank in range(3):
            picks.extend(rows[rank] for rows in per_year if len(rows) > rank)
        return picks

    # -------------------------------------------------------------- scraping

    def fetch_all(self) -> Generator[dict, None, None]:
        done = set(self._load_checkpoint())
        for year in self.years():
            if year in done:
                logger.info(f"{year}: already written (checkpoint), skipping")
                continue
            for row in self.year_items(year):
                yield row
            self._mark_year_done(year)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Orders issued -- or re-published -- on or after `since`."""
        cutoff = since.strftime("%Y-%m-%d")
        for year in range(max(FIRST_YEAR, since.year), self.years()[-1] + 1):
            for row in self.year_items(year):
                issued = list_date(row.get("WCBDateOrderIssued"))
                modified = list_date(row.get("Modified"))
                if (issued and issued >= cutoff) or (modified and modified >= cutoff):
                    yield row

    def normalize(self, raw: dict) -> Optional[dict]:
        url = raw["url"]
        doc_id = order_id(url)
        is_pdf = url.lower().endswith(".pdf")

        resp = self._get(url)
        if resp is None:
            logger.warning(f"Skipping unreachable order: {url}")
            return None

        if is_pdf:
            text = extract_pdf_markdown(
                "US/OR-WCB", doc_id, pdf_bytes=resp.content
            ) or ""
        else:
            resp.encoding = resp.apparent_encoding or "utf-8"
            text = html_to_text(resp.text)

        if len(text) < 300:
            logger.warning(f"Insufficient text ({len(text)} chars): {url}")
            return None

        head = text[:2500]
        parts = path_parts(url)
        category = parts[1].lower() if len(parts) > 2 else ""

        # The list row is the finder's own metadata, so it wins wherever it is
        # populated; the text parsers below stay as the fallback for rows the
        # board left blank (mostly the OSHA and Crime Victim series).
        claimant = re.sub(r"\s+", " ", raw.get("WCBClaimantName") or "").strip() or None
        if claimant is None:
            claimant_match = CLAIMANT_RE.search(head) or CLAIMANT_LINE_RE.search(head)
            if claimant_match:
                claimant = re.sub(r"[\s\)]+", " ", claimant_match.group(1)).strip(" ,")
                claimant = claimant.title() if claimant.isupper() else claimant

        case_numbers = []
        listed_case = (raw.get("WCBCase") or raw.get("Title") or "").strip()
        # WCBCase holds one docket or a comma-separated set of consolidated ones.
        for number in re.split(r"\s*,\s*", listed_case):
            number = number.strip().upper()
            if number and number not in case_numbers:
                case_numbers.append(number)
        for number in CASE_NO_RE.findall(head):
            number = number.upper()
            # Orders spell the same docket both ways -- "97-01606" and
            # "9701606" -- so restore the year separator, but only for the
            # all-digit form; "C041334" and "TP-99001" are already canonical.
            if re.fullmatch(r"\d{6,8}[A-Z]{0,3}", number):
                number = number[:2] + "-" + number[2:]
            if number not in case_numbers:
                case_numbers.append(number)

        listed_type = (raw.get("WCBOrderType") or "").strip()
        order_type = LIST_TYPE_NAMES.get(listed_type.lower()) or listed_type or None
        if not order_type:
            type_match = ORDER_TYPE_RE.search(head)
            if type_match:
                order_type = re.sub(r"\s+", " ", type_match.group(1)).strip().title()
        if not order_type:
            order_type = CATEGORY_NAMES.get(category, "Board Order")

        date = list_date(raw.get("WCBDateOrderIssued")) or parse_entered_date(text) \
            or url_date(url)

        volume = raw.get("WCBVanNattaVolume")
        page_number = raw.get("WCBVanNattaPage")
        citation = None
        if volume and page_number:
            year_text = date[:4] if date else raw.get("WCBYear")
            citation = f"{volume} Van Natta {page_number}"
            if year_text:
                citation += f" ({year_text})"
        else:
            volume = page_number = None
            # Only the running head is the order's own Van Natta cite. Orders
            # quote other decisions' cites throughout the body, and orders
            # published before ~2002 carry no running head at all, so anything
            # past the first few lines would be somebody else's citation.
            cite_match = CITE_RE.search(text[:200])
            if cite_match:
                volume = int(cite_match.group(1))
                page_number = int(cite_match.group(2))
                citation = f"{volume} Van Natta {page_number} ({cite_match.group(3)})"

        title_bits = [b for b in (claimant, order_type) if b]
        title = ", ".join(title_bits) if title_bits else doc_id
        if case_numbers and not claimant:
            title = f"{order_type} — WCB Case No. {case_numbers[0]}"

        return {
            "_id": doc_id,
            "_source": "US/OR-WCB",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "court": "Oregon Workers' Compensation Board",
            "jurisdiction": "US-OR",
            "docket_number": case_numbers[0] if case_numbers else None,
            "case_numbers": case_numbers or None,
            "parties": claimant,
            "order_type": order_type,
            "citation": citation,
            "volume": volume,
            "page": page_number,
            "year": int(date[:4]) if date else None,
            "source_format": "pdf" if is_pdf else "html",
            "language": "en",
        }

    # ------------------------------------------------------------------ CLI

    def test_api(self) -> bool:
        probe_year = self.years()[-2]
        try:
            rows = list(self.year_items(probe_year))
        except Exception as exc:
            logger.error(f"Order list unreachable: {exc}")
            return False
        if len(rows) < 50:
            logger.error(f"Order list holds only {len(rows)} rows for {probe_year}")
            return False
        record = self.normalize(rows[0])
        if not record or not record.get("text"):
            logger.error(f"No full text from probe order {rows[0].get('url')}")
            return False
        logger.info(
            f"OK: {len(rows)} orders listed for {probe_year}; probe "
            f"{record['_id']} ({record['date']}) {len(record['text'])} chars "
            f"— {record['title']}"
        )
        return True

    def run_curated_sample(self, size: int = 15) -> int:
        logger.info(f"=== SAMPLE MODE: {size} orders ===")
        records = []
        for row in self.sample_rows(size):
            if len(records) >= size:
                break
            try:
                record = self.normalize(row)
            except Exception as exc:
                logger.warning(f"normalize failed for {row.get('url')}: {exc}")
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
    parser = argparse.ArgumentParser(description="US/OR-WCB bootstrap")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "test-api"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    scraper = ORWCBScraper()
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
