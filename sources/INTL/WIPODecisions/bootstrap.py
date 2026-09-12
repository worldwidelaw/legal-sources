#!/usr/bin/env python3
"""
INTL/WIPODecisions - WIPO UDRP Domain Name Dispute Decisions Fetcher

Fetches UDRP panel decisions from the WIPO Arbitration and Mediation Center.
The archive runs 1999-present and publishes decisions in three shapes, all of
which this scraper reads (issue #1477):

  1999-2009  list.jsp links straight to /amc/en/domains/decisions/html/YYYY/*.html
  2010-2023  list.jsp links to search/text.jsp?case=..., which redirects to HTML
  2024+      the same text.jsp redirects to /amc/en/domains/decisions/pdf/YYYY/*.pdf

Data source: https://www.wipo.int/amc/en/domains/decisionsx/
License: WIPO Terms of Use

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap            # Full sweep -> data/records.jsonl
  python bootstrap.py bootstrap-fast       # Alias for the full sweep
  python bootstrap.py test                 # Test connectivity
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

BASE_URL = "https://www.wipo.int"
INDEX_URL = "https://www.wipo.int/amc/en/domains/decisionsx/index.html"
LIST_URL = "https://www.wipo.int/amc/en/domains/decisionsx/list.jsp"
SOURCE_ID = "INTL/WIPODecisions"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"
DATA_DIR = SOURCE_DIR / "data"
RECORDS_PATH = DATA_DIR / "records.jsonl"
CHECKPOINT_PATH = DATA_DIR / "checkpoint.json"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "text/html,application/xhtml+xml",
}

RATE_LIMIT_DELAY = 1.5
REQUEST_ATTEMPTS = 4
MIN_TEXT_CHARS = 200


class IndexUnavailable(RuntimeError):
    """The master index could not be read, so the corpus size is unknown.

    Fatal on purpose: returning an empty page list here would make a blocked or
    restructured index look like a corpus of zero decisions (#1477).
    """


def clean_html(html_str: str) -> str:
    """Strip HTML tags and decode entities."""
    if not html_str:
        return ""
    soup = BeautifulSoup(html_str, "html.parser")
    return unescape(soup.get_text(separator="\n", strip=True))


def _get(session: requests.Session, url: str, timeout: int = 45):
    """GET with retry/backoff. Returns the response, or None if unreachable."""
    backoff = 3.0
    for attempt in range(1, REQUEST_ATTEMPTS + 1):
        try:
            resp = session.get(url, headers=HEADERS, timeout=timeout, allow_redirects=True)
        except requests.RequestException as e:
            print(f"  Warning: {url} network error (attempt {attempt}): {e}")
        else:
            if resp.status_code == 200:
                return resp
            # 404 is a real answer for a withdrawn/terminated case — don't retry.
            if resp.status_code == 404:
                return None
            print(f"  Warning: {url} HTTP {resp.status_code} (attempt {attempt})")
        time.sleep(backoff)
        backoff = min(backoff * 2, 60)
    return None


def get_list_page_urls(session: requests.Session) -> list:
    """Parse the master index to get every list.jsp page, all years.

    The old code dropped everything after MAX_HTML_YEAR=2021 on the assumption
    that 2022+ is PDF-only and therefore unusable. It is PDF-only, but the PDFs
    carry the full decision text and we extract them (#1477).
    """
    resp = _get(session, INDEX_URL)
    if resp is None:
        raise IndexUnavailable(f"could not read the decision index at {INDEX_URL}")

    urls = []
    seen = set()
    for year, seq_min, seq_max in re.findall(
        r'list\.jsp\?prefix=D&year=(\d+)&seq_min=(\d+)&seq_max=(\d+)', resp.text
    ):
        key = (int(year), int(seq_min))
        if key in seen:
            continue
        seen.add(key)
        urls.append({
            "year": int(year),
            "seq_min": int(seq_min),
            "seq_max": int(seq_max),
            "url": f"{LIST_URL}?prefix=D&year={year}&seq_min={seq_min}&seq_max={seq_max}",
        })

    if not urls:
        raise IndexUnavailable(
            f"{INDEX_URL} returned 200 but exposed no list.jsp links — layout change "
            f"or block. Refusing to report an empty corpus."
        )

    urls.sort(key=lambda lp: (lp["year"], lp["seq_min"]))
    return urls


def parse_list_page(session: requests.Session, url: str) -> Optional[list]:
    """Parse a list.jsp page into case metadata. None means 'could not fetch'.

    Handles both link shapes the archive uses. Matching only `case=D\\d{4}-\\d{4}`
    silently dropped 1999-2009 entirely, because those years link directly to the
    decision HTML instead of going through text.jsp (#1477).
    """
    resp = _get(session, url)
    if resp is None:
        return None

    cases = []
    soup = BeautifulSoup(resp.text, "html.parser")

    for row in soup.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 5:
            continue

        link = cells[0].find("a", href=True)
        if not link:
            continue
        href = link["href"]

        # Shape A (2010+): search/text.jsp?case=D2020-0001
        m = re.search(r'case=(D\d{4}-\d{4,5})', href)
        # Shape B (1999-2009): /amc/en/domains/decisions/html/2005/d2005-0001.html
        if not m:
            m = re.search(r'/decisions/\w+/\d{4}/([a-zA-Z]+\d{4}-\d{4,5})\.', href)
        if not m:
            continue

        cases.append({
            "case_number": m.group(1).upper(),
            "url": href if href.startswith("http") else BASE_URL + href,
            "complainant": cells[1].get_text(strip=True),
            "respondent": cells[2].get_text(strip=True),
            "domain_names": cells[3].get_text(strip=True),
            "outcome": cells[4].get_text(strip=True),
        })

    return cases


def fetch_decision_text(session: requests.Session, case: dict) -> Optional[dict]:
    """Fetch a decision's full text, from HTML or PDF depending on the year."""
    resp = _get(session, case["url"], timeout=90)
    if resp is None:
        return None

    final_url = resp.url
    ctype = resp.headers.get("Content-Type", "").lower()

    if final_url.lower().endswith(".pdf") or "application/pdf" in ctype:
        # 2024+ decisions are published only as PDFs. They were skipped
        # outright before; the text is there, it just needs extracting.
        text = extract_pdf_markdown(
            SOURCE_ID,
            case["case_number"],
            pdf_bytes=resp.content,
            table="case_law",
            force=True,
        )
        if not text or len(text) < MIN_TEXT_CHARS:
            return None
        return {"text": text, "html_url": final_url, "meta": {}, "format": "pdf"}

    soup = BeautifulSoup(resp.text, "html.parser")
    meta = {t["name"]: t.get("content", "") for t in soup.find_all("meta", attrs={"name": True})}
    body = soup.find("body")
    text = unescape(body.get_text(separator="\n", strip=True)) if body else ""
    if len(text) < MIN_TEXT_CHARS:
        return None
    return {"text": text, "html_url": final_url, "meta": meta, "format": "html"}


MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}

# Panels sign off with "Date: February 26, 2025" or "Dated: March 11, 2005",
# always in the last few lines of the decision.
SIGNATURE_DATE_RE = re.compile(
    r'\bDate[d]?\s*:?\s*(' + "|".join(MONTHS) + r')\s+(\d{1,2}),?\s+(\d{4})',
    re.IGNORECASE,
)


def _date_from_text(text: str) -> Optional[str]:
    """Read the panel's signature date off the tail of the decision.

    The PDF-only years (2024+) carry no usable meta tags, so without this every
    one of them fell back to YYYY-01-01 (#1477).
    """
    matches = SIGNATURE_DATE_RE.findall(text[-2000:]) or SIGNATURE_DATE_RE.findall(text)
    if not matches:
        return None
    month, day, year = matches[-1]
    try:
        return datetime(int(year), MONTHS[month.lower()], int(day)).strftime("%Y-%m-%d")
    except ValueError:
        return None


def _date_for(case_number: str, meta: dict, text: str = "") -> Optional[str]:
    """Prefer the page's meta date, then the signed date, then the case year."""
    date_str = meta.get("date", "")
    if re.match(r'\d{4}-\d{2}-\d{2}$', date_str):
        return date_str
    signed = _date_from_text(text)
    if signed:
        return signed
    m = re.match(r'D(\d{4})-', case_number)
    return f"{m.group(1)}-01-01" if m else None


def normalize(case_meta: dict, decision: dict) -> dict:
    """Transform case metadata + decision text into the standard schema."""
    meta = decision.get("meta", {})
    case_num = case_meta["case_number"]

    # The listing cells are correctly encoded; the PDF-era meta tags are
    # double-encoded, so the listing wins wherever both carry a value.
    complainant = case_meta.get("complainant") or meta.get("complainants", "")
    respondent = case_meta.get("respondent", "")
    domains = case_meta.get("domain_names") or meta.get("domains", "")

    return {
        "_id": f"WIPO/{case_num}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": f"{complainant} v. {respondent or 'N/A'} ({case_num})",
        "text": decision["text"],
        "date": _date_for(case_num, meta, decision["text"]),
        "url": decision["html_url"],
        "case_number": case_num,
        "domain_names": domains,
        "complainant": complainant,
        "respondent": respondent,
        "outcome": case_meta.get("outcome", ""),
        "format": decision.get("format", "html"),
        "language": "en",
    }


def _load_checkpoint() -> set:
    try:
        with open(CHECKPOINT_PATH, encoding="utf-8") as f:
            return set(json.load(f).get("done_pages", []))
    except (OSError, ValueError, TypeError):
        return set()


def _save_checkpoint(done: set):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = CHECKPOINT_PATH.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"done_pages": sorted(done)}, f)
    tmp.replace(CHECKPOINT_PATH)


def fetch_all(session: requests.Session, sample: bool = False) -> Generator[dict, None, None]:
    """Yield every normalized decision record across the whole archive.

    Progress is checkpointed per list page so a relaunched run resumes instead
    of re-walking the years it already covered.
    """
    print("Fetching WIPO UDRP decision index...")
    list_pages = get_list_page_urls(session)
    years = sorted({lp["year"] for lp in list_pages})
    print(f"Found {len(list_pages)} list pages covering {years[0]}-{years[-1]}")

    if sample:
        # One page per publishing era so the committed samples exercise all
        # three shapes: direct HTML, text.jsp->HTML, and text.jsp->PDF.
        list_pages = [
            lp for lp in list_pages
            if lp["seq_min"] == 1 and lp["year"] in (2005, 2015, 2025)
        ]

    done = set() if sample else _load_checkpoint()
    if done:
        # The newest two years are still accruing decisions, so a checkpoint
        # entry for them means "done as of last run", not "done". Re-walking
        # them is what makes a refresh actually refresh; the ingest side
        # dedups on _id.
        refresh_from = max(years) - 1
        done -= {lp["url"] for lp in list_pages if lp["year"] >= refresh_from}
        print(f"Resuming — {len(done)} list pages already done, "
              f"re-walking {refresh_from}+ for new decisions")

    count = 0
    unreachable_pages = []

    for lp in list_pages:
        if lp["url"] in done:
            continue

        print(f"  List: year={lp['year']}, seq={lp['seq_min']}-{lp['seq_max']}...")
        cases = parse_list_page(session, lp["url"])
        time.sleep(RATE_LIMIT_DELAY)

        if cases is None:
            # A lost listing page costs every decision behind it — record it
            # rather than letting it disappear into a clean-looking run.
            unreachable_pages.append(lp["url"])
            print(f"    UNREACHABLE — {len(unreachable_pages)} list pages lost so far")
            continue

        if not cases:
            print(f"    0 cases parsed for year={lp['year']} seq={lp['seq_min']}")

        from_this_page = 0
        for case_meta in cases:
            if sample and (count >= 15 or from_this_page >= 5):
                break

            decision = fetch_decision_text(session, case_meta)
            time.sleep(RATE_LIMIT_DELAY)
            if decision is None:
                continue

            yield normalize(case_meta, decision)
            count += 1
            from_this_page += 1
            if count % 50 == 0:
                print(f"    Fetched {count} decisions...")

        if not sample:
            done.add(lp["url"])
            _save_checkpoint(done)

    print(f"Sweep complete: {count} decisions from {len(list_pages) - len(unreachable_pages)} list pages")
    if unreachable_pages:
        print(
            f"WARNING: {len(unreachable_pages)} list pages were unreachable and their "
            f"decisions are missing: {unreachable_pages[:5]}",
            file=sys.stderr,
        )


def test_connection():
    """Test connectivity across all three decision formats."""
    print("Testing WIPO AMC decisions...")
    session = requests.Session()

    list_pages = get_list_page_urls(session)
    years = sorted({lp["year"] for lp in list_pages})
    print(f"OK — index accessible: {len(list_pages)} list pages, years {years[0]}-{years[-1]}")

    # One page per era: direct-HTML, text.jsp->HTML, text.jsp->PDF.
    for year in (2005, 2015, 2025):
        lp = next((p for p in list_pages if p["year"] == year and p["seq_min"] == 1), None)
        if lp is None:
            print(f"  {year}: no list page")
            continue
        cases = parse_list_page(session, lp["url"])
        if not cases:
            print(f"  {year}: 0 cases parsed — FAILED")
            continue
        decision = fetch_decision_text(session, cases[0])
        if decision:
            print(f"  {year}: {len(cases)} cases; {cases[0]['case_number']} -> "
                  f"{len(decision['text'])} chars ({decision['format']})")
        else:
            print(f"  {year}: {len(cases)} cases; full text FAILED")
        time.sleep(RATE_LIMIT_DELAY)

    return True


def bootstrap(sample: bool = False):
    """Run the bootstrap. Full runs stream to data/records.jsonl."""
    session = requests.Session()
    saved = 0

    if sample:
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        for rec in fetch_all(session, sample=True):
            safe_id = re.sub(r'[^\w\-]', '_', rec["_id"])
            with open(SAMPLE_DIR / f"{safe_id}.json", "w", encoding="utf-8") as f:
                json.dump(rec, f, ensure_ascii=False, indent=2)
            saved += 1
            if saved >= 15:
                break
        print(f"Saved {saved} sample records to {SAMPLE_DIR}")
        return saved

    # The full path used to write every record into sample/, so the pipeline
    # found no records.jsonl and fell back to ingesting the committed samples.
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(RECORDS_PATH, "a", encoding="utf-8") as out:
        for rec in fetch_all(session, sample=False):
            out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            saved += 1
            if saved % 200 == 0:
                out.flush()

    print(f"Wrote {saved} records to {RECORDS_PATH}")
    return saved


def main():
    parser = argparse.ArgumentParser(description="INTL/WIPODecisions bootstrap")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument("--reset-checkpoint", action="store_true",
                        help="Discard resume state and sweep from 1999 again")
    args = parser.parse_args()

    if args.reset_checkpoint:
        CHECKPOINT_PATH.unlink(missing_ok=True)
        print("Checkpoint cleared — next sweep restarts at 1999")

    if args.command == "test":
        test_connection()
        return

    # The fleet wrapper invokes `bootstrap-fast`; argparse used to reject it,
    # which is what made the pipeline fall back to re-ingesting sample/.
    count = bootstrap(sample=args.sample)
    if count == 0:
        print("ERROR: No records fetched", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
