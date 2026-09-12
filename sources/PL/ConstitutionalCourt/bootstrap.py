#!/usr/bin/env python3
"""
PL/ConstitutionalCourt -- Polish Constitutional Court Data Fetcher

Fetches case law from the Polish Constitutional Court (Trybunał Konstytucyjny)
via the SAOS API (System Analizy Orzeczeń Sądowych).

Two fetch paths, merged in place (issue #1469):

  1. IPO -- ipo.trybunal.gov.pl, the Tribunal's own Internetowy Portal Orzeczeń.
     Covers 2016-01-01 to the present, which SAOS does not have at all.
       - Listing:  GET /ipo/SzukajDrukuj?cid=1&page=N  (print view of the default
                   search: every ruling, sorted by date descending, 25 per page)
       - Document: GET /ipo/Sprawa?cid=1&dokument={id}  (full text in #tekst_{id})

  2. SAOS -- www.saos.org.pl, the academic aggregator. Its Constitutional Tribunal
     corpus is frozen at 2015-12-09 and covers 1985-2015.
       - Search: GET /api/search/judgments?courtType=CONSTITUTIONAL_TRIBUNAL
       - Detail: GET /api/judgments/{id} returns full text in textContent

IPO is walked first so the decade SAOS is missing -- the whole rule-of-law period --
lands before the historical backfill, and so `--sample` exercises the newer path.
The two are reconciled on case number (sygnatura): a case already yielded by IPO is
not yielded again by SAOS. In practice the date ranges are disjoint.

Data Coverage:
  - Constitutional Tribunal rulings from 1985 to the present
  - Rulings (wyroki), decisions (postanowienia), resolutions (uchwały)
  - ~9,500 judgments from SAOS (1985-2015) + ~1,300 from IPO (2016-present)

Usage:
  python bootstrap.py bootstrap           # Full initial pull (IPO, then SAOS)
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py update              # Incremental update (recent judgments)
  python bootstrap.py test-api            # Quick SAOS API connectivity test
  python bootstrap.py test-ipo            # Quick IPO portal connectivity test
"""

import sys
import json
import logging
import re
import html
import time
import subprocess
import tempfile
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List, Tuple
from urllib.parse import urlencode

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests
from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PL.ConstitutionalCourt")

# API configuration
BASE_URL = "https://www.saos.org.pl/api"
COURT_TYPE = "CONSTITUTIONAL_TRIBUNAL"
PAGE_SIZE = 100
# Safety ceiling only -- pagination really stops on `info.totalResults` / an empty
# page. Kept high so the crawl is never silently truncated as the corpus grows
# (it stood at 9,503 judgments = 96 pages on 2026-08-20, and the old MAX_PAGES=100
# was about to start cutting the tail off).
MAX_PAGES = 2000

# SAOS is a small academic deployment and its search endpoint is slow and erratic:
# a cold query can sit for well over a minute. Sorting makes it dramatically worse
# -- `sortingField=JUDGMENT_DATE` measured 15-75s+ per page (frequently exceeding
# the old 60s timeout) against ~2-4s for the default DATABASE_ID ordering. We
# therefore never send an explicit sortingField: the default ordering is both far
# faster and stable under pagination, since new judgments append at the end
# instead of shifting every subsequent page.
REQUEST_TIMEOUT = 180
MAX_ATTEMPTS = 5
RETRY_STATUSES = {429, 500, 502, 503, 504}

# --- IPO (ipo.trybunal.gov.pl) configuration -------------------------------
#
# The Tribunal's own portal, used for everything SAOS never ingested.
#
# TRANSPORT: this host answers over HTTP/2 only. An HTTP/1.1 request is accepted
# at the TCP/TLS layer and then never answered -- it hangs until the client's own
# timeout fires -- so `requests`/`urllib3` (HTTP/1.1) cannot reach it at all and
# the failure looks exactly like an IP block. We therefore shell out to curl,
# which negotiates h2 via ALPN. Measured side by side: `curl --http2` 200 in
# 0.19s, `curl --http1.1` still hanging at 20s.
IPO_BASE = "https://ipo.trybunal.gov.pl/ipo"
# SAOS's Constitutional Tribunal corpus stops at 2015-12-09, so IPO owns 2016+.
# Kept as an explicit date rather than a rolling window: the IPO listing itself
# goes back to 1997, and a relative cutoff would open a fresh gap every new year.
IPO_CUTOFF_DATE = "2016-01-01"
IPO_PAGE_SIZE = 25          # fixed by the portal's print view
IPO_MAX_PAGES = 400         # safety ceiling; ~135 pages covered 1997-2026
IPO_TIMEOUT = 120

PL_MONTHS = {
    "stycznia": 1, "lutego": 2, "marca": 3, "kwietnia": 4,
    "maja": 5, "czerwca": 6, "lipca": 7, "sierpnia": 8,
    "września": 9, "wrzesnia": 9, "października": 10, "pazdziernika": 10,
    "listopada": 11, "grudnia": 12,
}

# One result row of /ipo/SzukajDrukuj: the case link, then "<kind> z dnia <date> r."
IPO_ROW_RE = re.compile(
    r'href="/ipo/Sprawa\?cid=1&amp;dokument=(?P<dokument>\d+)'
    r'(?:&amp;sprawa=(?P<sprawa>\d+))?"[^>]*>'
    r'<span class="sygnatura">(?P<sygnatura>[^<]+)</span></a>\s*'
    r'<br\s*/>\s*(?P<kind>.*?)\s*z dnia\s+(?P<date>\d{1,2}\s+\S+\s+\d{4})\s*r\.',
    re.S,
)
IPO_PAGER_RE = re.compile(r"Strona wyników:\s*(\d+)\s*z\s*(\d+)")
IPO_SUBJECT_RE = re.compile(
    r'<span style="display: inline-block; font-style: italic;">(.*?)</span>', re.S
)


class IpoUnavailable(RuntimeError):
    """The IPO portal could not be reached or answered nonsense."""


class ConstitutionalCourtScraper(BaseScraper):
    """
    Scraper for PL/ConstitutionalCourt -- Polish Constitutional Court.
    Country: PL
    URL: https://trybunal.gov.pl

    Data types: case_law
    Auth: none (Open Data via SAOS)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
            "Accept": "application/json",
        })

        # IPO portal state (curl-backed; see IPO_BASE)
        self._ipo_cookie_jar: Optional[Path] = None
        self._ipo_session_ready = False

    def _api_get(self, endpoint: str, params: dict = None,
                 timeout: int = REQUEST_TIMEOUT) -> dict:
        """
        GET a SAOS API endpoint, retrying transient failures.

        Raises RuntimeError once the attempts are exhausted. It must never return
        an empty result for a failed request: the previous version swallowed every
        exception and returned None, so a single read timeout on page 0 collapsed
        into "no more items, stopping" and the whole crawl exited 0-records with
        nothing but a warning in the log (issue #1468).
        """
        url = f"{BASE_URL}{endpoint}"
        last_err = None

        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                self.rate_limiter.wait()
                resp = self.session.get(url, params=params, timeout=timeout)

                if resp.status_code in RETRY_STATUSES:
                    last_err = f"HTTP {resp.status_code}"
                    delay = self._retry_delay(attempt, resp.headers.get("Retry-After"))
                    logger.warning(
                        f"{endpoint} returned {resp.status_code} "
                        f"(attempt {attempt}/{MAX_ATTEMPTS}), retrying in {delay:.0f}s"
                    )
                    time.sleep(delay)
                    continue

                resp.raise_for_status()
                return resp.json()

            except (requests.exceptions.Timeout,
                    requests.exceptions.ConnectionError,
                    ValueError) as e:
                # ValueError covers a truncated/non-JSON body, which SAOS returns
                # when it drops a slow query mid-flight.
                last_err = f"{type(e).__name__}: {e}"
                if attempt == MAX_ATTEMPTS:
                    break
                delay = self._retry_delay(attempt)
                logger.warning(
                    f"{endpoint} failed ({last_err}) "
                    f"(attempt {attempt}/{MAX_ATTEMPTS}), retrying in {delay:.0f}s"
                )
                time.sleep(delay)

        raise RuntimeError(
            f"SAOS API request to {endpoint} failed after {MAX_ATTEMPTS} attempts "
            f"(params={params}): {last_err}"
        )

    @staticmethod
    def _retry_delay(attempt: int, retry_after: str = None) -> float:
        """Exponential backoff, capped, honouring Retry-After when present."""
        if retry_after:
            try:
                return min(float(retry_after), 120.0)
            except ValueError:
                pass
        return min(5.0 * (2 ** (attempt - 1)), 120.0)

    def _search_judgments(self, page: int = 0, date_from: str = None,
                          date_to: str = None) -> Tuple[List[Dict[str, Any]], int]:
        """
        Search for Constitutional Court judgments.

        Returns (judgment summaries, totalResults reported by the API). No
        sortingField is sent -- see the note on REQUEST_TIMEOUT above.
        """
        params = {
            "courtType": COURT_TYPE,
            "pageSize": PAGE_SIZE,
            "pageNumber": page,
        }
        if date_from:
            params["judgmentDateFrom"] = date_from
        if date_to:
            params["judgmentDateTo"] = date_to

        data = self._api_get("/search/judgments", params=params)
        items = data.get("items") or []
        total = (data.get("info") or {}).get("totalResults", 0)
        return items, total

    def _get_judgment_details(self, judgment_id: int) -> Optional[Dict[str, Any]]:
        """
        Get full details and text for a specific judgment.

        Returns dict with full metadata and textContent, or None if this one
        judgment is genuinely unavailable (a per-document gap, not a crawl
        failure -- transport errors raise out of _api_get).
        """
        data = self._api_get(f"/judgments/{judgment_id}")
        return data.get("data")

    # ------------------------------------------------------------------
    # IPO portal (ipo.trybunal.gov.pl) -- 2016-present
    # ------------------------------------------------------------------

    def _ipo_get(self, path: str, params: Dict[str, Any]) -> str:
        """
        GET an IPO page over HTTP/2 via curl, retrying transient failures.

        curl rather than requests because the host never answers HTTP/1.1 (see
        the IPO_BASE note). The session cookies -- JSESSIONID plus the search
        filter cookies the portal sets on /Szukaj -- are kept in a jar file for
        the lifetime of the scraper; without them /Szukaj redirects to itself
        forever and the print view has no result set to page through.
        """
        query = urlencode(params)
        url = f"{IPO_BASE}{path}" + (f"?{query}" if query else "")
        last_err = None

        for attempt in range(1, MAX_ATTEMPTS + 1):
            self.rate_limiter.wait()
            proc = subprocess.run(
                [
                    "curl", "-sS", "--http2", "--compressed",
                    "--max-time", str(IPO_TIMEOUT),
                    "-b", str(self._ipo_cookie_jar), "-c", str(self._ipo_cookie_jar),
                    "-H", "User-Agent: LegalDataHunter/1.0 (Open Data Research)",
                    "-H", "Accept: text/html,application/xhtml+xml",
                    "-H", "Accept-Language: pl,en;q=0.8",
                    "-w", "\n%{http_code}",
                    url,
                ],
                capture_output=True,
                text=True,
            )

            if proc.returncode != 0:
                last_err = (
                    f"curl exit {proc.returncode}: "
                    f"{(proc.stderr or '').strip()[:200]}"
                )
            else:
                body, _, status = proc.stdout.rpartition("\n")
                status = status.strip()
                if status == "200":
                    return body
                last_err = f"HTTP {status}"
                if status not in {str(s) for s in RETRY_STATUSES}:
                    raise IpoUnavailable(f"IPO request to {url} returned {last_err}")

            if attempt == MAX_ATTEMPTS:
                break
            delay = self._retry_delay(attempt)
            logger.warning(
                f"IPO {path} failed ({last_err}) "
                f"(attempt {attempt}/{MAX_ATTEMPTS}), retrying in {delay:.0f}s"
            )
            time.sleep(delay)

        raise IpoUnavailable(
            f"IPO request to {url} failed after {MAX_ATTEMPTS} attempts: {last_err}"
        )

    def _ipo_open_session(self) -> None:
        """
        Establish the portal session: land on /ipo/ for a JSESSIONID, then on
        /Szukaj to have the server build the default result set (every ruling,
        newest first) that the print view pages through.
        """
        if self._ipo_session_ready:
            return
        if self._ipo_cookie_jar is None:
            self._ipo_cookie_jar = Path(
                tempfile.mkdtemp(prefix="ipo-tk-")
            ) / "cookies.txt"
        self._ipo_get("/", {})
        page = self._ipo_get("/Szukaj", {"cid": 1})
        if "SzukajDrukuj" not in page:
            raise IpoUnavailable(
                "IPO /Szukaj did not return a search result page "
                f"({len(page)} bytes). Cannot page through results."
            )
        self._ipo_session_ready = True

    @staticmethod
    def _parse_polish_date(text: str) -> Optional[str]:
        """'20 maja 2026' -> '2026-05-20'. Returns None if unparseable."""
        parts = text.strip().split()
        if len(parts) != 3:
            return None
        day, month, year = parts
        month_num = PL_MONTHS.get(month.lower())
        if not month_num or not day.isdigit() or not year.isdigit():
            return None
        return f"{int(year):04d}-{month_num:02d}-{int(day):02d}"

    def _ipo_list_rulings(self, cutoff: str) -> List[Dict[str, Any]]:
        """
        Page through the IPO print view and return every ruling handed down on
        or after `cutoff`, newest first.

        Pagination stops when a whole page predates the cutoff -- the listing is
        sorted by date descending -- or when a page has no rows. Page 0 having no
        rows is a failure, not an empty corpus.
        """
        self._ipo_open_session()

        rulings: List[Dict[str, Any]] = []
        seen_documents = set()
        total_pages = None

        for page in range(IPO_MAX_PAGES):
            body = self._ipo_get("/SzukajDrukuj", {"cid": 1, "page": page})

            if total_pages is None:
                pager = IPO_PAGER_RE.search(body)
                if pager:
                    total_pages = int(pager.group(2))
                    logger.info(f"IPO reports {total_pages} pages of rulings")

            rows = list(IPO_ROW_RE.finditer(body))
            if not rows:
                if page == 0:
                    raise IpoUnavailable(
                        "IPO print view returned no rulings on page 0. The "
                        "Tribunal's own portal is never empty -- treating this "
                        "as an upstream/transport failure rather than silently "
                        "reporting an empty crawl."
                    )
                logger.info(f"IPO page {page} empty, pagination complete")
                break

            subjects = IPO_SUBJECT_RE.findall(body)
            page_dates = []

            for idx, row in enumerate(rows):
                date = self._parse_polish_date(row.group("date"))
                if not date:
                    logger.warning(
                        f"IPO: unparseable date {row.group('date')!r} for "
                        f"{row.group('sygnatura')}, skipping"
                    )
                    continue
                page_dates.append(date)
                if date < cutoff:
                    continue

                document_id = row.group("dokument")
                if document_id in seen_documents:
                    continue
                seen_documents.add(document_id)

                rulings.append({
                    "document_id": document_id,
                    "case_id": row.group("sprawa"),
                    "case_number": self._clean_text(row.group("sygnatura")),
                    "kind": self._clean_text(row.group("kind")),
                    "date": date,
                    "subject": (
                        self._clean_text(subjects[idx]) if idx < len(subjects) else ""
                    ),
                })

            logger.info(
                f"IPO page {page}"
                + (f"/{total_pages}" if total_pages else "")
                + f": {len(rows)} rows "
                f"({page_dates[0] if page_dates else '?'} .. "
                f"{page_dates[-1] if page_dates else '?'}), "
                f"{len(rulings)} kept"
            )

            if page_dates and max(page_dates) < cutoff:
                logger.info(
                    f"IPO page {page} is entirely older than {cutoff}, stopping"
                )
                break
        else:
            logger.warning(
                f"Hit the IPO_MAX_PAGES={IPO_MAX_PAGES} ceiling -- the IPO "
                "listing may be truncated; raise the ceiling."
            )

        logger.info(f"IPO listing: {len(rulings)} rulings on or after {cutoff}")
        return rulings

    @staticmethod
    def _inner_div(page: str, start: int) -> str:
        """Inner HTML of the <div> whose opening tag begins at `start`."""
        open_end = page.index(">", start)
        depth = 1
        cursor = open_end + 1
        tag_re = re.compile(r"<(/?)div\b", re.I)
        while depth > 0:
            match = tag_re.search(page, cursor)
            if not match:
                return page[open_end + 1:]
            depth += -1 if match.group(1) else 1
            cursor = match.end()
        return page[open_end + 1:page.rindex("<", 0, cursor)]

    def _ipo_fetch_ruling(self, ruling: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Fetch one ruling page and pull out its full text plus metadata.

        Returns None when the page carries no document body (a per-document gap:
        a handful of listed cases have no published text yet). Transport failures
        raise out of _ipo_get.
        """
        params = {"cid": 1, "dokument": ruling["document_id"]}
        if ruling.get("case_id"):
            params["sprawa"] = ruling["case_id"]
        page = self._ipo_get("/Sprawa", params)

        marker = re.search(
            r'<div id="tekst_%s"' % re.escape(ruling["document_id"]), page
        )
        if not marker:
            return None

        text = self._clean_html_block(self._inner_div(page, marker.start()))
        if not text:
            return None

        properties: Dict[str, str] = {}
        for prop in re.finditer(
            r'<div class="prop">\s*<span class="name">(.*?)</span>\s*'
            r'<span class="value">(.*?)</span>',
            page, re.S,
        ):
            name = self._clean_text(prop.group(1))
            if name and name not in properties:
                properties[name] = self._clean_text(prop.group(2))

        judges = [
            self._clean_text(judge)
            for judge in re.findall(r'<a href="/ipo/Szukaj\?sedzia=\d+">(.*?)</a>', page)
        ]

        return {
            **ruling,
            "_ipo": True,
            "text": text,
            "properties": properties,
            "judges": sorted({judge for judge in judges if judge}),
        }

    def _walk_ipo(self, cutoff: str = IPO_CUTOFF_DATE) -> Generator[dict, None, None]:
        """Yield every IPO ruling from `cutoff` onwards, with full text."""
        rulings = self._ipo_list_rulings(cutoff)
        fetched = 0
        missing = 0

        for ruling in rulings:
            record = self._ipo_fetch_ruling(ruling)
            if record:
                fetched += 1
                yield record
            else:
                missing += 1
                logger.warning(
                    f"IPO document {ruling['document_id']} "
                    f"({ruling['case_number']}) has no published text, skipping"
                )

        logger.info(
            f"IPO fetch complete: {fetched} rulings with full text, "
            f"{missing} without ({len(rulings)} listed)"
        )

        if rulings and fetched == 0:
            raise IpoUnavailable(
                f"Listed {len(rulings)} IPO rulings but extracted 0 with text. "
                "Failing loud rather than reporting an empty crawl."
            )

    def _clean_html_block(self, fragment: str) -> str:
        """
        Turn a block of judgment HTML into plain text, keeping paragraph breaks.

        Unlike _clean_text (which flattens everything to one line for short
        metadata values) this preserves the structure of a ruling: the header,
        the operative part, and the numbered paragraphs of the reasoning.
        """
        if not fragment:
            return ""

        text = re.sub(r"(?is)<(script|style)\b.*?</\1>", " ", fragment)
        text = re.sub(r"(?i)<br\s*/?>", "\n", text)
        text = re.sub(r"(?i)</(p|div|tr|li|h[1-6]|table)>", "\n", text)
        text = re.sub(r"<[^>]+>", "", text)
        text = html.unescape(text)
        text = re.sub(r"[ \t\xa0]+", " ", text)
        text = "\n".join(line.strip() for line in text.split("\n"))
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    def _clean_text(self, html_text: str) -> str:
        """Clean HTML/text content and return plain text."""
        if not html_text:
            return ""

        # Remove HTML tags
        text = re.sub(r'<[^>]+>', ' ', html_text)

        # Decode HTML entities
        text = html.unescape(text)

        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'\n\s*\n', '\n\n', text)

        return text.strip()

    def _extract_case_number(self, court_cases: List[Dict]) -> str:
        """Extract primary case number from court cases list."""
        if court_cases and len(court_cases) > 0:
            return court_cases[0].get("caseNumber", "")
        return ""

    def _walk_search(self, date_from: str = None,
                     date_to: str = None) -> Generator[dict, None, None]:
        """
        Page through the search results and yield the full detail record for each
        judgment.

        Stops on `info.totalResults` or an empty page. Page 0 returning nothing is
        treated as a failure, not as an empty corpus: the Constitutional Tribunal
        has published continuously since 1985, so a zero there means SAOS refused
        or broke the query.
        """
        page = 0
        fetched = 0
        missing = 0
        total = None

        while page < MAX_PAGES:
            items, reported_total = self._search_judgments(
                page=page, date_from=date_from, date_to=date_to
            )

            if total is None:
                total = reported_total
                logger.info(f"SAOS reports {total} matching judgments")
                if page == 0 and not items:
                    if date_from:
                        # An update window legitimately can be empty.
                        logger.info("No judgments in the requested date window")
                        return
                    raise RuntimeError(
                        "SAOS search returned 0 judgments on page 0 for "
                        f"courtType={COURT_TYPE} (totalResults={reported_total}). "
                        "The Constitutional Tribunal corpus is never empty -- "
                        "treating this as an upstream failure rather than "
                        "silently reporting an empty crawl."
                    )

            if not items:
                logger.info(f"Page {page} empty, pagination complete")
                break

            logger.info(
                f"Page {page}: {len(items)} judgments "
                f"({fetched}/{total} details fetched so far)"
            )

            for item in items:
                judgment_id = item.get("id")
                if not judgment_id:
                    continue

                details = self._get_judgment_details(judgment_id)
                if details and details.get("textContent"):
                    fetched += 1
                    yield details
                else:
                    missing += 1
                    logger.warning(
                        f"Judgment {judgment_id} has no textContent, skipping"
                    )

            page += 1
        else:
            logger.warning(
                f"Hit the MAX_PAGES={MAX_PAGES} safety ceiling -- the corpus may "
                "be truncated; raise the ceiling."
            )

        logger.info(
            f"Fetch complete: {fetched} judgments with full text, "
            f"{missing} skipped without text (of {total} reported)"
        )

        if fetched == 0:
            raise RuntimeError(
                f"Paged through {page} page(s) of SAOS results but extracted 0 "
                f"judgments with text ({missing} lacked textContent). Failing "
                "loud rather than reporting an empty crawl."
            )

    @staticmethod
    def _case_key(case_number: str) -> str:
        """Normalised sygnatura, for reconciling the two fetch paths."""
        return re.sub(r"\s+", " ", (case_number or "")).strip().upper()

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all Constitutional Tribunal rulings with full text.

        IPO first (2016-present, absent from SAOS), then SAOS (1985-2015). Cases
        already seen from IPO are not yielded again from SAOS.
        """
        seen_cases = set()

        logger.info(
            f"Starting Constitutional Tribunal fetch: IPO portal "
            f"({IPO_CUTOFF_DATE} onwards) first, then the SAOS backfill"
        )
        for record in self._walk_ipo():
            seen_cases.add(self._case_key(record.get("case_number", "")))
            yield record

        logger.info("Continuing with the SAOS backfill (1985-2015)...")
        for record in self._walk_search():
            key = self._case_key(
                self._extract_case_number(record.get("courtCases", []))
            )
            if key and key in seen_cases:
                logger.debug(f"Skipping {key}: already fetched from IPO")
                continue
            yield record

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield rulings handed down since the given date.

        Recent rulings only ever come from IPO -- SAOS stopped ingesting
        Constitutional Tribunal judgments on 2015-12-09 -- so an update window
        that starts after that date skips the SAOS query entirely.
        """
        since_str = since.strftime("%Y-%m-%d")
        today_str = datetime.now().strftime("%Y-%m-%d")
        logger.info(f"Fetching updates from {since_str} to {today_str}...")

        seen_cases = set()
        for record in self._walk_ipo(cutoff=max(since_str, "1997-01-01")):
            seen_cases.add(self._case_key(record.get("case_number", "")))
            yield record

        if since_str >= IPO_CUTOFF_DATE:
            return

        for record in self._walk_search(date_from=since_str, date_to=today_str):
            key = self._case_key(
                self._extract_case_number(record.get("courtCases", []))
            )
            if key and key in seen_cases:
                continue
            yield record

    def _normalize_ipo(self, raw: dict) -> dict:
        """Transform an IPO portal ruling into the standard schema."""
        properties = raw.get("properties", {})
        case_number = raw.get("case_number", "")
        document_id = raw.get("document_id", "")

        kind = raw.get("kind") or properties.get("Rodzaj orzeczenia", "")
        title = f"{kind} {case_number}".strip() if case_number else kind
        subject = raw.get("subject") or properties.get("Dotyczy", "")

        url = f"{IPO_BASE}/Sprawa?cid=1&dokument={document_id}"
        if raw.get("case_id"):
            url += f"&sprawa={raw['case_id']}"

        return {
            # Required base fields
            "_id": f"PL/TK/{case_number}" if case_number else f"PL/TK/IPO-{document_id}",
            "_source": "PL/ConstitutionalCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": raw.get("text", ""),  # MANDATORY FULL TEXT
            "date": raw.get("date", ""),
            "url": url,
            # Source-specific fields
            "ipo_document_id": document_id,
            "ipo_case_id": raw.get("case_id"),
            "case_number": case_number,
            "all_case_numbers": [case_number] if case_number else [],
            "judgment_type": kind,
            "judges": [{"name": judge, "roles": []} for judge in raw.get("judges", [])],
            "keywords": [],
            "referenced_regulations": [],
            "subject": subject,
            "publication": properties.get("Miejsce publikacji", ""),
            "court_type": "CONSTITUTIONAL_TRIBUNAL",
            "language": "pl",
            "fetch_path": "ipo",
        }

    def normalize(self, raw: dict) -> dict:
        """
        Transform a raw record from either fetch path into the standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        if raw.get("_ipo"):
            return self._normalize_ipo(raw)

        judgment_id = raw.get("id", 0)
        court_cases = raw.get("courtCases", [])
        case_number = self._extract_case_number(court_cases)

        # Get judgment date
        judgment_date = raw.get("judgmentDate", "")

        # Get full text
        text_content = raw.get("textContent", "")
        clean_text = self._clean_text(text_content)

        # Build title from case number and type
        judgment_type = raw.get("judgmentType", "DECISION")
        type_names = {
            "DECISION": "Postanowienie",
            "SENTENCE": "Wyrok",
            "RESOLUTION": "Uchwała",
            "REASONS": "Uzasadnienie",
        }
        type_name = type_names.get(judgment_type, judgment_type)
        title = f"{type_name} {case_number}" if case_number else f"{type_name} (ID: {judgment_id})"

        # Get judges
        judges = []
        for judge in raw.get("judges", []):
            name = judge.get("name", "")
            roles = judge.get("specialRoles", [])
            if name:
                judges.append({
                    "name": name,
                    "roles": roles,
                })

        # Get source URL
        source_info = raw.get("source", {})
        source_url = source_info.get("judgmentUrl", "")
        if not source_url:
            source_url = f"https://www.saos.org.pl/judgments/{judgment_id}"

        # Get referenced regulations
        regulations = []
        for reg in raw.get("referencedRegulations", []):
            regulations.append(reg.get("text", ""))

        # Get keywords
        keywords = raw.get("keywords", [])

        return {
            # Required base fields
            "_id": f"PL/TK/{case_number}" if case_number else f"PL/TK/ID-{judgment_id}",
            "_source": "PL/ConstitutionalCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": clean_text,  # MANDATORY FULL TEXT
            "date": judgment_date,
            "url": source_url,
            # Source-specific fields
            "saos_id": judgment_id,
            "case_number": case_number,
            "all_case_numbers": [cc.get("caseNumber") for cc in court_cases if cc.get("caseNumber")],
            "judgment_type": judgment_type,
            "judges": judges,
            "keywords": keywords,
            "referenced_regulations": regulations,
            "court_type": raw.get("courtType", "CONSTITUTIONAL_TRIBUNAL"),
            "language": "pl",
            "fetch_path": "saos",
        }

    def test_api(self):
        """Quick connectivity and API test."""
        print("Testing SAOS API for Polish Constitutional Court...")

        # Test search endpoint
        print("\n1. Testing search endpoint...")
        items, total = self._search_judgments(page=0)
        if items:
            print(f"   Found {len(items)} judgments on first page of {total} total")
            print(f"   First: {items[0].get('courtCases', [{}])[0].get('caseNumber', 'N/A')}")
            print(f"   Date: {items[0].get('judgmentDate', 'N/A')}")
        else:
            print("   ERROR: No judgments returned")
            return

        # Test detail endpoint
        print("\n2. Testing detail endpoint...")
        if items:
            first_id = items[0].get("id")
            details = self._get_judgment_details(first_id)
            if details:
                text = details.get("textContent", "")
                clean = self._clean_text(text)
                print(f"   Judgment ID: {first_id}")
                print(f"   Text length: {len(clean)} characters")
                print(f"   Preview: {clean[:200]}...")
            else:
                print("   ERROR: Could not fetch details")

        # Confirm deep pagination still answers
        print("\n3. Checking deep pagination...")
        last_page = max(total - 1, 0) // PAGE_SIZE
        items_last, _ = self._search_judgments(page=last_page)
        print(f"   Last page ({last_page}) has {len(items_last)} items")
        print(f"   Total judgments: {total}")

        print("\nAPI test complete!")

    def test_ipo(self, pages: int = 2):
        """Connectivity and extraction test for the IPO portal path."""
        print("Testing the IPO portal (ipo.trybunal.gov.pl)...")

        print("\n1. Opening a portal session...")
        self._ipo_open_session()
        print("   OK -- search result set built")

        print(f"\n2. Reading the first {pages} listing page(s)...")
        listing = self._ipo_get("/SzukajDrukuj", {"cid": 1, "page": 0})
        pager = IPO_PAGER_RE.search(listing)
        print(f"   Pager: {pager.group(0) if pager else 'not found'}")

        rulings = []
        for page in range(pages):
            body = self._ipo_get("/SzukajDrukuj", {"cid": 1, "page": page})
            rows = list(IPO_ROW_RE.finditer(body))
            print(f"   Page {page}: {len(rows)} rows")
            for row in rows:
                rulings.append({
                    "document_id": row.group("dokument"),
                    "case_id": row.group("sprawa"),
                    "case_number": row.group("sygnatura"),
                    "kind": self._clean_text(row.group("kind")),
                    "date": self._parse_polish_date(row.group("date")),
                    "subject": "",
                })

        if not rulings:
            print("   ERROR: no rulings parsed")
            return

        print(f"\n3. Fetching full text for the first 3 of {len(rulings)} rulings...")
        for ruling in rulings[:3]:
            record = self._ipo_fetch_ruling(ruling)
            if not record:
                print(f"   {ruling['case_number']}: NO TEXT")
                continue
            normalized = self._normalize_ipo(record)
            print(
                f"   {normalized['_id']} | {normalized['date']} | "
                f"{normalized['judgment_type']} | {len(normalized['text'])} chars "
                f"| {len(normalized['judges'])} judges"
            )
            print(f"      {normalized['text'][:120]!r}")

        print("\nIPO test complete!")


def main():
    scraper = ConstitutionalCourtScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test-api|test-ipo] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test-api":
        scraper.test_api()

    elif command == "test-ipo":
        scraper.test_ipo()

    elif command == "bootstrap":
        if sample_mode:
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

    elif command == "bootstrap-fast":
        # The fleet wrapper invokes this name; without it argparse fell through to
        # "Unknown command" and the pipeline re-ingested the committed samples.
        stats = scraper.bootstrap_fast()
        print(
            f"\nbootstrap_fast complete: {stats.get('records_fetched', 0)} fetched, "
            f"{stats.get('records_new', 0)} new, {stats.get('errors', 0)} errors"
        )
        print(json.dumps(stats, indent=2, default=str))

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
