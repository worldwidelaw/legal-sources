#!/usr/bin/env python3
"""
UN/DigitalLibrary -- UN General Assembly Resolutions

Fetches GA resolutions with full text from the UN ODS document store.

Discovery does NOT use digitallibrary.un.org: that host now sits behind an AWS
WAF JS challenge which answers every request -- search API, OAI-PMH landing,
homepage alike -- with `HTTP 202 / x-amzn-waf-action: challenge` and an empty
body (issue #1598). The 202 is a challenge, not an async-search accept, so
polling it never resolves; it reproduces from residential IPs too, so it is not
a datacenter block.

documents.un.org (ODS) is NOT behind that WAF, and GA resolution symbols are
deterministic, so we enumerate symbols directly instead:

  - Walk sessions newest-first: A/RES/{session}/{number}, plus the emergency
    special sessions (A/RES/ES-{n}/{number}).
  - End a session after MAX_CONSECUTIVE_MISSES absent numbers (gaps are normal:
    sub-lettered resolutions such as A/RES/80/236/A-B leave the plain number
    empty, so the tolerance is deliberately generous).
  - Full text per symbol from documents.un.org/api/symbol/access:
      t=doc -> DOCX (>= session ~54) or OLE2 .doc, via common.doc_extract
      t=pdf -> fallback via common.pdf_extract for anything .doc cannot parse
    ODS answers HTTP 200 even for missing documents, returning a ~1.3 KB HTML
    stub, so absence is detected by payload shape rather than status code.

Coverage note: sessions up to ~47 (pre-1993) exist only as scanned PDFs with no
text layer, and sessions ~48-53 ship WordPerfect 5.1 .doc files. Those yield no
usable text and are skipped rather than stored as empty records.

Data: ~19,000 GA resolutions; roughly 7,000 with extractable full text.
Rate limit: ~1 req/sec to documents.un.org.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import re
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

import requests as _requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from common.base_scraper import BaseScraper
from common.doc_extract import extract_word_text
from common.pdf_extract import extract_pdf_markdown

# Below this, a "document" is a header fragment rather than a resolution body.
MIN_TEXT_CHARS = 200

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UN.DigitalLibrary")


def _make_session() -> _requests.Session:
    """Create a requests session for documents.un.org.

    Default python-requests headers get a less friendly reception than a plain
    curl-shaped request, so the defaults are cleared and only a minimal pair is
    sent.
    """
    s = _requests.Session()
    s.headers.clear()
    s.headers["User-Agent"] = "curl/8.7.1"
    s.headers["Accept"] = "*/*"
    retry = Retry(total=3, backoff_factor=1.0, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

ODS_API = "https://documents.un.org/api/symbol/access"

# A session's resolution numbers are walked until this many consecutive symbols
# come back absent. Gaps inside a session are normal (sub-lettered resolutions
# such as A/RES/80/236/A-B leave the plain number empty), hence the headroom.
MAX_CONSECUTIVE_MISSES = 15

# Hard ceiling on resolution numbers within one session; the busiest sessions
# land in the low 300s.
MAX_RESOLUTION_NUM = 400

# Sessions at or below this number only exist as scanned PDFs (no text layer) or
# WordPerfect 5.1 .doc files, neither of which yields usable text.
OLDEST_TEXT_SESSION = 54

# Emergency special sessions that carry an A/RES/ES-{n}/{num} symbol.
EMERGENCY_SESSIONS = ["ES-11", "ES-10"]

# ODS answers 200 for missing documents with a small HTML "not found" stub.
MISSING_DOC_MAX_BYTES = 4096


def current_ga_session(now: Optional[datetime] = None) -> int:
    """Return the number of the GA session currently in progress.

    Session 1 opened in 1946, and each subsequent session opens in September, so
    session N runs from September (1945 + N).
    """
    now = now or datetime.now(timezone.utc)
    session = now.year - 1945
    if now.month < 9:
        session -= 1
    return session


def is_missing_document(data: bytes) -> bool:
    """True if an ODS payload is the "not found" stub rather than a document.

    ODS returns HTTP 200 for symbols that do not exist, so absence has to be
    detected from the body: a short HTML page instead of a binary document.
    """
    if not data:
        return True
    if len(data) > MISSING_DOC_MAX_BYTES:
        return False
    head = data[:512].lstrip().lower()
    return head.startswith(b"<!doctype") or head.startswith(b"<html")


MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}

_ADOPTED_RE = re.compile(
    r"adopted by the General Assembly\s+on\s+(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})",
    re.IGNORECASE,
)
_ANY_DATE_RE = re.compile(r"\b(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})\b")

# "80/10.<tab>International Day of ..." — the resolution number, then its title.
_TITLE_RE = re.compile(r"^\s*(?:ES-)?\d+/\d+[A-Z\-]*\.\s*(.+)$", re.MULTILINE)


def extract_date(text: str) -> Optional[str]:
    """Return the adoption date as ISO 8601, or None.

    Prefers the explicit "adopted by the General Assembly on <date>" line;
    pre-2000 resolutions often omit it, in which case the first date in the
    document header is used.
    """
    m = _ADOPTED_RE.search(text) or _ANY_DATE_RE.search(text[:2000])
    if not m:
        return None
    day, month_name, year = m.group(1), m.group(2).lower(), m.group(3)
    month = MONTHS.get(month_name)
    if not month:
        return None
    return f"{year}-{month:02d}-{int(day):02d}"


def extract_title(text: str) -> Optional[str]:
    """Return the resolution's own title from its numbered heading line."""
    m = _TITLE_RE.search(text)
    if not m:
        return None
    title = re.sub(r"\s+", " ", m.group(1)).strip()
    return title if 3 < len(title) < 500 else None


def extract_text_from_docx(data: bytes) -> Optional[str]:
    """Extract plain text from a DOCX or OLE2 .doc payload.

    Delegates to the shared Word extractor, which handles both the modern DOCX
    zip container and the legacy OLE2 piece table.
    """
    try:
        text = extract_word_text(data)
    except Exception as e:  # noqa: BLE001 - never let one bad file kill the crawl
        logger.debug(f"Word extraction failed: {e}")
        return None
    return text if text and len(text) > MIN_TEXT_CHARS else None


class DigitalLibraryScraper(BaseScraper):
    """
    Scraper for UN/DigitalLibrary -- UN General Assembly Resolutions.
    Country: UN
    URL: https://digitallibrary.un.org

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = _make_session()

    def _download(self, symbol: str, fmt: str) -> Optional[bytes]:
        """Download one ODS rendition of a symbol, or None if it is absent.

        ODS replies 200 with an HTML stub for symbols that do not exist, so the
        stub check -- not the status code -- is what distinguishes absence.
        """
        params = {"s": symbol, "l": "en", "t": fmt}
        try:
            resp = self.session.get(ODS_API, params=params, timeout=120,
                                    allow_redirects=True)
        except _requests.RequestException as e:
            logger.warning(f"{symbol} t={fmt}: request failed: {e}")
            return None
        if resp is None or resp.status_code != 200:
            logger.warning(
                f"{symbol} t={fmt}: HTTP {resp.status_code if resp else 'None'}"
            )
            return None
        if is_missing_document(resp.content):
            return None
        return resp.content

    def _fetch_text(self, symbol: str) -> Optional[str]:
        """Return the full text of a resolution, or None if unavailable.

        Word first (clean text for both DOCX and OLE2 .doc), PDF as the fallback
        for anything the Word path cannot parse.
        """
        doc_data = self._download(symbol, "doc")
        if doc_data:
            text = extract_text_from_docx(doc_data)
            if text:
                return text
            logger.debug(f"{symbol}: Word payload not parseable, trying PDF")

        pdf_data = self._download(symbol, "pdf")
        if not pdf_data:
            return None
        try:
            text = extract_pdf_markdown(
                "UN/DigitalLibrary", symbol,
                pdf_bytes=pdf_data, table="legislation",
            )
        except Exception as e:  # noqa: BLE001 - a bad PDF must not kill the crawl
            logger.debug(f"{symbol}: PDF extraction failed: {e}")
            return None
        return text if text and len(text) > MIN_TEXT_CHARS else None

    def _iter_symbols(self) -> Generator[tuple, None, None]:
        """Yield (session, number, symbol) newest-first across all GA sessions.

        Newest-first matters for sample runs, which take the first N records:
        recent sessions are the ones with clean DOCX full text.
        """
        latest = current_ga_session()
        sessions = [str(s) for s in range(latest, OLDEST_TEXT_SESSION - 1, -1)]
        sessions.extend(EMERGENCY_SESSIONS)

        for session in sessions:
            for num in range(1, MAX_RESOLUTION_NUM + 1):
                yield session, num, f"A/RES/{session}/{num}"

    # -- Normalize ----------------------------------------------------------

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw record into standard schema."""
        text = raw.get("_text", "")
        if not text or len(text) < MIN_TEXT_CHARS:
            return None

        symbol = raw.get("symbol", "")
        doc_id = f"UN-GA-{symbol.replace('/', '-')}"

        return {
            "_id": doc_id,
            "_source": "UN/DigitalLibrary",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "symbol": symbol,
            "session": raw.get("session"),
            "title": extract_title(text) or raw.get("title", f"GA Resolution {symbol}"),
            "text": text,
            "date": extract_date(text),
            "url": f"https://docs.un.org/en/{symbol}",
            "body": "General Assembly",
            "doc_type": "resolution",
        }

    # -- Fetch methods ------------------------------------------------------

    def fetch_all(self, min_session: Optional[int] = None) -> Generator[dict, None, None]:
        """Yield GA resolutions with full text, newest session first.

        Args:
            min_session: Stop once sessions older than this are reached. Used by
                fetch_updates; None walks back to OLDEST_TEXT_SESSION.
        """
        yielded = 0
        skipped = 0
        misses = 0
        current_session = None

        for session, num, symbol in self._iter_symbols():
            if session != current_session:
                # New session: reset the end-of-session miss counter.
                current_session = session
                misses = 0
                logger.info(
                    f"Session {session} (yielded={yielded}, skipped={skipped})"
                )
            elif misses >= MAX_CONSECUTIVE_MISSES:
                # Already past the end of this session; skip its remaining numbers.
                continue

            if min_session is not None and self._session_sort_key(session) < min_session:
                logger.info(f"Reached session cutoff {min_session}, stopping")
                break

            time.sleep(1.0)  # ODS rate limit

            text = self._fetch_text(symbol)
            if not text:
                misses += 1
                skipped += 1
                continue

            misses = 0
            yielded += 1
            yield {
                "symbol": symbol,
                "session": session,
                "number": num,
                "title": f"General Assembly Resolution {symbol}",
                "_text": text,
            }

        logger.info(f"Done: {yielded} resolutions with text, {skipped} skipped")

    @staticmethod
    def _session_sort_key(session: str) -> int:
        """Numeric session number; emergency sessions sort as most recent."""
        m = re.search(r"(\d+)", session)
        if session.startswith("ES-"):
            return 10_000
        return int(m.group(1)) if m else 0

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield resolutions from sessions that were sitting since `since`.

        `since` is a crawl timestamp, so the comparator is which sessions could
        have published anything after it -- the session in progress at `since`
        and everything opened later -- not a per-document decision date.
        """
        if isinstance(since, str):
            since = datetime.fromisoformat(since)
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)

        min_session = max(current_ga_session(since), OLDEST_TEXT_SESSION)
        logger.info(f"Updates since {since.date()}: sessions >= {min_session}")
        yield from self.fetch_all(min_session=min_session)


# -- CLI ----------------------------------------------------------------

def main():
    import argparse

    parser = argparse.ArgumentParser(description="UN/DigitalLibrary Data Fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = DigitalLibraryScraper()

    if args.command == "test-api":
        logger.info(f"Current GA session: {current_ga_session()}")

        logger.info("Testing ODS full-text path...")
        text = scraper._fetch_text("A/RES/78/1")
        if text:
            logger.info(f"OK: {len(text)} chars extracted")
            logger.info(f"  Title: {extract_title(text)}")
            logger.info(f"  Date:  {extract_date(text)}")
        else:
            logger.error("Full-text fetch failed for A/RES/78/1")

        logger.info("Testing absent-symbol detection...")
        missing = scraper._download("A/RES/78/9999", "doc")
        logger.info(f"  A/RES/78/9999 -> {'absent (correct)' if missing is None else 'UNEXPECTED payload'}")
        return

    if args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        from datetime import timedelta
        since = datetime.now(timezone.utc) - timedelta(days=365)
        stats = scraper.bootstrap(sample_mode=False)
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
