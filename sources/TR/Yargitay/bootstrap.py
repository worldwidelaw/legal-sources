"""
Legal Data Hunter - Turkish Court of Cassation (Yargitay) Scraper

Fetches case law from the Turkish Court of Cassation (Yargıtay).
Data source: Bedesten API (https://bedesten.adalet.gov.tr)
Method: JSON API with HTML content extraction
Coverage: Civil and criminal supreme court decisions (~6 million total)
"""

import re
import sys
import json
import html
import time
import base64
import random
import logging
from pathlib import Path
from datetime import datetime, timezone, date, timedelta
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("TR/Yargitay")


class TurkishCourtOfCassationScraper(BaseScraper):
    """
    Scraper for: Turkish Court of Cassation (Yargıtay)
    Country: TR
    URL: https://www.yargitay.gov.tr

    Data types: case_law
    Auth: none

    Uses the Bedesten API (bedesten.adalet.gov.tr) which provides:
    - Search across all Yargıtay decisions by keyword, date, chamber
    - Full decision text in HTML format (base64 encoded)
    - ~6 million total decisions available
    """

    API_BASE = "https://bedesten.adalet.gov.tr"
    SEARCH_ENDPOINT = "/emsal-karar/searchDocuments"
    DOCUMENT_ENDPOINT = "/emsal-karar/getDocumentContent"

    # The API rejects an empty/"*" phrase ("Sadece harf ve rakam içeren
    # aramalar yapılabilir"), so the corpus can only be enumerated through a
    # search term. These two are near-universal: probed against single-day
    # windows they report the same total as "karar"/"madde"/"ile"/"olarak"
    # and their union adds no documents, i.e. one of them matches every
    # decision in the window. Both are probed per window and the larger
    # total wins; if they disagree the window is crawled under both and
    # de-duplicated, so a future indexing change degrades coverage
    # gracefully instead of silently truncating.
    PROBE_PHRASES = ["dava", "karar"]

    PAGE_SIZE = 100          # verified: the API honours 100 (the old code assumed 10)
    MIN_YEAR = 1940          # year probes make empty years cost 1 request each
    MAX_DIRTY_PAGES = 200    # bounded sweep for out-of-range decision dates

    # Transient statuses worth retrying. 429 is the real failure mode here and
    # is returned as a plain-text body, not JSON.
    RETRY_STATUSES = {429, 500, 502, 503, 504}

    # Chamber mappings for filtering
    CIVIL_CHAMBERS = [f"{i}. Hukuk Dairesi" for i in range(1, 24)]  # 1-23. Hukuk Dairesi
    CRIMINAL_CHAMBERS = [f"{i}. Ceza Dairesi" for i in range(1, 24)]  # 1-23. Ceza Dairesi
    GENERAL_COUNCILS = [
        "Hukuk Genel Kurulu",
        "Ceza Genel Kurulu",
        "Büyük Genel Kurul",
    ]

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=self.API_BASE,
            headers={
                "Accept": "*/*",
                "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
                "AdaletApplicationName": "UyapMevzuat",
                "Content-Type": "application/json; charset=utf-8",
                "Origin": "https://mevzuat.adalet.gov.tr",
                "Referer": "https://mevzuat.adalet.gov.tr/",
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
            },
        )

    # ── Checkpointing ─────────────────────────────────────────────

    @property
    def _checkpoint_path(self) -> Path:
        return Path(self.source_dir) / "data" / "crawl_checkpoint.json"

    def _load_checkpoint(self) -> dict:
        try:
            with open(self._checkpoint_path, encoding="utf-8") as fh:
                ck = json.load(fh)
            ck.setdefault("done_days", [])
            ck.setdefault("empty_years", [])
            ck.setdefault("empty_months", [])
            ck["done_days"] = set(ck["done_days"])
            ck["empty_years"] = set(ck["empty_years"])
            ck["empty_months"] = set(ck["empty_months"])
            logger.info(
                f"Resuming from checkpoint: {len(ck['done_days'])} days already crawled, "
                f"{len(ck['empty_years'])} empty years, {len(ck['empty_months'])} empty months"
            )
            return ck
        except FileNotFoundError:
            return {"done_days": set(), "empty_years": set(), "empty_months": set()}
        except Exception as e:
            logger.warning(f"Unreadable checkpoint ({e}); starting fresh")
            return {"done_days": set(), "empty_years": set(), "empty_months": set()}

    def _save_checkpoint(self, ck: dict):
        path = self._checkpoint_path
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(
                {
                    "done_days": sorted(ck["done_days"]),
                    "empty_years": sorted(ck["empty_years"]),
                    "empty_months": sorted(ck["empty_months"]),
                    "dirty_swept": bool(ck.get("dirty_swept")),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
                fh,
            )
        tmp.replace(path)

    # ── Crawl ─────────────────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield every Yargıtay decision the Bedesten API exposes.

        The API has no wildcard and no bulk endpoint, so the corpus is
        enumerated by walking decision-date windows newest-first and
        paginating each day to exhaustion. Year and month totals are probed
        first so empty stretches cost one request instead of 365.

        Only search metadata is yielded here; the per-document full-text
        download happens in normalize(), which bootstrap_fast runs on worker
        threads, so the expensive half of the crawl is parallelised.

        Completed days are checkpointed to data/crawl_checkpoint.json, so a
        re-launched run skips them with no network calls and advances
        monotonically through the ~10M-decision backlog across fleet slots.
        """
        ck = self._load_checkpoint()
        today = date.today()

        # Decisions carrying out-of-range dates (the API holds records stamped
        # e.g. 6006-09-20) can never appear in a sane date window, so sweep
        # them separately before the main walk.
        yield from self._sweep_dirty_dates(ck)

        days_done = 0
        for year in range(today.year, self.MIN_YEAR - 1, -1):
            if str(year) in ck["empty_years"]:
                continue
            if not self._window_total(f"{year}-01-01", f"{year}-12-31"):
                logger.info(f"Year {year}: no decisions, skipping")
                ck["empty_years"].add(str(year))
                self._save_checkpoint(ck)
                continue

            for month in range(12, 0, -1):
                if year == today.year and month > today.month:
                    continue
                mkey = f"{year}-{month:02d}"
                if mkey in ck["empty_months"]:
                    continue
                last = self._last_day_of_month(year, month)
                phrases = self._phrases_for_window(f"{mkey}-01", f"{mkey}-{last:02d}", mkey)
                if not phrases:
                    ck["empty_months"].add(mkey)
                    self._save_checkpoint(ck)
                    continue

                for dom in range(last, 0, -1):
                    dkey = f"{mkey}-{dom:02d}"
                    if dkey in ck["done_days"]:
                        continue
                    if date(year, month, dom) > today:
                        continue

                    yield from self._crawl_day(dkey, phrases)

                    ck["done_days"].add(dkey)
                    days_done += 1
                    if days_done % 10 == 0:
                        self._save_checkpoint(ck)

                self._save_checkpoint(ck)

        self._save_checkpoint(ck)
        logger.info(f"Full crawl finished: {len(ck['done_days'])} day-windows crawled")

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        """
        Yield decisions whose decision date falls on or after `since`.

        `since` may be a datetime, date or string — the fleet passes a
        datetime, so it is normalised rather than assumed.
        """
        start = as_date_str(since)
        logger.info(f"Fetching updates since {start}")

        start_d = datetime.strptime(start, "%Y-%m-%d").date()
        today = date.today()

        day = today
        while day >= start_d:
            yield from self._crawl_day(day.isoformat())
            day -= timedelta(days=1)

    def _phrases_for_window(self, start: str, end: str, label: str) -> list:
        """
        Decide which phrases are needed to cover a window, and confirm it holds.

        The probe phrases are compared once per month rather than per day: the
        API throttles aggressively, so a redundant request per day costs far
        more than the check is worth. If the totals agree, one phrase covers
        the window and the days below it are crawled under that phrase alone.
        If they disagree, neither term is universal here and the days are
        crawled under both and de-duplicated.

        Returns [] when the window holds no decisions at all.
        """
        totals = {p: self._window_total(start, end, p) or 0 for p in self.PROBE_PHRASES}
        best = max(totals, key=lambda p: totals[p])
        if not totals[best]:
            return []
        if len(set(totals.values())) > 1:
            logger.warning(
                f"{label}: probe phrases disagree {totals} — crawling union"
            )
            return sorted(totals, key=lambda p: totals[p], reverse=True)
        return [best]

    def _crawl_day(self, day: str, phrases: list = None) -> Generator[dict, None, None]:
        """
        Paginate one decision-date window to exhaustion.

        Page 1 carries both the window total and the first batch of results,
        so an empty day costs a single request and no separate count probe.
        """
        phrases = phrases or [self.PROBE_PHRASES[0]]
        seen = set()
        reported = 0

        for phrase in phrases:
            page = 1
            collected = 0
            expected = None
            while True:
                total, decisions = self._search_page(phrase, day, page)
                if expected is None:
                    expected = total or 0
                    reported = max(reported, expected)
                    if not expected:
                        break
                if not decisions:
                    break
                for decision in decisions:
                    doc_id = decision.get("documentId")
                    if not doc_id or doc_id in seen:
                        continue
                    seen.add(doc_id)
                    yield decision
                collected += len(decisions)
                if len(decisions) < self.PAGE_SIZE or collected >= expected:
                    break
                page += 1
                if page > (expected // self.PAGE_SIZE) + 5:
                    break

            if expected and collected < expected:
                # Surface under-collection instead of silently truncating.
                logger.warning(
                    f"{day} phrase '{phrase}': collected {collected} of {expected} reported"
                )
                self.record_coverage_gap(
                    day, "incomplete_pagination", collected=collected, expected=expected
                )

        if seen:
            logger.info(f"{day}: {len(seen)} decisions (reported {reported})")

    def _sweep_dirty_dates(self, ck: dict) -> Generator[dict, None, None]:
        """
        Collect decisions stamped with dates outside any window we walk.

        Sorted descending with no date filter, corrupt future dates sort
        first; stop as soon as the results reach the present.
        """
        if ck.get("dirty_swept"):
            return
        logger.info("Sweeping decisions with out-of-range decision dates...")
        cutoff = date.today().year
        count = 0
        for page in range(1, self.MAX_DIRTY_PAGES + 1):
            _, decisions = self._search_page(self.PROBE_PHRASES[0], None, page)
            if not decisions:
                break
            reached_present = False
            for decision in decisions:
                parsed = self._parse_decision_date(decision.get("kararTarihi", ""))
                year = int(parsed[:4]) if parsed else None
                if year and year <= cutoff:
                    reached_present = True
                    continue
                count += 1
                yield decision
            if reached_present:
                break
        logger.info(f"Out-of-range date sweep: {count} decisions")
        ck["dirty_swept"] = True
        self._save_checkpoint(ck)

    @staticmethod
    def _last_day_of_month(year: int, month: int) -> int:
        if month == 12:
            return 31
        return (date(year, month + 1, 1) - timedelta(days=1)).day

    def _window_total(self, start: str, end: str, phrase: str = None) -> int:
        """Report how many decisions a date window holds (1 request)."""
        payload = self._search_payload(phrase or self.PROBE_PHRASES[0], start, end, page=1, page_size=1)
        data = self._api_post(self.SEARCH_ENDPOINT, payload).get("data") or {}
        return data.get("total") or 0

    def _search_page(self, phrase: str, day: Optional[str], page: int) -> tuple:
        """
        Return (total, decisions) for one page of a day window (or unfiltered).

        The total rides along with every page, which is what lets an empty day
        cost one request instead of a probe plus a fetch.
        """
        payload = self._search_payload(phrase, day, day, page=page, page_size=self.PAGE_SIZE)
        data = self._api_post(self.SEARCH_ENDPOINT, payload).get("data") or {}
        return data.get("total") or 0, data.get("emsalKararList") or []

    def _search_payload(
        self, phrase: str, start: Optional[str], end: Optional[str], page: int, page_size: int
    ) -> dict:
        payload = {
            "data": {
                "pageSize": page_size,
                "pageNumber": page,
                "itemTypeList": ["YARGITAYKARARI"],
                "phrase": phrase,
                "sortFields": ["KARAR_TARIHI"],
                "sortDirection": "desc",
            },
            "applicationName": "UyapMevzuat",
            "paging": True,
        }
        if start:
            payload["data"]["kararTarihiStart"] = f"{start}T00:00:00.000Z"
        if end:
            payload["data"]["kararTarihiEnd"] = f"{end}T23:59:59.999Z"
        return payload

    def _api_post(self, endpoint: str, payload: dict, attempts: int = 6) -> dict:
        """
        POST to the Bedesten API, backing off on throttling.

        The API answers a throttled request with HTTP 429 and a plain-text
        "Too Many Requests" body, which json() cannot parse. The previous
        implementation caught that as a generic exception and broke out of
        pagination, silently truncating the corpus — so retries are honoured
        here and exhaustion raises rather than returning empty.
        """
        last_error = None
        for attempt in range(attempts):
            try:
                self.rate_limiter.wait()
                resp = self.client.post(endpoint, json_data=payload)

                if resp.status_code in self.RETRY_STATUSES:
                    delay = self._retry_delay(resp, attempt)
                    logger.warning(
                        f"HTTP {resp.status_code} from Bedesten; retrying in {delay:.1f}s "
                        f"(attempt {attempt + 1}/{attempts})"
                    )
                    if resp.status_code == 429:
                        self.rate_limiter.record_429(delay)
                    time.sleep(delay)
                    last_error = f"HTTP {resp.status_code}"
                    continue

                resp.raise_for_status()
                parsed = resp.json()
                self.rate_limiter.record_success()
                return parsed

            except Exception as e:
                last_error = e
                delay = min(120, (2 ** attempt) * 2) + random.uniform(0, 1)
                logger.warning(
                    f"Bedesten request failed ({e}); retrying in {delay:.1f}s "
                    f"(attempt {attempt + 1}/{attempts})"
                )
                time.sleep(delay)

        raise RuntimeError(
            f"Bedesten API unreachable after {attempts} attempts: {last_error}"
        )

    @staticmethod
    def _retry_delay(resp, attempt: int) -> float:
        retry_after = resp.headers.get("Retry-After")
        if retry_after:
            try:
                return min(120.0, float(retry_after))
            except ValueError:
                pass
        return min(120.0, (2 ** attempt) * 2) + random.uniform(0, 1)

    def _fetch_document_content(self, document_id: str) -> Optional[str]:
        """
        Fetch full document content from the API.
        Returns cleaned text extracted from the HTML content.
        """
        try:
            payload = {
                "data": {"documentId": document_id},
                "applicationName": "UyapMevzuat",
            }

            data = self._api_post(self.DOCUMENT_ENDPOINT, payload)

            if not data.get("data") or not data["data"].get("content"):
                return None

            content_b64 = data["data"]["content"]
            mime_type = data["data"].get("mimeType", "text/html")

            # Decode base64 content
            content_bytes = base64.b64decode(content_b64)

            if mime_type == "text/html":
                html_content = content_bytes.decode("utf-8")
                return self._clean_html(html_content)
            elif mime_type == "application/pdf":
                # PDF extraction would require additional libraries
                logger.warning(f"PDF content for {document_id} - skipping")
                return None
            else:
                return content_bytes.decode("utf-8", errors="ignore")

        except Exception as e:
            logger.error(f"Error fetching document {document_id}: {e}")
            return None

    def _clean_html(self, html_content: str) -> str:
        """Clean HTML content to plain text."""
        if not html_content:
            return ""

        # Decode HTML entities
        text = html.unescape(html_content)

        # Remove script and style elements
        text = re.sub(r"<script[^>]*>.*?</script>", "", text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)

        # Replace common block elements with newlines
        text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"</?(?:p|div|tr|li|h[1-6])[^>]*>", "\n", text, flags=re.IGNORECASE)
        text = re.sub(r"</?(?:td|th)[^>]*>", " ", text, flags=re.IGNORECASE)

        # Remove all remaining HTML tags
        text = re.sub(r"<[^>]+>", "", text)

        # Clean up whitespace
        text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
        text = re.sub(r" +", " ", text)
        text = re.sub(r"^\s+", "", text, flags=re.MULTILINE)

        return text.strip()

    def _parse_decision_date(self, date_str: str) -> Optional[str]:
        """
        Parse decision date from API format.
        API returns ISO format like "2026-01-21T21:00:00.000+00:00"
        Returns date in YYYY-MM-DD format.

        The API stores decision dates as the Turkish local midnight expressed
        in UTC, i.e. 21:00Z (UTC+3) or 22:00Z for pre-2016 decisions under
        UTC+2. Reading the UTC date directly therefore reports every decision
        one day early, so shift into Turkish time before taking the date.
        """
        if not date_str:
            return None

        try:
            # Parse ISO format
            if "T" in date_str:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                return (dt + timedelta(hours=3)).strftime("%Y-%m-%d")

            # Try other formats
            for fmt in ["%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"]:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    continue

        except Exception:
            pass

        return None

    def _classify_chamber(self, chamber_name: str) -> dict:
        """Classify chamber as civil, criminal, or general council."""
        if not chamber_name:
            return {"division_type": "unknown", "division_number": None}

        # Civil chamber (Hukuk Dairesi)
        hukuk_match = re.search(r"(\d+)\.\s*Hukuk\s*Dairesi", chamber_name, re.IGNORECASE)
        if hukuk_match:
            return {"division_type": "civil", "division_number": int(hukuk_match.group(1))}

        # Criminal chamber (Ceza Dairesi)
        ceza_match = re.search(r"(\d+)\.\s*Ceza\s*Dairesi", chamber_name, re.IGNORECASE)
        if ceza_match:
            return {"division_type": "criminal", "division_number": int(ceza_match.group(1))}

        # General councils
        if "Hukuk Genel Kurulu" in chamber_name:
            return {"division_type": "civil_general_council", "division_number": None}
        if "Ceza Genel Kurulu" in chamber_name:
            return {"division_type": "criminal_general_council", "division_number": None}
        if "Büyük Genel Kurul" in chamber_name:
            return {"division_type": "grand_general_council", "division_number": None}

        return {"division_type": "other", "division_number": None}

    def normalize(self, raw: dict) -> dict:
        """
        Transform a raw API document into the standard schema.

        CRITICAL: Includes FULL TEXT from HTML documents.

        fetch_all() yields search metadata only and the full text is
        downloaded here, because bootstrap_fast runs normalize() on worker
        threads — so the one-request-per-document half of the crawl runs
        concurrently instead of serialising behind pagination.
        """
        doc_id = raw.get("documentId", "")
        chamber_name = raw.get("birimAdi", "")

        full_text = raw.get("full_text") or ""
        if not full_text and doc_id:
            full_text = self._fetch_document_content(doc_id) or ""
        if not full_text:
            # No body, no record — a metadata-only row is worthless downstream.
            logger.warning(f"No full text for {doc_id}; skipping")
            return None

        # Parse dates
        decision_date = self._parse_decision_date(raw.get("kararTarihi", ""))
        decision_date_str = raw.get("kararTarihiStr", "")

        # Build case/decision numbers
        case_number = raw.get("esasNo", "")
        decision_number = raw.get("kararNo", "")

        # Build title
        title_parts = []
        if chamber_name:
            title_parts.append(chamber_name)
        if case_number:
            title_parts.append(f"E. {case_number}")
        if decision_number:
            title_parts.append(f"K. {decision_number}")

        title = " - ".join(title_parts) if title_parts else f"Yargıtay Kararı {doc_id}"

        # Classify chamber
        chamber_info = self._classify_chamber(chamber_name)

        # Extract years from case/decision numbers
        esas_yil = raw.get("esasNoYil")
        karar_yil = raw.get("kararNoYil")

        return {
            "_id": f"TR/Yargitay/{doc_id}",
            "_source": "TR/Yargitay",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),

            # Standard required fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": decision_date,
            "url": f"https://mevzuat.adalet.gov.tr/ictihat/{doc_id}",

            # Source-specific fields
            "document_id": doc_id,
            "chamber": chamber_name,
            "division_type": chamber_info["division_type"],
            "division_number": chamber_info["division_number"],
            "case_number": case_number,
            "decision_number": decision_number,
            "case_year": esas_yil,
            "decision_year": karar_yil,
            "decision_date_display": decision_date_str,

            # Keep raw metadata
            "_raw_metadata": {
                "documentId": doc_id,
                "birimAdi": chamber_name,
                "esasNo": case_number,
                "kararNo": decision_number,
                "kararTarihi": raw.get("kararTarihi"),
            },
        }


# ── CLI Entry Point ───────────────────────────────────────────────

def main():
    scraper = TurkishCourtOfCassationScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|sample] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, {stats['records_updated']} updated, {stats['records_skipped']} skipped")
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
    elif command == "sample":
        # Direct sample mode
        stats = scraper.run_sample(n=sample_size)
        print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
