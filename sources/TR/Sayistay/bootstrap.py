"""
Legal Data Hunter - Turkish Court of Accounts (Sayıştay) Scraper

Fetches case law from all three decision bodies of Sayıştay:
  - Daire (Chamber decisions) — ~22K decisions
  - Temyiz Kurulu (Appeals Board) — ~29K decisions
  - Genel Kurul (General Assembly) — ~1K decisions
Method: DataTables server-side API + HTML detail page scraping
Coverage: All published decisions with full text
"""

import re
import sys
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
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
logger = logging.getLogger("TR/Sayistay")

BASE_URL = "https://www.sayistay.gov.tr"

# Decision body configurations
DECISION_BODIES = {
    "daire": {
        "name": "Daire",
        "label": "Sayıştay Dairesi",
        "page_url": "/KararlarDaire",
        "list_url": "/KararlarDaire/DataTablesList",
        "detail_url": "/KararlarDaire/Detay/{id}/",
        "form_prefix": "KararlarDaireAra",
        "form_fields": [
            "YARGILAMADAIRESI", "KARARTRHBaslangic", "KARARTRHBitis",
            "ILAMNO", "KAMUIDARESITURU", "HESAPYILI",
            "WEBKARARKONUSU", "WEBKARARMETNI",
        ],
        "columns": [
            ("YARGILAMADAIRESI", True),
            ("KARARTRH", True),
            ("KARARNO", True),
            ("YARGILAMADAIRESI", True),
            ("WEBKARARMETNI", False),
            ("", False),
        ],
        "date_field": "KARARTRH",
        "order_column": 1,
        # Partition the full-corpus scan by fiscal year (HESAPYILI). Each year
        # holds at most ~900 records, so offset pagination stays shallow (≤10
        # pages) instead of 223 deep pages. The Sayıştay DataTables endpoint is
        # session-backed and, on datacenter IPs, intermittently drops the
        # session/CSRF cookie and ignores deep `start` offsets — repeating page
        # 1 forever (issue #962). Shallow per-year offsets are far less likely
        # to be ignored, and missing a dropped session only costs one small
        # year. Verified: per-year HESAPYILI totals sum to exactly recordsTotal.
        "year_field": "HESAPYILI",
    },
    "temyiz": {
        "name": "Temyiz",
        "label": "Temyiz Kurulu",
        "page_url": "/KararlarTemyiz",
        "list_url": "/KararlarTemyiz/DataTablesList",
        "detail_url": "/KararlarTemyiz/Detay/{id}/",
        "form_prefix": "KararlarTemyizAra",
        "form_fields": [
            "ILAMDAIRESI", "YILI", "KARARTRHBaslangic", "KARARTRHBitis",
            "KAMUIDARESITURU", "ILAMNO", "DOSYANO",
            "TEMYIZTUTANAKNO", "TEMYIZKARAR", "WEBKARARKONUSU",
        ],
        "columns": [
            ("TEMYIZTUTANAKTARIHI", False),
            ("TEMYIZTUTANAKTARIHI", True),
            ("ILAMDAIRESI", True),
            ("TEMYIZKARAR", False),
            ("", False),
        ],
        "date_field": "TEMYIZTUTANAKTARIHI",
        "order_column": 1,
        # Partition by decision year (YILI); per-year totals sum to exactly
        # recordsTotal. Max ~2,800 records/year → ≤29 shallow pages. See the
        # HESAPYILI note above for why this matters (issue #962).
        "year_field": "YILI",
    },
    "genel_kurul": {
        "name": "GenelKurul",
        "label": "Genel Kurul",
        "page_url": "/KararlarGenelKurul",
        "list_url": "/KararlarGenelKurul/DataTablesList",
        "detail_url": "/KararlarGenelKurul/Detay/{id}/",
        "form_prefix": "KararlarGenelKurulAra",
        "form_fields": [
            "KARARNO", "KARAREK", "KARARTARIHBaslangic",
            "KARARTARIHBitis", "KARARTAMAMI",
        ],
        "columns": [
            ("KARARNO", False),
            ("KARARNO", True),
            ("KARARTARIH", True),
            ("KARAROZETI", False),
            ("", False),
        ],
        "date_field": "KARARTARIH",
        "order_column": 2,
        # General Assembly holds only a handful of decisions — no partition
        # needed (a single shallow scan never reaches deep offsets).
        "year_field": None,
    },
}


class SayistayScraper(BaseScraper):
    """
    Scraper for: Turkish Court of Accounts (Sayıştay)
    Country: TR
    URL: https://www.sayistay.gov.tr

    Data types: case_law
    Auth: none

    Uses DataTables server-side processing API to list decisions,
    then fetches individual detail pages for full text.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept-Language": "tr-TR,tr;q=0.9,en;q=0.8",
            },
        )
        # Cache for CSRF tokens per decision body
        self._csrf_tokens = {}
        self._session_cookies = {}

    def _get_csrf_token(self, body_key: str) -> str:
        """Get a CSRF token for a decision body by loading its page."""
        body = DECISION_BODIES[body_key]
        page_url = body["page_url"]

        self.rate_limiter.wait()
        resp = self.client.get(page_url)
        html = resp.text

        # Extract anti-forgery token
        m = re.search(
            r'name="__RequestVerificationToken"\s+type="hidden"\s+value="([^"]+)"',
            html,
        )
        if not m:
            raise ValueError(f"Could not find CSRF token on {page_url}")

        token = m.group(1)

        # Store cookies from the response
        if hasattr(resp, "cookies"):
            self._session_cookies[body_key] = dict(resp.cookies)

        logger.info(f"Got CSRF token for {body['name']}")
        return token

    def _build_datatables_request(
        self, body_key: str, start: int, length: int,
        extra_filters: Optional[dict] = None,
    ) -> dict:
        """Build the DataTables POST parameters for a decision body.

        extra_filters maps form-field names (without the form prefix) to values,
        e.g. {"HESAPYILI": "2020"} to restrict the result set to a single year.
        """
        body = DECISION_BODIES[body_key]

        if body_key not in self._csrf_tokens:
            self._csrf_tokens[body_key] = self._get_csrf_token(body_key)

        data = {
            "__RequestVerificationToken": self._csrf_tokens[body_key],
            "draw": "1",
            "start": str(start),
            "length": str(length),
            f"order[0][column]": str(body["order_column"]),
            "order[0][dir]": "desc",
            "search[value]": "",
            "search[regex]": "false",
        }

        # Add form fields (empty = no filter)
        prefix = body["form_prefix"]
        extra_filters = extra_filters or {}
        for field in body["form_fields"]:
            data[f"{prefix}[{field}]"] = str(extra_filters.get(field, ""))

        # Add column definitions
        for i, (col_data, orderable) in enumerate(body["columns"]):
            data[f"columns[{i}][data]"] = col_data
            data[f"columns[{i}][name]"] = ""
            data[f"columns[{i}][searchable]"] = "true"
            data[f"columns[{i}][orderable]"] = "true" if orderable else "false"
            data[f"columns[{i}][search][value]"] = ""
            data[f"columns[{i}][search][regex]"] = "false"

        return data

    def _fetch_list_page(
        self, body_key: str, start: int, length: int = 100,
        extra_filters: Optional[dict] = None,
    ) -> dict:
        """Fetch a page of decisions from the DataTables API."""
        body = DECISION_BODIES[body_key]
        data = self._build_datatables_request(body_key, start, length, extra_filters)

        self.rate_limiter.wait()
        resp = self.client.post(
            body["list_url"],
            data=data,
            headers={
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Referer": f"{BASE_URL}{body['page_url']}",
                "Origin": BASE_URL,
            },
        )

        if resp.status_code == 500:
            # CSRF token may have expired — refresh and retry once
            logger.warning(f"Got 500 from {body['name']} list, refreshing CSRF token")
            self._csrf_tokens[body_key] = self._get_csrf_token(body_key)
            data = self._build_datatables_request(body_key, start, length, extra_filters)
            self.rate_limiter.wait()
            resp = self.client.post(
                body["list_url"],
                data=data,
                headers={
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "X-Requested-With": "XMLHttpRequest",
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "Referer": f"{BASE_URL}{body['page_url']}",
                    "Origin": BASE_URL,
                },
            )

        if resp.status_code != 200:
            raise RuntimeError(
                f"DataTables API returned {resp.status_code} for {body['name']}"
            )

        ct = resp.headers.get("Content-Type", "")
        if "json" not in ct:
            raise RuntimeError(
                f"Expected JSON from {body['name']} but got {ct}"
            )

        return resp.json()

    def _fetch_detail_text(self, body_key: str, record_id: int) -> str:
        """Fetch the full decision text from a detail page."""
        body = DECISION_BODIES[body_key]
        url = body["detail_url"].format(id=record_id)

        self.rate_limiter.wait()
        resp = self.client.get(url)

        if resp.status_code != 200:
            logger.warning(f"Detail page {url} returned {resp.status_code}")
            return ""

        html = resp.text

        # Find the text div (id="metin")
        idx = html.find('id="metin"')
        if idx < 0:
            logger.warning(f"No metin div found for {body['name']} ID {record_id}")
            return ""

        start = html.find(">", idx) + 1
        depth = 1
        pos = start
        while depth > 0 and pos < len(html):
            open_tag = html.find("<div", pos)
            close_tag = html.find("</div>", pos)
            if close_tag == -1:
                break
            if open_tag != -1 and open_tag < close_tag:
                depth += 1
                pos = open_tag + 4
            else:
                depth -= 1
                if depth == 0:
                    content = html[start:close_tag]
                    break
                pos = close_tag + 6
        else:
            content = html[start:start + 50000]

        # Clean HTML
        text = re.sub(r"<br\s*/?>", "\n", content)
        text = re.sub(r"<[^>]+>", " ", text)
        text = unescape(text)
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
        text = text.strip()

        return text

    def _extract_metadata(self, body_key: str, detail_html: str) -> dict:
        """Extract metadata fields from a detail page."""
        meta = {}
        # Look for label-value pairs in col-3/col-9 structure
        pairs = re.findall(
            r'class="col-3[^"]*">\s*<b>([^<]+)</b>\s*</div>\s*<div\s+class="col-9[^"]*">\s*([^<]+)',
            detail_html,
        )
        for label, value in pairs:
            label = unescape(label).strip()
            value = unescape(value).strip()
            if label == "Daire":
                meta["chamber"] = value
            elif label == "Karar Tarihi":
                meta["decision_date"] = value
            elif label in ("Karar No", "Karar Numarası"):
                meta["decision_number"] = value
            elif label == "İlam No":
                meta["ilam_number"] = value
            elif label == "Konu":
                meta["subject"] = value
            elif label == "Hesap Yılı":
                meta["fiscal_year"] = value
            elif label == "Kamu İdaresi Türü":
                meta["institution_type"] = value
            elif label == "Tutanak Tarihi":
                meta["decision_date"] = value
        return meta

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse Turkish date format (dd.MM.yyyy) to ISO 8601."""
        if not date_str:
            return None
        date_str = date_str.strip()
        for fmt in ("%d.%m.%Y", "%d/%m/%Y", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(date_str, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _process_records(
        self, body_key: str, records: list, new_ids: set,
    ) -> Generator[dict, None, None]:
        """Fetch detail text for each new record in a list page and yield raw."""
        body = DECISION_BODIES[body_key]
        for rec in records:
            rec_id = rec.get("Id")
            if not rec_id or rec_id not in new_ids:
                continue
            try:
                full_text = self._fetch_detail_text(body_key, rec_id)
                if not full_text or len(full_text) < 50:
                    logger.warning(
                        f"Insufficient text for {body['name']} ID {rec_id} "
                        f"({len(full_text)} chars), skipping"
                    )
                    continue
                raw = {
                    "_body_key": body_key,
                    "id": rec_id,
                    "full_text": full_text,
                    "date_field": rec.get(body["date_field"], ""),
                }
                raw.update(rec)
                yield raw
            except Exception as e:
                logger.warning(f"Failed to process {body['name']} ID {rec_id}: {e}")

    def _fetch_partition(
        self, body_key: str, extra_filters: Optional[dict], seen_ids: set,
        global_total: Optional[int] = None,
    ) -> Generator[dict, None, None]:
        """Paginate one partition (e.g. a single year) with shallow offsets.

        The Sayıştay DataTables endpoint is session-backed and, on datacenter
        IPs, intermittently drops the session/CSRF cookie and (a) ignores the
        `start` offset — silently repeating page 1 — and (b) ignores the year
        filter, so a single-year partition reports the FULL corpus total and
        paginates hundreds of deep pages re-fetching duplicates (issue #962,
        #1060: "49.5K fetched, 100 new"). Guards:
          1. `seen_ids` (shared across the whole body) drops any repeated rows.
          2. Filter-ignored detection: if a year-filtered partition's
             recordsTotal equals the global unfiltered total, the session was
             dropped and the filter didn't apply — refresh the session and
             retry; if it stays ignored, skip the partition instead of
             paginating the whole corpus as duplicates.
          3. When a page yields 0 new IDs, refresh the session and retry the
             SAME offset a couple of times before giving up on the partition —
             a fresh session usually restores correct offset handling.
        Partitioning keeps offsets shallow so the server rarely ignores them.
        """
        body = DECISION_BODIES[body_key]
        page_size = 100
        start = 0
        total = None
        refresh_attempts = 0
        filter_refresh = 0
        MAX_REFRESH = 2
        is_filtered = bool(extra_filters)

        while True:
            try:
                result = self._fetch_list_page(body_key, start, page_size, extra_filters)
            except Exception as e:
                logger.error(
                    f"Failed to fetch {body['name']} page at offset {start} "
                    f"({extra_filters}): {e}"
                )
                break

            if total is None:
                total = result.get("recordsTotal", 0)
                # Detect a dropped session that silently ignored the year
                # filter: the single-year total should be far below the global
                # corpus total. If it matches the global total, the filter was
                # ignored — refresh and retry, then skip rather than paginate
                # the whole corpus (re-fetching duplicates).
                if is_filtered and global_total and total >= global_total:
                    if filter_refresh < MAX_REFRESH:
                        filter_refresh += 1
                        logger.warning(
                            f"{body['name']} {extra_filters}: recordsTotal "
                            f"{total} == global {global_total} (year filter "
                            f"ignored — dropped session); refreshing and "
                            f"retrying ({filter_refresh}/{MAX_REFRESH})"
                        )
                        self._csrf_tokens.pop(body_key, None)
                        self._csrf_tokens[body_key] = self._get_csrf_token(body_key)
                        total = None
                        continue
                    logger.warning(
                        f"{body['name']} {extra_filters}: year filter still "
                        f"ignored after {MAX_REFRESH} refreshes — skipping "
                        f"partition to avoid duplicate re-fetch."
                    )
                    return

            records = result.get("data", [])
            if not records:
                break

            page_ids = [r.get("Id") for r in records if r.get("Id")]
            new_ids = {i for i in page_ids if i not in seen_ids}
            if not new_ids:
                if refresh_attempts < MAX_REFRESH:
                    refresh_attempts += 1
                    logger.warning(
                        f"{body['name']} {extra_filters}: offset {start} returned "
                        f"0 new IDs — refreshing session and retrying "
                        f"({refresh_attempts}/{MAX_REFRESH})"
                    )
                    self._csrf_tokens.pop(body_key, None)
                    self._csrf_tokens[body_key] = self._get_csrf_token(body_key)
                    continue
                logger.warning(
                    f"{body['name']} {extra_filters}: offset {start} still "
                    f"repeating after {MAX_REFRESH} refreshes — stopping partition."
                )
                break

            refresh_attempts = 0
            seen_ids.update(new_ids)
            yield from self._process_records(body_key, records, new_ids)

            start += page_size
            if start >= (total or 0):
                break

    def _fetch_body_decisions(
        self, body_key: str, sample: bool = False
    ) -> Generator[dict, None, None]:
        """Fetch all decisions for a decision body, partitioned by year.

        Partitioning by year (HESAPYILI / YILI) keeps offset pagination shallow,
        which is what makes the scan robust to the session-drop / ignored-offset
        behaviour seen on datacenter IPs (issue #962). `seen_ids` is shared
        across all partitions so any cross-year duplicates are filtered too.
        """
        body = DECISION_BODIES[body_key]
        year_field = body.get("year_field")
        max_records = 6 if sample else None
        fetched = 0
        seen_ids = set()

        logger.info(f"Starting {body['name']} decision fetch...")

        global_total = None
        if year_field:
            current_year = datetime.now(timezone.utc).year
            partitions = [
                {year_field: str(y)} for y in range(current_year + 1, 1989, -1)
            ]
            # Capture the global unfiltered total once so each year partition
            # can detect a dropped session that silently ignored its filter
            # (issue #1060). Best-effort: if this probe fails, the per-partition
            # check is simply skipped.
            try:
                probe = self._fetch_list_page(body_key, 0, 1, {})
                global_total = probe.get("recordsTotal") or None
                logger.info(f"{body['name']} global total: {global_total}")
            except Exception as e:
                logger.warning(f"Could not probe global total for {body['name']}: {e}")
        else:
            partitions = [None]

        for extra_filters in partitions:
            for raw in self._fetch_partition(
                body_key, extra_filters, seen_ids, global_total
            ):
                yield raw
                fetched += 1
                if max_records and fetched >= max_records:
                    logger.info(
                        f"Sample limit reached for {body['name']} ({fetched} records)"
                    )
                    return

        logger.info(
            f"Fetched {fetched} {body['name']} decisions "
            f"({len(seen_ids)} unique list rows seen)"
        )

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all decisions from all three decision bodies."""
        for body_key in DECISION_BODIES:
            try:
                yield from self._fetch_body_decisions(body_key)
            except Exception as e:
                logger.error(f"Failed to fetch {body_key}: {e}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield decisions published since the given datetime."""
        logger.info(f"Fetching updates since {since.isoformat()}")
        for doc in self.fetch_all():
            date_str = self._parse_date(doc.get("date_field", ""))
            if date_str:
                try:
                    doc_date = datetime.strptime(date_str, "%Y-%m-%d").replace(
                        tzinfo=timezone.utc
                    )
                    if doc_date >= since:
                        yield doc
                    else:
                        break
                except ValueError:
                    yield doc
            else:
                yield doc

    def normalize(self, raw: dict) -> dict:
        """Transform a raw record into the standard schema."""
        body_key = raw.get("_body_key", "daire")
        body = DECISION_BODIES[body_key]
        rec_id = raw.get("Id") or raw.get("id")
        full_text = raw.get("full_text", "")

        # Parse date
        date_raw = raw.get("date_field", "")
        date_iso = self._parse_date(date_raw)

        # Build title from available fields
        subject = raw.get("WEBKARARKONUSU", "")
        decision_no = (
            raw.get("KARARNO", "") or raw.get("KARARNO", "")
        )
        if isinstance(decision_no, str):
            decision_no = decision_no.strip()

        if subject:
            title = f"Sayıştay {body['label']} Kararı — {subject}"
        else:
            # Use first line of text as title
            first_line = full_text[:200].split("\n")[0].strip()
            title = f"Sayıştay {body['label']} — {first_line}"

        if len(title) > 250:
            title = title[:247] + "..."

        detail_url = f"{BASE_URL}{body['detail_url'].format(id=rec_id)}"

        # Chamber info
        chamber = None
        chamber_raw = raw.get("YARGILAMADAIRESI") or raw.get("ILAMDAIRESI")
        if chamber_raw:
            chamber = f"{chamber_raw}. Daire"

        return {
            "_id": f"TR/Sayistay/{body['name']}-{rec_id}",
            "_source": "TR/Sayistay",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": date_iso,
            "url": detail_url,
            "decision_body": body["label"],
            "chamber": chamber,
            "decision_number": decision_no if decision_no else None,
            "ilam_number": str(raw.get("ILAMNO", "")).strip() or None,
            "subject": subject or None,
            "institution_type": raw.get("KAMUIDARESITURU"),
            "fiscal_year": raw.get("HESAPYILI"),
        }


# ── CLI entry point ─────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="TR/Sayistay scraper")
    parser.add_argument("command", choices=["bootstrap", "update"])
    parser.add_argument("--sample", action="store_true", help="Fetch only sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = SayistayScraper()

    if args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0

        if args.sample:
            # In sample mode, fetch a few from each body
            for body_key in DECISION_BODIES:
                try:
                    for raw in scraper._fetch_body_decisions(body_key, sample=True):
                        normalized = scraper.normalize(raw)
                        out_path = sample_dir / f"{normalized['_id'].replace('/', '_')}.json"
                        with open(out_path, "w", encoding="utf-8") as f:
                            json.dump(normalized, f, ensure_ascii=False, indent=2)
                        count += 1
                        text_len = len(normalized.get("text", ""))
                        logger.info(
                            f"[{count}] {normalized['_id']} — "
                            f"{text_len} chars — {normalized.get('date', 'no date')}"
                        )
                except Exception as e:
                    logger.error(f"Failed {body_key}: {e}")
        else:
            for raw in scraper.fetch_all():
                normalized = scraper.normalize(raw)
                if args.full:
                    out_path = sample_dir / f"{normalized['_id'].replace('/', '_')}.json"
                    with open(out_path, "w", encoding="utf-8") as f:
                        json.dump(normalized, f, ensure_ascii=False, indent=2)
                count += 1
                if count % 100 == 0:
                    logger.info(f"Processed {count} records...")

        logger.info(f"Bootstrap complete: {count} records")
    elif args.command == "update":
        since = datetime.now(timezone.utc).replace(day=1)
        count = 0
        for raw in scraper.fetch_updates(since):
            normalized = scraper.normalize(raw)
            count += 1
        logger.info(f"Update complete: {count} new records")
