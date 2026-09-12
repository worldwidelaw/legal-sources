#!/usr/bin/env python3
"""
KR/CourtCLIS -- Korean Court Decisions via law.go.kr DRF API

Fetches Korean court precedents (판례) from the official law.go.kr DRF API.
~171K decisions from Supreme Court, lower courts, and specialized courts.
Full text including holdings, summaries, and complete opinions.

API endpoints:
  - GET /DRF/lawSearch.do?OC=test&target=prec&type=XML  (paginated listing)
  - GET /DRF/lawService.do?OC=test&target=prec&type=XML&ID=...  (full text)

Usage:
  python bootstrap.py bootstrap            # Full initial pull (~171K decisions)
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS/fleet)
  python bootstrap.py update               # Same as bootstrap (no date filter)
  python bootstrap.py test-api             # Quick connectivity test
"""

import sys
import json
import time
import logging
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KR.CourtCLIS")

BASE_URL = "https://www.law.go.kr/DRF"
OC = "test"
PAGE_SIZE = 100  # Max allowed by API

#: The listing is ordered 선고일자-descending, so a refresh walks it from page 1
#: and stops once it is this far into already-seen territory. Twenty pages of
#: known IDs, so a handful of re-ordered or withdrawn entries cannot end the
#: walk early — and twenty listing requests is a rounding error next to the
#: ~1,705-page full crawl.
STOP_AFTER_SEEN = 20 * PAGE_SIZE

#: law.go.kr publishes a decision well after it was handed down, so a refresh
#: keeps walking until the listing is this far behind `since` even when every ID
#: on the way is already known. Without the lag the walk would stop at the first
#: block of known IDs and never reach a decision published today but dated last
#: quarter.
PUBLICATION_LAG_DAYS = 400


class CourtCLISScraper(BaseScraper):
    """
    Scraper for KR/CourtCLIS -- Korean court decisions via law.go.kr DRF API.
    Country: KR
    URL: https://www.law.go.kr/
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url="",
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
                "Accept": "application/xml",
            },
            timeout=120,
        )
        # Checkpoint/resume: the prec target has ~170K precedents (totalCnt),
        # each needing a listing share + a detail fetch, so a full crawl can't
        # finish inside one 100h fleet slot. Persist the last fully-completed
        # listing page (deep-paging is verified stable + total order, so page
        # number is a sufficient resume cursor); on restart we skip already-done
        # pages with NO network calls and advance monotonically. See #1090.
        self._checkpoint_path = self.source_dir / "data" / "clis_checkpoint.json"
        self._done_page, self._seen_ids = self._load_checkpoint()

    def _load_checkpoint(self) -> tuple:
        """Return (highest listing page already fully fetched, seen 판례일련번호).

        The page cursor drives the full crawl's resume; the ID set is what the
        refresh compares against (#1502). Either may be absent — a checkpoint
        written before the ID set existed still resumes correctly.
        """
        try:
            with open(self._checkpoint_path, encoding="utf-8") as f:
                data = json.load(f)
            page = int(data.get("done_page", 0))
            seen = set(data.get("seen_ids") or [])
            if page:
                logger.info(f"Resuming from checkpoint: pages 1..{page} already done")
            if seen:
                logger.info(f"Checkpoint holds {len(seen)} known precedent IDs")
            return page, seen
        except (FileNotFoundError, json.JSONDecodeError, OSError, ValueError):
            return 0, set()

    def _save_checkpoint(self, done_page: int) -> None:
        """Persist the resume cursor and the seen-ID set (atomic).

        The write is atomic because a truncated checkpoint would re-yield the
        whole 170K corpus on the next refresh.
        """
        try:
            self._checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._checkpoint_path.with_suffix(".json.tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "done_page": done_page,
                        "seen_ids": sorted(self._seen_ids),
                        "count": len(self._seen_ids),
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    },
                    f,
                )
            tmp.replace(self._checkpoint_path)
        except OSError as e:
            logger.warning(f"Could not write checkpoint: {e}")

    @staticmethod
    def _listing_date(prec: dict) -> Optional[str]:
        """ISO 선고일자 from a listing row, or None for the 00010101 placeholder."""
        return CourtCLISScraper._parse_date(prec.get("선고일자", ""))

    def _search_with_retry(self, page: int, retries: int = 4):
        """Listing fetch that distinguishes a transient failure/empty from a
        genuine end-of-list: retry with backoff before trusting an empty page.
        Returns (total_count, precs). Raises only after exhausting retries."""
        last_exc = None
        for attempt in range(retries + 1):
            try:
                total_cnt, precs = self._search_precedents(page=page, display=PAGE_SIZE)
                # A non-empty page is trustworthy. An empty page MAY be a real
                # end-of-list or a transient throttle (OC=test quota) — only
                # trust it as "empty" if the page is genuinely past the ceiling.
                if precs or page * PAGE_SIZE >= (total_cnt or 0):
                    return total_cnt, precs
                logger.warning(
                    f"  Page {page} empty but below ceiling (total={total_cnt}); "
                    f"retrying (attempt {attempt + 1})"
                )
            except Exception as e:
                last_exc = e
                logger.warning(f"  Page {page} fetch error (attempt {attempt + 1}): {e}")
            time.sleep(3 * (attempt + 1))
        if last_exc is not None:
            raise last_exc
        return 0, []

    def _search_precedents(self, page: int = 1, display: int = PAGE_SIZE, sort: str = "ddes") -> tuple:
        """Fetch a page of precedent listings. Returns (total_count, list of prec dicts).

        sort='ddes' is 선고일자-descending: page 1 holds the newest decisions and
        the 00010101-placeholder records sort to the very end. That is also the
        server's default, but the refresh walk depends on it, so it is now asked
        for explicitly rather than inherited (the previous 'date' was not one of
        the API's sort codes and only worked by falling back to the default).
        """
        url = (
            f"{BASE_URL}/lawSearch.do?OC={OC}&target=prec&type=XML"
            f"&display={display}&page={page}&sort={sort}&mobileYn=Y"
        )
        self.rate_limiter.wait()
        resp = self.client.get(url)
        resp.raise_for_status()

        root = ET.fromstring(resp.content)
        total = int(root.findtext("totalCnt", "0"))

        precs = []
        for el in root.findall("prec"):
            precs.append({
                "판례일련번호": el.findtext("판례일련번호", ""),
                "사건명": el.findtext("사건명", ""),
                "사건번호": el.findtext("사건번호", ""),
                "선고일자": el.findtext("선고일자", ""),
                "법원명": el.findtext("법원명", ""),
                "법원종류코드": el.findtext("법원종류코드", ""),
                "사건종류명": el.findtext("사건종류명", ""),
                "사건종류코드": el.findtext("사건종류코드", ""),
                "판결유형": el.findtext("판결유형", ""),
                "선고": el.findtext("선고", ""),
                "데이터출처명": el.findtext("데이터출처명", ""),
            })

        return total, precs

    def _fetch_precedent_detail(self, prec_id: str) -> Optional[bytes]:
        """Fetch full text XML for a precedent by its 판례일련번호."""
        url = (
            f"{BASE_URL}/lawService.do?OC={OC}&target=prec&type=XML"
            f"&ID={prec_id}&mobileYn=Y"
        )
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            logger.warning(f"  Failed to fetch detail for ID={prec_id}: {e}")
            return None

    @staticmethod
    def _clean_html(text: str) -> str:
        """Strip HTML tags and decode entities from text."""
        if not text:
            return ""
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'&lt;', '<', text)
        text = re.sub(r'&gt;', '>', text)
        text = re.sub(r'&amp;', '&', text)
        text = re.sub(r'&nbsp;', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _extract_from_xml(self, xml_bytes: bytes) -> tuple:
        """Extract full text and metadata from precedent detail XML.
        Returns (text, metadata_dict)."""
        try:
            root = ET.fromstring(xml_bytes)
        except ET.ParseError as e:
            logger.warning(f"  XML parse error: {e}")
            return "", {}

        metadata = {
            "판례정보일련번호": root.findtext("판례정보일련번호", ""),
            "사건명": root.findtext("사건명", ""),
            "사건번호": root.findtext("사건번호", ""),
            "선고일자": root.findtext("선고일자", ""),
            "법원명": root.findtext("법원명", ""),
            "사건종류명": root.findtext("사건종류명", ""),
            "판결유형": root.findtext("판결유형", ""),
            "판시사항": root.findtext("판시사항", ""),
            "판결요지": root.findtext("판결요지", ""),
            "참조조문": root.findtext("참조조문", ""),
            "참조판례": root.findtext("참조판례", ""),
        }

        # Full text is in 판례내용
        full_text = self._clean_html(root.findtext("판례내용", ""))

        # Also clean HTML from metadata text fields
        for key in ("판시사항", "판결요지", "참조조문", "참조판례"):
            if metadata.get(key):
                metadata[key] = self._clean_html(metadata[key])

        return full_text, metadata

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all precedents with full text."""
        # Resume: skip listing pages already fully fetched in a prior slot.
        page = self._done_page + 1
        total = None
        fetched_count = 0
        skipped = 0

        while True:
            total_cnt, precs = self._search_with_retry(page)
            if total is None:
                total = total_cnt
                logger.info(f"Total precedents in API: {total} (resuming at page {page})")

            # After retries, an empty page here means genuine end-of-list.
            if not precs:
                break

            for prec in precs:
                prec_id = prec.get("판례일련번호", "")
                if not prec_id:
                    continue

                xml_bytes = self._fetch_precedent_detail(prec_id)
                if xml_bytes is None:
                    skipped += 1
                    continue

                full_text, detail_meta = self._extract_from_xml(xml_bytes)
                if not full_text:
                    logger.warning(f"  ID={prec_id}: no text extracted, skipping")
                    skipped += 1
                    continue

                self._seen_ids.add(prec_id)
                yield {
                    "prec_id": prec_id,
                    "search_meta": prec,
                    "detail_meta": detail_meta,
                    "full_text": full_text,
                }
                fetched_count += 1

            # Page fully processed — checkpoint so re-runs resume past it.
            self._done_page = page
            self._save_checkpoint(page)

            if total and page * PAGE_SIZE >= total:
                break
            page += 1
            if page % 10 == 0:
                logger.info(f"  Page {page}, {fetched_count} precedents fetched, {skipped} skipped")

        # The walk reached the end of the listing, so the resume cursor has done
        # its job. Leaving it parked on the last page would make every later full
        # crawl start past the ceiling and yield nothing — a silent 0-record run
        # the fleet cannot tell from a dead host (#1502). Rewind it; the seen-ID
        # set is what stops the next refresh early, and it is kept.
        self._done_page = 0
        self._save_checkpoint(0)
        logger.info(f"Done: {fetched_count} precedents fetched, {skipped} skipped")

    def fetch_updates(self, since: datetime = None) -> Generator[dict, None, None]:
        """Yield only precedents no previous run has emitted.

        The old body delegated to `fetch_all()`, which resumes from the page
        checkpoint — so a refresh restarted deep in the 1,705-page listing and
        could never reach the new decisions, which land on page 1 (#1502).

        Two facts about the DRF listing drive the replacement, both verified
        against the live API:

        * `sort=ddes` (also the server default) orders by 선고일자 descending —
          page 1 spans the last few months, page 400 lands in 2016 — so a
          newest-first walk reaches everything recent within a few pages.
        * `display` is capped at 100, and only the *detail* fetch is expensive.
          Filtering against the checkpoint before that fetch is where the whole
          saving comes from: a listing page of known IDs costs one request.

        The comparator is a seen-ID checkpoint rather than 선고일자, because the
        sentencing date is not when law.go.kr published the decision — the delay
        runs to months, and 29,180 records carry the 00010101 placeholder and no
        real date at all (#1496). A date cutoff would silently drop every
        late-published decision, which is the failure this issue is about. An ID
        never emitted is new whatever date it carries.

        `since` still sets how deep to walk: the loop keeps going until the
        listing is `PUBLICATION_LAG_DAYS` behind it even while every ID is
        already known, so a refresh after a long gap sweeps the whole gap rather
        than stopping at the first block of familiar IDs.

        Known limit: a record carrying the 00010101 placeholder sorts to the very
        end of the listing (page ~1414 onward, the COMWEL 산재판례 block), so a
        newly added one is out of reach of a newest-first walk. Those arrive as
        bulk loads rather than a daily trickle, and the periodic full crawl —
        which now rewinds its cursor on completion — is what picks them up.
        """
        if not self._seen_ids:
            logger.info(
                "No seen-ID checkpoint — this refresh walks the whole corpus so "
                "nothing is missed; later refreshes stop after "
                f"{STOP_AFTER_SEEN} consecutive known IDs."
            )
            yield from self.fetch_all()
            return

        floor_date = None
        since_date = as_date_str(since)
        if since_date:
            try:
                floor_date = (
                    datetime.strptime(since_date, "%Y-%m-%d")
                    - timedelta(days=PUBLICATION_LAG_DAYS)
                ).strftime("%Y-%m-%d")
            except ValueError:
                logger.warning(f"Unparseable since={since!r}; walking on ID alone")
        logger.info(
            f"Incremental refresh: {len(self._seen_ids)} known IDs, "
            f"walking back to at least {floor_date or 'the first known block'}"
        )

        page = 1
        emitted = 0
        skipped = 0
        consecutive_seen = 0

        while True:
            total_cnt, precs = self._search_with_retry(page)
            if not precs:
                logger.info(f"  Page {page} empty — end of listing")
                break

            for prec in precs:
                prec_id = prec.get("판례일련번호", "")
                if not prec_id:
                    continue
                if prec_id in self._seen_ids:
                    consecutive_seen += 1
                    continue

                xml_bytes = self._fetch_precedent_detail(prec_id)
                if xml_bytes is None:
                    skipped += 1
                    continue
                full_text, detail_meta = self._extract_from_xml(xml_bytes)
                if not full_text:
                    logger.warning(f"  ID={prec_id}: no text extracted, skipping")
                    skipped += 1
                    continue

                consecutive_seen = 0
                self._seen_ids.add(prec_id)
                emitted += 1
                yield {
                    "prec_id": prec_id,
                    "search_meta": prec,
                    "detail_meta": detail_meta,
                    "full_text": full_text,
                }

            # Oldest dated row on this page. Placeholder-dated rows carry no date
            # and so cannot satisfy the floor — but they only appear once the
            # walk is already past 1962, far below any plausible `since`.
            page_dates = [d for d in (self._listing_date(p) for p in precs) if d]
            page_oldest = min(page_dates) if page_dates else None

            deep_enough = floor_date is None or (
                page_oldest is not None and page_oldest < floor_date
            )
            if consecutive_seen >= STOP_AFTER_SEEN and deep_enough:
                logger.info(
                    f"  Stopping at page {page}: {consecutive_seen} consecutive "
                    f"known IDs and the listing is back to {page_oldest}"
                )
                break

            if total_cnt and page * PAGE_SIZE >= total_cnt:
                break
            page += 1

        self._save_checkpoint(self._done_page)
        logger.info(
            f"Update: {emitted} new precedents ({skipped} skipped) "
            f"after walking {page} listing page(s)"
        )

    # ── Field normalisation helpers (#1496) ──────────────────────────

    #: Korea's modern judiciary dates from the 1895 재판소구성법. Anything
    #: older is a placeholder, and anything past next year is a data error —
    #: including Dangi (檀紀) years, which run ~2333 ahead of the Gregorian
    #: calendar and so land far in the future when read as CE.
    MIN_YEAR = 1894

    @staticmethod
    def _field(*values) -> str:
        """First usable value. The API writes the literal string 'null' into
        empty elements (<판결유형>null</판결유형>), which must not be stored."""
        for value in values:
            if value is None:
                continue
            text = str(value).strip()
            if text and text.lower() != "null":
                return text
        return ""

    @classmethod
    def _parse_date(cls, raw_date: str) -> Optional[str]:
        """Return an ISO 8601 date, or None when the API has no real one.

        Accepts both formats the API uses -- YYYYMMDD (detail endpoint) and
        YYYY.MM.DD (listing endpoint). Rejects the 00010101 placeholder,
        Dangi-calendar years and any other out-of-range or unparseable value
        instead of fabricating a date the source does not assert.
        """
        digits = re.sub(r"\D", "", raw_date or "")
        if len(digits) != 8:
            return None
        try:
            parsed = datetime.strptime(digits, "%Y%m%d")
        except ValueError:
            return None
        if not cls.MIN_YEAR <= parsed.year <= datetime.now(timezone.utc).year + 1:
            return None
        return parsed.strftime("%Y-%m-%d")

    @classmethod
    def _case_year(cls, case_number: str) -> Optional[int]:
        """Filing year from a Korean case number ('2005구단4445' -> 2005,
        '대법원-2025-두-34754' -> 2025). None when absent or implausible."""
        match = re.search(r"(?<!\d)(\d{4})(?!\d)", case_number or "")
        if not match:
            return None
        year = int(match.group(1))
        if cls.MIN_YEAR <= year <= datetime.now(timezone.utc).year + 1:
            return year
        return None

    @staticmethod
    def _doc_url(prec_id: str, case_number: str) -> str:
        """Public permalink. 29K records have no case number, for which the
        /판례/{사건번호} form degrades to a bare prefix -- use the stable
        서비스 link keyed on 판례일련번호 for those."""
        if case_number:
            return f"https://www.law.go.kr/판례/{case_number}"
        return (
            "https://www.law.go.kr/DRF/lawService.do"
            f"?OC={OC}&target=prec&ID={prec_id}&type=HTML&mobileYn=Y"
        )

    def normalize(self, raw: dict) -> dict:
        """Transform raw precedent data into standard schema."""
        search = raw.get("search_meta", {})
        detail = raw.get("detail_meta", {})
        prec_id = raw.get("prec_id", "")

        title = self._field(detail.get("사건명"), search.get("사건명"))
        case_number = self._field(detail.get("사건번호"), search.get("사건번호"))
        court = self._field(detail.get("법원명"), search.get("법원명"))

        # Sentencing date. The detail endpoint returns YYYYMMDD, the listing
        # returns YYYY.MM.DD, and 29,180 records (mostly 근로복지공단산재판례)
        # carry the placeholder 00010101 meaning "no sentencing date on file".
        # See #1496 — never fabricate a date from the placeholder.
        date = self._parse_date(
            self._field(detail.get("선고일자"), search.get("선고일자"))
        )
        # For the dateless records the case number still carries a filing year
        # (2005구단4445 -> 2005), which is a legitimate separate field. It is
        # NOT a decision date, so it never populates `date`.
        filing_year = self._case_year(case_number)

        # Build comprehensive text: holdings + summary + full opinion
        text_parts = []
        holdings = detail.get("판시사항", "")
        if holdings:
            text_parts.append(f"[판시사항]\n{holdings}")
        summary = detail.get("판결요지", "")
        if summary:
            text_parts.append(f"[판결요지]\n{summary}")
        full_text = raw.get("full_text", "")
        if full_text:
            text_parts.append(f"[판례내용]\n{full_text}")

        combined_text = "\n\n".join(text_parts)

        return {
            "_id": f"KR-PREC-{prec_id}",
            "_source": "KR/CourtCLIS",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": combined_text,
            "date": date,
            "url": self._doc_url(prec_id, case_number),
            "case_number": case_number,
            "court": court,
            "case_type": self._field(detail.get("사건종류명"), search.get("사건종류명")),
            "judgment_type": self._field(detail.get("판결유형"), search.get("판결유형")),
            "filing_year": filing_year,
            "data_origin": self._field(search.get("데이터출처명")),
        }


# ── CLI entrypoint ────────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = CourtCLISScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|update|test-api] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "bootstrap":
        result = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        print(json.dumps(result, indent=2, default=str))

    elif command == "bootstrap-fast":
        # The fleet wrapper invokes this name; without it the run exited 1 and
        # fell back to re-ingesting the committed samples.
        result = scraper.bootstrap_fast()
        print(json.dumps(result, indent=2, default=str))

    elif command == "update":
        result = scraper.bootstrap(sample_mode=False)
        print(json.dumps(result, indent=2, default=str))

    elif command == "test-api":
        print("Testing law.go.kr precedent DRF API...")
        try:
            total, precs = scraper._search_precedents(page=1, display=3)
            print(f"  Search OK: {total} total precedents, got {len(precs)} in page 1")
            if precs:
                pid = precs[0]["판례일련번호"]
                xml_bytes = scraper._fetch_precedent_detail(pid)
                if xml_bytes:
                    text, meta = scraper._extract_from_xml(xml_bytes)
                    print(f"  Detail OK: '{meta.get('사건명', '')}' — {len(text)} chars of text")
                else:
                    print("  Detail FAILED")
        except Exception as e:
            print(f"  FAILED: {e}")
            sys.exit(1)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
