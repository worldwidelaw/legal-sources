#!/usr/bin/env python3
"""
IN/NCLT -- National Company Law Tribunal

Fetches NCLT insolvency and corporate resolution orders from IBBI's
aggregation portal at ibbi.gov.in/orders/nclt.

Strategy:
  - Paginate through ibbi.gov.in/orders/nclt?page=N (20 rows/page)
  - Parse each table row for date, case title, case number, order type
    and the order PDF link
  - Download order PDFs from /uploads/order/{hash}.pdf
  - Extract full text (common.pdf_extract, pdfplumber fallback)
  - Normalize into standard schema

Data:
  - The pager self-reports 31,541 orders across 1,578 pages (2026-09-02)
  - Covers insolvency admissions, liquidation, resolution plans,
    dissolution, and related IBC proceedings across all NCLT benches
  - PDFs are digital (text-extractable)
  - No authentication required

Note on the listing markup (issue #1537): the portal used to open each order
through an `onclick` handler and now links it as a plain
`<a href=/uploads/order/*.pdf download>` — with an *unquoted* href. The old
row regex matched neither, so every page parsed to "0 entries" while still
returning HTTP 200, which read like a datacenter-IP block. Both shapes are
accepted below, and a page-1 parse that finds no rows now raises instead of
quietly yielding nothing.

Usage:
  python bootstrap.py bootstrap          # Full crawl -> data/records.jsonl
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py bootstrap-fast     # Fleet entry point (same as full)
  python bootstrap.py update             # Fetch orders from last 90 days
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import time
import hashlib
import logging
import threading
from pathlib import Path
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor
from typing import Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IN.NCLT")

BASE_URL = "https://ibbi.gov.in/orders/nclt"

SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"
DATA_DIR = SOURCE_DIR / "data"
CHECKPOINT_FILE = DATA_DIR / "checkpoint.json"

# Fallback only — the pager exposes the real last page and is read first.
FALLBACK_TOTAL_PAGES = 1578

# One page's PDFs are downloaded and extracted concurrently; the listing itself
# stays serial, so the aggregate request rate on ibbi.gov.in stays modest.
PDF_WORKERS = 4
PAGE_DELAY = 1.5

# Order PDFs are single decisions (the largest seen is a few MB); anything past
# these bounds is a slow or wrong response, not a document worth waiting for.
PDF_TIMEOUT = 120

# Bench code → full name mapping (from case number abbreviations).
#
# The codes carrying a count are the ones actually observed in case numbers
# sampled across the corpus (pages 1/100/400/700/1000/1300/1560); the rest are
# kept as tolerated variants. `CB` is Cuttack, not Chandigarh: the listing has
# `IA (IB) No.270/CB/2025 in CP (IB) No. 142/CTB/2019`, and an interlocutory
# application is heard by the bench holding its parent petition.
BENCH_CODES = {
    "MB": "Mumbai", "MAH": "Mumbai", "MUM": "Mumbai",
    "ND": "New Delhi", "PB": "New Delhi",
    "CHE": "Chennai", "CHN": "Chennai",
    "AHM": "Ahmedabad", "AH": "Ahmedabad",
    "ALD": "Allahabad", "ALH": "Allahabad", "ALL": "Allahabad",
    "AMR": "Amaravati", "AMA": "Amaravati",
    "BB": "Bengaluru", "BLR": "Bengaluru", "BNG": "Bengaluru", "KAR": "Bengaluru",
    "CH": "Chandigarh", "CHD": "Chandigarh", "CHND": "Chandigarh",
    "CB": "Cuttack", "CTB": "Cuttack", "CTK": "Cuttack",
    "GB": "Guwahati", "GUW": "Guwahati",
    "HDB": "Hyderabad", "HYD": "Hyderabad",
    "IND": "Indore", "MP": "Indore",
    "JPR": "Jaipur",
    "KOB": "Kochi", "KOC": "Kochi",
    "KB": "Kolkata", "KOL": "Kolkata", "CAL": "Kolkata",
}

# Bench codes sit between the delimiters of a case number:
# `C.P.(IB)/208(MB)/2026`, `CP (IB) 112/ALD/2025`, `IA (IB) No.270/CB/2025`.
BENCH_TOKEN_RE = re.compile(r"[(/\s\-]([A-Za-z]{2,4})[)/\s\-.,]")

# The order PDF is either a plain (often unquoted) href or, on older markup,
# the argument of an onclick handler.
PDF_LINK_RE = re.compile(
    r"""(?:href|onclick)\s*=\s*["']?[^"'>]*?["']?(/uploads/[^"'\s>]+?\.pdf)""",
    re.I,
)


class SourceUnavailableError(RuntimeError):
    """The listing could not be read — fail loud instead of writing 0 records."""


class NCLTScraper:
    """Scraper for IN/NCLT -- National Company Law Tribunal orders via IBBI."""

    def __init__(self):
        self._local = threading.local()
        self._seen_ids = set()
        self.stats = {
            "rows_seen": 0,
            "skipped_no_pdf": 0,
            "skipped_no_text": 0,
            "duplicate_listings": 0,
        }

    @property
    def session(self) -> requests.Session:
        """Per-thread session (requests.Session is not safe to share)."""
        sess = getattr(self._local, "session", None)
        if sess is None:
            sess = requests.Session()
            sess.headers.update({
                "User-Agent": "LegalDataHunter/1.0 (legal research)",
                "Accept": "text/html, */*",
            })
            self._local.session = sess
        return sess

    def _get(self, url: str, timeout: int = 60, attempts: int = 4):
        """GET with backoff on 429/5xx and transport errors."""
        last = None
        for attempt in range(attempts):
            try:
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code in (429, 500, 502, 503, 504):
                    last = f"HTTP {resp.status_code}"
                    retry_after = resp.headers.get("Retry-After")
                    delay = float(retry_after) if (retry_after or "").isdigit() \
                        else min(60, 2 ** attempt * 5)
                else:
                    resp.raise_for_status()
                    return resp
            except requests.RequestException as e:
                last = str(e)
                delay = min(60, 2 ** attempt * 5)
            if attempt < attempts - 1:
                logger.warning("Retrying %s in %.0fs (%s)", url, delay, last)
                time.sleep(delay)
        raise SourceUnavailableError(f"{url}: {last}")

    def _parse_page(self, page: int) -> list:
        """Fetch and parse a single page of NCLT orders from IBBI.

        Raises SourceUnavailableError if the page cannot be fetched — a
        swallowed error here is what turns a blocked crawl into a silent
        0-record run (issue #1537).
        """
        url = f"{BASE_URL}?page={page}"
        html = self._get(url).text
        entries = []

        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.S)
        for row in rows:
            cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.S)
            if len(cells) < 4:
                continue  # header / layout row
            self.stats["rows_seen"] += 1

            pdf_match = PDF_LINK_RE.search(row)
            if not pdf_match:
                # A real listing row that carries no order file. Counted rather
                # than dropped silently so the tally reconciles with the pager.
                self.stats["skipped_no_pdf"] += 1
                continue
            pdf_path = pdf_match.group(1)

            serial = re.sub(r"<[^>]+>", "", cells[0]).strip()
            date_str = re.sub(r"<[^>]+>", "", cells[1]).strip()
            title_raw = re.sub(r"<[^>]+>", "", cells[2]).strip()
            order_type = re.sub(r"<[^>]+>", "", cells[3]).strip()

            # Clean up title — remove file size, &nbsp;, extra whitespace
            title_raw = re.sub(r"\s*\(\d+[\d.]*\s*[KMG]B\)", "", title_raw, flags=re.I)
            title_raw = title_raw.replace("&nbsp;", " ").replace("\xa0", " ")
            title_raw = re.sub(r"\s+", " ", title_raw).strip()

            # Extract case number from title
            case_no_match = re.search(r"\[([^\]]+)\]", title_raw)
            case_no = case_no_match.group(1).strip() if case_no_match else ""

            # Extract company name (before the bracket)
            company = re.sub(r"\[.*?\]", "", title_raw).strip()
            company = re.sub(r"^In the matter of\s+", "", company, flags=re.I).strip()

            entries.append({
                "serial": serial,
                "date_str": date_str,
                "title": title_raw,
                "company": company,
                "case_no": case_no,
                "order_type": order_type,
                "bench": self._detect_bench(case_no),
                "pdf_path": pdf_path,
                "page": page,
            })

        logger.info("Page %d: %d entries", page, len(entries))
        return entries

    def _detect_bench(self, case_no: str) -> str:
        """Detect the NCLT bench from the case number abbreviation.

        Every candidate token is tested, not just the first one: almost every
        case number opens with the statute tag — `C.P.(IB)/208(MB)/2026` — so
        matching once returned `IB`, found no bench, and gave up. That left the
        column empty for all but the rare case number whose first token is the
        bench (1 of 15 samples before this).
        """
        if not case_no:
            return ""
        text = case_no.replace("&amp;", "&")
        for token in BENCH_TOKEN_RE.findall(f" {text} "):
            bench = BENCH_CODES.get(token.upper())
            if bench:
                return bench
        # Pattern like NCLT-MAH-2016
        m = re.search(r"NCLT[- ]([A-Za-z]{2,4})", text)
        if m:
            return BENCH_CODES.get(m.group(1).upper(), "")
        return ""

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse 'DD Mon, YYYY' format to ISO 8601."""
        if not date_str:
            return None
        for fmt in ("%d %b, %Y", "%d %B, %Y", "%d-%m-%Y", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(date_str.strip(), fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _download_pdf_text(self, pdf_path: str, doc_id: str) -> Optional[str]:
        """Download order PDF and extract text."""
        pdf_url = f"https://ibbi.gov.in{pdf_path}"

        # Try common.pdf_extract first
        try:
            text = extract_pdf_markdown(
                source="IN/NCLT",
                source_id=doc_id,
                pdf_url=pdf_url,
                table="case_law",
            )
            if text and len(text.strip()) > 100:
                return text.strip()
        except Exception:
            pass

        # Fallback: direct download + pdfplumber
        try:
            import pdfplumber
            resp = self.session.get(pdf_url, timeout=PDF_TIMEOUT)
            resp.raise_for_status()

            content_type = resp.headers.get("Content-Type", "")
            if "pdf" not in content_type and len(resp.content) < 1000:
                logger.warning("Non-PDF response for %s: %s", doc_id, content_type)
                return None

            if len(resp.content) == 0:
                return None

            text_parts = []
            with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass

            full_text = "\n\n".join(text_parts)
            if len(full_text.strip()) > 100:
                return full_text.strip()
        except Exception as e:
            logger.warning("PDF extraction failed for %s: %s", doc_id, e)

        return None

    def _get_total_pages(self) -> int:
        """Read the last page from the pager (`<li class="last">`)."""
        try:
            html = self._get(BASE_URL, timeout=30).text
        except SourceUnavailableError as e:
            logger.error("Failed to detect total pages: %s", e)
            return FALLBACK_TOTAL_PAGES

        total_records = re.search(r"Total\s+Records\s*:\s*([\d,]+)", html)
        if total_records:
            logger.info("Pager reports %s total orders", total_records.group(1))

        pages = re.findall(r"page=(\d+)", html)
        if pages:
            return max(int(p) for p in pages)
        logger.error("No pager links found — falling back to %d pages",
                     FALLBACK_TOTAL_PAGES)
        return FALLBACK_TOTAL_PAGES

    # --- Checkpoint -------------------------------------------------------
    #
    # A fleet slot is torn down at the wall-clock cap. Without a checkpoint the
    # next run restarts at page 1 and re-downloads what it already has.

    def load_checkpoint(self) -> dict:
        if CHECKPOINT_FILE.exists():
            try:
                ckpt = json.loads(CHECKPOINT_FILE.read_text(encoding="utf-8"))
                if isinstance(ckpt, dict) and "next_page" in ckpt:
                    return ckpt
                logger.warning("Checkpoint has unexpected shape, starting fresh")
            except (json.JSONDecodeError, OSError) as e:
                logger.warning("Invalid checkpoint (%s), starting fresh", e)
        return {"next_page": 1, "count": 0}

    def save_checkpoint(self, ckpt: dict) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        tmp = CHECKPOINT_FILE.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(ckpt, indent=2), encoding="utf-8")
        tmp.replace(CHECKPOINT_FILE)

    def clear_checkpoint(self) -> None:
        if CHECKPOINT_FILE.exists():
            CHECKPOINT_FILE.unlink()
            logger.info("Checkpoint cleared (crawl complete)")

    def fetch_all(self, start_page: int = 1) -> Generator:
        """Yield all NCLT order entries from start_page onwards."""
        total_pages = self._get_total_pages()
        logger.info("Total pages to process: %d (from page %d)",
                    total_pages, start_page)

        for page in range(start_page, total_pages + 1):
            entries = self._parse_page(page)
            if page == start_page and not entries:
                # 200 OK with no parseable row is the #1537 signature: a markup
                # change or a stripped page served to this vantage. Either way
                # the crawl must not report success with 0 records.
                raise SourceUnavailableError(
                    f"page {page} returned no order rows out of "
                    f"{self.stats['rows_seen']} table rows — listing markup "
                    "changed or the page is being stripped for this vantage"
                )
            for entry in entries:
                yield entry
            time.sleep(PAGE_DELAY)

    def fetch_updates(self, since: datetime) -> Generator:
        """Yield recent orders (page through until we pass the since date)."""
        for page in range(1, 200):
            entries = self._parse_page(page)
            if not entries:
                break

            all_old = True
            for entry in entries:
                iso_date = self._parse_date(entry["date_str"])
                if iso_date:
                    entry_dt = datetime.strptime(iso_date, "%Y-%m-%d")
                    if entry_dt >= since.replace(tzinfo=None):
                        all_old = False
                        yield entry
                    else:
                        continue
                else:
                    yield entry
                    all_old = False

            if all_old:
                logger.info("All entries on page %d are older than %s, stopping",
                            page, since.isoformat())
                break
            time.sleep(PAGE_DELAY)

    def doc_id(self, raw: dict) -> str:
        """Stable id, keyed on the upload hash.

        The hash is what identifies the order file; ids for the classic
        /uploads/order/{hash}.pdf paths are unchanged, so rows already in Neon
        keep their key. Newer uploads prefix the same hash with an upload
        timestamp (/uploads/order/2026-08-22-043346-28n7b-{hash}.pdf) and the
        old `/({hash})\\.pdf` anchor missed them, so those orders fell through
        to a case-number id — and the portal lists some orders under *both*
        path shapes, which then landed as two rows for one document. Match the
        trailing hash wherever it sits, and hash the path rather than the case
        number when there is none: case numbers repeat across orders (an IA and
        its parent CP share one), so they cannot key a document.
        """
        pdf_path = raw.get("pdf_path", "")
        pdf_hash = re.search(r"([a-f0-9]{20,})\.pdf\b", pdf_path, re.I)
        if pdf_hash:
            return f"NCLT-{pdf_hash.group(1).lower()[:16]}"
        if pdf_path:
            digest = hashlib.sha256(pdf_path.encode("utf-8")).hexdigest()
            return f"NCLT-p{digest[:16]}"
        return f"NCLT-{raw.get('serial', 'unknown')}-p{raw.get('page', 0)}"

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw entry into standard schema."""
        doc_id = self.doc_id(raw)

        # Download and extract PDF text
        text = self._download_pdf_text(raw.get("pdf_path", ""), doc_id)
        if not text:
            self.stats["skipped_no_text"] += 1
            logger.warning("No text extracted for %s (%s)", doc_id,
                           raw.get("case_no", ""))
            return None

        # Build title
        title = raw.get("title", "")
        company = raw.get("company", "")
        case_no = raw.get("case_no", "")
        if not title and company:
            title = f"In the matter of {company}"
            if case_no:
                title += f" [{case_no}]"

        return {
            "_id": doc_id,
            "_source": "IN/NCLT",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": self._parse_date(raw.get("date_str", "")),
            "url": f"https://ibbi.gov.in{raw.get('pdf_path', '')}",
            "case_no": case_no,
            "company": company,
            "order_type": raw.get("order_type", ""),
            "bench": raw.get("bench", ""),
        }

    def normalize_page(self, entries: list) -> list:
        """Normalize one page's entries concurrently, preserving order.

        Orders relisted under a second path (see doc_id) are dropped before the
        download rather than after, so one document costs one PDF fetch and the
        run's record count matches the number of distinct orders.
        """
        fresh = []
        for entry in entries:
            doc_id = self.doc_id(entry)
            if doc_id in self._seen_ids:
                self.stats["duplicate_listings"] += 1
                continue
            self._seen_ids.add(doc_id)
            fresh.append(entry)
        if not fresh:
            return []
        with ThreadPoolExecutor(max_workers=PDF_WORKERS) as pool:
            return [r for r in pool.map(self.normalize, fresh) if r]


def run_sample(scraper: NCLTScraper, limit: int = 15) -> int:
    """Write `limit` records to sample/ (validation set, committed to the repo)."""
    SAMPLE_DIR.mkdir(exist_ok=True)
    count = 0
    page = 1
    while count < limit:
        entries = scraper._parse_page(page)
        if not entries:
            if page == 1:
                raise SourceUnavailableError("page 1 returned no order rows")
            break
        for rec in scraper.normalize_page(entries):
            count += 1
            (SAMPLE_DIR / f"{rec['_id']}.json").write_text(
                json.dumps(rec, ensure_ascii=False, indent=2), encoding="utf-8")
            logger.info("[%d] Saved %s (%d chars text)", count, rec["_id"],
                        len(rec["text"]))
            if count >= limit:
                break
        page += 1
        time.sleep(PAGE_DELAY)
    logger.info("Sample complete: %d records saved to %s", count, SAMPLE_DIR)
    return count


def run_full(scraper: NCLTScraper, resume: bool = True) -> int:
    """Stream the whole corpus to data/records.jsonl (what the pipeline ingests).

    The previous implementation wrote one JSON file per order into sample/, so
    even a successful full run left the pipeline with nothing to ingest beyond
    the committed samples (the #798 / #1113 sample-only class).
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path = DATA_DIR / "records.jsonl"

    ckpt = scraper.load_checkpoint() if resume else {"next_page": 1, "count": 0}
    resuming = ckpt["next_page"] > 1
    if resuming:
        logger.info("Resuming at page %d (%d records already written)",
                    ckpt["next_page"], ckpt["count"])
    elif out_path.exists():
        out_path.unlink()

    count = ckpt["count"]
    page = ckpt["next_page"]
    with open(out_path, "a" if resuming else "w", encoding="utf-8") as f:
        total_pages = scraper._get_total_pages()
        while page <= total_pages:
            entries = scraper._parse_page(page)
            if page == ckpt["next_page"] and not entries and not resuming:
                raise SourceUnavailableError(
                    f"page {page} returned no order rows out of "
                    f"{scraper.stats['rows_seen']} table rows — listing markup "
                    "changed or the page is being stripped for this vantage"
                )
            for rec in scraper.normalize_page(entries):
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                count += 1
            f.flush()
            page += 1
            scraper.save_checkpoint({"next_page": page, "count": count})
            time.sleep(PAGE_DELAY)

    scraper.clear_checkpoint()
    logger.info("bootstrap_fast complete: %d fetched -> %s", count, out_path)
    logger.info("Skips: %d rows without a PDF link, %d without extractable text, "
                "%d relisted under a second path (of %d listing rows)",
                scraper.stats["skipped_no_pdf"], scraper.stats["skipped_no_text"],
                scraper.stats["duplicate_listings"], scraper.stats["rows_seen"])
    if count == 0:
        raise SourceUnavailableError(
            "full crawl wrote 0 records — refusing to exit 0 on an empty corpus")
    return count


def main() -> int:
    # `bootstrap-fast` is the fleet runner's entry point; this CLI dispatches on
    # the literal command name, so alias it onto the full bootstrap rather than
    # exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    import argparse

    parser = argparse.ArgumentParser(description="IN/NCLT bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Full crawl (default) or sample")
    boot.add_argument("--sample", action="store_true", help="Fetch only 15 sample records")
    boot.add_argument("--full", action="store_true", help="Full crawl (the default)")
    boot.add_argument("--no-resume", action="store_true",
                      help="Ignore any existing checkpoint and crawl from page 1")

    upd = sub.add_parser("update", help="Fetch recent orders")
    upd.add_argument("--days", type=int, default=90, help="Look back N days (default 90)")
    upd.add_argument("--since", type=str, help="Fetch orders since YYYY-MM-DD")

    sub.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    scraper = NCLTScraper()

    if args.command == "test":
        logger.info("Testing NCLT/IBBI connectivity...")
        entries = scraper._parse_page(1)
        logger.info("Page 1: %d entries (%d rows, %d without a PDF link)",
                    len(entries), scraper.stats["rows_seen"],
                    scraper.stats["skipped_no_pdf"])
        if not entries:
            logger.error("Test FAILED: no order rows parsed")
            return 1
        sample = entries[0]
        logger.info("Sample: case=%s, date=%s, type=%s, pdf=%s",
                    sample["case_no"], sample["date_str"], sample["order_type"],
                    sample["pdf_path"])
        total = scraper._get_total_pages()
        logger.info("Total pages: %d (approx %d orders)", total, total * 20)
        logger.info("Test PASSED")
        return 0

    if args.command == "bootstrap":
        if args.sample:
            return 0 if run_sample(scraper) else 1
        run_full(scraper, resume=not args.no_resume)
        return 0

    if args.command == "update":
        if args.since:
            since = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        else:
            since = datetime.now(timezone.utc) - timedelta(days=args.days)

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        out_path = DATA_DIR / "records.jsonl"
        count = 0
        with open(out_path, "w", encoding="utf-8") as f:
            batch = []
            for raw in scraper.fetch_updates(since):
                batch.append(raw)
                if len(batch) >= PDF_WORKERS * 2:
                    for rec in scraper.normalize_page(batch):
                        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                        count += 1
                    batch = []
            for rec in scraper.normalize_page(batch):
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                count += 1
        logger.info("Update complete: %d records -> %s", count, out_path)
        return 0

    parser.print_help()
    return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SourceUnavailableError as e:
        logger.error("IN/NCLT unavailable: %s", e)
        sys.exit(1)
