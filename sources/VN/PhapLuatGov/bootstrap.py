#!/usr/bin/env python3
"""
Vietnam National Legal Document Database Fetcher

Rebuilt for issue #1445. The Ministry of Justice portal this source originally
read (vbpl.vn) now sits behind a site-wide JavaScript security challenge that
returns HTTP 403 to every non-browser client, from datacenter *and* residential
vantages alike, so the old pKetQuaTimKiem.aspx / vbpq-toanvan.aspx path is dead.

The same corpus of central-level Vietnamese legal instruments is published by
the Government Office's "Hệ thống văn bản" at vanban.chinhphu.vn, which serves
plain server-rendered HTML and reports 47,710 documents under classid=1
(văn bản quy phạm pháp luật). Full text is the official signed file attached to
each record on datafiles.chinhphu.vn (PDF, and DOC/DOCX/RTF for older acts).

Endpoints:
  - Listing: /he-thong-van-ban?classid=1&mode=1  (ASP.NET GridView, 50 rows,
    paged by __doPostBack('...$grvDocument', 'Page$N') — `page=`/`maxresults=`
    query params are accepted but silently ignored)
  - Detail:  /?pageid=27160&docid={docid}&classid=1
  - Files:   https://datafiles.chinhphu.vn/cpp/files/vbpq/...
"""

import argparse
import html as html_mod
import io
import json
import logging
import os
import re
import sys
import time
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import requests

# Roughly half of the attached files are image-only scans whose only text layer
# is the digital-signature stamp, so they reach the OCR fallback in
# common/pdf_extract. That fallback defaults to English; reading Vietnamese with
# an English model returns diacritic-stripped nonsense that then fails the
# is_vietnamese_text() gate below. Must be set before pdf_extract is imported.
os.environ.setdefault("PDF_OCR_LANG", "vie")

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SOURCE_ID = "VN/PhapLuatGov"
BASE_URL = "https://vanban.chinhphu.vn"
LIST_URL = BASE_URL + "/he-thong-van-ban?classid=1&mode=1"
DETAIL_URL = BASE_URL + "/?pageid=27160&docid={docid}&classid=1"
ROWS_PER_PAGE = 50
DELAY = 1.0
TIMEOUT = 90
MAX_FILE_BYTES = 60_000_000
# How many documents may fail before a run with nothing to show for itself is
# called a block rather than a run of bad attachments.
BLOCK_PROBE_DOCS = 40

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

DATA_DIR = Path(__file__).parent / "data"
INDEX_FILE = DATA_DIR / "index.json"
RECORDS_FILE = DATA_DIR / "records.jsonl"

# Vietnamese instrument abbreviations, read off the document number suffix
# (e.g. "61/2026/TT-BXD" -> TT -> Thông tư).
DOC_TYPE_BY_ABBREV = {
    "QH": "Luật / Nghị quyết của Quốc hội",
    "UBTVQH": "Pháp lệnh / Nghị quyết của Ủy ban Thường vụ Quốc hội",
    "PL": "Pháp lệnh",
    "L": "Luật",
    "LCT": "Luật (lệnh công bố)",
    "CTN": "Lệnh của Chủ tịch nước",
    "NĐ": "Nghị định",
    "ND": "Nghị định",
    "NQ": "Nghị quyết",
    "QĐ": "Quyết định",
    "QD": "Quyết định",
    "TT": "Thông tư",
    "TTLT": "Thông tư liên tịch",
    "CT": "Chỉ thị",
    "HP": "Hiến pháp",
}


def strip_html(text: str) -> str:
    """Remove HTML tags and entities, collapse whitespace."""
    text = re.sub(r"<(?:style|script)[^>]*>.*?</(?:style|script)>", "", text, flags=re.DOTALL | re.I)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"</(?:p|div|tr|li|h[1-6])>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = html_mod.unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n\n", text)
    return text.strip()


def parse_vn_date(value: str) -> Optional[str]:
    """Parse a Vietnamese DD/MM/YYYY or DD-MM-YYYY date into ISO 8601."""
    if not value:
        return None
    value = value.strip()
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def classify(document_number: str) -> Optional[str]:
    """Map a document number such as '61/2026/TT-BXD' to an instrument type."""
    if not document_number:
        return None
    tail = document_number.split("/")[-1]
    abbrev = tail.split("-")[0].strip().upper()
    for key, label in DOC_TYPE_BY_ABBREV.items():
        if abbrev == key.upper():
            return label
    return None


def issuing_body_code(document_number: str) -> Optional[str]:
    """The issuing-body suffix of a document number ('TT-BXD' -> 'BXD')."""
    if not document_number or "-" not in document_number:
        return None
    return document_number.rsplit("-", 1)[-1].strip() or None


# Vietnamese-specific letters. A genuine Vietnamese text layer is ~20% of these;
# a PDF that embeds a legacy TCVN3/VNI font with a private glyph mapping extracts
# as Latin mojibake ("NGÂN HÀNG NHÀ NƯỚC" -> "xcAxuAIc NEA NTI6c") and scores 0.
_VIET_CHARS = set(
    "ăâđêôơư"
    "àáảãạằắẳẵặầấẩẫậ"
    "èéẻẽẹềếểễệ"
    "ìíỉĩị"
    "òóỏõọồốổỗộờớởỡợ"
    "ùúủũụừứửữự"
    "ỳýỷỹỵ"
)
MIN_VIET_DENSITY = 0.02


def is_vietnamese_text(text: str) -> bool:
    """
    True if `text` reads as Vietnamese rather than legacy-font mojibake.

    The two populations are far apart — real extractions land at 0.19-0.22
    diacritic density, private-encoding ones at exactly 0.0 — so a single
    threshold separates them without tuning.
    """
    if not text:
        return False
    lowered = text.lower()
    hits = sum(1 for ch in lowered if ch in _VIET_CHARS)
    return hits / len(lowered) >= MIN_VIET_DENSITY


def extract_file_text(content: bytes, url: str, doc_id: str) -> Optional[str]:
    """Extract plain text from an attached PDF / DOC / DOCX / RTF."""
    ext = urllib.parse.urlparse(url).path.rsplit(".", 1)[-1].lower()

    if ext == "pdf" or content[:5] == b"%PDF-":
        from common.pdf_extract import extract_pdf_markdown

        return extract_pdf_markdown(
            SOURCE_ID, doc_id, pdf_bytes=content, table="legislation", force=True
        )

    if ext == "docx":
        try:
            import docx

            document = docx.Document(io.BytesIO(content))
            return "\n".join(p.text for p in document.paragraphs).strip() or None
        except Exception as exc:
            logger.warning("docx extraction failed for %s: %s", url, exc)
            return None

    if ext == "doc":
        from common.doc_extract import extract_doc_text

        # Vietnamese legacy Word files store 8-bit runs in the Windows-1258
        # Vietnamese code page, not the cp1253 default.
        return extract_doc_text(content, encoding="cp1258")

    if ext == "rtf":
        try:
            from striprtf.striprtf import rtf_to_text

            return rtf_to_text(content.decode("utf-8", "replace"), errors="ignore").strip() or None
        except Exception as exc:
            logger.warning("rtf extraction failed for %s: %s", url, exc)
            return None

    logger.warning("Unsupported attachment type %r for %s", ext, url)
    return None


class VanBanFetcher:
    """Fetcher for Vietnamese legislation from vanban.chinhphu.vn."""

    def __init__(self, delay: float = DELAY):
        self.delay = delay
        self.mojibake = 0  # documents dropped for an unreadable legacy-font text layer
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": USER_AGENT,
                "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
                "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
            }
        )

    # ---------------------------------------------------------------- listing

    def _request(self, method: str, url: str, attempts: int = 5, **kwargs) -> requests.Response:
        """
        One listing request, retried through transient failures.

        The grid pages by sequential postback, so a single dropped request used
        to abort the whole walk — and because the walk ran to completion before
        any record was written, that cost the entire run (issue #1445).
        """
        delay = 5.0
        last: Optional[Exception] = None
        for attempt in range(1, attempts + 1):
            try:
                resp = self.session.request(method, url, timeout=TIMEOUT, **kwargs)
                if resp.status_code in (429, 500, 502, 503, 504):
                    raise requests.HTTPError(f"HTTP {resp.status_code} from {url}")
                resp.raise_for_status()
                return resp
            except Exception as exc:
                last = exc
                if attempt == attempts:
                    break
                logger.warning("Listing request failed (%s/%s): %s", attempt, attempts, exc)
                time.sleep(delay)
                delay = min(delay * 2, 120.0)
        raise RuntimeError(f"Listing unreachable after {attempts} attempts: {last}")

    @staticmethod
    def _hidden_fields(page_html: str) -> Dict[str, str]:
        """Collect the ASP.NET hidden postback fields (__VIEWSTATE et al.)."""
        fields = {}
        for tag in re.findall(r'<input[^>]*type="hidden"[^>]*>', page_html, re.I):
            name = re.search(r'name="([^"]*)"', tag)
            if not name:
                continue
            value = re.search(r'value="([^"]*)"', tag)
            fields[html_mod.unescape(name.group(1))] = (
                html_mod.unescape(value.group(1)) if value else ""
            )
        return fields

    @staticmethod
    def total_count(page_html: str) -> int:
        """Read the 'from - to | total' counter the grid prints below itself."""
        m = re.search(r'document_page_info"?>\s*[\d\s\-]*\|\s*(\d+)', page_html)
        return int(m.group(1)) if m else 0

    @staticmethod
    def parse_rows(page_html: str) -> List[Dict[str, Any]]:
        """Parse one grid page into row dicts (docid, number, date, title, files)."""
        start = page_html.find('class="table search-result"')
        if start == -1:
            return []
        end = page_html.find("</table>", start)
        grid = page_html[start : end if end != -1 else len(page_html)]

        rows = []
        for cell in re.split(r"<tr[^>]*>", grid)[1:]:
            docid = re.search(r"docid=(\d+)", cell)
            if not docid:
                continue
            number = re.search(r'<span class="code">(.*?)</span>', cell, re.DOTALL)
            title = re.search(r'<span class="substract">(.*?)</span>', cell, re.DOTALL)
            date = re.search(r'<span class="issued-date">(.*?)</span>', cell, re.DOTALL)
            files = re.findall(r'<div class="bl-doc-file"><a href="([^"]+)"', cell)

            raw_number = strip_html(number.group(1)) if number else ""
            rows.append(
                {
                    "docid": docid.group(1),
                    "document_number": None if raw_number in ("", ".") else raw_number,
                    "title": strip_html(title.group(1)) if title else "",
                    "date": parse_vn_date(strip_html(date.group(1))) if date else None,
                    "files": [html_mod.unescape(f) for f in files],
                }
            )
        return rows

    def iter_index(self, max_pages: Optional[int] = None) -> Iterator[Dict[str, Any]]:
        """
        Walk the whole listing, yielding one row dict per document.

        The grid pages only via __doPostBack, so pages must be requested in
        order — each response carries the __VIEWSTATE needed for the next.
        """
        page_html = self._request("GET", LIST_URL).text
        total = self.total_count(page_html)
        if not total:
            # An empty counter means the grid did not render: a block page or a
            # layout change, never a genuinely empty corpus (the portal reports
            # ~47,700 documents). Fail loud rather than write zero records.
            raise RuntimeError(
                f"{LIST_URL} returned no result counter — the listing is blocked "
                "or its markup changed; refusing to report an empty corpus"
            )
        total_pages = (total + ROWS_PER_PAGE - 1) // ROWS_PER_PAGE
        if max_pages:
            total_pages = min(total_pages, max_pages)
        logger.info("Listing reports %d documents across %d pages", total, total_pages)

        target = re.search(r"__doPostBack\(&#39;([^&]+?)&#39;,&#39;Page\$", page_html)
        event_target = html_mod.unescape(target.group(1)) if target else None

        seen = set()
        for page in range(1, total_pages + 1):
            if page > 1:
                if not event_target:
                    logger.warning("No grid postback target found; stopping at page 1")
                    break
                fields = self._hidden_fields(page_html)
                fields["__EVENTTARGET"] = event_target
                fields["__EVENTARGUMENT"] = f"Page${page}"
                page_html = self._request(
                    "POST", LIST_URL, data=fields, headers={"Referer": LIST_URL}
                ).text
                time.sleep(self.delay)

            rows = self.parse_rows(page_html)
            if not rows:
                logger.warning("Page %d parsed 0 rows — stopping", page)
                break
            fresh = 0
            for row in rows:
                if row["docid"] in seen:
                    continue
                seen.add(row["docid"])
                fresh += 1
                yield row
            logger.info("Page %d/%d: %d rows (%d new, %d total)", page, total_pages, len(rows), fresh, len(seen))
            if fresh == 0:
                # The grid stopped advancing — treat as end of corpus rather
                # than spinning on a repeated page.
                logger.warning("Page %d repeated the previous page — stopping", page)
                break

    def walk_index(self) -> Iterator[Dict[str, Any]]:
        """
        Yield every listing row, reusing a completed cached index if one exists.

        Callers consume this lazily and fetch each document as it arrives, so a
        run that dies at page 700 still leaves ~35,000 records on disk. The
        cache is written page by page and only marked complete once the walk
        reaches the end, so a partial file is never mistaken for the corpus.
        """
        cache = self._load_index_cache()
        if cache is not None:
            logger.info("Loaded cached index: %d documents", len(cache))
            yield from cache
            return

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        rows: List[Dict[str, Any]] = []
        complete = False
        try:
            for row in self.iter_index():
                rows.append(row)
                if len(rows) % ROWS_PER_PAGE == 0:
                    self._save_index_cache(rows, complete=False)
                yield row
            complete = True
        finally:
            self._save_index_cache(rows, complete=complete)

    @staticmethod
    def _load_index_cache() -> Optional[List[Dict[str, Any]]]:
        if not INDEX_FILE.exists():
            return None
        try:
            cached = json.loads(INDEX_FILE.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.warning("Unreadable index cache, rebuilding: %s", exc)
            return None
        if isinstance(cached, dict):
            return cached["rows"] if cached.get("complete") else None
        # Pre-#1445 caches were a bare list with no completeness marker; they
        # may be a truncated walk, so re-walk rather than trust them.
        return None

    @staticmethod
    def _save_index_cache(rows: List[Dict[str, Any]], complete: bool) -> None:
        INDEX_FILE.write_text(
            json.dumps({"complete": complete, "rows": rows}, ensure_ascii=False),
            encoding="utf-8",
        )

    # --------------------------------------------------------------- document

    def fetch_text(self, row: Dict[str, Any]) -> Optional[str]:
        """Download the first attachment that yields usable text."""
        for url in row["files"]:
            try:
                resp = self.session.get(url, timeout=TIMEOUT, stream=True)
                resp.raise_for_status()
                content = resp.raw.read(MAX_FILE_BYTES + 1, decode_content=True)
            except Exception as exc:
                logger.warning("Download failed for %s: %s", url, exc)
                continue
            if len(content) > MAX_FILE_BYTES:
                logger.warning("Attachment over %d bytes, skipping: %s", MAX_FILE_BYTES, url)
                continue

            text = extract_file_text(content, url, row["docid"])
            if not text or len(text.strip()) <= 200:
                continue
            if not is_vietnamese_text(text):
                # Legacy-font PDF: the text layer decodes to mojibake, which is
                # worse than no record at all. Recovering it needs OCR with a
                # Vietnamese language pack, which the fleet image lacks.
                self.mojibake += 1
                logger.warning("Legacy-font (unreadable) text layer, skipping: %s", url)
                continue
            return text.strip()
        return None

    def normalize(self, raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Turn an index row plus its attachment text into the standard schema."""
        text = raw.get("text") or self.fetch_text(raw)
        if not text:
            return None

        number = raw.get("document_number")
        return {
            "_id": f"VN-VBCP-{raw['docid']}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title") or number or f"Văn bản {raw['docid']}",
            "text": text,
            "date": raw.get("date"),
            "url": DETAIL_URL.format(docid=raw["docid"]),
            "document_number": number,
            "document_type": classify(number or ""),
            "issuing_authority": issuing_body_code(number or ""),
            "file_url": raw["files"][0] if raw.get("files") else None,
            "language": "vi",
            "country": "VN",
            "docid": raw["docid"],
        }

    def fetch_all(self, limit: Optional[int] = None, skip_ids: Optional[set] = None) -> Iterator[Dict[str, Any]]:
        """Yield every document with full text, streaming as the listing is walked."""
        skip_ids = skip_ids or set()
        emitted = 0
        attempted = 0
        for row in self.walk_index():
            if f"VN-VBCP-{row['docid']}" in skip_ids:
                continue
            if not row.get("files"):
                continue
            attempted += 1
            try:
                doc = self.normalize(row)
            except Exception as exc:
                # One unreadable attachment must not end the crawl.
                logger.warning("docid=%s failed: %s", row["docid"], exc)
                doc = None
            time.sleep(self.delay)
            if not doc:
                logger.warning("No full text for docid=%s", row["docid"])
                if attempted >= BLOCK_PROBE_DOCS and emitted == 0:
                    raise RuntimeError(
                        f"{attempted} documents attempted and none yielded text — "
                        "datafiles.chinhphu.vn is refusing this vantage or the "
                        "attachment scheme changed"
                    )
                continue
            yield doc
            emitted += 1
            if limit and emitted >= limit:
                return

    def fetch_updates(self, since: str) -> Iterator[Dict[str, Any]]:
        """Yield documents issued on or after `since` (the listing is newest-first)."""
        since_date = since[:10]
        for row in self.iter_index():
            if row.get("date") and row["date"] < since_date:
                return
            if not row.get("files"):
                continue
            doc = self.normalize(row)
            time.sleep(self.delay)
            if doc:
                yield doc


def _existing_ids() -> set:
    """Ids already streamed to records.jsonl, so a re-launch resumes."""
    if not RECORDS_FILE.exists():
        return set()
    ids = set()
    with open(RECORDS_FILE, encoding="utf-8") as fh:
        for line in fh:
            try:
                ids.add(json.loads(line)["_id"])
            except Exception:
                continue
    logger.info("Resuming: %d records already written", len(ids))
    return ids


def bootstrap_sample(count: int = 15) -> int:
    """Fetch a handful of documents into sample/ for validation."""
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(parents=True, exist_ok=True)
    fetcher = VanBanFetcher()

    saved = 0
    for row in fetcher.iter_index(max_pages=2):
        if saved >= count:
            break
        if not row.get("files"):
            continue
        doc = fetcher.normalize(row)
        time.sleep(fetcher.delay)
        if not doc:
            continue
        (sample_dir / f"{doc['_id']}.json").write_text(
            json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        saved += 1
        logger.info("[%d/%d] %s — %d chars", saved, count, doc["_id"], len(doc["text"]))
    logger.info("Bootstrap complete: %d documents saved to %s", saved, sample_dir)
    return saved


def bootstrap_full() -> int:
    """Stream the whole corpus to data/records.jsonl, resuming if interrupted."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    fetcher = VanBanFetcher()
    skip = _existing_ids()
    written = len(skip)

    with open(RECORDS_FILE, "a", encoding="utf-8") as out:
        for doc in fetcher.fetch_all(skip_ids=skip):
            out.write(json.dumps(doc, ensure_ascii=False) + "\n")
            out.flush()
            written += 1
            if written % 100 == 0:
                logger.info("bootstrap_fast progress: %d written", written)
    logger.info(
        "bootstrap_fast complete: %d fetched (%d dropped — legacy-font text layer)",
        written,
        fetcher.mojibake,
    )
    return written


def main() -> int:
    parser = argparse.ArgumentParser(description="Vietnam VBQPPL fetcher (vanban.chinhphu.vn)")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "update"])
    parser.add_argument("--sample", action="store_true", help="fetch a 15-record sample")
    parser.add_argument("--full", action="store_true", help="stream the full corpus")
    parser.add_argument("--since", help="ISO date for `update`")
    args = parser.parse_args()

    if args.command == "update":
        fetcher = VanBanFetcher()
        since = args.since or datetime.now(timezone.utc).strftime("%Y-01-01")
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        count = 0
        with open(RECORDS_FILE, "a", encoding="utf-8") as out:
            for doc in fetcher.fetch_updates(since):
                out.write(json.dumps(doc, ensure_ascii=False) + "\n")
                count += 1
        logger.info("update complete: %d records since %s", count, since)
        return 0

    if args.command == "bootstrap-fast" or args.full:
        return 0 if bootstrap_full() > 0 else 1

    saved = bootstrap_sample()
    if saved < 10:
        logger.error("Only %d documents saved, expected at least 10", saved)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
