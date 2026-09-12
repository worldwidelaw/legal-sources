#!/usr/bin/env python3
"""
AR/NormasGBA -- Buenos Aires Province Normative System Data Fetcher

Fetches legislation from normas.gba.gob.ar (Sistema de Información Normativa y
Documental, SIND) — the consolidated norm collection of the Province of Buenos
Aires.

Strategy:
  - Partition each document type's date axis by BINARY SUBDIVISION until every
    window holds <= 200 hits (the server's hard result-window cap), instead of
    walking fixed weekly windows. Sparse types cost a handful of requests
    instead of ~1,900 empty ones (issue #1364).
  - The partition and the per-window completion state live in
    data/checkpoint.json, so a fleet re-run resumes instead of restarting.
  - Extract document URLs from the search result pages (10 per page, 20 pages).
  - Fetch each document detail page for metadata + the full-text HTML link.
  - Download and clean the full text (HTML preferred, PDF fallback).

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py bootstrap --full     # Full bootstrap -> data/records.jsonl
  python bootstrap.py bootstrap-fast       # Alias the fleet wrapper calls
  python bootstrap.py test-api             # Quick API connectivity test
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import date, datetime, timedelta, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import urlencode, urljoin

try:
    import requests
    from requests.adapters import HTTPAdapter
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.http_client import CappedRetry, request_with_deadline

# Setup
SOURCE_ID = "AR/NormasGBA"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"
DATA_DIR = SOURCE_DIR / "data"
RECORDS_PATH = DATA_DIR / "records.jsonl"
CHECKPOINT_PATH = DATA_DIR / "checkpoint.json"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AR.NormasGBA")

BASE_URL = "https://normas.gba.gob.ar"
SEARCH_URL = f"{BASE_URL}/resultados"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Accept": "text/html,application/xhtml+xml,*/*",
}

DOCUMENT_TYPES = [
    "Law",
    "DecreeLaw",
    "Decree",
    "Resolution",
    "Disposition",
    "GeneralOrdinance",
    "JointResolution",
]

TYPE_LABELS = {
    "Law": "Ley",
    "DecreeLaw": "Decreto-ley",
    "Decree": "Decreto",
    "Resolution": "Resolución",
    "Disposition": "Disposición",
    "GeneralOrdinance": "Ordenanza General",
    "JointResolution": "Resolución Conjunta",
}

# The result window is capped server-side: page 20 and page 50 return the same
# bytes, so a query can never surrender more than 20 pages x 10 hits. Partition
# the date axis until every window fits under that ceiling.
PAGE_SIZE = 10
MAX_PAGES = 20
WINDOW_CAP = PAGE_SIZE * MAX_PAGES

# Decretos-ley are a pre-1983 instrument: the old fixed 1990 start year matched
# zero of the 2,381 that exist. Start where the collection actually starts.
FIRST_DAY = date(1800, 1, 1)

REQUEST_DELAY = 0.8      # seconds between requests, raised on throttling
MAX_DELAY = 20.0
WALL_TIMEOUT = 90        # hard ceiling per request, incl. urllib3's own retries
SOCKET_TIMEOUT = (10, 30)


def _build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    retry = CappedRetry(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_maxsize=4)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


session = _build_session()

# Adaptive pacing: a sustained 429/503 run slows the crawl instead of hammering
# the host until it drops us (the fleet has to live on one IP for days).
_delay = {"value": REQUEST_DELAY, "ok_streak": 0}


def _http_get(url: str, attempts: int = 4) -> Optional[requests.Response]:
    """GET with a wall-clock deadline, backoff, and adaptive pacing."""
    for attempt in range(1, attempts + 1):
        time.sleep(_delay["value"])
        try:
            resp = request_with_deadline(
                session, "GET", url,
                wall_timeout=WALL_TIMEOUT,
                timeout=SOCKET_TIMEOUT,
            )
        except Exception as exc:
            logger.warning(f"  request failed ({attempt}/{attempts}) {url}: {exc}")
            _delay["value"] = min(MAX_DELAY, _delay["value"] * 2)
            _delay["ok_streak"] = 0
            continue

        if resp.status_code in (429, 500, 502, 503, 504):
            logger.warning(f"  HTTP {resp.status_code} ({attempt}/{attempts}) {url}")
            _delay["value"] = min(MAX_DELAY, _delay["value"] * 2)
            _delay["ok_streak"] = 0
            continue

        if resp.status_code != 200:
            return None

        _delay["ok_streak"] += 1
        if _delay["ok_streak"] >= 50 and _delay["value"] > REQUEST_DELAY:
            _delay["value"] = max(REQUEST_DELAY, _delay["value"] / 2)
            _delay["ok_streak"] = 0
        _force_utf8(resp)
        return resp

    return None


def _force_utf8(resp: requests.Response) -> None:
    """Pin the decoder to UTF-8 whenever the host omits the charset.

    /documentos/*.html is served as a bare `text/html`, so requests falls back to
    ISO-8859-1 per RFC 2616 and every accent comes out double-encoded
    ("CÃMARA", "DeclÃ¡rase"). That is what corrupted the whole corpus in #1406.
    Doing this in `_http_get` covers every call site, not just the one that was
    known to be broken.
    """
    if "charset=" not in (resp.headers.get("content-type") or "").lower():
        resp.encoding = "utf-8"


_MOJIBAKE_RE = re.compile(r'Ã[-¿]|Â[ -¿]')


def fix_mojibake(text: str) -> str:
    """Reverse UTF-8-bytes-read-as-Latin-1 damage if any slipped through.

    A second line of defence behind `_force_utf8`: if the host ever starts
    declaring a wrong charset, the round-trip below still recovers the original
    text. Returns the input unchanged when it is not actually mojibake.
    """
    if not text or not _MOJIBAKE_RE.search(text):
        return text
    try:
        repaired = text.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text
    return repaired if not _MOJIBAKE_RE.search(repaired) else text


def clean_html(html: str) -> str:
    """Remove HTML tags and clean up text content."""
    if not html:
        return ""
    html = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<style[^>]*>.*?</style>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<header[^>]*>.*?</header>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<nav[^>]*>.*?</nav>', '', html, flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<footer[^>]*>.*?</footer>', '', html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<br\s*/?>', '\n', html)
    text = re.sub(r'</p>', '\n\n', text)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return fix_mojibake(text.strip())


def _fmt(d: date) -> str:
    return d.strftime("%d/%m/%Y")


def search_documents(doc_type: str, date_from: str, date_to: str, page: int = 1) -> Optional[str]:
    """Search normas.gba.gob.ar with filters. Returns HTML response."""
    params = {
        "q[terms][raw_type]": doc_type,
        "q[date_ranges][publication_date][gte]": date_from,
        "q[date_ranges][publication_date][lte]": date_to,
        "q[sort]": "by_publication_date_desc",
        "page": page,
    }
    resp = _http_get(f"{SEARCH_URL}?{urlencode(params)}")
    return resp.text if resp else None


def extract_result_urls(html: str) -> list:
    """Extract document detail URLs from search results HTML."""
    # Pattern: /ar-b/{type}/{year}/{number}/{hash}
    urls = re.findall(r'href="(/ar-b/[^"]+)"', html)
    seen = set()
    unique = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            unique.append(u)
    return unique


def count_results(html: str) -> int:
    """Extract total result count from search page."""
    match = re.search(r'(\d+)\s+resultado', html)
    if match:
        return int(match.group(1))
    return 0


def _field(html: str, label: str) -> Optional[str]:
    """Read a `<span class=field-name>Label:</span><span class=field-info>V</span>` pair."""
    m = re.search(
        re.escape(label) + r'\s*:?\s*(?:</span>\s*)?(?:<[^>]+>\s*)*([^<]+)',
        html,
    )
    if m:
        value = fix_mojibake(unescape(m.group(1)).strip())
        return value or None
    return None


def extract_document_metadata(html: str, url: str) -> dict:
    """Extract metadata from a document detail page."""
    meta = {"url": url}

    title_match = re.search(r'<h1[^>]*>(.*?)</h1>', html, re.DOTALL)
    if title_match:
        meta["title"] = clean_html(title_match.group(1)).strip()

    summary_match = re.search(r'<blockquote[^>]*>(.*?)</blockquote>', html, re.DOTALL)
    if summary_match:
        meta["summary"] = clean_html(summary_match.group(1)).strip()

    # The value sits in a sibling <span>, so the old `[^<]*<[^>]*>` pattern never
    # matched and every record fell back to a synthetic {year}-01-01 date.
    for key, label in (
        ("publication_date", "Fecha de publicación"),
        ("promulgation_date", "Fecha de promulgación"),
        ("sanction_date", "Fecha de sanción"),
        ("boletin_oficial", "Número de Boletín Oficial"),
    ):
        value = _field(html, label)
        if value:
            meta[key] = value

    meta["document_links"] = re.findall(r'href="(/documentos/[^"]+\.html)"', html)
    meta["pdf_links"] = re.findall(r'href="(/documentos/[^"]+\.pdf)"', html)

    return meta


def fetch_full_text(doc_path: str) -> Optional[str]:
    """Fetch full text from a /documentos/*.html path."""
    resp = _http_get(urljoin(BASE_URL, doc_path))
    if not resp:
        return None
    # Charset is pinned in `_http_get`/`_force_utf8` — this path is the one the
    # host serves without a charset (see #1406).
    content = resp.text

    body_match = re.search(r'<body[^>]*>(.*?)</body>', content, re.DOTALL | re.IGNORECASE)
    if body_match:
        return clean_html(body_match.group(1))
    return clean_html(content)


def fetch_pdf_text(doc_path: str) -> Optional[str]:
    """Fallback for detail pages that only link a scanned/born-digital PDF."""
    try:
        from common.pdf_extract import extract_pdf_markdown
    except Exception:
        return None
    resp = _http_get(urljoin(BASE_URL, doc_path))
    if not resp or len(resp.content) < 500:
        return None
    try:
        return extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=doc_path,
            pdf_bytes=resp.content,
            table="legislation",
        )
    except Exception as exc:
        logger.warning(f"  PDF extraction failed for {doc_path}: {exc}")
        return None


def parse_date_ar(date_str: str) -> Optional[str]:
    """Parse Argentine date format (DD/MM/YYYY) to ISO 8601."""
    if not date_str:
        return None
    for fmt in ["%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"]:
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def extract_type_and_number(url_path: str) -> tuple:
    """Extract document type, year, number from URL path like /ar-b/ley/2024/15513/xxx."""
    parts = url_path.strip("/").split("/")
    if len(parts) >= 4:
        return parts[1], parts[2], parts[3]
    return "", "", ""


def normalize(meta: dict, full_text: str) -> dict:
    """Transform scraped data to standard schema."""
    url_path = meta.get("url", "")
    doc_type, year, number = extract_type_and_number(url_path)

    title = meta.get("title", "")
    if not title:
        label = doc_type.replace("-", " ").title()
        title = f"{label} {number}/{year}" if number and year else url_path

    date_iso = (
        parse_date_ar(meta.get("publication_date", ""))
        or parse_date_ar(meta.get("promulgation_date", ""))
        or parse_date_ar(meta.get("sanction_date", ""))
    )
    if not date_iso and year.isdigit():
        date_iso = f"{year}-01-01"

    doc_id = f"ar-b-{doc_type}-{year}-{number}".lower()
    doc_id = re.sub(r'[^a-z0-9-]', '-', doc_id)

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": full_text,
        "date": date_iso,
        "url": urljoin(BASE_URL, url_path) if url_path.startswith("/") else url_path,
        "document_type": doc_type,
        "number": number,
        "year": year,
        "summary": meta.get("summary", ""),
        "boletin_oficial": meta.get("boletin_oficial", ""),
    }


def fetch_document(detail_path: str) -> Optional[dict]:
    """Fetch a single document: detail page + full text."""
    resp = _http_get(urljoin(BASE_URL, detail_path))
    if not resp:
        logger.warning(f"Failed to fetch document {detail_path}")
        return None

    meta = extract_document_metadata(resp.text, detail_path)

    full_text = ""
    for doc_link in meta.get("document_links", []):
        text = fetch_full_text(doc_link)
        if text and len(text) > 50:
            full_text = text
            break

    if not full_text:
        for pdf_link in meta.get("pdf_links", []):
            text = fetch_pdf_text(pdf_link)
            if text and len(text) > 50:
                full_text = text
                break

    if not full_text:
        logger.warning(f"No full text found for {detail_path}")
        return None

    return normalize(meta, full_text)


# ---------------------------------------------------------------------------
# Date-axis partitioning + checkpoint
# ---------------------------------------------------------------------------
def _load_checkpoint() -> dict:
    if CHECKPOINT_PATH.exists():
        try:
            with open(CHECKPOINT_PATH, encoding="utf-8") as f:
                data = json.load(f)
            data.setdefault("partitions", {})
            data.setdefault("done", {})
            return data
        except Exception as exc:
            logger.warning(f"Ignoring unreadable checkpoint ({exc})")
    return {"partitions": {}, "done": {}}


def _save_checkpoint(ckpt: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = CHECKPOINT_PATH.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(ckpt, f)
    tmp.replace(CHECKPOINT_PATH)


def partition_type(doc_type: str, lo: date, hi: date) -> list:
    """Split [lo, hi] until every window holds <= WINDOW_CAP hits.

    Binary subdivision, so a type with nothing in a century costs one request
    instead of the ~1,900 empty weekly probes that made the DecreeLaw phase
    look wedged for 18 minutes with no log output (#1364).
    """
    windows = []
    stack = [(lo, hi)]
    probes = 0

    while stack:
        a, b = stack.pop()
        html = search_documents(doc_type, _fmt(a), _fmt(b))
        probes += 1
        if html is None:
            logger.warning(f"  partition probe failed for {doc_type} {_fmt(a)}-{_fmt(b)}; skipping")
            continue

        n = count_results(html)
        if n == 0:
            continue

        if n <= WINDOW_CAP or a == b:
            if n > WINDOW_CAP:
                logger.warning(
                    f"  {doc_type} {_fmt(a)}: {n} hits on a single day exceeds the "
                    f"{WINDOW_CAP} result window — {n - WINDOW_CAP} unreachable"
                )
            windows.append([a.isoformat(), b.isoformat(), n])
            continue

        mid = a + (b - a) // 2
        stack.append((mid + timedelta(days=1), b))
        stack.append((a, mid))

    windows.sort()
    total = sum(w[2] for w in windows)
    logger.info(
        f"  partitioned {doc_type} into {len(windows)} windows "
        f"({total} documents) using {probes} probes"
    )
    return windows


def _window_urls(doc_type: str, date_from: str, date_to: str, expected: int) -> list:
    """Collect every detail URL in one window, walking its result pages."""
    urls = []
    seen = set()
    total_pages = min((expected + PAGE_SIZE - 1) // PAGE_SIZE, MAX_PAGES) or 1
    for page in range(1, total_pages + 1):
        html = search_documents(doc_type, date_from, date_to, page=page)
        if html is None:
            logger.warning(f"  page {page} unavailable for {doc_type} {date_from}-{date_to}")
            continue
        for u in extract_result_urls(html):
            if u not in seen:
                seen.add(u)
                urls.append(u)
    return urls


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Yield all documents. If sample=True, fetch only ~15 recent Leyes."""
    count = 0
    sample_limit = 15

    if sample:
        urls = _window_urls("Law", "01/01/2024", "31/12/2024", (sample_limit + 5) * 2)
        if not urls:
            raise RuntimeError("normas.gba.gob.ar search returned no result URLs")
        for detail_url in urls:
            doc = fetch_document(detail_url)
            if doc and doc.get("text"):
                count += 1
                yield doc
                logger.info(f"  [{count}] {doc['title'][:70]} ({len(doc['text'])} chars)")
                if count >= sample_limit:
                    return
        return

    ckpt = _load_checkpoint()
    today = date.today()
    seen_ids = set()

    for doc_type in DOCUMENT_TYPES:
        label = TYPE_LABELS.get(doc_type, doc_type)
        done = set(ckpt["done"].get(doc_type, []))

        windows = ckpt["partitions"].get(doc_type)
        if windows is None:
            logger.info(f"Partitioning type: {doc_type} ({label})")
            windows = partition_type(doc_type, FIRST_DAY, today)
            ckpt["partitions"][doc_type] = windows
            _save_checkpoint(ckpt)

        remaining = len(windows) - len(done)
        logger.info(
            f"Type {doc_type} ({label}): {len(windows)} windows, "
            f"{len(done)} already done, {remaining} to crawl"
        )

        for idx, (date_from_iso, date_to_iso, expected) in enumerate(windows):
            if idx in done:
                continue

            date_from = _fmt(date.fromisoformat(date_from_iso))
            date_to = _fmt(date.fromisoformat(date_to_iso))
            logger.info(
                f"  [{doc_type} {idx + 1}/{len(windows)}] "
                f"{date_from}-{date_to}: {expected} results"
            )

            urls = _window_urls(doc_type, date_from, date_to, expected)
            for detail_url in urls:
                doc = fetch_document(detail_url)
                if not doc or not doc.get("text"):
                    continue
                if doc["_id"] in seen_ids:
                    continue
                seen_ids.add(doc["_id"])
                count += 1
                yield doc
                if count % 25 == 0:
                    logger.info(f"  ... {count} documents written")

            done.add(idx)
            ckpt["done"][doc_type] = sorted(done)
            _save_checkpoint(ckpt)

    logger.info(f"Total documents fetched: {count}")


def fetch_updates(since: Optional[str] = None) -> Generator[dict, None, None]:
    """Yield documents published since `since` (ISO date), default last 60 days."""
    start = date.fromisoformat(since) if since else date.today() - timedelta(days=60)
    today = date.today()
    for doc_type in DOCUMENT_TYPES:
        for date_from_iso, date_to_iso, expected in partition_type(doc_type, start, today):
            urls = _window_urls(
                doc_type,
                _fmt(date.fromisoformat(date_from_iso)),
                _fmt(date.fromisoformat(date_to_iso)),
                expected,
            )
            for detail_url in urls:
                doc = fetch_document(detail_url)
                if doc and doc.get("text"):
                    yield doc


def save_sample(records: list) -> None:
    """Save sample records to sample/ directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    for i, record in enumerate(records):
        path = SAMPLE_DIR / f"record_{i+1:03d}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
    logger.info(f"Saved {len(records)} sample records to {SAMPLE_DIR}")


def test_api() -> bool:
    """Test connectivity to normas.gba.gob.ar."""
    try:
        resp = _http_get(BASE_URL)
        if not resp:
            logger.error("Homepage unreachable")
            return False
        logger.info(f"Homepage: HTTP {resp.status_code}, {len(resp.text)} bytes")

        html = search_documents("Law", "01/01/2024", "31/01/2024")
        if html is None:
            logger.error("Search unreachable")
            return False
        n = count_results(html)
        urls = extract_result_urls(html)
        logger.info(f"Search test: {n} results, {len(urls)} detail URLs extracted")

        if urls:
            doc = fetch_document(urls[0])
            if doc:
                logger.info(
                    f"Document test: '{doc['title'][:60]}' — "
                    f"{len(doc.get('text', ''))} chars, date {doc.get('date')}"
                )
                return bool(doc.get("date"))
            logger.warning("Document test: failed to get full text")
            return False

        return n > 0
    except Exception as e:
        logger.error(f"API test failed: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="AR/NormasGBA data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "update", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (~15 records)")
    parser.add_argument("--full", action="store_true", help="Full bootstrap (all records)")
    parser.add_argument("--since", help="ISO date for `update`")
    args = parser.parse_args()

    if args.command == "test-api":
        sys.exit(0 if test_api() else 1)

    if args.command == "update":
        stream = fetch_updates(args.since)
    else:
        # `bootstrap-fast` is what the fleet wrapper invokes; it means the full
        # corpus, not the 15-record sample path.
        is_sample = args.sample and args.command == "bootstrap"
        stream = fetch_all(sample=is_sample)

    if args.command == "bootstrap" and args.sample:
        records = list(stream)
        if not records:
            logger.error("No records fetched!")
            sys.exit(1)
        save_sample(records)
        texts = [r for r in records if r.get("text") and len(r["text"]) > 50]
        logger.info(f"Records with full text: {len(texts)}/{len(records)}")
        if texts:
            logger.info(f"Average text length: {sum(len(r['text']) for r in texts) // len(texts)} chars")
        return

    # Full path streams to data/records.jsonl so a crash keeps everything
    # written so far — the 4,700 salvaged Leyes in #1364 came from this file.
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    written = 0
    with open(RECORDS_PATH, "a", encoding="utf-8") as out:
        for doc in stream:
            out.write(json.dumps(doc, ensure_ascii=False) + "\n")
            out.flush()
            written += 1

    logger.info(f"Bootstrap complete: {written} records -> {RECORDS_PATH}")
    if written == 0:
        logger.error("No records written!")
        sys.exit(1)


if __name__ == "__main__":
    main()
