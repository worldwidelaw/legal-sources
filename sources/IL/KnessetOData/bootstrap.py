#!/usr/bin/env python3
"""
Israel Knesset OData API — Legislation Fetcher

Fetches Israeli primary and secondary legislation from the Knesset OData
service, downloads the official gazette (Reshumot) PDFs published on
fs.knesset.gov.il, and extracts full Hebrew text.

Entity model (this is where the previous implementation went wrong, #1438)
-------------------------------------------------------------------------
The service exposes two *different* identifier spaces:

* ``KNS_IsraelLaw.IsraelLawID`` (2,024 rows, IDs 2000001+) — the *consolidated*
  law, i.e. "the Telecommunications Law" as an abstract, still-amended entity.
  It carries no documents of its own.
* ``KNS_Law.LawID`` (61,348 rows, IDs 2001428+) — the individual legislative
  *act*: an original law, an amendment, a piece of secondary legislation, a
  statutory report. This is what actually gets published in Reshumot, and it is
  what ``KNS_DocumentLaw.LawID`` points at.

The old code filtered ``KNS_DocumentLaw`` by ``IsraelLawID`` values. Those IDs
never appear in that column, so every request returned ``{"value": []}`` with
HTTP 200 — the crawl enumerated nothing and exited without an error to show
for it. ``KNS_DocumentIsraelLaw`` looks like the missing join table but is
empty (verified: ``/$count`` returns 0).

The correct traversal is therefore document-first:

    KNS_DocumentLaw (FilePath) -> KNS_Law (title, publication date)
                              \\-> KNS_LawBinding -> KNS_IsraelLaw (parent law)

Driving from ``KNS_DocumentLaw`` also guarantees every emitted record has a
file behind it, rather than walking 2,024 consolidated laws of which most have
no document at all.

Hebrew text direction
---------------------
pdfminer/pdfplumber emit these PDFs in *visual* order, so every line comes out
character-reversed (``קוח`` instead of ``חוק``) and nothing downstream can match
it. PyMuPDF applies bidi reordering and returns logical order, so it is the
primary backend here; the pdfminer fallback (for hosts without PyMuPDF) repairs
the reversal from the line level.

Data source: https://knesset.gov.il/Odata/ParliamentInfo.svc
License: Open Government Data (Israel)
"""

import io
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Iterable, Optional

import requests

ODATA_BASE = "https://knesset.gov.il/Odata/ParliamentInfo.svc"
LAWS_ENDPOINT = f"{ODATA_BASE}/KNS_Law"
ISRAEL_LAWS_ENDPOINT = f"{ODATA_BASE}/KNS_IsraelLaw"
BINDING_ENDPOINT = f"{ODATA_BASE}/KNS_LawBinding"
DOCS_ENDPOINT = f"{ODATA_BASE}/KNS_DocumentLaw"

# The service caps a page at 100 rows regardless of what $top asks for.
PAGE_SIZE = 100

# Lowest LawID that carries documents; used to skip the legacy ID space when
# bulk-loading the join tables.
MIN_DOC_LAW_ID = 2001428

ODATA_DELAY = 0.3
PDF_DELAY = 1.0

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal research; open data collection)",
    "Accept": "application/json",
}

HEBREW_RE = re.compile(r"[֐-׿]")


class KnessetODataError(RuntimeError):
    """Raised when the OData service stops answering the way we expect."""


# knesset.gov.il answers a rate limit with a non-standard 481 rather than 429,
# so requests' own retry heuristics never see it as retryable.
RATE_LIMIT_STATUSES = (429, 481)
RETRY_STATUSES = RATE_LIMIT_STATUSES + (500, 502, 503, 504)
MAX_ATTEMPTS = 7
BACKOFF_CAP = 120.0

# Steady-state pace between OData calls. Raised whenever the service rate-limits
# us and relaxed again after a stretch of clean responses, so a long crawl
# settles just under whatever ceiling the service is enforcing that day.
_pace = {"odata_delay": ODATA_DELAY, "clean_streak": 0}
PACE_CEILING = 8.0
RELAX_AFTER = 200


def _note_rate_limited() -> None:
    _pace["odata_delay"] = min(_pace["odata_delay"] * 2 or 0.3, PACE_CEILING)
    _pace["clean_streak"] = 0
    print(f"    rate limited — OData pace now {_pace['odata_delay']:.1f}s",
          file=sys.stderr)


def _note_clean() -> None:
    _pace["clean_streak"] += 1
    if _pace["clean_streak"] >= RELAX_AFTER and _pace["odata_delay"] > ODATA_DELAY:
        _pace["odata_delay"] = max(ODATA_DELAY, _pace["odata_delay"] / 2)
        _pace["clean_streak"] = 0
        print(f"    pace relaxed to {_pace['odata_delay']:.1f}s", file=sys.stderr)


def odata_sleep() -> None:
    time.sleep(_pace["odata_delay"])


def _request(url: str, *, params: Optional[dict] = None,
             headers: Optional[dict] = None, timeout: int = 60):
    """GET with backoff on rate limits (429/481) and transient 5xx.

    Raises the last error once the attempts are exhausted, so a genuine outage
    still fails loud instead of silently truncating the corpus.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(MAX_ATTEMPTS):
        try:
            resp = requests.get(url, params=params, headers=headers or HEADERS,
                                timeout=timeout)
            if resp.status_code in RETRY_STATUSES:
                if resp.status_code in RATE_LIMIT_STATUSES:
                    _note_rate_limited()
                retry_after = resp.headers.get("Retry-After")
                try:
                    wait = float(retry_after) if retry_after else None
                except ValueError:
                    wait = None
                if wait is None:
                    wait = min(BACKOFF_CAP, 2.0 * (2 ** attempt))
                if attempt == MAX_ATTEMPTS - 1:
                    resp.raise_for_status()
                print(f"    HTTP {resp.status_code} from {url} — retrying in "
                      f"{wait:.0f}s (attempt {attempt + 1}/{MAX_ATTEMPTS})",
                      file=sys.stderr)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            _note_clean()
            return resp
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as exc:
            last_exc = exc
            if attempt == MAX_ATTEMPTS - 1:
                raise
            wait = min(BACKOFF_CAP, 2.0 * (2 ** attempt))
            print(f"    {type(exc).__name__} from {url} — retrying in {wait:.0f}s "
                  f"(attempt {attempt + 1}/{MAX_ATTEMPTS})", file=sys.stderr)
            time.sleep(wait)
    raise last_exc or KnessetODataError(f"exhausted retries for {url}")


def odata_get(url: str, params: Optional[dict] = None) -> dict:
    """Make an OData request and return the JSON response."""
    params = dict(params or {})
    params["$format"] = "json"
    return _request(url, params=params).json()


def odata_count(url: str, filter_expr: Optional[str] = None) -> int:
    params = {"$filter": filter_expr} if filter_expr else None
    # $count answers text/plain; asking for application/json gets a 415.
    headers = dict(HEADERS, Accept="text/plain")
    resp = _request(f"{url}/$count", params=params, headers=headers)
    return int(resp.text.strip())


def odata_pages(url: str, params: Optional[dict] = None,
                page_size: int = PAGE_SIZE) -> Generator[list, None, None]:
    """Yield successive pages of an entity set."""
    skip = 0
    while True:
        page_params = dict(params or {})
        page_params.update({"$top": str(page_size), "$skip": str(skip)})
        rows = odata_get(url, page_params).get("value", [])
        if not rows:
            return
        yield rows
        if len(rows) < page_size:
            return
        skip += len(rows)
        odata_sleep()


# ---------------------------------------------------------------------------
# Text extraction
# ---------------------------------------------------------------------------

def _repair_visual_rtl(text: str) -> str:
    """Undo visual-order glyph emission for a Hebrew text layer.

    Extractors that do not apply bidi reordering emit each line right-to-left,
    so the line reads backwards. Reversing the line restores logical order; any
    embedded Latin/digit run (dates, section numbers, "2024") was already
    logical inside the reversed line, so it has to be flipped back.
    """
    out = []
    for line in text.split("\n"):
        if not HEBREW_RE.search(line):
            out.append(line)
            continue
        flipped = line[::-1]
        flipped = re.sub(r"[0-9A-Za-z]+", lambda m: m.group(0)[::-1], flipped)
        out.append(flipped)
    return "\n".join(out)


def _extract_with_fitz(pdf_bytes: bytes) -> Optional[str]:
    try:
        import fitz  # PyMuPDF
    except ImportError:
        return None
    try:
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            return "\n".join(page.get_text() for page in doc).strip()
    except Exception as exc:  # pragma: no cover - corrupt PDFs
        print(f"    fitz failed: {exc}", file=sys.stderr)
        return None


def _extract_with_pdfminer(pdf_bytes: bytes) -> Optional[str]:
    try:
        from pdfminer.high_level import extract_text
    except ImportError:
        return None
    try:
        raw = extract_text(io.BytesIO(pdf_bytes)).strip()
    except Exception as exc:  # pragma: no cover - corrupt PDFs
        print(f"    pdfminer failed: {exc}", file=sys.stderr)
        return None
    # pdfminer has no bidi pass, so a Hebrew page arrives reversed.
    return _repair_visual_rtl(raw)


def extract_pdf_text(pdf_bytes: bytes) -> Optional[str]:
    """Extract logical-order text from a Knesset gazette PDF."""
    if len(pdf_bytes) < 100 or not pdf_bytes[:5].startswith(b"%PDF"):
        return None
    text = _extract_with_fitz(pdf_bytes)
    if not text or len(text) < 50:
        text = _extract_with_pdfminer(pdf_bytes)
    if not text or len(text) < 50:
        return None
    return text


def download_pdf_text(pdf_url: str) -> Optional[str]:
    """Download a PDF and extract its text content."""
    try:
        resp = _request(pdf_url, headers={"User-Agent": HEADERS["User-Agent"]},
                        timeout=120)
        return extract_pdf_text(resp.content)
    except Exception as exc:
        print(f"    PDF fetch error ({pdf_url}): {exc}", file=sys.stderr)
        return None


# ---------------------------------------------------------------------------
# Metadata indexes
# ---------------------------------------------------------------------------

DATA_DIR = Path(__file__).parent / "data"
RECORDS_PATH = DATA_DIR / "records.jsonl"
LAW_CACHE_PATH = DATA_DIR / "kns_law_index.json"
LINK_CACHE_PATH = DATA_DIR / "kns_israel_law_links.json"


def _read_json_cache(path: Path) -> Optional[Any]:
    if not path.exists():
        return None
    try:
        with path.open(encoding="utf-8") as fh:
            return json.load(fh)
    except (ValueError, OSError) as exc:
        print(f"  ignoring unreadable cache {path.name}: {exc}", file=sys.stderr)
        return None


def _write_json_cache(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False)
    tmp.replace(path)


class LawIndex:
    """LawID -> act metadata, resolved lazily or bulk-loaded up front.

    The bulk load is ~614 paged requests and used to be redone from scratch on
    every restart, which is where #1443 died: the run was rate-limited partway
    through the index and lost everything. The completed table is cached to disk
    so a relaunch resumes past it with no network calls.
    """

    def __init__(self) -> None:
        self._cache: dict[int, dict] = {}
        self._bulk_loaded = False

    def bulk_load(self) -> None:
        """Page the whole KNS_Law table, or reuse the cached copy."""
        if self._bulk_loaded:
            return

        cached = _read_json_cache(LAW_CACHE_PATH)
        if cached:
            self._cache = {int(k): v for k, v in cached.items()}
            self._bulk_loaded = True
            print(f"KNS_Law index: {len(self._cache)} acts (from cache)",
                  file=sys.stderr)
            return

        print("Loading KNS_Law index...", file=sys.stderr)
        loaded = 0
        for rows in odata_pages(LAWS_ENDPOINT,
                                {"$filter": f"LawID ge {MIN_DOC_LAW_ID}",
                                 "$orderby": "LawID"}):
            for row in rows:
                self._cache[row["LawID"]] = row
            loaded += len(rows)
            if loaded % 5000 == 0:
                print(f"  {loaded} acts indexed", file=sys.stderr)
        self._bulk_loaded = True
        _write_json_cache(LAW_CACHE_PATH,
                          {str(k): v for k, v in self._cache.items()})
        print(f"KNS_Law index: {loaded} acts", file=sys.stderr)

    def get(self, law_id: int) -> Optional[dict]:
        if law_id in self._cache:
            return self._cache[law_id]
        if self._bulk_loaded:
            return None
        odata_sleep()
        rows = odata_get(LAWS_ENDPOINT, {"$filter": f"LawID eq {law_id}"}).get("value", [])
        row = rows[0] if rows else None
        self._cache[law_id] = row
        return row


def load_israel_law_links() -> tuple[dict[int, int], dict[int, dict]]:
    """Return (LawID -> IsraelLawID, IsraelLawID -> consolidated-law metadata)."""
    cached = _read_json_cache(LINK_CACHE_PATH)
    if cached:
        law_to_israel = {int(k): v for k, v in cached["law_to_israel"].items()}
        israel_laws = {int(k): v for k, v in cached["israel_laws"].items()}
        print(f"  {len(law_to_israel)} act->law links, {len(israel_laws)} "
              "consolidated laws (from cache)", file=sys.stderr)
        return law_to_israel, israel_laws

    print("Loading KNS_LawBinding / KNS_IsraelLaw...", file=sys.stderr)
    law_to_israel: dict[int, int] = {}
    for rows in odata_pages(BINDING_ENDPOINT,
                            {"$filter": f"LawID ge {MIN_DOC_LAW_ID}",
                             "$orderby": "LawBindingID"}):
        for row in rows:
            if row.get("IsraelLawID"):
                law_to_israel.setdefault(row["LawID"], row["IsraelLawID"])

    israel_laws: dict[int, dict] = {}
    for rows in odata_pages(ISRAEL_LAWS_ENDPOINT, {"$orderby": "IsraelLawID"}):
        for row in rows:
            israel_laws[row["IsraelLawID"]] = row

    _write_json_cache(LINK_CACHE_PATH, {
        "law_to_israel": {str(k): v for k, v in law_to_israel.items()},
        "israel_laws": {str(k): v for k, v in israel_laws.items()},
    })
    print(f"  {len(law_to_israel)} act->law links, {len(israel_laws)} consolidated laws",
          file=sys.stderr)
    return law_to_israel, israel_laws


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

def _iso_date(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    return value.split("T")[0] or None


def normalize(doc: dict, act: Optional[dict], israel_law: Optional[dict],
              text: str) -> dict:
    act = act or {}
    israel_law = israel_law or {}

    title = act.get("Name") or israel_law.get("Name") or doc.get("GroupTypeDesc") \
        or f"Knesset document {doc['DocumentLawID']}"

    # PublicationDate is the gazette date and is the right temporal key; a few
    # administrative acts carry none, so fall back to the record's own dates
    # rather than emitting null (a null date fails validation downstream).
    date = (_iso_date(act.get("PublicationDate"))
            or _iso_date(israel_law.get("PublicationDate"))
            or _iso_date(doc.get("LastUpdatedDate"))
            or _iso_date(act.get("LastUpdatedDate")))

    return {
        "_id": f"il-knesset-doc-{doc['DocumentLawID']}",
        "_source": "IL/KnessetOData",
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date,
        "url": doc["FilePath"],
        "document_id": str(doc["DocumentLawID"]),
        "law_id": doc["LawID"],
        "law_type": act.get("TypeDesc"),
        "law_subtype": act.get("SubTypeDesc"),
        "document_group": doc.get("GroupTypeDesc"),
        "knesset_num": act.get("KnessetNum") or israel_law.get("KnessetNum"),
        "publication_series": act.get("PublicationSeriesDesc"),
        "gazette_number": act.get("MagazineNumber"),
        "gazette_page": act.get("PageNumber"),
        "israel_law_id": israel_law.get("IsraelLawID"),
        "israel_law_name": israel_law.get("Name"),
        "is_basic_law": israel_law.get("IsBasicLaw"),
        "validity": israel_law.get("LawValidityDesc"),
        "last_updated": _iso_date(doc.get("LastUpdatedDate")),
    }


# ---------------------------------------------------------------------------
# Crawl
# ---------------------------------------------------------------------------

def _is_pdf(doc: dict) -> bool:
    path = (doc.get("FilePath") or "").lower()
    return path.endswith(".pdf")


def already_written_ids(path: Path = RECORDS_PATH) -> set:
    """DocumentLawIDs already present in records.jsonl from an earlier attempt."""
    if not path.exists():
        return set()
    done = set()
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                done.add(json.loads(line)["document_id"])
            except (ValueError, KeyError):
                continue
    return done


def fetch_all(limit: Optional[int] = None,
              since: Optional[str] = None,
              skip_ids: Optional[set] = None) -> Generator[dict, None, None]:
    """Yield normalized legislation documents with full text.

    ``since`` filters on the document row's LastUpdatedDate, which is what the
    Knesset service touches when a file is (re)published.
    """
    total_docs = odata_count(DOCS_ENDPOINT)
    if total_docs == 0:
        raise KnessetODataError(
            f"KNS_DocumentLaw reported 0 rows at {DOCS_ENDPOINT} — the OData "
            "service is unreachable or its schema changed (expected ~10,600)."
        )
    print(f"KNS_DocumentLaw holds {total_docs} documents", file=sys.stderr)

    law_to_israel, israel_laws = load_israel_law_links()
    law_index = LawIndex()
    if limit is None:
        law_index.bulk_load()

    doc_params = {"$orderby": "DocumentLawID"}
    if since:
        doc_params["$filter"] = f"LastUpdatedDate ge datetime'{since}T00:00:00'"

    skip_ids = skip_ids or set()
    seen = 0
    pdf_seen = 0
    emitted = 0
    resumed = 0

    for rows in odata_pages(DOCS_ENDPOINT, doc_params):
        for doc in rows:
            seen += 1
            if not _is_pdf(doc):
                continue
            pdf_seen += 1

            # Already downloaded on an earlier attempt: skip without touching
            # the network, so a relaunch after a rate-limit kill advances.
            if str(doc["DocumentLawID"]) in skip_ids:
                resumed += 1
                if resumed % 500 == 0:
                    print(f"  resumed past {resumed} already-written documents",
                          file=sys.stderr)
                continue

            time.sleep(PDF_DELAY)
            text = download_pdf_text(doc["FilePath"])
            if not text:
                print(f"  [{pdf_seen}] no text: {doc['FilePath']}", file=sys.stderr)
                continue

            act = law_index.get(doc["LawID"])
            israel_law = israel_laws.get(law_to_israel.get(doc["LawID"], -1))
            record = normalize(doc, act, israel_law, text)

            emitted += 1
            print(f"  [{emitted}/{pdf_seen}] {record['title'][:70]} "
                  f"({len(text)} chars)", file=sys.stderr)
            yield record

            if limit and emitted >= limit:
                print(f"Reached limit={limit}", file=sys.stderr)
                return

    print(f"\nTotal: {seen} documents scanned, {pdf_seen} PDFs, {emitted} with text"
          + (f", {resumed} already written" if resumed else ""),
          file=sys.stderr)

    if emitted == 0 and not resumed:
        raise KnessetODataError(
            f"Scanned {seen} KNS_DocumentLaw rows ({pdf_seen} PDFs) but extracted "
            "no text — fs.knesset.gov.il is refusing this vantage or the PDF "
            "backends are missing."
        )


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """Yield documents whose Knesset record changed on/after ``since`` (YYYY-MM-DD)."""
    yield from fetch_all(since=since)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _write_records(records: Iterable[dict], out_path: Path,
                   append: bool = False) -> int:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with out_path.open("a" if append else "w", encoding="utf-8") as fh:
        for record in records:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            fh.flush()
            count += 1
    return count


def bootstrap_sample(num_records: int = 15) -> int:
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    count = 0
    for record in fetch_all(limit=num_records):
        (sample_dir / f"{record['_id']}.json").write_text(
            json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
        count += 1

    print(f"\nSample complete: {count} documents saved to {sample_dir}",
          file=sys.stderr)
    return count


def bootstrap_full(since: Optional[str] = None) -> int:
    """Full crawl, resuming from whatever a killed earlier attempt already wrote.

    The old behaviour opened records.jsonl with "w", so the #1443 run — which
    was rate-limited during the KNS_Law index build, before any document was
    emitted — left a truncated, empty file and the pipeline fell back to the 15
    committed samples.
    """
    done = already_written_ids()
    if done:
        print(f"Resuming: {len(done)} documents already in {RECORDS_PATH.name}",
              file=sys.stderr)
    count = _write_records(fetch_all(since=since, skip_ids=done),
                           RECORDS_PATH, append=bool(done))
    total = len(done) + count
    print(f"\nbootstrap complete: {count} new records written to {RECORDS_PATH} "
          f"({total} total)", file=sys.stderr)
    return total


def run_test() -> int:
    """Connectivity probe: does the OData service still answer as expected?"""
    docs = odata_count(DOCS_ENDPOINT)
    laws = odata_count(LAWS_ENDPOINT)
    print(f"KNS_DocumentLaw: {docs} rows")
    print(f"KNS_Law: {laws} rows")
    page = odata_get(DOCS_ENDPOINT, {"$top": "50", "$orderby": "DocumentLawID desc"})
    pdfs = [d for d in page.get("value", []) if _is_pdf(d)]
    print(f"Newest page: {len(page.get('value', []))} rows, {len(pdfs)} PDFs")
    if not pdfs:
        print("FAIL: no PDF documents on the newest page")
        return 1
    # A minority of secondary-law files are image-only scans with no text layer,
    # so probing a single PDF gives a false FAIL depending on what happens to be
    # newest. Try a handful and pass on the first that yields text.
    for candidate in pdfs[:8]:
        text = download_pdf_text(candidate["FilePath"])
        print(f"Sample PDF {candidate['FilePath']} -> {len(text or '')} chars")
        if text:
            print("PASS")
            return 0
        time.sleep(PDF_DELAY)
    print("FAIL: none of the probed PDFs yielded text")
    return 1


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="IL/KnessetOData data fetcher")
    parser.add_argument("command",
                        choices=["bootstrap", "bootstrap-fast", "update", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample data only")
    parser.add_argument("--full", action="store_true",
                        help="Full corpus (default for bootstrap/bootstrap-fast)")
    parser.add_argument("--since", default=None,
                        help="Only documents updated on/after YYYY-MM-DD")
    parser.add_argument("--limit", type=int, default=None,
                        help="Stop after N records")
    args = parser.parse_args()

    if args.command == "test":
        sys.exit(run_test())

    if args.command == "update":
        if not args.since:
            parser.error("update requires --since YYYY-MM-DD")
        out = Path(__file__).parent / "data" / "records.jsonl"
        n = _write_records(fetch_updates(args.since), out)
        print(f"\nupdate complete: {n} records written to {out}", file=sys.stderr)
        sys.exit(0 if n else 1)

    # bootstrap / bootstrap-fast
    if args.sample:
        n = bootstrap_sample()
    else:
        n = bootstrap_full(since=args.since)
    sys.exit(0 if n else 1)
