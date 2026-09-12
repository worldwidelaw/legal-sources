#!/usr/bin/env python3
"""
DE/BASE -- Bundesamt für die Sicherheit der nuklearen Entsorgung (BASE)

Fetches the *Amtliches Dokumentenverzeichnis* — the official document register
BASE is required to publish under § 6 Standortauswahlgesetz (StandAG) so that
the search for a deep geological repository for high-level radioactive waste
stays traceable. It holds ~775 documents authored by BASE (the federal nuclear
waste safety regulator) and by the Bundesgesellschaft für Endlagerung (BGE),
the implementing body:

  - Gutachten           (expert opinions)
  - Stellungnahmen      (formal statements / supervisory positions)
  - Berichte            (reports)
  - Konzepte            (concepts)
  - Rechtsquellen       (legal sources)
  - Protokolle          (minutes of supervisory meetings)
  - Korrespondenz       (regulatory correspondence)
  - Präsentationen, parlamentarische Dokumente

Source:    https://www.base.bund.de/
Register:  /SiteGlobals/Forms/Suche/Dokumentenverzeichnis/
Discovery: paginated Government Site Builder result list (50 hits/page)
Content:   every register entry has a detail page plus a born-digital PDF at the
           same path; full text comes from the PDF via common/pdf_extract.

Usage:
    python bootstrap.py bootstrap --sample
    python bootstrap.py bootstrap --full
    python bootstrap.py bootstrap-fast          # alias for --full (fleet wrapper)
    python bootstrap.py updates --since YYYY-MM-DD
    python bootstrap.py validate
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Generator, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"
DATA_DIR = SCRIPT_DIR / "data"
RECORDS_PATH = DATA_DIR / "records.jsonl"
CHECKPOINT_PATH = DATA_DIR / "checkpoint.json"
ROOT_DIR = SCRIPT_DIR.parent.parent.parent

sys.path.insert(0, str(ROOT_DIR))

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SOURCE_ID = "DE/BASE"
BASE_URL = "https://www.base.bund.de"
SEARCH_PATH = (
    "/SiteGlobals/Forms/Suche/Dokumentenverzeichnis/"
    "DokumentenverzeichnisSuche_Formular.html"
)
# The GSB result list is keyed on the content node (nn) and paged through the
# `gtp` parameter; %253D is the double-encoded "=" the CMS expects.
SEARCH_NN = "615470"
PAGE_PARAM = "328868_list%253D"
RESULTS_PER_PAGE = 50
MAX_PAGES = 200

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 LegalDataHunter/1.0"
)
REQUEST_DELAY = 1.5
MIN_TEXT_CHARS = 200

DATE_RE = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------
class BaseRegisterClient:
    """Client for the BASE Amtliches Dokumentenverzeichnis."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
        })

    # -- low level ----------------------------------------------------------
    def _get(self, url: str, attempts: int = 4) -> Optional[requests.Response]:
        delay = 2.0
        for attempt in range(attempts):
            try:
                resp = self.session.get(url, timeout=60)
            except requests.RequestException as exc:
                if attempt == attempts - 1:
                    print(f"       request failed: {exc}", file=sys.stderr)
                    return None
                time.sleep(delay)
                delay = min(delay * 2, 60)
                continue

            if resp.status_code == 200:
                return resp
            if resp.status_code in (429, 500, 502, 503, 504):
                retry_after = resp.headers.get("Retry-After")
                wait = float(retry_after) if retry_after and retry_after.isdigit() else delay
                time.sleep(min(wait, 120))
                delay = min(delay * 2, 60)
                continue
            # 404 and other hard errors are not worth retrying.
            return None
        return None

    def _page_url(self, page: int) -> str:
        url = f"{BASE_URL}{SEARCH_PATH}?nn={SEARCH_NN}&resultsPerPage={RESULTS_PER_PAGE}"
        if page > 1:
            url += f"&gtp={PAGE_PARAM}{page}"
        return url

    # -- discovery ----------------------------------------------------------
    def list_documents(self, max_pages: int = MAX_PAGES) -> List[Dict]:
        """Walk the paginated register and return one dict per entry."""
        docs: List[Dict] = []
        seen: set = set()

        for page in range(1, max_pages + 1):
            resp = self._get(self._page_url(page))
            if resp is None:
                raise RuntimeError(
                    f"{SOURCE_ID}: register page {page} unreachable — "
                    "base.bund.de is refusing this vantage, aborting loudly "
                    "rather than reporting a truncated corpus"
                )

            page_docs = self._parse_result_page(resp.text)
            if not page_docs:
                break

            fresh = [d for d in page_docs if d["id"] not in seen]
            for d in fresh:
                seen.add(d["id"])
            docs.extend(fresh)

            if not fresh:
                # The CMS clamps out-of-range pages to the last one.
                break

            print(f"  page {page}: +{len(fresh)} entries (total {len(docs)})")
            time.sleep(REQUEST_DELAY)

        if not docs:
            raise RuntimeError(
                f"{SOURCE_ID}: register returned 0 entries — layout change or block"
            )
        return docs

    @staticmethod
    def _parse_result_page(html: str) -> List[Dict]:
        soup = BeautifulSoup(html, "html.parser")
        out: List[Dict] = []

        for teaser in soup.select("div.c-teaser-search"):
            doc_id = (teaser.get("aria-labelledby") or "").strip()
            link = teaser.select_one("a.c-teaser-search__link--full") or \
                teaser.select_one("a.c-teaser-search__link")
            if not doc_id or not link or not link.get("href"):
                continue

            detail_url = urljoin(BASE_URL + "/", link["href"].split("?")[0])

            title_el = teaser.select_one("h2.c-teaser-search__title")
            title = title_el.get_text(" ", strip=True) if title_el else ""

            meta_parts = [
                span.get_text(" ", strip=True)
                for span in teaser.select("span.c-teaser-search__headline")
            ]
            meta = " ".join(meta_parts)

            doc_type, topic, publisher = "", "", ""
            if meta_parts:
                head = [p.strip() for p in meta_parts[0].split("|") if p.strip()]
                if head:
                    doc_type = head[0]
                if len(head) > 1:
                    topic = head[1]
            for part in meta_parts[1:]:
                if "Veröffentlicht von" in part:
                    publisher = part.split("Veröffentlicht von", 1)[1].strip(" |")

            date = ""
            m = DATE_RE.search(meta)
            if m:
                date = f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

            out.append({
                "id": doc_id,
                "title": title,
                "url": detail_url,
                "doc_type": doc_type,
                "topic": topic,
                "publisher": publisher,
                "date": date,
            })

        return out

    # -- detail -------------------------------------------------------------
    def load_detail(self, doc: Dict) -> Dict:
        """Add the abstract and the resolved PDF URL to a register entry."""
        detail = dict(doc)
        detail["pdf_url"] = self._derive_pdf_url(doc["url"])
        detail["description"] = ""

        resp = self._get(doc["url"])
        if resp is None:
            return detail

        soup = BeautifulSoup(resp.text, "html.parser")

        intro = soup.select_one("div.c-publication__text") or \
            soup.select_one("div.c-intro") or soup.select_one("p.c-intro__text")
        if intro:
            detail["description"] = intro.get_text(" ", strip=True)

        stem = doc["url"].rsplit("/", 1)[-1].rsplit(".", 1)[0]
        candidates = []
        for a in soup.select('a[href*="__blob=publicationFile"]'):
            href = urljoin(BASE_URL + "/", a["href"])
            if ".pdf" not in href.lower():
                continue
            candidates.append(href)
            if f"/{stem}.pdf" in href:
                detail["pdf_url"] = href
                return detail

        if candidates:
            detail["pdf_url"] = candidates[0]
        return detail

    @staticmethod
    def _derive_pdf_url(detail_url: str) -> str:
        return detail_url.rsplit(".html", 1)[0] + ".pdf?__blob=publicationFile"

    def download_pdf(self, pdf_url: str) -> Optional[bytes]:
        resp = self._get(pdf_url)
        if resp is None:
            return None
        if not resp.content.startswith(b"%PDF"):
            return None
        return resp.content


# ---------------------------------------------------------------------------
# Text extraction
# ---------------------------------------------------------------------------
def _extract_text(pdf_bytes: bytes, doc_id: str) -> str:
    """Extract plain text from a BASE PDF using the shared helper chain."""
    try:
        from common.pdf_extract import extract_pdf_markdown
        text = extract_pdf_markdown(
            SOURCE_ID,
            source_id=doc_id,
            pdf_bytes=pdf_bytes,
            table="doctrine",
            force=True,
        )
        if text:
            return text.strip()
    except (ImportError, TypeError):
        pass

    try:
        import io
        import pdfplumber
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages = []
            for page in pdf.pages:
                pages.append(page.extract_text() or "")
                page.flush_cache()
                page.get_textmap.cache_clear()
        return "\n\n".join(p for p in pages if p).strip()
    except ImportError:
        pass

    try:
        import io
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(pdf_bytes))
        return "\n\n".join(
            (p.extract_text() or "") for p in reader.pages
        ).strip()
    except ImportError:
        pass

    return ""


# ---------------------------------------------------------------------------
# Normalize
# ---------------------------------------------------------------------------
def normalize(doc: Dict, text: str) -> Dict:
    """Transform a register entry plus its PDF text into the standard schema."""
    return {
        "_id": f"DE-BASE-{doc['id']}",
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": doc.get("title", ""),
        "text": text,
        "date": doc.get("date") or None,
        "url": doc.get("url", ""),
        "pdf_url": doc.get("pdf_url", ""),
        "doc_type": doc.get("doc_type", ""),
        "topic": doc.get("topic", ""),
        "publisher": doc.get("publisher", ""),
        "description": doc.get("description", ""),
        "language": "de",
        "jurisdiction": "DE",
    }


# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------
def _load_checkpoint() -> set:
    if not CHECKPOINT_PATH.exists():
        return set()
    try:
        with open(CHECKPOINT_PATH, "r", encoding="utf-8") as fh:
            return set(json.load(fh).get("done", []))
    except (json.JSONDecodeError, OSError):
        return set()


def _save_checkpoint(done: set) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = CHECKPOINT_PATH.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump({"done": sorted(done)}, fh)
    tmp.replace(CHECKPOINT_PATH)


# ---------------------------------------------------------------------------
# Fetch logic
# ---------------------------------------------------------------------------
def _build_record(client: BaseRegisterClient, doc: Dict) -> Optional[Dict]:
    detail = client.load_detail(doc)
    time.sleep(REQUEST_DELAY)

    pdf_bytes = client.download_pdf(detail["pdf_url"])
    if not pdf_bytes:
        return None

    text = _extract_text(pdf_bytes, detail["id"])
    if len(text) < MIN_TEXT_CHARS:
        return None

    return normalize(detail, text)


def fetch_sample(count: int = 15) -> List[Dict]:
    client = BaseRegisterClient()
    print("Listing the BASE Amtliches Dokumentenverzeichnis...")
    docs = client.list_documents(max_pages=1)
    print(f"Found {len(docs)} entries on the first page\n")

    records: List[Dict] = []
    for doc in docs:
        if len(records) >= count:
            break
        print(f"  [{len(records) + 1}/{count}] {doc['title'][:65]}...")
        record = _build_record(client, doc)
        if record is None:
            print("       skipped: no PDF text")
            continue
        records.append(record)
        print(f"       OK: {len(record['text']):,} chars")
        time.sleep(REQUEST_DELAY)

    return records


def fetch_all(since: Optional[str] = None) -> Generator[Dict, None, None]:
    client = BaseRegisterClient()
    print("Listing the BASE Amtliches Dokumentenverzeichnis...")
    docs = client.list_documents()
    print(f"Total register entries: {len(docs)}\n")

    done = _load_checkpoint()
    yielded = 0
    skipped = 0

    for doc in docs:
        if doc["id"] in done:
            continue
        if since and doc.get("date") and doc["date"] < since:
            continue

        record = _build_record(client, doc)
        done.add(doc["id"])

        if record is None:
            skipped += 1
        else:
            yielded += 1
            yield record

        if (yielded + skipped) % 25 == 0:
            _save_checkpoint(done)
            print(f"  progress: {yielded:,} fetched, {skipped} skipped")
        time.sleep(REQUEST_DELAY)

    _save_checkpoint(done)
    print(f"\nTotal: {yielded:,} fetched, {skipped} skipped")


def run_full(since: Optional[str] = None) -> int:
    """Stream the full corpus to data/records.jsonl (what the fleet ingests)."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    count = 0
    with open(RECORDS_PATH, "a", encoding="utf-8") as fh:
        for record in fetch_all(since=since):
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            fh.flush()
            count += 1
    print(f"Wrote {count:,} records to {RECORDS_PATH}")
    return count


# ---------------------------------------------------------------------------
# Save / validate
# ---------------------------------------------------------------------------
def save_samples(records: List[Dict]) -> None:
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    for i, record in enumerate(records):
        with open(SAMPLE_DIR / f"record_{i:04d}.json", "w", encoding="utf-8") as fh:
            json.dump(record, fh, ensure_ascii=False, indent=2)
    with open(SAMPLE_DIR / "all_samples.json", "w", encoding="utf-8") as fh:
        json.dump(records, fh, ensure_ascii=False, indent=2)
    print(f"\nSaved {len(records)} samples to {SAMPLE_DIR}")


def validate_samples() -> bool:
    samples = sorted(SAMPLE_DIR.glob("record_*.json"))
    if len(samples) < 10:
        print(f"FAIL: only {len(samples)} samples, need >= 10")
        return False

    ok = True
    lengths = []
    for path in samples:
        with open(path, "r", encoding="utf-8") as fh:
            rec = json.load(fh)
        text = rec.get("text", "")
        lengths.append(len(text))
        if not text:
            print(f"FAIL: {path.name} missing text")
            ok = False
        for field in ("_id", "_source", "_type", "title", "url"):
            if not rec.get(field):
                print(f"FAIL: {path.name} missing {field}")
                ok = False
        if rec.get("_source") != SOURCE_ID:
            print(f"FAIL: {path.name} _source is {rec.get('_source')!r}")
            ok = False
        if rec.get("date") and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", rec["date"]):
            print(f"FAIL: {path.name} date not ISO 8601: {rec['date']}")
            ok = False
        if text and re.search(r"<[a-z]+[^>]*>", text, re.IGNORECASE):
            print(f"WARN: {path.name} may contain HTML tags")

    print("\nValidation:")
    print(f"  Samples:  {len(samples)}")
    print(f"  Avg text: {sum(lengths) / len(lengths):,.0f} chars")
    print(f"  Min text: {min(lengths):,} chars")
    print(f"  Max text: {max(lengths):,} chars")
    print(f"  Valid:    {ok}")
    return ok


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="DE/BASE fetcher")
    sub = parser.add_subparsers(dest="command")

    bp = sub.add_parser("bootstrap", help="Initial data fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample only")
    bp.add_argument("--full", action="store_true", help="Full fetch")

    # The fleet wrapper invokes `bootstrap-fast`; route it to the full fetch so a
    # missing subcommand can never silently degrade to re-ingesting sample/.
    sub.add_parser("bootstrap-fast", help="Full fetch (fleet alias)")

    up = sub.add_parser("updates", help="Fetch updates")
    up.add_argument("--since", required=True, help="YYYY-MM-DD")

    sub.add_parser("validate", help="Validate samples")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "validate":
        sys.exit(0 if validate_samples() else 1)

    if args.command == "bootstrap-fast":
        run_full()
        return

    if args.command == "bootstrap":
        if args.sample:
            print("Fetching sample BASE register documents...")
            records = fetch_sample()
            if not records:
                print("No records fetched!", file=sys.stderr)
                sys.exit(1)
            save_samples(records)
            sys.exit(0 if validate_samples() and len(records) >= 10 else 1)
        # Bare `bootstrap` defaults to the full fetch (see #1363).
        run_full()
        return

    if args.command == "updates":
        run_full(since=args.since)


if __name__ == "__main__":
    main()
