#!/usr/bin/env python3
"""
DE/BfS -- Bundesamt für Strahlenschutz (Federal Office for Radiation Protection)

Fetches DORIS (Digitales Online Repositorium und Informations-System), the BfS
digital repository at doris.bfs.de — the office's own publication channel for
the technical and regulatory material underpinning German radiation protection
law (StrlSchG / StrlSchV):

  - BfS-Schriften and the Ressortforschungsberichte commissioned by the BMUV,
    which the office states are used as decision aids when drafting radiation
    protection rules
  - Strahlenschutzforschung Programmreporte
  - "Umweltradioaktivität und Strahlenbelastung" annual reports
  - Guidance, position papers and technical reports on EMF, UV, medical and
    occupational exposure, nuclear emergency preparedness

Source:    https://www.bfs.de/  →  https://doris.bfs.de/
Discovery: DSpace JSPUI browse-by-issue-date listing (100 items/page)
Metadata:  Dublin Core <meta> tags on each item page
Content:   PDF bitstream per item, extracted via common/pdf_extract

⚠️ DORIS content is licensed NON-COMMERCIALLY (CC BY-NC-SA 3.0 DE, adapted).
See README.md.

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
SOURCE_ID = "DE/BfS"
BASE_URL = "https://doris.bfs.de"
BROWSE_URL = f"{BASE_URL}/jspui/browse"
AGENCY_URL = "https://www.bfs.de/"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 LegalDataHunter/1.0"
)
REQUEST_DELAY = 1.5
PAGE_SIZE = 100
MAX_PAGES = 100
MIN_TEXT_CHARS = 200

MONTHS = {
    "Jan": "01", "Feb": "02", "Mar": "03", "Apr": "04", "May": "05", "Jun": "06",
    "Jul": "07", "Aug": "08", "Sep": "09", "Oct": "10", "Nov": "11", "Dec": "12",
}


def _parse_browse_date(raw: str) -> str:
    """Turn a DSpace browse date ('30-Jul-2026', 'Jul-2026', '1999') into ISO 8601."""
    raw = raw.strip()
    m = re.fullmatch(r"(\d{1,2})-([A-Za-z]{3})-(\d{4})", raw)
    if m and m.group(2) in MONTHS:
        return f"{m.group(3)}-{MONTHS[m.group(2)]}-{int(m.group(1)):02d}"
    m = re.fullmatch(r"([A-Za-z]{3})-(\d{4})", raw)
    if m and m.group(1) in MONTHS:
        return f"{m.group(2)}-{MONTHS[m.group(1)]}-01"
    m = re.fullmatch(r"(\d{4})", raw)
    if m:
        return f"{m.group(1)}-01-01"
    return ""


def _normalize_issued(raw: str) -> str:
    """DCTERMS.issued is usually ISO already but can be partial."""
    raw = (raw or "").strip()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        return raw
    if re.fullmatch(r"\d{4}-\d{2}", raw):
        return f"{raw}-01"
    if re.fullmatch(r"\d{4}", raw):
        return f"{raw}-01-01"
    return ""


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------
class DorisClient:
    """Client for the BfS DORIS DSpace repository."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
        })

    def _get(self, url: str, attempts: int = 4) -> Optional[requests.Response]:
        delay = 2.0
        for attempt in range(attempts):
            try:
                resp = self.session.get(url, timeout=90)
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
            return None
        return None

    # -- discovery ----------------------------------------------------------
    def list_items(self, max_pages: int = MAX_PAGES) -> List[Dict]:
        """Walk browse-by-issue-date and return one dict per repository item."""
        items: List[Dict] = []
        seen: set = set()

        for page in range(max_pages):
            offset = page * PAGE_SIZE
            url = (
                f"{BROWSE_URL}?type=dateissued&sort_by=2&order=DESC"
                f"&rpp={PAGE_SIZE}&etal=-1&offset={offset}"
            )
            resp = self._get(url)
            if resp is None:
                raise RuntimeError(
                    f"{SOURCE_ID}: DORIS browse offset {offset} unreachable — "
                    "doris.bfs.de is refusing this vantage, aborting loudly rather "
                    "than reporting a truncated corpus"
                )

            page_items = self._parse_browse_page(resp.text)
            fresh = [i for i in page_items if i["handle"] not in seen]
            for i in fresh:
                seen.add(i["handle"])
            items.extend(fresh)

            print(f"  offset {offset}: +{len(fresh)} items (total {len(items)})")
            if len(page_items) < PAGE_SIZE or not fresh:
                break
            time.sleep(REQUEST_DELAY)

        if not items:
            raise RuntimeError(
                f"{SOURCE_ID}: DORIS browse returned 0 items — layout change or block"
            )
        return items

    @staticmethod
    def _parse_browse_page(html: str) -> List[Dict]:
        soup = BeautifulSoup(html, "html.parser")
        table = soup.select_one("table.miscTable")
        if not table:
            return []

        out: List[Dict] = []
        for row in table.find_all("tr"):
            link = row.select_one('a[href^="/jspui/handle/"]')
            if not link:
                continue
            cells = row.find_all("td")
            date = _parse_browse_date(cells[0].get_text(" ", strip=True)) if cells else ""
            handle = link["href"].split("/jspui/handle/", 1)[1].strip("/")
            out.append({
                "handle": handle,
                "url": urljoin(BASE_URL, link["href"]),
                "title": link.get_text(" ", strip=True),
                "date": date,
            })
        return out

    # -- item ---------------------------------------------------------------
    def load_item(self, item: Dict) -> Dict:
        """Enrich a browse row with Dublin Core metadata and the PDF bitstream URL."""
        detail = dict(item)
        detail["pdf_url"] = ""
        detail["authors"] = []
        detail["publishers"] = []
        detail["abstract"] = ""
        detail["series"] = []
        detail["language"] = "de"

        resp = self._get(item["url"])
        if resp is None:
            return detail

        soup = BeautifulSoup(resp.text, "html.parser")

        def meta_all(name: str) -> List[str]:
            return [
                t["content"].strip()
                for t in soup.find_all("meta", attrs={"name": name})
                if t.get("content", "").strip()
            ]

        def meta_one(name: str) -> str:
            values = meta_all(name)
            return values[0] if values else ""

        detail["title"] = meta_one("DC.title") or detail["title"]
        detail["alt_title"] = meta_one("DCTERMS.alternative")
        detail["authors"] = meta_all("DC.creator")
        detail["publishers"] = meta_all("DC.publisher")
        detail["abstract"] = meta_one("DCTERMS.abstract")
        detail["series"] = meta_all("DC.relation")
        detail["language"] = meta_one("DC.language") or "de"
        detail["urn"] = meta_one("DC.identifier") or item["handle"]

        issued = _normalize_issued(meta_one("DCTERMS.issued"))
        if issued:
            detail["date"] = issued

        # Bitstreams: prefer PDF, largest listed first is normally the full report.
        for a in soup.select('a[href*="/jspui/bitstream/"]'):
            href = a["href"]
            if href.lower().split("?")[0].endswith(".pdf"):
                detail["pdf_url"] = urljoin(BASE_URL, href)
                break

        return detail

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
        return "\n\n".join((p.extract_text() or "") for p in reader.pages).strip()
    except ImportError:
        pass

    return ""


# ---------------------------------------------------------------------------
# Normalize
# ---------------------------------------------------------------------------
def _slug(handle: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "-", handle).strip("-")


def normalize(item: Dict, text: str) -> Dict:
    return {
        "_id": f"DE-BfS-{_slug(item['handle'])}",
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": item.get("title", ""),
        "text": text,
        "date": item.get("date") or None,
        "url": item.get("url", ""),
        "pdf_url": item.get("pdf_url", ""),
        "urn": item.get("urn", item["handle"]),
        "alt_title": item.get("alt_title", ""),
        "authors": item.get("authors", []),
        "publisher": "; ".join(item.get("publishers", [])),
        "series": "; ".join(item.get("series", [])),
        "abstract": item.get("abstract", ""),
        "language": item.get("language", "de"),
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
def _build_record(client: DorisClient, item: Dict) -> Optional[Dict]:
    detail = client.load_item(item)
    time.sleep(REQUEST_DELAY)

    if not detail.get("pdf_url"):
        return None

    pdf_bytes = client.download_pdf(detail["pdf_url"])
    if not pdf_bytes:
        return None

    text = _extract_text(pdf_bytes, detail["handle"])
    if len(text) < MIN_TEXT_CHARS:
        return None

    return normalize(detail, text)


def fetch_sample(count: int = 15) -> List[Dict]:
    client = DorisClient()
    print("Listing DORIS items...")
    items = client.list_items(max_pages=1)
    print(f"Found {len(items)} items on the first page\n")

    records: List[Dict] = []
    for item in items:
        if len(records) >= count:
            break
        print(f"  [{len(records) + 1}/{count}] {item['title'][:65]}...")
        record = _build_record(client, item)
        if record is None:
            print("       skipped: no PDF text")
            continue
        records.append(record)
        print(f"       OK: {len(record['text']):,} chars")
        time.sleep(REQUEST_DELAY)

    return records


def fetch_all(since: Optional[str] = None) -> Generator[Dict, None, None]:
    client = DorisClient()
    print("Listing DORIS items...")
    items = client.list_items()
    print(f"Total repository items: {len(items)}\n")

    done = _load_checkpoint()
    yielded = 0
    skipped = 0

    for item in items:
        if item["handle"] in done:
            continue
        if since and item.get("date") and item["date"] < since:
            continue

        record = _build_record(client, item)
        done.add(item["handle"])

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
    parser = argparse.ArgumentParser(description="DE/BfS (DORIS) fetcher")
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
            print("Fetching sample DORIS documents...")
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
