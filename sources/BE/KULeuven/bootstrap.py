#!/usr/bin/env python3
"""
BE/KULeuven -- KU Leuven Law Library Open-Access Archive

*** BLOCKED: terms_of_use_prohibit_scraping. THIS SCRAPER MUST NOT BE RUN. ***

All three archive pages forbid the reuse this project makes -- /archieven/boeken
states "Commercieel en publiek (her)gebruik van deze bestanden is niet toegelaten"
(commercial and public reuse of these files is not permitted), and the overview
page allows personal use only. The per-title "open access" tag means
free-to-read, not free-to-reuse: these are in-copyright publisher monographs
digitised under a permission that runs to KU Leuven, not to redistributors.
Legal Data Hunter trips both limbs -- it is commercial and it republishes text.

The code below is retained only as a record of the (successful) technical
research, so a future session does not redo it. Every entry point raises. It
becomes runnable only if KU Leuven Bibliotheken and the underlying publishers
grant written permission; see README.md. Do not re-probe the endpoint -- the
blocker is licensing, not availability.

KU Leuven Libraries republishes, with publisher permission, a large archive of
Belgian legal monographs ("juridische klassiekers") as born-digital PDFs. It is
the most complete open-access collection of Belgian legal doctrine, mostly
published up to ~2006.

Strategy:
  - The listing page carries every document as a direct <a href> to
    /rbib/collectie/archieven/boeken/{slug}-{year}.pdf — no search API, no
    pagination, no JS. One request enumerates the whole corpus (~2,815 PDFs).
  - Download each PDF and extract full text via common.pdf_extract.
  - Author, title and year come from the anchor text; the year is confirmed
    against the filename suffix.

Note: the tijdschriften (journals) page is catalogue-only — it links no PDFs,
so periodicals are deliberately out of scope.

Usage:
  python bootstrap.py bootstrap --sample   # ~15 sample records
  python bootstrap.py bootstrap --full     # Full corpus -> data/records.jsonl
  python bootstrap.py bootstrap-fast       # Alias the fleet wrapper calls
  python bootstrap.py test-api             # Connectivity test
"""

import argparse
import hashlib
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BE.KULeuven")

SOURCE_ID = "BE/KULeuven"
SOURCE_DIR = Path(__file__).resolve().parent
SAMPLE_DIR = SOURCE_DIR / "sample"
DATA_DIR = SOURCE_DIR / "data"
RECORDS_PATH = DATA_DIR / "records.jsonl"
CHECKPOINT_PATH = DATA_DIR / "checkpoint.json"

BASE_URL = "https://bib.kuleuven.be"
LISTING_URLS = [
    f"{BASE_URL}/rbib/collectie/archieven/boeken/juridische-klassiekers",
    f"{BASE_URL}/rbib/collectie/archieven/tijdschriften",
]

PDF_RE = re.compile(
    r'<a[^>]+href="(https://bib\.kuleuven\.be/rbib/collectie/archieven/[^"]+\.pdf)"[^>]*>(.*?)</a>',
    re.DOTALL | re.IGNORECASE,
)

REQUEST_DELAY = 1.5

BLOCKED_MESSAGE = (
    "BE/KULeuven is blocked: terms_of_use_prohibit_scraping. "
    "bib.kuleuven.be/rbib/collectie/archieven permits personal use only and "
    "explicitly forbids commercial and public reuse of these files. Running this "
    "scraper would breach the publisher's terms. Unblocking requires written "
    "permission from KU Leuven Bibliotheken and the underlying publishers -- see "
    "sources/BE/KULeuven/README.md. This is not a network or availability issue; "
    "do not re-probe."
)


def _refuse() -> None:
    """Guard every entry point. Blocked on terms, so there is no run path."""
    raise RuntimeError(BLOCKED_MESSAGE)


def _strip_tags(fragment: str) -> str:
    text = re.sub(r"<[^>]+>", " ", fragment)
    text = unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _client() -> HttpClient:
    return HttpClient(
        base_url=BASE_URL,
        headers={
            "User-Agent": "LegalDataHunter/1.0 (open access research)",
            "Accept": "text/html,application/xhtml+xml,application/pdf,*/*",
        },
        timeout=(15, 120),
        wall_timeout=300,
    )


def discover() -> list:
    """Enumerate every archive PDF from the listing pages."""
    _refuse()
    http = _client()
    found = {}
    for listing_url in LISTING_URLS:
        resp = http.get(listing_url)
        if resp is None or resp.status_code != 200:
            logger.warning(f"Listing unavailable: {listing_url}")
            continue
        resp.encoding = resp.encoding or "utf-8"
        hits = PDF_RE.findall(resp.text)
        logger.info(f"{listing_url}: {len(hits)} PDF links")
        for url, anchor in hits:
            found.setdefault(url, _strip_tags(anchor))
        time.sleep(REQUEST_DELAY)

    if not found:
        # Fail loud: a silent 0 here would look like an empty corpus rather
        # than a listing whose markup moved.
        raise RuntimeError(
            "bib.kuleuven.be listings produced 0 PDF links — markup or URL scheme changed"
        )

    docs = [{"url": u, "anchor": a} for u, a in sorted(found.items())]
    logger.info(f"Total archive PDFs discovered: {len(docs)}")
    return docs


def parse_entry(doc: dict) -> dict:
    """Derive title, author and year from the anchor text and the filename."""
    filename = doc["url"].rsplit("/", 1)[-1][: -len(".pdf")]
    year = None
    m = re.search(r"[-_](1[5-9]\d{2}|20\d{2})$", filename)
    if m:
        year = m.group(1)

    anchor = doc.get("anchor", "")
    author, title = "", anchor
    # Anchors read "AUTHOR, Title of the work" or "AUTHOR & OTHER, Title".
    m = re.match(r"^([^,]{2,60}),\s*(.+)$", anchor)
    if m and m.group(1).upper() == m.group(1):
        author, title = m.group(1).strip(), m.group(2).strip()

    if not title:
        title = filename.replace("-", " ").replace("_", " ").strip()

    if year and not re.search(r"\b" + year + r"\b", title):
        title = f"{title} ({year})"

    return {"title": title, "author": author, "year": year, "filename": filename}


def normalize(doc: dict, text: str) -> dict:
    meta = parse_entry(doc)
    doc_id = hashlib.md5(doc["url"].encode()).hexdigest()[:16]
    return {
        "_id": f"be-kuleuven-{doc_id}",
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": meta["title"],
        "text": text,
        "date": f"{meta['year']}-01-01" if meta["year"] else None,
        "url": doc["url"],
        "author": meta["author"],
        "year": meta["year"],
        "publisher_archive": "KU Leuven Libraries — rechtsbibliotheek archief",
        "language": "nl",
    }


def _load_done() -> set:
    if CHECKPOINT_PATH.exists():
        try:
            with open(CHECKPOINT_PATH, encoding="utf-8") as f:
                return set(json.load(f).get("done", []))
        except Exception as exc:
            logger.warning(f"Ignoring unreadable checkpoint ({exc})")
    return set()


def _save_done(done: set) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = CHECKPOINT_PATH.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump({"done": sorted(done)}, f)
    tmp.replace(CHECKPOINT_PATH)


def fetch_all(sample: bool = False, limit: int = 15) -> Generator[dict, None, None]:
    """Yield every archive volume with full text."""
    http = _client()
    docs = discover()
    # These are whole monographs: several thousand PDFs averaging a few MB, so
    # a run spans more than one fleet slot. Skip what is already written.
    done = set() if sample else _load_done()
    count = 0

    for i, doc in enumerate(docs):
        if doc["url"] in done:
            continue
        if sample and count >= limit:
            return

        try:
            resp = http.get(doc["url"])
        except Exception as exc:
            logger.warning(f"  download failed {doc['url']}: {exc}")
            continue
        if resp is None or resp.status_code != 200 or len(resp.content) < 1000:
            logger.warning(f"  unavailable ({doc['url']})")
            continue

        try:
            text = extract_pdf_markdown(
                source=SOURCE_ID,
                source_id=doc["url"],
                pdf_bytes=resp.content,
                table="doctrine",
            )
        except Exception as exc:
            logger.warning(f"  extraction failed {doc['url']}: {exc}")
            continue

        if not text or len(text.strip()) < 500:
            logger.warning(f"  insufficient text ({len(text or '')} chars) {doc['url']}")
            continue

        record = normalize(doc, text)
        count += 1
        yield record
        logger.info(f"  [{count}/{len(docs)}] {record['title'][:70]} ({len(text)} chars)")

        if not sample:
            done.add(doc["url"])
            if count % 10 == 0:
                _save_done(done)

        time.sleep(REQUEST_DELAY)

    if not sample:
        _save_done(done)
    logger.info(f"Total volumes fetched: {count}")


def fetch_updates(since: Optional[str] = None) -> Generator[dict, None, None]:
    """The archive is a static republication set; a full sweep is the update."""
    yield from fetch_all()


def test_api() -> bool:
    try:
        docs = discover()
        if len(docs) < 100:
            logger.error(f"FAIL — only {len(docs)} PDFs discovered")
            return False
        record = next(fetch_all(sample=True, limit=1), None)
        if not record:
            logger.error("FAIL — no document could be extracted")
            return False
        logger.info(
            f"OK — {len(docs)} PDFs; sample '{record['title'][:50]}' "
            f"({len(record['text'])} chars, {record['date']})"
        )
        return True
    except Exception as exc:
        logger.error(f"FAIL — {exc}")
        return False


def main():
    logger.error(BLOCKED_MESSAGE)
    sys.exit(1)


def _unreachable_main():
    parser = argparse.ArgumentParser(description="BE/KULeuven fetcher")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "update", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch ~15 sample records")
    parser.add_argument("--full", action="store_true", help="Full corpus")
    args = parser.parse_args()

    if args.command == "test-api":
        sys.exit(0 if test_api() else 1)

    # `bootstrap-fast` is what the fleet wrapper invokes; it means the full
    # corpus, not the sample path.
    sample_mode = args.sample and args.command == "bootstrap"

    if sample_mode:
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        records = list(fetch_all(sample=True))
        for i, record in enumerate(records):
            with open(SAMPLE_DIR / f"record_{i+1:03d}.json", "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
        logger.info(f"Saved {len(records)} sample records to {SAMPLE_DIR}")
        sys.exit(0 if records else 1)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    written = 0
    with open(RECORDS_PATH, "a", encoding="utf-8") as out:
        for record in fetch_all():
            out.write(json.dumps(record, ensure_ascii=False) + "\n")
            out.flush()
            written += 1

    logger.info(f"Bootstrap complete: {written} records -> {RECORDS_PATH}")
    if written == 0:
        logger.error("No records written!")
        sys.exit(1)


if __name__ == "__main__":
    main()
