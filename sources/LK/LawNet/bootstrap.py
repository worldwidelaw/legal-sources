#!/usr/bin/env python3
"""
LK/LawNet - Sri Lanka Legal Data (Acts + Court Judgments)

Fetches Sri Lankan legal documents from open GitHub datasets by nuuuwan:
- Acts of Parliament (1981-2026): ~2,865 acts from parliament.lk
- Supreme Court Judgments (2009-2026): ~2,641 decisions from supremecourt.lk
- Appeal Court Judgments (2012-2026): ~10,574 decisions from courtofappeal.lk

Data is CC BY 4.0 licensed. Full text extracted from official government PDFs.

Usage:
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records (5 per type)
  python bootstrap.py bootstrap --full     # Full extraction -> data/records.jsonl
  python bootstrap.py bootstrap-fast       # Alias for the fleet wrapper (== --full)
  python bootstrap.py test                 # Test connectivity
"""

import argparse
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import requests

SOURCE_ID = "LK/LawNet"
SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"
DATA_DIR = SCRIPT_DIR / "data"
RECORDS_PATH = DATA_DIR / "records.jsonl"
CHECKPOINT_PATH = DATA_DIR / "checkpoint.json"

# GitHub raw content base URLs
ACTS_INDEX_URL = "https://raw.githubusercontent.com/nuuuwan/lk_acts_data/main/data/hf/acts.json"
ACTS_DATA_BASE = "https://raw.githubusercontent.com/nuuuwan/lk_acts_data/main/data/acts"

SC_INDEX_URL = "https://raw.githubusercontent.com/nuuuwan/lk_supreme_court_judgements/data/data/lk_supreme_court_judgements/docs_all.tsv"
SC_DATA_BASE = "https://raw.githubusercontent.com/nuuuwan/lk_supreme_court_judgements/data/data/lk_supreme_court_judgements"

CA_INDEX_URL = "https://raw.githubusercontent.com/nuuuwan/lk_appeal_court_judgements/data/data/lk_appeal_court_judgements/docs_all.tsv"
CA_DATA_BASE = "https://raw.githubusercontent.com/nuuuwan/lk_appeal_court_judgements/data/data/lk_appeal_court_judgements"

# Every request is bounded so a stalled connection can never hang the run.
REQUEST_TIMEOUT = (10, 30)  # (connect, read)
MAX_WORKERS = 8
BATCH_SIZE = 64

_local = threading.local()


def log(msg: str) -> None:
    """Print with an explicit flush — the fleet wrapper pipes stdout, so the
    default block buffering made a running job look hung for an hour."""
    print(msg, flush=True)


def session() -> requests.Session:
    """One requests.Session per worker thread."""
    s = getattr(_local, "session", None)
    if s is None:
        s = requests.Session()
        s.headers.update({"User-Agent": "LegalDataHunter/1.0 (research)"})
        _local.session = s
    return s


def get_decade(year: int) -> str:
    """Get decade folder name from year."""
    decade = (year // 10) * 10
    return f"{decade}s"


def _request(url: str, attempts: int = 3):
    """GET a URL with retries and 429 backoff. Returns a Response or None."""
    for attempt in range(attempts):
        try:
            resp = session().get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 404:
                return None
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After", "")
                delay = int(retry_after) if retry_after.isdigit() else 30 * (attempt + 1)
                log(f"  429 from GitHub, backing off {delay}s")
                time.sleep(delay)
                continue
            resp.raise_for_status()
            return resp
        except Exception as e:
            if attempt < attempts - 1:
                time.sleep(2 * (attempt + 1))
            else:
                log(f"  Failed to fetch {url}: {e}")
    return None


def fetch_json(url: str):
    """Fetch JSON from a URL with retries."""
    resp = _request(url)
    if resp is None:
        return None
    try:
        return resp.json()
    except ValueError:
        return None


def fetch_text(url: str):
    """Fetch text content from a URL with retries."""
    resp = _request(url)
    return resp.text if resp is not None else None


def clean_text(raw: str) -> str:
    """Strip the extractor's block marker preamble."""
    text = (raw or "").strip()
    if text.startswith("==== BLOCKS ===="):
        text = text[len("==== BLOCKS ===="):].strip()
    return text


def normalize_act(act_meta: dict, full_text: str) -> dict:
    """Normalize an act record."""
    act_id = act_meta.get("act_id", "")
    title = act_meta.get("act_description") or act_meta.get("description", "")
    date = act_meta.get("act_date") or act_meta.get("date", "")
    source_url = act_meta.get("act_source_url") or act_meta.get("url_pdf_en", "")

    return {
        "_id": f"LK-ACT-{act_id}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": full_text,
        "date": date if date else None,
        "url": source_url if source_url else "https://www.lawnet.gov.lk/",
        "language": "en",
        "act_id": act_id,
        "act_type": act_meta.get("act_type", ""),
        "text_quality": act_meta.get("_text_quality", "extracted"),
    }


def normalize_judgment(doc_meta: dict, full_text: str, court: str) -> dict:
    """Normalize a court judgment record."""
    doc_id = doc_meta.get("doc_id", "")
    title = doc_meta.get("description", "")
    date = doc_meta.get("date_str", "")
    url = doc_meta.get("url_pdf", "") or doc_meta.get("url_metadata", "")
    court_label = "Supreme Court" if court == "sc" else "Appeal Court"

    return {
        "_id": f"LK-{court.upper()}-{doc_id}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": f"[{court_label}] {title}" if title else f"[{court_label}] {doc_id}",
        "text": full_text,
        "date": date if date else None,
        "url": url if url else f"https://{'supremecourt' if court == 'sc' else 'courtofappeal'}.lk/",
        "language": "en",
        "case_number": doc_meta.get("num", ""),
        "court": court_label,
        "parties": doc_meta.get("parties", ""),
        "judge": doc_meta.get("judgement_by", ""),
    }


def normalize(raw: dict) -> dict:
    """Normalize a raw (index entry + text) pair into the standard schema."""
    kind = raw.get("_kind")
    if kind == "acts":
        return normalize_act(raw["meta"], raw["text"])
    return normalize_judgment(raw["meta"], raw["text"], kind)


# --- Index loaders -----------------------------------------------------------

def parse_tsv_index(tsv_text: str) -> list:
    """Parse a TSV index into list of dicts with doc_id and metadata."""
    lines = tsv_text.strip().split("\n")
    if len(lines) < 2:
        return []
    headers = lines[0].split("\t")
    results = []
    for line in lines[1:]:
        if not line.strip():
            continue
        parts = line.split("\t")
        entry = {}
        for i, h in enumerate(headers):
            entry[h.strip()] = parts[i].strip() if i < len(parts) else ""
        if entry.get("doc_id"):
            results.append(entry)
    return results


def load_acts_index() -> list:
    index = fetch_json(ACTS_INDEX_URL) or []
    entries = []
    for entry in index:
        act_id = entry.get("act_id", "")
        if not act_id:
            continue
        year_str = entry.get("act_year") or act_id.split("-")[0]
        try:
            entry["_year"] = int(year_str)
        except (ValueError, TypeError):
            continue
        entries.append(entry)
    return entries


def _load_judgment_index(index_url: str) -> list:
    tsv_text = fetch_text(index_url)
    if not tsv_text:
        return []
    entries = []
    for entry in parse_tsv_index(tsv_text):
        try:
            entry["_year"] = int(entry["doc_id"][:4])
        except (ValueError, IndexError):
            continue
        entries.append(entry)
    return entries


def load_sc_index() -> list:
    return _load_judgment_index(SC_INDEX_URL)


def load_ca_index() -> list:
    return _load_judgment_index(CA_INDEX_URL)


# --- Per-document fetchers ---------------------------------------------------

# Acts ship their text under three different filenames depending on how the
# upstream extractor fared. Roughly: 2005+ acts are born-digital (en.txt), a few
# only have blocks.txt, and pre-2005 scanned gazettes have en.txt.fail alongside
# an en.ocr.txt. Trying only en.txt dropped ~3/4 of the acts corpus.
ACT_TEXT_FILES = [("en.txt", "extracted"), ("blocks.txt", "extracted"), ("en.ocr.txt", "ocr")]


def fetch_act(entry: dict):
    """Fetch an act's full text, trying each upstream text filename in turn."""
    act_id = entry["act_id"]
    base = f"{ACTS_DATA_BASE}/{get_decade(entry['_year'])}/{entry['_year']}/{act_id}"

    text, quality = None, None
    for filename, label in ACT_TEXT_FILES:
        candidate = fetch_text(f"{base}/{filename}")
        if candidate and len(candidate.strip()) >= 50:
            text, quality = candidate, label
            break
    if not text:
        return None

    meta = dict(entry)
    detail = fetch_json(f"{base}/metadata.json")
    if detail:
        meta.update(detail)
    meta["_text_quality"] = quality
    return {"_kind": "acts", "meta": meta, "text": clean_text(text)}


def _fetch_judgment(entry: dict, base_url: str, kind: str):
    url = f"{base_url}/{get_decade(entry['_year'])}/{entry['_year']}/{entry['doc_id']}/doc.txt"
    text = fetch_text(url)
    if not text or len(text.strip()) < 50:
        return None
    return {"_kind": kind, "meta": entry, "text": clean_text(text)}


def fetch_sc(entry: dict):
    return _fetch_judgment(entry, SC_DATA_BASE, "sc")


def fetch_ca(entry: dict):
    return _fetch_judgment(entry, CA_DATA_BASE, "ca")


COLLECTIONS = [
    ("acts", "Acts of Parliament", load_acts_index, fetch_act),
    ("sc", "Supreme Court judgments", load_sc_index, fetch_sc),
    ("ca", "Appeal Court judgments", load_ca_index, fetch_ca),
]


# --- Crawl -------------------------------------------------------------------

def iter_collection(label: str, entries: list, fetcher, start: int = 0, limit: int = 0):
    """Yield (position, record) for entries[start:], fetched concurrently in
    ordered batches so the caller can checkpoint by index position."""
    yielded = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        for batch_start in range(start, len(entries), BATCH_SIZE):
            batch = entries[batch_start:batch_start + BATCH_SIZE]
            for offset, raw in enumerate(pool.map(fetcher, batch)):
                position = batch_start + offset + 1
                if raw is None:
                    continue
                yield position, normalize(raw)
                yielded += 1
                if limit and yielded >= limit:
                    return
            log(f"  [{label}] {batch_start + len(batch)}/{len(entries)} scanned, {yielded} written")


def fetch_all(limit_per_collection: int = 0):
    """Yield every document with full text, across all three collections."""
    for label, title, loader, fetcher in COLLECTIONS:
        log(f"\n--- {title} ---")
        entries = loader()
        log(f"  index: {len(entries)} entries")
        for _, record in iter_collection(label, entries, fetcher, limit=limit_per_collection):
            yield record


def fetch_updates(since: str):
    """Yield documents dated on or after `since` (ISO YYYY-MM-DD).

    Every index carries a date, so updates are a filter over the index — no
    need to re-download the whole corpus.
    """
    for label, title, loader, fetcher in COLLECTIONS:
        log(f"\n--- {title} (since {since}) ---")
        entries = [
            e for e in loader()
            if (e.get("act_date") or e.get("date_str") or "") >= since
        ]
        log(f"  {len(entries)} entries dated since {since}")
        for _, record in iter_collection(label, entries, fetcher):
            yield record


# --- Checkpointing -----------------------------------------------------------

def load_checkpoint() -> dict:
    if CHECKPOINT_PATH.exists():
        try:
            return json.loads(CHECKPOINT_PATH.read_text())
        except (ValueError, OSError):
            pass
    return {}


def save_checkpoint(state: dict) -> None:
    tmp = CHECKPOINT_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state))
    tmp.replace(CHECKPOINT_PATH)


def bootstrap_full():
    """Stream the full corpus to data/records.jsonl, resumable via checkpoint.

    Resume matters: without it a relaunched fleet worker re-walks every index
    from the top and re-appends the same first N records.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    state = load_checkpoint()
    resuming = bool(state)
    if resuming:
        log(f"Resuming from checkpoint: {state}")

    written = 0
    with open(RECORDS_PATH, "a" if resuming else "w", encoding="utf-8") as out:
        for label, title, loader, fetcher in COLLECTIONS:
            start = state.get(label, 0)
            log(f"\n--- {title} ---")
            entries = loader()
            if not entries:
                log(f"  WARNING: {label} index empty or unreachable, skipping")
                continue
            log(f"  index: {len(entries)} entries (starting at {start})")
            if start >= len(entries):
                log(f"  [{label}] already complete")
                continue

            for position, record in iter_collection(label, entries, fetcher, start=start):
                out.write(json.dumps(record, ensure_ascii=False) + "\n")
                out.flush()
                written += 1
                state[label] = position
                if written % 200 == 0:
                    save_checkpoint(state)

            state[label] = len(entries)
            save_checkpoint(state)

    save_checkpoint(state)
    log(f"\nbootstrap_fast complete: {written} fetched, written to {RECORDS_PATH}")


def bootstrap_sample():
    """Fetch 5 records of each type into sample/."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    all_records = []

    for record in fetch_all(limit_per_collection=5):
        out_path = SAMPLE_DIR / f"record_{len(all_records):04d}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        all_records.append(record)

    with open(SAMPLE_DIR / "all_samples.json", "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)

    saved = len(all_records)
    log(f"\nBootstrap complete: {saved} records saved to {SAMPLE_DIR}")

    text_count = sum(1 for r in all_records if r.get("text") and len(r["text"]) > 100)
    leg_count = sum(1 for r in all_records if r["_type"] == "legislation")
    case_count = sum(1 for r in all_records if r["_type"] == "case_law")

    log(f"  Legislation records: {leg_count}")
    log(f"  Case law records: {case_count}")
    log(f"  Records with substantial text: {text_count}/{saved}")

    if saved > 0 and text_count < saved * 0.5:
        log("WARNING: Less than 50% of records have substantial text")


def test_connectivity():
    """Test connectivity to all three data sources."""
    log("Testing connectivity to nuuuwan datasets...")

    for name, url in [
        ("Acts index", ACTS_INDEX_URL),
        ("SC index", SC_INDEX_URL),
        ("CA index", CA_INDEX_URL),
    ]:
        try:
            resp = session().head(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            log(f"  {name}: HTTP {resp.status_code} {'OK' if resp.status_code == 200 else 'FAIL'}")
        except Exception as e:
            log(f"  {name}: FAILED ({e})")

    log("\nTesting sample document fetch...")
    for label, title, loader, fetcher in COLLECTIONS:
        entries = loader()
        raw = fetcher(entries[0]) if entries else None
        if raw:
            log(f"  {label}: {len(entries)} indexed, first doc {len(raw['text'])} chars")
        else:
            log(f"  {label}: {len(entries)} indexed, could not fetch first doc")

    log("\nConnectivity test complete")


def main():
    parser = argparse.ArgumentParser(description="LK/LawNet Sri Lanka Legal Data Fetcher")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast", "test"], help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (15 records)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument("--updates", action="store_true", help="Fetch only documents dated since --since")
    parser.add_argument("--since", help="With --updates: ISO date (YYYY-MM-DD)")
    args = parser.parse_args()

    if args.command == "test":
        test_connectivity()
    elif args.command == "bootstrap-fast":
        bootstrap_full()
    elif args.command == "bootstrap":
        if args.updates:
            if not args.since:
                parser.error("--updates requires --since YYYY-MM-DD")
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            count = 0
            with open(RECORDS_PATH, "w", encoding="utf-8") as out:
                for record in fetch_updates(args.since):
                    out.write(json.dumps(record, ensure_ascii=False) + "\n")
                    out.flush()
                    count += 1
            log(f"\nUpdates complete: {count} records written to {RECORDS_PATH}")
        elif args.sample:
            bootstrap_sample()
        else:
            bootstrap_full()


if __name__ == "__main__":
    main()
