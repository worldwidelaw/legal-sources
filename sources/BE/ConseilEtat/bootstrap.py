#!/usr/bin/env python3
"""
Belgian Council of State (Conseil d'État / Raad van State) Data Fetcher

Extracts administrative court decisions from raadvst-consetat.be.
  - PDF downloads via the arr.php endpoint, full text via common/pdf_extract
  - Published decisions run from arrêt n° 1 up to the current number (~267,000)
  - Decisions are in French, Dutch or German; arr.php serves whichever language
    the arrêt was written in regardless of the `l=` parameter

Two access paths, used for two different jobs:

  * full crawl — arr.php is keyed by decision number with no listing behind it,
    so the corpus is walked by descending number. Numbers that were never
    published (aliens cases, sealed decisions) answer 200 with a ~126-byte
    "is niet toegankelijk" HTML stub instead of a PDF. Those gaps are wide —
    40/40 consecutive misses around n° 30,000 — so the walk must not treat a
    run of misses as the end of the corpus (see fetch_all).

  * refresh — `?page=lastmonth_MM` is the court's own rolling 12-month index of
    *published* decisions, and stamps each one with `[Ajouté le DD/MM/YYYY]`,
    the date it became available to us. That is the correct comparator for an
    incremental run: a decision's own date can be months older than the day it
    appeared (#1502).

Data source: https://www.raadvst-consetat.be
License: Open Government Data
"""

import argparse
import json
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from html import unescape
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown  # noqa: E402

SOURCE_ID = "BE/ConseilEtat"
BASE_URL = "https://www.raadvst-consetat.be"
PDF_ENDPOINT = f"{BASE_URL}/arr.php"
MONTHLY_URL = f"{BASE_URL}/?lang=fr&page=lastmonth_{{mm}}"

SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"
DATA_DIR = SCRIPT_DIR / "data"
RECORDS_PATH = DATA_DIR / "records.jsonl"
CHECKPOINT_PATH = DATA_DIR / "checkpoint.json"

HEADERS = {
    "User-Agent": "Legal Data Hunter/1.0 (EU Legal Research)",
    "Accept": "application/pdf,text/html,*/*",
    "Accept-Language": "fr-BE,fr;q=0.9,nl-BE;q=0.8,en;q=0.7",
}

#: The lowest arrêt number that exists. n° 1 is served, so the walk runs to 1.
FLOOR_NR = 1
#: Used only when the monthly index is unreachable and no checkpoint exists.
FALLBACK_MAX_NR = 267_500

# Politeness. A miss is a 126-byte response, so it gets the shorter pause.
HIT_DELAY = 0.4
MISS_DELAY = 0.15

# Persist the descending cursor this often so a killed slot loses at most this
# many numbers, and a relaunched slot resumes instead of restarting (#1502).
CHECKPOINT_EVERY = 100

_MONTHS = {
    # French
    "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4, "mai": 5,
    "juin": 6, "juillet": 7, "août": 8, "aout": 8, "septembre": 9,
    "octobre": 10, "novembre": 11, "décembre": 12, "decembre": 12,
    # Dutch — roughly half the corpus, and the old French-only regex silently
    # fell through to a decision-number-to-year guess for every one of them.
    "januari": 1, "februari": 2, "maart": 3, "april": 4, "mei": 5, "juni": 6,
    "juli": 7, "augustus": 8, "oktober": 10, "december": 12,
    # German
    "januar": 1, "februar": 2, "märz": 3, "maerz": 3, "august": 8,
    "dezember": 12,
}

_DATE_RE = re.compile(r"\b(\d{1,2})\s+([A-Za-zÀ-ÿ]{3,12})\.?\s+((?:18|19|20)\d{2})\b")
# Every arrêt opens "A R R E S T nr. 80.000 van 29 april 1999" / "no 267.465 du
# 7 août 2026". Anchoring on that beats scanning for the first date anywhere in
# the head: the next date down is usually the *contested* administrative act's,
# which is what a generic scan silently returned whenever the header month was
# unrecognised.
_HEADER_DATE_RE = re.compile(
    r"n[or°]{0,2}\.?\s*[\d.]+\s+(?:du|van|vom)\s+"
    r"(\d{1,2})\s+([A-Za-zÀ-ÿ]{3,12})\.?\s+((?:18|19|20)\d{2})\b",
    re.I,
)
_ECLI_RE = re.compile(r"ECLI:BE:RVSCE:\d{4}:[A-Z]+\.[\d.]+")
_LISTING_ITEM_RE = re.compile(
    r"arr\.php\?nr=(\d+)[^>]*>\s*<li>(.*?)</li>", re.S | re.I
)
_LISTING_DATE_RE = re.compile(r"(\d{2})/(\d{2})/((?:19|20)\d{2})")
# Greedy up to the `)` that closes the label, because topics nest parentheses
# ("Contentieux scolaire (échec, refus d'inscription, ...)").
_LISTING_TOPIC_RE = re.compile(r"\((.{2,300})\)\s*(?:<BR>|<br>|$)", re.S)


def _session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


# ---------------------------------------------------------------------------
# Checkpoint
# ---------------------------------------------------------------------------

def load_checkpoint() -> dict:
    """Return the persisted crawl state, or an empty one.

    Keys:
      full_cursor  next arrêt number the descending full crawl should try
      max_nr       highest arrêt number ever fetched successfully
      last_added   newest `Ajouté le` date observed in the monthly index
    """
    try:
        with open(CHECKPOINT_PATH, encoding="utf-8") as handle:
            state = json.load(handle)
        if isinstance(state, dict):
            return state
    except (OSError, ValueError):
        pass
    return {}


def save_checkpoint(state: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    state = dict(state, updated_at=datetime.now(timezone.utc).isoformat())
    tmp = CHECKPOINT_PATH.with_suffix(".json.tmp")
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2)
    tmp.replace(CHECKPOINT_PATH)


# ---------------------------------------------------------------------------
# Monthly index — the court's own "recently published" listing
# ---------------------------------------------------------------------------

def fetch_month_index(session: requests.Session, month: int) -> List[dict]:
    """Parse one `?page=lastmonth_MM` page into {nr, topic, added} entries."""
    url = MONTHLY_URL.format(mm=f"{month:02d}")
    try:
        response = session.get(url, timeout=45)
    except requests.RequestException as exc:
        print(f"Monthly index {month:02d} failed: {exc}")
        return []
    if response.status_code != 200:
        print(f"Monthly index {month:02d} returned HTTP {response.status_code}")
        return []

    # The response carries no charset in its Content-Type header, so requests
    # falls back to ISO-8859-1 and every accented topic label arrives mojibaked
    # ("MarchÃ©s publics"). The page itself declares UTF-8 in a meta tag.
    body = response.content.decode("utf-8", errors="replace")

    entries = []
    for match in _LISTING_ITEM_RE.finditer(body):
        nr = int(match.group(1))
        blob = match.group(2)
        added = None
        date_match = _LISTING_DATE_RE.search(blob)
        if date_match:
            day, mon, year = date_match.groups()
            added = f"{year}-{mon}-{day}"
        topic_match = _LISTING_TOPIC_RE.search(blob)
        entries.append({
            "nr": nr,
            "topic": unescape(topic_match.group(1)).strip() if topic_match else None,
            "added": added,
        })
    return entries


def fetch_recent_index(session: requests.Session, months: int = 12) -> Dict[int, dict]:
    """Merge the rolling monthly listings into {nr: entry}, newest first.

    The site keys these pages by calendar month, so twelve pages is the whole
    window it keeps. Asking for fewer only narrows how far back a refresh can
    see; it never makes the crawl cheaper per decision.
    """
    index: Dict[int, dict] = {}
    current = datetime.now(timezone.utc).month
    for offset in range(max(1, min(months, 12))):
        month = current - offset
        if month <= 0:
            month += 12
        for entry in fetch_month_index(session, month):
            existing = index.get(entry["nr"])
            # Keep the entry that carries a date if the other does not.
            if existing is None or (existing.get("added") is None and entry.get("added")):
                index[entry["nr"]] = entry
        time.sleep(0.3)
    return index


def fetch_recent_decision_numbers(session: requests.Session, months: int = 3) -> List[int]:
    """Backwards-compatible helper: recent arrêt numbers, highest first."""
    return sorted(fetch_recent_index(session, months=months), reverse=True)


# ---------------------------------------------------------------------------
# Single decision
# ---------------------------------------------------------------------------

def _parse_decision_date(text: str) -> Optional[str]:
    """ISO date from the arrêt's own `n° X du <date>` line, else None.

    Returns None rather than guessing. The previous version mapped an
    unparsed decision onto a year bracket derived from its number, which
    stamped every pre-2022 arrêt — including n° 1 — with "2022".
    """
    head = text[:4000]
    header = _HEADER_DATE_RE.search(head)
    if header:
        # Trust the header even if it fails to resolve — a date from further
        # down the page belongs to a different act.
        return _to_iso(*header.groups())
    for match in _DATE_RE.finditer(head):
        iso = _to_iso(*match.groups())
        if iso:
            return iso
    return None


def _to_iso(day: str, name: str, year: str) -> Optional[str]:
    month = _MONTHS.get(name.lower().rstrip("."))
    if not month:
        return None
    day_i, year_i = int(day), int(year)
    if 1 <= day_i <= 31 and 1830 <= year_i <= datetime.now().year + 1:
        return f"{year_i:04d}-{month:02d}-{day_i:02d}"
    return None


def _parse_subject(text: str) -> Optional[str]:
    for line in text.split("\n")[:12]:
        lowered = line.lower()
        if "en cause" in lowered or "inzake" in lowered or "requête" in lowered:
            stripped = line.strip()
            if stripped:
                return stripped[:200]
    return None


def fetch_decision(
    session: requests.Session,
    nr: int,
    lang: str = "fr",
    added: Optional[str] = None,
    topic: Optional[str] = None,
) -> Optional[dict]:
    """Fetch one arrêt by number. Returns None when it was never published."""
    url = f"{PDF_ENDPOINT}?nr={nr}&l={lang}"

    try:
        response = session.get(url, timeout=60)
    except requests.RequestException as exc:
        print(f"Error fetching decision {nr}: {exc}")
        return None

    if response.status_code != 200:
        return None

    content_type = response.headers.get("Content-Type", "")
    if "pdf" not in content_type.lower() and response.content[:4] != b"%PDF":
        # The unpublished-decision stub, not an error worth logging per number.
        return None

    source_id = f"BE_CONSETAT_{nr}"
    full_text = extract_pdf_markdown(
        source=SOURCE_ID, source_id=source_id,
        pdf_bytes=response.content, table="case_law",
    )
    if not full_text or len(full_text.strip()) < 100:
        return None

    date_iso = _parse_decision_date(full_text)
    ecli_match = _ECLI_RE.search(full_text)

    return {
        "_id": source_id,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "arret_nr": str(nr),
        "year": int(date_iso[:4]) if date_iso else None,
        "title": f"Arrêt n° {nr} - Conseil d'État",
        "date": date_iso,
        "published_at": added,
        "court": "Belgian Council of State",
        "court_type": "administrative",
        "language": lang,
        "subject": _parse_subject(full_text),
        "topic": topic,
        "ecli": ecli_match.group(0) if ecli_match else None,
        "url": url,
        "text": full_text,
        "text_length": len(full_text),
    }


# ---------------------------------------------------------------------------
# Crawls
# ---------------------------------------------------------------------------

def discover_max_nr(session: requests.Session) -> int:
    index = fetch_recent_index(session, months=3)
    if index:
        return max(index)
    print(f"Monthly index empty, falling back to {FALLBACK_MAX_NR}")
    return FALLBACK_MAX_NR


def fetch_all(
    start_nr: int = None,
    end_nr: int = None,
    sample: bool = False,
    limit: int = None,
    resume: bool = True,
) -> Iterator[dict]:
    """Walk the arrêt-number space downwards, yielding every published decision.

    There is no listing behind arr.php, so the number space *is* the index.
    Unpublished numbers come in long contiguous bands (measured: 40/40 misses
    at n° 30,000, ~33/40 at n° 150,000), which is why this does not stop after
    a run of consecutive misses the way the previous version did — that guard
    ended the crawl in the first wide gap below the recent decisions and is the
    reason the corpus stalled around 47K of a ~200K-decision archive.
    """
    session = _session()
    state = load_checkpoint() if resume else {}

    if start_nr is None:
        if resume and state.get("full_cursor"):
            start_nr = int(state["full_cursor"])
            print(f"Resuming full crawl at n° {start_nr}")
        else:
            start_nr = discover_max_nr(session)
            print(f"Highest decision number: {start_nr}")

    if end_nr is None:
        end_nr = start_nr - 500 if sample else FLOOR_NR

    # The monthly index only covers the last 12 months, so it annotates the top
    # of the walk and nothing below it — `published_at`/`topic` are null for the
    # historic bulk, which is the court's own limit, not a parse failure.
    index = fetch_recent_index(session, months=3 if sample else 12)

    count = 0
    target_limit = limit if limit else (100 if sample else None)
    max_nr = int(state.get("max_nr") or 0)

    exhausted = True
    for nr in range(start_nr, max(end_nr, FLOOR_NR) - 1, -1):
        if target_limit is not None and count >= target_limit:
            # Stopped on the limit, not at the floor — the crawl is unfinished,
            # so leave the cursor where it is for the next slot to pick up.
            exhausted = False
            break

        entry = index.get(nr, {})
        record = fetch_decision(
            session, nr, added=entry.get("added"), topic=entry.get("topic")
        )
        if record:
            count += 1
            max_nr = max(max_nr, nr)
            print(f"  [{count}] {record['arret_nr']} - {record['text_length']} chars")
            yield record
            time.sleep(HIT_DELAY)
        else:
            time.sleep(MISS_DELAY)

        if resume and nr % CHECKPOINT_EVERY == 0:
            save_checkpoint({**state, "full_cursor": nr - 1, "max_nr": max_nr})

    if resume:
        if exhausted:
            save_checkpoint({**state, "full_cursor": None, "max_nr": max_nr,
                             "completed_at": datetime.now(timezone.utc).isoformat()})
        else:
            save_checkpoint({**state, "full_cursor": nr, "max_nr": max_nr})


def fetch_updates(since=None) -> Iterator[dict]:
    """Yield only decisions the court published on or after `since`.

    The comparator is `[Ajouté le ...]` from the monthly index — when a
    decision became available to us — not the arrêt's own date, which can be
    months earlier (n° 266,583 was added 25/08/2026, five days *after* the
    higher-numbered 267,389). Filtering on the decision date, or on the number
    alone, would skip those back-filled arrêts entirely.

    The old body was `yield from fetch_all()`: every refresh slot re-walked the
    whole number space and downloaded ~47K PDFs so the loader could discard all
    of them (#1502). A quiet month now costs 12 listing requests.
    """
    session = _session()
    state = load_checkpoint()

    cutoff = _as_date_str(since)
    if not cutoff:
        cutoff = state.get("last_added")
    if not cutoff:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    print(f"Fetching decisions published since {cutoff}")

    index = fetch_recent_index(session, months=12)
    if not index:
        raise RuntimeError(
            "raadvst-consetat.be returned no monthly index — refusing to report "
            "an empty refresh as success"
        )

    fresh = [
        entry for entry in index.values()
        if entry.get("added") and entry["added"] >= cutoff
    ]
    fresh.sort(key=lambda e: (e["added"], e["nr"]), reverse=True)
    print(f"{len(fresh)} of {len(index)} indexed decisions published since {cutoff}")

    newest_in_index = max(
        (e["added"] for e in index.values() if e.get("added")), default=""
    )
    # Nothing new: the whole indexed window is already behind us, so record the
    # newest date it holds. Leaving `last_added` unset would make the next
    # cutoff-less refresh fall back to a blind 30-day window every time.
    newest_added = newest_in_index if not fresh else (state.get("last_added") or "")
    max_nr = int(state.get("max_nr") or 0)
    for entry in fresh:
        record = fetch_decision(
            session, entry["nr"], added=entry.get("added"), topic=entry.get("topic")
        )
        if not record:
            continue
        max_nr = max(max_nr, entry["nr"])
        newest_added = max(newest_added, entry["added"])
        print(f"  {record['arret_nr']} ({entry['added']}) - {record['text_length']} chars")
        yield record
        time.sleep(HIT_DELAY)

    save_checkpoint({**state, "max_nr": max_nr,
                     "last_added": newest_added or state.get("last_added")})


def _as_date_str(since) -> str:
    """Reduce whatever the runner passes into a plain YYYY-MM-DD string."""
    if since is None:
        return ""
    if isinstance(since, datetime):
        return since.date().isoformat()
    text = str(since).strip()
    if not text:
        return ""
    return text[:10]


def normalize(raw: dict) -> dict:
    """Transform raw data into standard schema. Data is already normalized."""
    return raw


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def write_samples(limit: int = 15) -> int:
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    records = []
    for record in fetch_all(sample=True, limit=limit, resume=False):
        records.append(record)
        with open(SAMPLE_DIR / f"{record['_id']}.json", "w", encoding="utf-8") as handle:
            json.dump(record, handle, indent=2, ensure_ascii=False)

    if records:
        with open(SAMPLE_DIR / "all_samples.json", "w", encoding="utf-8") as handle:
            json.dump(records, handle, indent=2, ensure_ascii=False)
        avg = sum(r["text_length"] for r in records) // len(records)
        print(f"\nSaved {len(records)} samples to {SAMPLE_DIR}")
        print(f"Average text length: {avg} chars")
    return len(records)


def write_records(records: Iterator[dict]) -> int:
    """Stream to data/records.jsonl — the file the fleet ingests.

    `bootstrap` used to call the sample writer regardless of --full, so a full
    run produced 100 JSON files under sample/ and nothing the pipeline reads.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    count = 0
    with open(RECORDS_PATH, "a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
            handle.flush()
            count += 1
    print(f"\nWrote {count} records to {RECORDS_PATH}")
    return count


def main():
    parser = argparse.ArgumentParser(description="Belgian Council of State Data Fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "sample", "update", "test"],
        help="Command to execute",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample data only")
    parser.add_argument("--limit", type=int, help="Maximum records to fetch")
    parser.add_argument("--start-nr", type=int, help="Start decision number")
    parser.add_argument("--end-nr", type=int, help="End decision number")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument("--since", help="Cutoff (YYYY-MM-DD) for update")
    parser.add_argument("--no-resume", action="store_true", help="Ignore the checkpoint")

    args = parser.parse_args()

    if args.command == "test":
        session = _session()
        index = fetch_recent_index(session, months=1)
        if not index:
            print("FAILED: monthly index returned no decisions")
            sys.exit(1)
        print(f"Monthly index: {len(index)} decisions, highest n° {max(index)}")
        dated = [e for e in index.values() if e.get("added")]
        print(f"  {len(dated)} carry an 'Ajouté le' date")
        nr = max(index)
        record = fetch_decision(session, nr, added=index[nr].get("added"))
        if not record:
            print(f"FAILED: no PDF for n° {nr}")
            sys.exit(1)
        print(f"  n° {nr}: {record['text_length']} chars, date={record['date']}, "
              f"published_at={record['published_at']}, ecli={record['ecli']}")
        print(f"  {record['text'][:200]}...")
        return

    if args.command == "sample" or (args.command == "bootstrap" and args.sample):
        written = write_samples(limit=args.limit or 15)
        if written < 10:
            print(f"FAILED: only {written} samples")
            sys.exit(1)
        return

    if args.command == "update":
        write_records(fetch_updates(args.since))
        return

    # bootstrap / bootstrap-fast: the fleet entrypoints. Both stream the full
    # corpus to data/records.jsonl and resume from the checkpoint.
    write_records(fetch_all(
        start_nr=args.start_nr,
        end_nr=args.end_nr,
        limit=args.limit,
        resume=not args.no_resume,
    ))


if __name__ == "__main__":
    main()
