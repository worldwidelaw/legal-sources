#!/usr/bin/env python3
"""
LU/TribAdmin-TaxDecisions - Luxembourg Administrative Tribunal Tax Decisions

Fiscal (tax) judgments of the Tribunal administratif and Cour administrative
of the Grand Duchy of Luxembourg.

Access path
-----------
The searchable listing on justice.public.lu (``?r=f/ja_subject_type/fiscal``)
is now behind a FriendlyCaptcha proof-of-work challenge — every request to
``/fr/jurisprudence/juridictions-administratives.html`` is 302'd to
``/challenge.html`` (verified 2026-08-03, issue #1357), which is why the old
listing scraper enumerated nothing.

The decision PDFs themselves are served unauthenticated and unchallenged from
``ja.public.lu`` under a deterministic roll-number scheme:

    https://ja.public.lu/{folder}/{roll}{suffix}.pdf
    folder = "1-15000" for roll <= 15000, else 5000-blocks ("15001-20000", ...)
    suffix = ""   Tribunal administratif judgment
             "C"  Cour administrative (appeal) judgment
             "a"/"A"/"b"/"Ca"/"aC"/"C2"/"CA"  continuation / rectifying rulings

So the corpus is rebuilt by sweeping the roll-number space, extracting the PDF
text, and keeping the decisions that are fiscal on the strength of their own
text (the ``ja_subject_type/fiscal`` facet is not reachable any more). The
classifier keys on the markers that only appear in Luxembourg tax litigation:
the Administration des contributions directes / de l'enregistrement, the loi
générale des impôts (Abgabenordnung), tax assessments (bulletins d'impôt) and
the L.I.R.
"""

import argparse
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Iterable, Optional

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


SOURCE_ID = "LU/TribAdmin-TaxDecisions"

PDF_HOST = "https://ja.public.lu"
# Legacy listing, kept for the `url` of the corpus as a whole / documentation.
LISTING_URL = "https://justice.public.lu/fr/jurisprudence/juridictions-administratives.html"

ID_MIN = 1
# Highest roll number ever probed. The sweep stops early once it has seen
# CEILING_GAP consecutive empty roll numbers past the last hit.
ID_HARD_MAX = 70000
CEILING_GAP = 600

PRIMARY_SUFFIXES = ["", "C"]
EXTRA_SUFFIXES = ["a", "A", "b", "Ca", "aC", "C2", "CA"]

PROBE_WORKERS = 8
REQUEST_DELAY = 0.4  # seconds between PDF downloads
MIN_TEXT_CHARS = 400

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal research; open data collection)",
    "Accept": "application/pdf,*/*;q=0.8",
}

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"
CHECKPOINT_PATH = DATA_DIR / "checkpoint.json"
RECORDS_PATH = DATA_DIR / "records.jsonl"


# --------------------------------------------------------------------------
# Fiscal classification
# --------------------------------------------------------------------------

# Any single strong marker is enough: these phrases do not occur in the
# immigration / urbanism / civil-service decisions that make up the bulk of the
# administrative courts' docket.
STRONG_MARKERS = [
    r"administration des contributions directes",
    r"administration de l['’]enregistrement",
    r"loi g[ée]n[ée]rale des imp[ôo]ts",
    r"\bAbgabenordnung\b",
    r"bulletins?\s+(?:de\s+l['’]|d['’])imp[ôo]t",
    r"bulletins?\s+d['’][ée]tablissement",
    r"imp[ôo]t sur le revenu des collectivit[ée]s",
    r"imp[ôo]t commercial communal",
    r"imp[ôo]t sur la fortune",
    r"§\s*\d+\s*(?:,\s*alin[ée]a\s*\d+\s*,)?\s*AO\b",
]

# Two or more distinct weak markers (>=3 hits total) also qualify.
WEAK_MARKERS = [
    r"\bL\.\s?I\.\s?R\.",
    r"\bLIR\b",
    r"imp[ôo]t sur le revenu",
    r"taxe sur la valeur ajout[ée]e",
    r"directeur des contributions",
    r"r[ée]clamation.{0,40}directeur",
    r"mati[èe]re fiscale",
    r"situation fiscale",
]

_STRONG_RE = [re.compile(p, re.I) for p in STRONG_MARKERS]
_WEAK_RE = [re.compile(p, re.I) for p in WEAK_MARKERS]


def is_fiscal(text: str) -> bool:
    """True when the decision text is a tax decision."""
    for rx in _STRONG_RE:
        if rx.search(text):
            return True
    distinct = 0
    total = 0
    for rx in _WEAK_RE:
        n = len(rx.findall(text))
        if n:
            distinct += 1
            total += n
    return distinct >= 2 and total >= 3


# --------------------------------------------------------------------------
# URL scheme
# --------------------------------------------------------------------------

def folder_for(roll: int) -> str:
    """ja.public.lu groups PDFs in roll-number folders."""
    if roll <= 15000:
        return "1-15000"
    lo = ((roll - 1) // 5000) * 5000 + 1
    return f"{lo}-{lo + 4999}"


def pdf_url(roll: int, suffix: str = "") -> str:
    return f"{PDF_HOST}/{folder_for(roll)}/{roll}{suffix}.pdf"


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def exists(session: requests.Session, url: str, attempts: int = 3) -> bool:
    """HEAD probe; ja.public.lu answers 200 application/pdf or 404."""
    for attempt in range(attempts):
        try:
            resp = session.head(url, timeout=30, allow_redirects=True)
            if resp.status_code == 200:
                return True
            if resp.status_code == 404:
                return False
            # 403/429/5xx -> back off and retry
        except requests.RequestException:
            pass
        time.sleep(1.5 * (attempt + 1))
    return False


# --------------------------------------------------------------------------
# Parsing
# --------------------------------------------------------------------------

FR_MONTHS = {
    "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4, "mai": 5,
    "juin": 6, "juillet": 7, "août": 8, "aout": 8, "septembre": 9,
    "octobre": 10, "novembre": 11, "décembre": 12, "decembre": 12,
}

_HEARING_RE = re.compile(
    r"Audience publique\s*(?:extraordinaire\s*)?du\s+(\d{1,2})(?:er)?\s+"
    r"([A-Za-zéèêûôàç]+)\s+(\d{4})", re.I)
_ECLI_RE = re.compile(r"ECLI:\s*(LU:[A-Z]+:\d{4}:[0-9A-Za-z]+)")
_CHAMBER_RE = re.compile(r"\b(\d)(?:e|re|ère)\s+chambre", re.I)


def parse_date(text: str) -> Optional[str]:
    m = _HEARING_RE.search(text[:4000])
    if not m:
        return None
    day, month_name, year = m.group(1), m.group(2).lower(), m.group(3)
    month = FR_MONTHS.get(month_name)
    if not month:
        return None
    try:
        return datetime(int(year), month, int(day)).date().isoformat()
    except ValueError:
        return None


def build_record(roll: int, suffix: str, url: str, text: str) -> dict:
    case_number = f"{roll}{suffix}"
    head = text[:1200]

    is_appeal = "cour administrative" in head[:400].lower() or suffix.upper().startswith("C")
    court = "Cour administrative" if is_appeal else "Tribunal administratif"

    chamber = None
    m = _CHAMBER_RE.search(head)
    if m:
        chamber = m.group(1)

    ecli = None
    m = _ECLI_RE.search(head)
    if m:
        ecli = "ECLI:" + m.group(1)

    date = parse_date(text)
    title = f"{court} N° {case_number} du rôle"
    if date:
        title += f" ({date})"

    return {
        "_id": f"LU-TAX-{case_number}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date,
        "url": url,
        "court": court,
        "case_number": case_number,
        "ecli": ecli,
        "instance": "Cour" if is_appeal else "Tribunal",
        "chamber": chamber,
        "subject_type": "fiscal",
        "jurisdiction": "Luxembourg",
        "language": "fr",
    }


# --------------------------------------------------------------------------
# Checkpoint
# --------------------------------------------------------------------------

def load_checkpoint() -> dict:
    if CHECKPOINT_PATH.exists():
        try:
            return json.loads(CHECKPOINT_PATH.read_text(encoding="utf-8"))
        except (ValueError, OSError):
            pass
    return {"done_below": None, "seen": []}


def save_checkpoint(state: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = CHECKPOINT_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state), encoding="utf-8")
    tmp.replace(CHECKPOINT_PATH)


# --------------------------------------------------------------------------
# Sweep
# --------------------------------------------------------------------------

def discover_ceiling(session: requests.Session, start: int = 54000) -> int:
    """Walk upward in blocks until CEILING_GAP consecutive roll numbers miss."""
    highest = start
    roll = start
    misses = 0
    while roll < ID_HARD_MAX and misses < CEILING_GAP:
        block = list(range(roll, min(roll + 200, ID_HARD_MAX)))
        with ThreadPoolExecutor(PROBE_WORKERS) as ex:
            hits = list(ex.map(lambda r: exists(session, pdf_url(r)), block))
        for r, hit in zip(block, hits):
            if hit:
                highest = r
                misses = 0
            else:
                misses += 1
        roll += 200
    print(f"  Ceiling detected: roll {highest}")
    return highest


def existing_urls(session: requests.Session, rolls: Iterable[int]) -> list:
    """For a block of roll numbers, return the (roll, suffix, url) that exist."""
    rolls = list(rolls)
    jobs = [(r, s) for r in rolls for s in PRIMARY_SUFFIXES]
    with ThreadPoolExecutor(PROBE_WORKERS) as ex:
        hits = list(ex.map(lambda j: exists(session, pdf_url(*j)), jobs))

    found = [(r, s) for (r, s), hit in zip(jobs, hits) if hit]
    live_rolls = sorted({r for r, _ in found})

    # Continuation rulings only exist where a primary one does.
    if live_rolls:
        extra_jobs = [(r, s) for r in live_rolls for s in EXTRA_SUFFIXES]
        with ThreadPoolExecutor(PROBE_WORKERS) as ex:
            extra_hits = list(ex.map(lambda j: exists(session, pdf_url(*j)), extra_jobs))
        found += [(r, s) for (r, s), hit in zip(extra_jobs, extra_hits) if hit]

    found.sort(key=lambda t: (t[0], t[1]))
    return [(r, s, pdf_url(r, s)) for r, s in found]


def fetch_document(session: requests.Session, roll: int, suffix: str, url: str) -> Optional[dict]:
    try:
        resp = session.get(url, timeout=180)
        resp.raise_for_status()
    except requests.RequestException as exc:
        print(f"    download error {url}: {exc}", file=sys.stderr)
        return None

    if not resp.content.startswith(b"%PDF"):
        return None

    try:
        text = extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=f"{roll}{suffix}",
            pdf_bytes=resp.content,
            table="case_law",
        ) or ""
    except Exception as exc:  # extraction backends can throw anything
        print(f"    extract error {url}: {exc}", file=sys.stderr)
        return None

    if len(text) < MIN_TEXT_CHARS:
        return None
    if not is_fiscal(text):
        return None
    return build_record(roll, suffix, url, text)


def fetch_all(limit: Optional[int] = None,
              resume: bool = True,
              block: int = 200) -> Generator[dict, None, None]:
    """Sweep the roll-number space newest-first and yield fiscal decisions."""
    session = _session()

    state = load_checkpoint() if resume else {"done_below": None}
    ceiling = state.get("ceiling")
    if not ceiling:
        ceiling = discover_ceiling(session)
        state["ceiling"] = ceiling
        save_checkpoint(state)

    top = state.get("done_below") or (ceiling + 1)
    count = 0
    scanned_since_flush = 0

    print(f"Sweeping ja.public.lu roll numbers {ID_MIN}..{top - 1} (newest first)")

    while top > ID_MIN:
        lo = max(ID_MIN, top - block)
        rolls = range(lo, top)
        try:
            candidates = existing_urls(session, rolls)
        except Exception as exc:
            print(f"  probe error for {lo}-{top - 1}: {exc}", file=sys.stderr)
            candidates = []

        if candidates:
            print(f"  rolls {lo}-{top - 1}: {len(candidates)} PDFs")
        for roll, suffix, url in reversed(candidates):
            record = fetch_document(session, roll, suffix, url)
            time.sleep(REQUEST_DELAY)
            if record is None:
                continue
            count += 1
            print(f"  [{count}] {roll}{suffix} {len(record['text'])} chars")
            yield record
            if limit and count >= limit:
                return

        top = lo
        scanned_since_flush += block
        if resume and scanned_since_flush >= block:
            state["done_below"] = top
            save_checkpoint(state)
            scanned_since_flush = 0

    print(f"  Total: {count} fiscal decisions")


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Recent decisions: sweep down from the ceiling until dates predate `since`."""
    session = _session()
    ceiling = discover_ceiling(session)
    top = ceiling + 1
    stale = 0
    while top > ID_MIN and stale < 3:
        lo = max(ID_MIN, top - 200)
        for roll, suffix, url in reversed(existing_urls(session, range(lo, top))):
            record = fetch_document(session, roll, suffix, url)
            time.sleep(REQUEST_DELAY)
            if record is None:
                continue
            if record.get("date") and record["date"] < since.date().isoformat():
                stale += 1
                continue
            stale = 0
            yield record
        top = lo


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def bootstrap_sample(sample_size: int = 15) -> None:
    sample_dir = SCRIPT_DIR / "sample"
    sample_dir.mkdir(exist_ok=True)

    print(f"Bootstrapping {sample_size} sample tax decisions...")
    count = 0
    total = 0
    for record in fetch_all(limit=sample_size, resume=False):
        filename = re.sub(r"[^\w\-.]", "_", record["_id"]) + ".json"
        with open(sample_dir / filename, "w", encoding="utf-8") as fh:
            json.dump(record, fh, indent=2, ensure_ascii=False)
        total += len(record["text"])
        count += 1
        print(f"  Saved: {filename} ({len(record['text'])} chars)")

    if count:
        print(f"\nSample complete: {count} records, avg {total / count:.0f} chars/doc")
    else:
        raise SystemExit("No records fetched — ja.public.lu enumeration returned nothing")


def bootstrap_full() -> None:
    """Stream the whole fiscal corpus to data/records.jsonl (fleet path)."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    written = 0
    with open(RECORDS_PATH, "a", encoding="utf-8") as fh:
        for record in fetch_all(resume=True):
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            fh.flush()
            written += 1
    print(f"complete: {written} written -> {RECORDS_PATH}")
    if written == 0:
        raise SystemExit("No records written — ja.public.lu enumeration returned nothing")


def run_test() -> None:
    session = _session()
    print("Testing ja.public.lu roll-number scheme...")
    probe = existing_urls(session, range(55000, 55040))
    print(f"  rolls 55000-55039: {len(probe)} PDFs")
    for roll, suffix, url in probe[:3]:
        print(f"    {url}")
    if not probe:
        raise SystemExit("ja.public.lu returned no PDFs for a known-populated range")
    roll, suffix, url = probe[0]
    record = fetch_document(session, roll, suffix, url)
    if record:
        print(f"  fiscal sample: {record['_id']} {len(record['text'])} chars, date={record['date']}")
    else:
        print("  first probe was not a fiscal decision (expected — most are not)")


def main():
    parser = argparse.ArgumentParser(
        description="LU/TribAdmin-TaxDecisions - Luxembourg Administrative Tax Decisions")
    parser.add_argument("command",
                        choices=["bootstrap", "bootstrap-fast", "fetch", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample data only")
    parser.add_argument("--limit", type=int, default=15, help="Number of records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    if args.command in ("bootstrap", "bootstrap-fast"):
        if args.command == "bootstrap-fast" or args.full:
            bootstrap_full()
        else:
            bootstrap_sample(args.limit)
    elif args.command == "fetch":
        lim = None if args.full else args.limit
        for record in fetch_all(limit=lim, resume=False):
            print(json.dumps(record, ensure_ascii=False))
    elif args.command == "test":
        run_test()


if __name__ == "__main__":
    main()
