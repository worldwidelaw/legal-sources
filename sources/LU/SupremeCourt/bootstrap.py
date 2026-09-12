#!/usr/bin/env python3
"""
LU/SupremeCourt - Luxembourg Court of Cassation (Cour de Cassation)

Data source: data.public.lu Open Data Portal
Dataset: cour-de-cassation-1 (6a26a933faed83c788068c82)
Format: one ZIP archive per year, each holding born-digital decision PDFs
License: CC BY-ND
Coverage: 1971-present, ~2,500+ arrets

The Cour de Cassation reviews decisions from tribunals and appellate courts
at both criminal and civil level. It only decides questions of law or
application of law, ensuring harmonious application of laws through its
jurisprudence.

Strategy (rewritten 2026-07-30 for GH-1268):
  - The portal renamed the dataset slug (`cour-de-cassation` -> `cour-de-cassation-1`)
    AND changed its shape: it no longer publishes one PDF resource per decision,
    it publishes 48 yearly ZIP archives (~440 MB total). `_resolve_dataset()`
    tries the configured slug and falls back to the portal search API so a
    future rename degrades to a warning instead of a hard 404.
  - Each year archive is streamed to a temp file, opened with `zipfile`, and
    every PDF member is text-extracted in memory (no per-decision HTTP).
  - Completed years are checkpointed to `data/checkpoint.json` so a restarted
    fleet run resumes instead of re-appending the same records.

Usage:
  python bootstrap.py bootstrap --sample   # 15 sample records
  python bootstrap.py bootstrap --full     # full corpus -> data/records.jsonl
  python bootstrap.py bootstrap-fast       # same, fleet entry point
  python bootstrap.py update               # recently-updated years only
  python bootstrap.py test                 # connectivity / shape check
"""

import argparse
import json
import re
import sys
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown  # noqa: E402

# Configuration
SOURCE_ID = "LU/SupremeCourt"
DATASET_SLUG = "cour-de-cassation-1"
DATASET_SEARCH = "cour de cassation"
API_BASE = "https://data.public.lu/api/1"
USER_AGENT = "legal-data-hunter/1.0 (+https://legaldatahunter.com)"

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"
CHECKPOINT_PATH = DATA_DIR / "checkpoint.json"
RECORDS_PATH = DATA_DIR / "records.jsonl"

MIN_TEXT_CHARS = 100


# --------------------------------------------------------------------------
# Dataset discovery
# --------------------------------------------------------------------------

def _get(url: str, **kwargs) -> requests.Response:
    kwargs.setdefault("timeout", 60)
    headers = kwargs.pop("headers", {})
    headers.setdefault("User-Agent", USER_AGENT)
    return requests.get(url, headers=headers, **kwargs)


def _resolve_dataset() -> dict:
    """Return the dataset payload, tolerating a renamed slug.

    data.public.lu renamed `cour-de-cassation` to `cour-de-cassation-1` in 2026
    (GH-1268), which 404'd the whole crawl. Search is the fallback so the next
    rename is survivable.
    """
    resp = _get(f"{API_BASE}/datasets/{DATASET_SLUG}/")
    if resp.status_code == 200:
        return resp.json()

    print(
        f"[WARN] dataset slug {DATASET_SLUG!r} returned HTTP {resp.status_code} — "
        f"falling back to portal search",
        file=sys.stderr,
    )
    search = _get(f"{API_BASE}/datasets/", params={"q": DATASET_SEARCH, "page_size": 20})
    search.raise_for_status()
    for candidate in search.json().get("data", []):
        title = (candidate.get("title") or "").strip().lower()
        if "cassation" in title:
            slug = candidate.get("slug")
            print(f"[WARN] resolved dataset via search: slug={slug}", file=sys.stderr)
            detail = _get(f"{API_BASE}/datasets/{slug}/")
            detail.raise_for_status()
            return detail.json()

    raise RuntimeError(
        f"Could not resolve the Cour de cassation dataset on data.public.lu "
        f"(slug {DATASET_SLUG!r} is gone and search for {DATASET_SEARCH!r} found nothing)"
    )


def get_year_archives() -> list[dict]:
    """Return the yearly ZIP resources, newest year first."""
    dataset = _resolve_dataset()
    archives = []
    for resource in dataset.get("resources", []):
        url = resource.get("url", "")
        title = resource.get("title", "")
        if (resource.get("format", "").lower() != "zip") and not url.lower().endswith(".zip"):
            continue
        year_match = re.search(r"(\d{4})", title or url)
        if not year_match:
            continue
        archives.append(
            {
                "year": int(year_match.group(1)),
                "title": title,
                "url": url,
                "last_modified": resource.get("last_modified") or resource.get("created_at"),
            }
        )
    archives.sort(key=lambda a: a["year"], reverse=True)
    return archives


# --------------------------------------------------------------------------
# Metadata parsing
# --------------------------------------------------------------------------

def extract_metadata_from_filename(filename: str) -> dict:
    """Extract case metadata from a decision PDF filename.

    Both separator conventions occur in the archives:
      20240104_CAS-2023-00015_04_pseudonymise-accessible.pdf   (2020s)
      20260129-cas-2025-00151-32-pseudonymise-accessible.pdf   (legacy layout)
      19961128_CASS_1283_pseudonymise-accessible.pdf           (1990s)
      20130117_3081a-accessible.pdf                            (sequence-only)
    """
    metadata = {"date": None, "case_number": None, "decision_number": None}
    stem = Path(filename).name

    date_match = re.match(r"^(\d{4})(\d{2})(\d{2})", stem)
    if date_match:
        year, month, day = date_match.groups()
        metadata["date"] = f"{year}-{month}-{day}"

    case_match = re.search(r"cas[s]?[-_](\d{4})[-_](\d+)", stem, re.IGNORECASE)
    if case_match:
        year, num = case_match.groups()
        metadata["case_number"] = f"CAS-{year}-{num.zfill(5)}"
    else:
        case_match = re.search(r"cas[s]?[-_](\d+)", stem, re.IGNORECASE)
        if case_match:
            metadata["case_number"] = f"CASS-{case_match.group(1)}"

    decision_match = re.search(r"[-_](\d+)[-_](?:pseudonymis|accessible)", stem, re.IGNORECASE)
    if decision_match:
        metadata["decision_number"] = decision_match.group(1)

    return metadata


def build_record_id(filename: str, meta: dict) -> str:
    """Stable _id, kept compatible with the pre-2026 `LU-CASS-{case}-{dec}` scheme."""
    case_num = meta.get("case_number")
    if case_num:
        dec_num = meta.get("decision_number") or "0"
        core = case_num.replace("CAS-", "").replace("CASS-", "")
        return f"LU-CASS-{core}-{dec_num}"

    # Sequence-only filenames (e.g. 20130117_3081a-accessible.pdf) carry no case
    # number; fall back to the filename stem so they cannot all collide on
    # LU-CASS-UNKNOWN-0.
    stem = Path(filename).stem
    stem = re.sub(r"[-_](pseudonymis[eé]?|accessible)", "", stem, flags=re.IGNORECASE)
    stem = re.sub(r"[^0-9A-Za-z]+", "-", stem).strip("-")
    return f"LU-CASS-{stem}"


def extract_title_from_text(text: str, filename: str) -> str:
    """Extract a meaningful title from the decision text."""
    title_match = re.search(
        r"N°\s*(\d+)\s*/\s*(\d{4})\s*(?:p[ée]n(?:al)?\.?)?\s*du\s*(\d{1,2}[./]\d{1,2}[./]\d{4})",
        text[:500],
    )
    if title_match:
        num, year, date = title_match.groups()
        return f"Arrêt N° {num}/{year} du {date}"

    simple_match = re.search(r"N°\s*(\d+)\s*/\s*(\d+)", text[:300])
    if simple_match:
        num, year = simple_match.groups()
        return f"Arrêt N° {num}/{year}"

    return Path(filename).stem.replace("_", " ").replace("-", " ").strip()


def normalize(raw: dict, text: str) -> Optional[dict]:
    """Transform a raw archive member into the normalized schema."""
    if not text or len(text) < MIN_TEXT_CHARS:
        return None

    filename = raw.get("filename", "")
    meta = extract_metadata_from_filename(filename)

    date = meta.get("date")
    if not date and raw.get("year"):
        date = f"{raw['year']}-01-01"

    return {
        "_id": build_record_id(filename, meta),
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": extract_title_from_text(text, filename),
        "text": text,
        "date": date,
        "url": raw.get("archive_url", ""),
        "case_number": meta.get("case_number"),
        "decision_number": meta.get("decision_number"),
        "pdf_filename": filename,
        "year": raw.get("year"),
        "court": "Cour de Cassation",
        "jurisdiction": "Luxembourg",
        "language": "fr",
    }


# --------------------------------------------------------------------------
# Crawl
# --------------------------------------------------------------------------

def _load_checkpoint() -> dict:
    if CHECKPOINT_PATH.exists():
        try:
            return json.loads(CHECKPOINT_PATH.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            pass
    return {"completed_years": []}


def _save_checkpoint(state: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def _iter_archive_pdfs(archive: dict) -> Generator[dict, None, None]:
    """Download one year ZIP and yield {filename, pdf_bytes} for each member PDF."""
    with tempfile.NamedTemporaryFile(suffix=".zip") as tmp:
        with _get(archive["url"], stream=True, timeout=300) as resp:
            resp.raise_for_status()
            for chunk in resp.iter_content(chunk_size=1 << 20):
                if chunk:
                    tmp.write(chunk)
        tmp.flush()

        try:
            zf = zipfile.ZipFile(tmp.name)
        except zipfile.BadZipFile as exc:
            print(f"[ERROR] {archive['title']}: bad zip ({exc})", file=sys.stderr)
            return

        with zf:
            for name in sorted(zf.namelist()):
                if not name.lower().endswith(".pdf"):
                    continue
                try:
                    payload = zf.read(name)
                except (KeyError, zipfile.BadZipFile) as exc:
                    print(f"[ERROR] {archive['title']}/{name}: {exc}", file=sys.stderr)
                    continue
                yield {
                    "filename": Path(name).name,
                    "pdf_bytes": payload,
                    "year": archive["year"],
                    "archive_url": archive["url"],
                }


def fetch_all(
    limit: Optional[int] = None,
    use_checkpoint: bool = False,
    years: Optional[list[int]] = None,
) -> Generator[dict, None, None]:
    """Yield every decision, newest year first."""
    archives = get_year_archives()
    if years is not None:
        archives = [a for a in archives if a["year"] in years]

    state = _load_checkpoint() if use_checkpoint else {"completed_years": []}
    done = set(state.get("completed_years", []))

    print(f"Found {len(archives)} year archives on data.public.lu")
    count = 0

    for archive in archives:
        if archive["year"] in done:
            print(f"  skip {archive['year']} (checkpointed)")
            continue

        print(f"  [{archive['year']}] downloading {archive['title']}")
        year_count = 0
        for raw in _iter_archive_pdfs(archive):
            if limit and count >= limit:
                print(f"\nTotal records fetched: {count}")
                return
            try:
                text = extract_pdf_markdown(
                    source=SOURCE_ID,
                    source_id=build_record_id(
                        raw["filename"], extract_metadata_from_filename(raw["filename"])
                    ),
                    pdf_bytes=raw["pdf_bytes"],
                    table="case_law",
                )
            except Exception as exc:  # noqa: BLE001 - one bad PDF must not kill the run
                print(f"    [ERROR] {raw['filename']}: {exc}", file=sys.stderr)
                continue

            record = normalize(raw, text or "")
            if record is None:
                continue

            yield record
            count += 1
            year_count += 1

        print(f"  [{archive['year']}] {year_count} decisions")
        if use_checkpoint:
            done.add(archive["year"])
            state["completed_years"] = sorted(done)
            _save_checkpoint(state)

    print(f"\nTotal records fetched: {count}")


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Yield decisions from year archives republished since `since`."""
    recent_years = []
    for archive in get_year_archives():
        stamp = archive.get("last_modified") or ""
        try:
            modified = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            recent_years.append(archive["year"])
            continue
        if modified.tzinfo is None:
            modified = modified.replace(tzinfo=timezone.utc)
        if modified >= since:
            recent_years.append(archive["year"])

    if not recent_years:
        print("No year archives republished since the given date")
        return

    print(f"Refreshing year archives: {sorted(recent_years, reverse=True)}")
    yield from fetch_all(years=recent_years)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def bootstrap_sample(sample_size: int = 15) -> None:
    sample_dir = SCRIPT_DIR / "sample"
    sample_dir.mkdir(exist_ok=True)

    print(f"Bootstrapping {sample_size} sample records...")
    count = 0
    total_text_len = 0

    for record in fetch_all(limit=sample_size):
        filepath = sample_dir / f"{record['_id']}.json"
        with open(filepath, "w", encoding="utf-8") as fh:
            json.dump(record, fh, indent=2, ensure_ascii=False)
        text_len = len(record.get("text", ""))
        total_text_len += text_len
        count += 1
        print(f"  Saved: {filepath.name} ({text_len} chars)")

    if count:
        print(f"\nSample complete: {count} records, avg {total_text_len / count:.0f} chars/doc")
    else:
        print("\nNo records fetched!", file=sys.stderr)
        sys.exit(1)


def bootstrap_full() -> None:
    """Stream the full corpus to data/records.jsonl (fleet entry point)."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    count = 0
    with open(RECORDS_PATH, "a", encoding="utf-8") as fh:
        for record in fetch_all(use_checkpoint=True):
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            fh.flush()
            count += 1

    print(f"bootstrap_fast complete: {count} fetched")
    if count == 0:
        print("[ERROR] No records written!", file=sys.stderr)
        sys.exit(1)


def run_test() -> None:
    print("Testing connection to data.public.lu...")
    archives = get_year_archives()
    print(f"Dataset exposes {len(archives)} year archives "
          f"({archives[-1]['year']}-{archives[0]['year']})" if archives else "no archives")
    if not archives:
        sys.exit(1)

    newest = archives[0]
    for raw in _iter_archive_pdfs(newest):
        text = extract_pdf_markdown(
            source=SOURCE_ID, source_id="test",
            pdf_bytes=raw["pdf_bytes"], table="case_law", force=True,
        ) or ""
        print(f"\nTesting PDF: {raw['filename']}")
        print(f"Extracted {len(text)} chars")
        print(f"First 500 chars:\n{text[:500]}")
        break


def main() -> None:
    parser = argparse.ArgumentParser(
        description="LU/SupremeCourt - Luxembourg Court of Cassation fetcher"
    )
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "fetch", "update", "test"],
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample data only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument("--limit", type=int, default=15)
    parser.add_argument("--since", type=str, default=None, help="ISO date for update")

    args = parser.parse_args()

    if args.command == "bootstrap-fast" or (args.command == "bootstrap" and args.full):
        bootstrap_full()
    elif args.command == "bootstrap":
        bootstrap_sample(args.limit)
    elif args.command == "fetch":
        for record in fetch_all(limit=args.limit or None):
            print(json.dumps(record, ensure_ascii=False))
    elif args.command == "update":
        since = (
            datetime.fromisoformat(args.since)
            if args.since
            else datetime.now(timezone.utc).replace(month=1, day=1)
        )
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        for record in fetch_updates(since):
            print(json.dumps(record, ensure_ascii=False))
    elif args.command == "test":
        run_test()


if __name__ == "__main__":
    main()
