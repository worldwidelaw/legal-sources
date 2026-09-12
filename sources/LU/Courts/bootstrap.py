#!/usr/bin/env python3
"""
LU/Courts - Luxembourg Courts Decisions

Strategy (rewritten 2026-08-02 for GH-1331):
  justice.public.lu put every `/fr/jurisprudence/*` listing page behind a
  FriendlyCaptcha V2 challenge (`/challenge.html?return=...`), so the old
  HTML-listing crawl received a 1,497-byte challenge stub for every page and
  wrote 0 records. Only search-engine crawler user-agents are exempted, which
  we do not impersonate. Both halves of the corpus are re-pointed at access
  paths the publisher serves without a challenge:

  1. Judicial courts -> data.public.lu bulk open data.
     The "Administration judiciaire" organisation publishes 96 datasets (one
     per court/chamber/matter) holding ~1,557 yearly ZIP archives (~5 GB) of
     the same pseudonymised decision PDFs. Each ZIP is streamed to a temp
     file, opened with `zipfile`, and every PDF member is text-extracted in
     memory. `cour-de-cassation-1` is skipped: it is already LU/SupremeCourt.

  2. Administrative courts -> ja.public.lu direct PDF sweep.
     ja.public.lu serves `{bucket}/{role}.pdf` and `{bucket}/{role}C.pdf`
     (C = Cour administrative appeal) with no challenge; only directory
     indexes are 403. Rôle numbers are swept over the live ID space
     (~12,000-55,000, ~50% dense) instead of read off the gated listing.

  Completed resources and the last swept rôle number are checkpointed to
  `data/checkpoint.json` so a restarted fleet run resumes instead of
  re-appending the same records.

Usage:
  python bootstrap.py bootstrap --sample   # 15 sample records
  python bootstrap.py bootstrap --full     # full corpus -> data/records.jsonl
  python bootstrap.py bootstrap-fast       # same, fleet entry point
  python bootstrap.py update               # recently-updated datasets only
  python bootstrap.py test                 # connectivity / shape check
"""

import argparse
import json
import re
import sys
import tempfile
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

import requests

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown  # noqa: E402


SOURCE_ID = "LU/Courts"

# --- judicial: data.public.lu bulk open data ------------------------------
API_BASE = "https://data.public.lu/api/1"
ORG_ID = "656ed2c324e7ba9489a8025f"  # Administration judiciaire
# Cour de Cassation is already covered end-to-end by LU/SupremeCourt.
EXCLUDE_SLUGS = {"cour-de-cassation-1"}

# --- administrative: ja.public.lu direct PDF sweep ------------------------
JA_HOST = "https://ja.public.lu"
JA_BUCKET_SIZE = 5000
# Live probe 2026-08-03: the archive starts at the `15001-20000` bucket. Every
# rôle in 1-15,000 404s (probed 1-300, 5,000-5,300, 11,800-12,100, 13,000-13,150,
# 14,000-15,000 exhaustively, both suffixes: zero hits), so starting at 1 costs
# ~60,000 dead round-trips before the first record.
JA_MIN_ID = 15001
# Hits run ~15,001-55,300 at ~30-65% density; 55,300-56,050 is empty. Sweep well
# past the observed ceiling so future rôle numbers are picked up instead of being
# silently truncated by a stale hardcoded max.
JA_MAX_ID = 60000
JA_SUFFIXES = ("", "C")

# --- gated listing pages (kept only so `test` can report the block) --------
JUDICIAL_URL = "https://justice.public.lu/fr/jurisprudence/juridictions-judiciaires.html"
ADMIN_URL = "https://justice.public.lu/fr/jurisprudence/juridictions-administratives.html"

USER_AGENT = "legal-data-hunter/1.0 (+https://legaldatahunter.com)"
HEADERS = {"User-Agent": USER_AGENT}

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"
CHECKPOINT_PATH = DATA_DIR / "checkpoint.json"
RECORDS_PATH = DATA_DIR / "records.jsonl"

MIN_TEXT_CHARS = 100
REQUEST_DELAY = 0.4


# --------------------------------------------------------------------------
# HTTP helpers
# --------------------------------------------------------------------------

def _session() -> requests.Session:
    session = requests.Session()
    session.headers.update(HEADERS)
    return session


def _get(session: requests.Session, url: str, *, stream: bool = False,
         timeout: int = 120, attempts: int = 4) -> Optional[requests.Response]:
    """GET with backoff. Returns None on a clean 404, raises after retries."""
    delay = 2.0
    last_exc = None
    for _ in range(attempts):
        try:
            resp = session.get(url, stream=stream, timeout=timeout)
            if resp.status_code == 404:
                return None
            if resp.status_code in (429, 500, 502, 503, 504):
                retry_after = resp.headers.get("Retry-After", "")
                wait = float(retry_after) if retry_after.isdigit() else delay
                time.sleep(min(wait, 120))
                delay = min(delay * 2, 120)
                continue
            resp.raise_for_status()
            return resp
        except requests.RequestException as exc:
            last_exc = exc
            time.sleep(delay)
            delay = min(delay * 2, 120)
    if last_exc:
        raise last_exc
    raise RuntimeError(f"exhausted retries for {url}")


# --------------------------------------------------------------------------
# Checkpointing
# --------------------------------------------------------------------------

def _load_checkpoint() -> dict:
    if CHECKPOINT_PATH.exists():
        try:
            with open(CHECKPOINT_PATH, encoding="utf-8") as fh:
                data = json.load(fh)
            data.setdefault("done_resources", [])
            data.setdefault("admin_next_id", JA_MIN_ID)
            return data
        except (json.JSONDecodeError, OSError):
            pass
    return {"done_resources": [], "admin_next_id": JA_MIN_ID}


def _save_checkpoint(checkpoint: dict) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = CHECKPOINT_PATH.with_suffix(".tmp")
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(checkpoint, fh)
    tmp.replace(CHECKPOINT_PATH)


# --------------------------------------------------------------------------
# Text / metadata extraction
# --------------------------------------------------------------------------

def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    return extract_pdf_markdown(
        source=SOURCE_ID,
        source_id="",
        pdf_bytes=pdf_bytes,
        table="case_law",
    ) or ""


_MONTHS = {
    "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4, "mai": 5,
    "juin": 6, "juillet": 7, "août": 8, "aout": 8, "septembre": 9,
    "octobre": 10, "novembre": 11, "décembre": 12, "decembre": 12,
}


def _date_from_filename(filename: str) -> Optional[str]:
    """Judicial PDFs are named `YYYYMMDD_...` / `YYYYMMDD-...`."""
    match = re.match(r"(\d{4})(\d{2})(\d{2})[_-]", filename)
    if not match:
        return None
    year, month, day = (int(group) for group in match.groups())
    if not (1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31):
        return None
    return f"{year:04d}-{month:02d}-{day:02d}"


def _date_from_text(text: str) -> Optional[str]:
    """Administrative PDFs carry `Audience publique ... du 26 mars 2026`."""
    match = re.search(
        r"[Aa]udience\s+publique[^\n]{0,60}?du\s+(\d{1,2})(?:er)?\s+"
        r"([A-Za-zéûôàèç]+)\s+(\d{4})",
        text[:2000],
    )
    if not match:
        return None
    month = _MONTHS.get(match.group(2).lower())
    if not month:
        return None
    return f"{int(match.group(3)):04d}-{month:02d}-{int(match.group(1)):02d}"


def _case_number_from_filename(filename: str) -> Optional[str]:
    """`20260327_CA4_CAL-2024-00067_pseudonymisé-accessible.pdf` -> CAL-2024-00067."""
    # Court codes vary in case and separator: CACHACO, CA-ChaCo11, "CA ChaCo11".
    name = re.sub(r"\s+", "-", filename)
    match = re.search(r"_([A-Z]{2,}-\d{4}-\d+)[_-]", name)
    if match:
        return match.group(1)
    match = re.search(r"[_-]([A-Za-z][A-Za-z0-9-]*)_(\d+[A-Za-z]?)[_-]", name)
    if match:
        return f"{match.group(1).upper()}-{match.group(2)}"
    return None


_FILENAME_SUFFIX_RE = re.compile(
    r"[_-]?(pseudonymis[eé]|anonymis[eé]e?)?[_-]?accessible$", re.IGNORECASE
)


def _slug_id(value: str) -> str:
    """Slugify a PDF stem, dropping the boilerplate `-pseudonymisé-accessible` tail."""
    stem = _FILENAME_SUFFIX_RE.sub("", value)
    return re.sub(r"[^A-Za-z0-9._-]+", "-", stem).strip("-_.")[:120]


def _title_from_text(text: str, fallback: str) -> str:
    head = text[:600]
    match = re.search(
        r"(Tribunal administratif|Cour administrative)[^\n]*?N[°o]\s*(\d+[A-Z]?)\s*du rôle",
        head,
    )
    if match:
        return f"{match.group(1)} N° {match.group(2)}"
    match = re.search(r"(Arrêt|Jugement|Ordonnance)\s*N[°o]\s*([\w/-]+)", head)
    if match:
        return f"{match.group(1)} N° {match.group(2)}"
    first_line = next((line.strip() for line in head.splitlines() if line.strip()), "")
    if 10 <= len(first_line) <= 160:
        return first_line
    return fallback


# --------------------------------------------------------------------------
# Normalisation
# --------------------------------------------------------------------------

def normalize_judicial(meta: dict, text: str) -> dict:
    filename = meta["filename"]
    case_number = _case_number_from_filename(filename)
    ident = case_number or _slug_id(filename.rsplit(".", 1)[0])
    fallback_title = filename.replace("-accessible.pdf", "").replace("_", " ")
    return {
        "_id": f"LU-JUD-{ident}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": _title_from_text(text, fallback_title),
        "text": text,
        "date": _date_from_filename(filename) or _date_from_text(text),
        "url": meta["url"],
        "court": meta["court"],
        "court_type": "judicial",
        "case_number": case_number,
        "ecli": None,
        "chamber": meta.get("chamber"),
        "content_type": meta.get("content_type"),
        "jurisdiction": "Luxembourg",
        "language": "fr",
        "pdf_filename": filename,
        "dataset": meta.get("dataset"),
    }


def normalize_admin(role: str, url: str, text: str) -> dict:
    court = "Cour administrative" if role.endswith("C") else "Tribunal administratif"
    ecli_match = re.search(r"ECLI:(LU:[A-Z]+:\d{4}:\d+[A-Z]?)", text[:2000])
    chamber_match = re.search(r"(\d+)e?\s+chambre", text[:800], re.IGNORECASE)
    return {
        "_id": f"LU-ADM-{role}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": _title_from_text(text, f"{court} N° {role}"),
        "text": text,
        "date": _date_from_text(text),
        "url": url,
        "court": court,
        "court_type": "administrative",
        "case_number": role,
        "ecli": "ECLI:" + ecli_match.group(1) if ecli_match else None,
        "chamber": chamber_match.group(1) if chamber_match else None,
        "content_type": "Administratif",
        "jurisdiction": "Luxembourg",
        "language": "fr",
        "pdf_filename": f"{role}.pdf",
        "dataset": None,
    }


# --------------------------------------------------------------------------
# Judicial: data.public.lu bulk ZIPs
# --------------------------------------------------------------------------

def list_judicial_datasets(session: requests.Session) -> list:
    """Return the judiciary organisation's datasets, each with its resources."""
    url = f"{API_BASE}/organizations/{ORG_ID}/datasets/?page_size=200"
    datasets = []
    while url:
        resp = _get(session, url, timeout=90)
        if resp is None:
            raise RuntimeError(f"data.public.lu returned 404 for {url}")
        payload = resp.json()
        datasets.extend(entry for entry in payload.get("data", [])
                        if entry["slug"] not in EXCLUDE_SLUGS)
        url = payload.get("next_page")
    if not datasets:
        raise RuntimeError(
            "data.public.lu returned no datasets for the Administration judiciaire "
            "organisation — the bulk open-data path moved or the API is down"
        )
    return datasets


def _iter_zip_pdfs(session: requests.Session,
                   resource: dict) -> Generator[tuple, None, None]:
    """Stream one yearly ZIP to disk and yield (member_name, pdf_bytes)."""
    resp = _get(session, resource["url"], stream=True, timeout=600)
    if resp is None:
        print(f"    resource 404: {resource['url']}", file=sys.stderr)
        return
    with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
        tmp_path = Path(tmp.name)
        for chunk in resp.iter_content(chunk_size=1 << 20):
            if chunk:
                tmp.write(chunk)
    try:
        with zipfile.ZipFile(tmp_path) as archive:
            for name in sorted(archive.namelist()):
                if not name.lower().endswith(".pdf"):
                    continue
                try:
                    yield name.split("/")[-1], archive.read(name)
                except (KeyError, zipfile.BadZipFile, RuntimeError) as exc:
                    print(f"    member error {name}: {exc}", file=sys.stderr)
    except zipfile.BadZipFile as exc:
        print(f"    bad zip {resource['url']}: {exc}", file=sys.stderr)
    finally:
        tmp_path.unlink(missing_ok=True)


def fetch_judicial(session: requests.Session, limit: Optional[int] = None,
                   checkpoint: Optional[dict] = None,
                   datasets: Optional[list] = None) -> Generator[dict, None, None]:
    datasets = datasets if datasets is not None else list_judicial_datasets(session)
    done = set(checkpoint["done_resources"]) if checkpoint else set()
    count = 0

    print(f"\nJudicial: {len(datasets)} data.public.lu datasets")
    for dataset in datasets:
        court = dataset["title"]
        for resource in dataset.get("resources", []):
            if limit and count >= limit:
                return
            if (resource.get("format") or "").lower() != "zip":
                continue
            resource_id = resource.get("id") or resource["url"]
            if resource_id in done:
                continue

            print(f"  {dataset['slug']} / {resource.get('title')}")
            for filename, pdf_bytes in _iter_zip_pdfs(session, resource):
                if limit and count >= limit:
                    return
                text = extract_text_from_pdf(pdf_bytes)
                if len(text) < MIN_TEXT_CHARS:
                    continue
                yield normalize_judicial(
                    {
                        "filename": filename,
                        "url": resource["url"],
                        "court": court,
                        "dataset": dataset["slug"],
                    },
                    text,
                )
                count += 1

            if checkpoint is not None:
                checkpoint["done_resources"].append(resource_id)
                _save_checkpoint(checkpoint)
            time.sleep(REQUEST_DELAY)

    print(f"  Judicial: {count} records")


# --------------------------------------------------------------------------
# Administrative: ja.public.lu rôle-number sweep
# --------------------------------------------------------------------------

def _ja_bucket(role_num: int) -> str:
    low = ((role_num - 1) // JA_BUCKET_SIZE) * JA_BUCKET_SIZE + 1
    return f"{low}-{low + JA_BUCKET_SIZE - 1}"


def ja_url(role_num: int, suffix: str = "") -> str:
    return f"{JA_HOST}/{_ja_bucket(role_num)}/{role_num}{suffix}.pdf"


def fetch_admin(session: requests.Session, limit: Optional[int] = None,
                checkpoint: Optional[dict] = None,
                start_id: Optional[int] = None,
                end_id: int = JA_MAX_ID) -> Generator[dict, None, None]:
    start = start_id if start_id is not None else (
        checkpoint["admin_next_id"] if checkpoint else JA_MIN_ID
    )
    count = 0
    print(f"\nAdministrative: sweeping ja.public.lu rôle {start}-{end_id}")

    for role_num in range(start, end_id + 1):
        for suffix in JA_SUFFIXES:
            if limit and count >= limit:
                return
            url = ja_url(role_num, suffix)
            try:
                resp = _get(session, url, timeout=120)
            except requests.RequestException as exc:
                print(f"    {role_num}{suffix}: {exc}", file=sys.stderr)
                continue
            if resp is None:
                continue
            if "pdf" not in (resp.headers.get("Content-Type") or "").lower():
                continue
            text = extract_text_from_pdf(resp.content)
            if len(text) < MIN_TEXT_CHARS:
                print(f"    {role_num}{suffix}: {len(text)} chars, skipped",
                      file=sys.stderr)
                continue
            yield normalize_admin(f"{role_num}{suffix}", url, text)
            count += 1
            time.sleep(REQUEST_DELAY)

        if checkpoint is not None and role_num % 100 == 0:
            checkpoint["admin_next_id"] = role_num + 1
            _save_checkpoint(checkpoint)

    if checkpoint is not None:
        checkpoint["admin_next_id"] = end_id + 1
        _save_checkpoint(checkpoint)
    print(f"  Administrative: {count} records")


# --------------------------------------------------------------------------
# Public entry points
# --------------------------------------------------------------------------

def fetch_all(limit: Optional[int] = None,
              checkpoint: Optional[dict] = None) -> Generator[dict, None, None]:
    session = _session()
    judicial_limit = max(1, limit // 2) if limit else None
    admin_limit = (limit - judicial_limit) if limit else None

    yield from fetch_judicial(session, limit=judicial_limit, checkpoint=checkpoint)
    yield from fetch_admin(session, limit=admin_limit, checkpoint=checkpoint)


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Judicial datasets touched since `since`, plus the newest rôle numbers."""
    session = _session()
    fresh = []
    for dataset in list_judicial_datasets(session):
        resources = []
        for resource in dataset.get("resources", []):
            stamp = resource.get("last_modified") or resource.get("created_at") or ""
            try:
                modified = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
            except ValueError:
                resources.append(resource)
                continue
            if modified.tzinfo is None:
                modified = modified.replace(tzinfo=timezone.utc)
            if modified >= since:
                resources.append(resource)
        if resources:
            fresh.append({**dataset, "resources": resources})

    if fresh:
        yield from fetch_judicial(session, datasets=fresh)

    # Administrative rôle numbers are allocated in ascending order, so re-sweep
    # the tail of the ID space to pick up newly published decisions.
    yield from fetch_admin(session, start_id=max(JA_MIN_ID, JA_MAX_ID - 8000))


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def _write_sample(sample_dir: Path, record: dict) -> None:
    filename = re.sub(r"[^\w.-]", "_", record["_id"]) + ".json"
    with open(sample_dir / filename, "w", encoding="utf-8") as fh:
        json.dump(record, fh, indent=2, ensure_ascii=False)
    print(f"  Saved: {filename} ({len(record['text'])} chars)")


def bootstrap_sample(sample_size: int = 15) -> None:
    sample_dir = SCRIPT_DIR / "sample"
    sample_dir.mkdir(exist_ok=True)
    print(f"Bootstrapping {sample_size} sample records...")

    session = _session()
    judicial_target = sample_size // 2
    count = 0
    total_chars = 0

    for record in fetch_judicial(session, limit=judicial_target):
        _write_sample(sample_dir, record)
        total_chars += len(record["text"])
        count += 1
    # Start the sample sweep at the dense recent end of the rôle space so the
    # sample builds in seconds rather than walking 12,000 empty IDs.
    for record in fetch_admin(session, limit=sample_size - judicial_target,
                              start_id=53850):
        _write_sample(sample_dir, record)
        total_chars += len(record["text"])
        count += 1

    if not count:
        raise RuntimeError("No sample records fetched — both access paths failed")
    print(f"\nSample complete: {count} records, avg {total_chars / count:.0f} chars/doc")


def bootstrap_full() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    checkpoint = _load_checkpoint()
    seen = set()
    count = 0

    with open(RECORDS_PATH, "a", encoding="utf-8") as fh:
        for record in fetch_all(checkpoint=checkpoint):
            if record["_id"] in seen:
                continue
            seen.add(record["_id"])
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            fh.flush()
            count += 1
            if count % 500 == 0:
                print(f"  ... {count} records written")

    print(f"bootstrap_fast complete: {count} fetched -> {RECORDS_PATH}")
    if count == 0:
        raise RuntimeError(
            "0 records written — both the data.public.lu bulk path and the "
            "ja.public.lu rôle sweep failed; do not treat this as a completed run"
        )


def run_test() -> None:
    session = _session()
    print("data.public.lu (judicial bulk):")
    datasets = list_judicial_datasets(session)
    resources = sum(len(d.get("resources", [])) for d in datasets)
    size = sum((r.get("filesize") or 0) for d in datasets for r in d.get("resources", []))
    print(f"  {len(datasets)} datasets, {resources} resources, {size / 1e9:.2f} GB")

    print("ja.public.lu (administrative sweep):")
    hits = sum(1 for role_num in range(53860, 53870)
               if session.head(ja_url(role_num), timeout=30).status_code == 200)
    print(f"  {hits}/10 probe rôle numbers served")

    print("justice.public.lu listing pages (expected: captcha-gated):")
    for label, url in (("judicial", JUDICIAL_URL), ("administrative", ADMIN_URL)):
        resp = session.get(url, timeout=60, allow_redirects=True)
        gated = "challenge.html" in resp.url
        print(f"  {label}: HTTP {resp.status_code} "
              f"{'FriendlyCaptcha challenge' if gated else resp.url}")


def main() -> None:
    parser = argparse.ArgumentParser(description="LU/Courts fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "bootstrap_fast", "fetch",
                 "update", "test"],
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample data only")
    parser.add_argument("--full", action="store_true", help="Fetch the full corpus")
    parser.add_argument("--limit", type=int, default=15)
    parser.add_argument("--since", type=str, default=None,
                        help="ISO date for `update` (default: start of this month)")
    args = parser.parse_args()

    if args.command in ("bootstrap-fast", "bootstrap_fast"):
        bootstrap_full()
    elif args.command == "bootstrap":
        if args.full:
            bootstrap_full()
        else:
            bootstrap_sample(args.limit)
    elif args.command == "fetch":
        for record in fetch_all(limit=args.limit):
            print(json.dumps(record, ensure_ascii=False))
    elif args.command == "update":
        since = (datetime.fromisoformat(args.since) if args.since
                 else datetime.now(timezone.utc).replace(day=1))
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        for record in fetch_updates(since):
            print(json.dumps(record, ensure_ascii=False))
    elif args.command == "test":
        run_test()


if __name__ == "__main__":
    main()
