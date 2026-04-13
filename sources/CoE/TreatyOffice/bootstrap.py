#!/usr/bin/env python3
"""
CoE/TreatyOffice — Council of Europe Treaty Office

Fetches all CoE treaties (conventions, charters, protocols) via the
conventions-ws.coe.int JSON API, downloads English PDF full texts from
rm.coe.int, and extracts text via common/pdf_extract.

Data coverage: ~232 treaties with full text in English and French.
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, Optional

import requests

# ── API configuration ──────────────────────────────────────────────────
SEARCH_URL = "https://conventions-ws.coe.int/WS_LFRConventions/api/traites/search"
API_TOKEN = "hfghhgp2q5vgwg1hbn532kw71zgtww7e"
HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "token": API_TOKEN,
}
RATE_LIMIT_DELAY = 1.0

# ── PDF extraction setup ──────────────────────────────────────────────
# Try to use the centralized pdf_extract utility; fall back to simple
# requests + pdfplumber if the common module isn't on the path.
_pdf_extract_available = False
try:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from common.pdf_extract import extract_pdf_markdown, preload_existing_ids
    _pdf_extract_available = True
except ImportError:
    pass


def _download_pdf_text(url: str) -> Optional[str]:
    """Download a PDF and extract text using pdfplumber (fallback)."""
    try:
        import pdfplumber
        import io
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
            pages = [p.extract_text() or "" for p in pdf.pages]
        text = "\n\n".join(p for p in pages if p.strip())
        return text if text.strip() else None
    except Exception as e:
        print(f"  Warning: PDF extraction failed for {url}: {e}")
        return None


def fetch_all_treaties() -> list:
    """Fetch all treaty metadata from the API in a single POST."""
    body = {
        "NumsSte": [],
        "CodePays": None,
        "AnneeOuverture": None,
        "AnneeVigueur": None,
        "CodeLieuSTE": None,
        "CodeMatieres": [],
        "TitleKeywords": [],
    }
    resp = requests.post(SEARCH_URL, json=body, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _parse_date(date_str: Optional[str]) -> Optional[str]:
    """Parse a date string to ISO 8601 date."""
    if not date_str:
        return None
    try:
        dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return date_str[:10] if len(date_str) >= 10 else date_str


def normalize(raw: Dict[str, Any], text: str) -> Dict[str, Any]:
    """Normalize a treaty record to the standard schema."""
    treaty_num = raw.get("Numero_traite", "")
    title_en = raw.get("Libelle_titre_ENG", "") or raw.get("Nom_commun_ENG", "")
    title_fr = raw.get("Libelle_titre_FRE", "")
    common_name = raw.get("Nom_commun_ENG", "")
    date_opened = _parse_date(raw.get("Date_ste"))
    date_force = _parse_date(raw.get("Date_vigueur_ste"))
    place = raw.get("Lieu_ste", "")

    return {
        "_id": f"CETS-{treaty_num}",
        "_source": "CoE/TreatyOffice",
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title_en,
        "title_fr": title_fr,
        "common_name": common_name,
        "text": text,
        "date": date_opened,
        "date_entry_into_force": date_force,
        "treaty_number": treaty_num,
        "place_of_signature": place,
        "url": f"https://www.coe.int/en/web/conventions/full-list/-/conventions/treaty/{treaty_num}",
        "pdf_url_en": raw.get("Lien_pdf_traite_ENG", ""),
        "pdf_url_fr": raw.get("Lien_pdf_traite_FRE", ""),
        "language": "en",
    }


def fetch_all(
    max_records: Optional[int] = None,
) -> Generator[Dict[str, Any], None, None]:
    """Fetch all treaties with full text from PDFs."""
    treaties = fetch_all_treaties()
    print(f"Found {len(treaties)} treaties in API")

    existing = set()
    if _pdf_extract_available:
        try:
            existing = preload_existing_ids("CoE/TreatyOffice", table="legislation")
            print(f"  {len(existing)} already in Neon — will skip")
        except Exception:
            pass

    fetched = 0
    for treaty in treaties:
        if max_records and fetched >= max_records:
            return

        treaty_num = treaty.get("Numero_traite", "unknown")
        source_id = f"CETS-{treaty_num}"

        if source_id in existing:
            print(f"  Skipping CETS {treaty_num} (already in Neon)")
            continue

        pdf_url = treaty.get("Lien_pdf_traite_ENG", "")
        if not pdf_url:
            print(f"  Skipping CETS {treaty_num}: no English PDF URL")
            continue

        print(f"  Fetching CETS {treaty_num}: {treaty.get('Libelle_titre_ENG', '')[:60]}...")
        time.sleep(RATE_LIMIT_DELAY)

        # Extract text from PDF
        text = None
        if _pdf_extract_available:
            try:
                text = extract_pdf_markdown(
                    source="CoE/TreatyOffice",
                    source_id=source_id,
                    pdf_url=pdf_url,
                    table="legislation",
                )
            except Exception as e:
                print(f"    extract_pdf_markdown failed: {e}")

        if not text:
            text = _download_pdf_text(pdf_url)

        if not text:
            print(f"    Warning: No text extracted for CETS {treaty_num}")
            continue

        record = normalize(treaty, text)
        yield record
        fetched += 1


def fetch_updates(since: datetime) -> Generator[Dict[str, Any], None, None]:
    """Fetch treaties updated since a given date.

    The API doesn't support date filtering, so we fetch all and filter
    by date_opened (treaties don't change once signed).
    """
    since_str = since.strftime("%Y-%m-%d")
    for record in fetch_all():
        if record.get("date") and record["date"] >= since_str:
            yield record


def bootstrap_sample(sample_dir: Path, count: int = 15) -> int:
    """Fetch sample treaty records."""
    sample_dir.mkdir(parents=True, exist_ok=True)
    saved = 0

    for record in fetch_all(max_records=count):
        doc_id = record["_id"].replace("/", "_")
        filepath = sample_dir / f"{doc_id}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        text_len = len(record.get("text", ""))
        print(f"  Saved {filepath.name} (date={record.get('date')}, {text_len:,} chars)")
        saved += 1

    return saved


def validate_samples(sample_dir: Path) -> bool:
    """Validate sample records."""
    samples = list(sample_dir.glob("*.json"))

    if len(samples) < 10:
        print(f"FAIL: Only {len(samples)} samples, need at least 10")
        return False

    total_text_len = 0
    all_valid = True

    for sample_path in samples:
        with open(sample_path, "r", encoding="utf-8") as f:
            record = json.load(f)

        text = record.get("text", "")
        if not text:
            print(f"FAIL: {sample_path.name} has no text")
            all_valid = False
        elif len(text) < 100:
            print(f"WARN: {sample_path.name} has short text ({len(text)} chars)")

        total_text_len += len(text)

        for field in ["_id", "_source", "_type", "title", "date"]:
            if not record.get(field):
                print(f"WARN: {sample_path.name} missing {field}")

    avg_len = total_text_len // len(samples) if samples else 0
    print(f"\nValidation summary:")
    print(f"  Samples: {len(samples)}")
    print(f"  Average text length: {avg_len:,} chars")
    print(f"  All valid: {all_valid}")

    return all_valid and len(samples) >= 10


def main():
    parser = argparse.ArgumentParser(description="CoE/TreatyOffice fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "validate", "fetch"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample records")
    parser.add_argument("--count", type=int, default=15, help="Number of records")
    parser.add_argument("--since", type=str, help="Fetch since date (YYYY-MM-DD)")

    args = parser.parse_args()
    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if args.command == "bootstrap":
        if args.sample:
            saved = bootstrap_sample(sample_dir, args.count)
            print(f"\nSaved {saved} sample records to {sample_dir}")
            valid = validate_samples(sample_dir)
            sys.exit(0 if saved >= 10 and valid else 1)
        else:
            print("Use --sample for bootstrap mode")
            sys.exit(1)

    elif args.command == "validate":
        valid = validate_samples(sample_dir)
        sys.exit(0 if valid else 1)

    elif args.command == "fetch":
        if args.since:
            since = datetime.fromisoformat(args.since)
            for record in fetch_updates(since):
                print(json.dumps(record, ensure_ascii=False))
        else:
            for record in fetch_all(max_records=args.count):
                print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
