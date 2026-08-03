#!/usr/bin/env python3
"""
FR/OrdrePedicuresPodologues - Disciplinary jurisprudence of the French National
Order of Chiropodists / Podiatrists (Ordre National des Pédicures-Podologues,
ONPP).

Fetches the anonymised disciplinary decisions published on the Order's
consolidated public jurisprudence database. Covers the chambres disciplinaires
de première instance (regional/inter-regional), the Chambre disciplinaire
nationale, the sections des assurances sociales, and reproduced Conseil d'État
cassation decisions involving the profession.

Data source: https://www.onpp.fr/juridique/jurisprudence/

The consolidated jurisprudence page embeds, in an inline ``<script>`` block, a
JavaScript array ``const data = [ ... ]`` with one object per decision:

    {
        date: 'DD-MM-YYYY',
        titre: 'CDPI 29 avril 2026 n°2025-03',
        region: 'Pays de la Loire',
        link: 'CDPI_PDL_29042026_C.pdf',     # PDF filename
        type: 'Pays de la Loire',
        juridiction: 'Chambre disciplinaire de première instance',
        abstract: '...',                       # short summary
        keywords: [ 'Hygiène et sécurité', ... ],
    }

The ``link`` is the filename of the PDF that holds the FULL anonymised decision,
served from ``/assets/jurisprudence/{link}``. Full text is obtained by
downloading that PDF and extracting it with pdfplumber (per-page cache flush to
bound memory). The ``abstract`` is kept as metadata.

Records are de-duplicated by PDF link.

Usage:
  python bootstrap.py bootstrap --sample          # 15 sample records
  python bootstrap.py bootstrap                    # full bootstrap -> data/records.jsonl
  python bootstrap.py bootstrap-fast               # VPS wrapper alias for full
  python bootstrap.py updates --since YYYY-MM-DD   # decisions dated since a date
"""

import argparse
import io
import json
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import quote

import pdfplumber
import requests

SOURCE_ID = "FR/OrdrePedicuresPodologues"
BASE_URL = "https://www.onpp.fr"
JURIS_URL = BASE_URL + "/juridique/jurisprudence/"
PDF_BASE = BASE_URL + "/assets/jurisprudence/"

HEADERS = {
    "User-Agent": "Legal Data Hunter/1.0 (EU Legal Research; Open Data Collection)",
    "Accept": "text/html,application/json,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

SAMPLE_DIR = Path(__file__).parent / "sample"
DATA_DIR = Path(__file__).parent / "data"

REQUEST_DELAY = 1.2

# Map the page's juridiction labels to a clean instance name.
JURIDICTION_LABELS = {
    "Chambre disciplinaire de première instance": "Chambre disciplinaire de première instance",
    "Chambre disciplinaire nationale": "Chambre disciplinaire nationale",
    "Sections des Assurances sociales": "Section des assurances sociales",
    "Conseil État": "Conseil d'État",
    "Conseil d'État": "Conseil d'État",
}


def clean_text(text: str) -> str:
    """Strip tags, decode entities, collapse whitespace."""
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _js_unescape(value: str) -> str:
    """Decode the limited set of escapes used in the page's JS string literals."""
    return (
        value.replace("\\'", "'")
        .replace('\\"', '"')
        .replace("\\n", " ")
        .replace("\\t", " ")
        .replace("\\/", "/")
        .replace("\\\\", "\\")
    )


def fetch_data_array(session: requests.Session) -> list:
    """Download the jurisprudence page and parse its inline `const data = [...]`."""
    resp = session.get(JURIS_URL, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    html = resp.text

    start = html.find("const data = [")
    if start == -1:
        raise RuntimeError("could not locate 'const data = [' in jurisprudence page")
    # The array is closed by the first '];' after the marker.
    end = html.find("];", start)
    if end == -1:
        raise RuntimeError("could not locate end of data array")
    blob = html[start : end + 1]

    # Split into per-object chunks on the '{ ... }' boundaries. Objects are flat
    # (no nested braces other than the keywords array which uses brackets), so a
    # simple brace split is reliable here.
    objects = re.findall(r"\{(.*?)\}", blob, re.DOTALL)
    records = []
    for obj in objects:
        rec = {}
        # Scalar string fields: key: '...'
        for key in ("date", "titre", "region", "link", "type", "juridiction", "abstract"):
            m = re.search(rf"{key}\s*:\s*'((?:[^'\\]|\\.)*)'", obj, re.DOTALL)
            if m:
                rec[key] = _js_unescape(m.group(1)).strip()
        # keywords: [ '...', '...' ]
        kw_m = re.search(r"keywords\s*:\s*\[(.*?)\]", obj, re.DOTALL)
        if kw_m:
            rec["keywords"] = [
                _js_unescape(k).strip()
                for k in re.findall(r"'((?:[^'\\]|\\.)*)'", kw_m.group(1))
                if k.strip()
            ]
        else:
            rec["keywords"] = []
        if rec.get("link"):
            records.append(rec)
    return records


def parse_date(date_str: str) -> Optional[str]:
    """Convert a 'DD-MM-YYYY' string to ISO 'YYYY-MM-DD'."""
    if not date_str:
        return None
    m = re.match(r"(\d{1,2})-(\d{1,2})-(\d{4})", date_str.strip())
    if not m:
        return None
    day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return datetime(year, month, day).strftime("%Y-%m-%d")
    except ValueError:
        return None


def extract_decision_number(titre: str) -> Optional[str]:
    """Pull a decision number like n°2025-03 from the title."""
    m = re.search(r"n[°ºo]\s*([0-9]{2,4}[-/][0-9]+)", titre, re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r"n[°ºo]\s*([0-9]{4,})", titre, re.IGNORECASE)
    return m.group(1) if m else None


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes with pdfplumber (per-page cache flush)."""
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages.append(text)
                try:
                    page.flush_cache()
                    page.get_textmap.cache_clear()
                except Exception:
                    pass
            return "\n\n".join(pages).strip()
    except Exception:
        return ""


def download_pdf(session: requests.Session, link: str) -> Optional[bytes]:
    """Download a decision PDF by its filename, percent-encoding the path."""
    url = PDF_BASE + quote(link, safe="/")
    try:
        resp = session.get(url, headers=HEADERS, timeout=90)
        if resp.status_code == 200 and resp.content[:4] == b"%PDF":
            return resp.content
    except requests.RequestException:
        pass
    return None


def make_id(link: str) -> str:
    """Stable id from the PDF filename."""
    stem = re.sub(r"\.pdf$", "", link, flags=re.IGNORECASE)
    slug = re.sub(r"[^A-Za-z0-9]+", "-", stem).strip("-").lower()
    return f"onpp-{slug}"


def normalize(entry: dict, pdf_url: str, full_text: str) -> dict:
    """Normalize a parsed data entry + extracted PDF text into the schema."""
    titre = clean_text(entry.get("titre", ""))
    juris_raw = entry.get("juridiction", "")
    instance = JURIDICTION_LABELS.get(juris_raw, juris_raw or "Chambre disciplinaire")
    abstract = clean_text(entry.get("abstract", ""))

    return {
        "_id": make_id(entry["link"]),
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": titre,
        "text": full_text,
        "date": parse_date(entry.get("date", "")),
        "url": pdf_url,
        "pdf_url": pdf_url,
        "decision_number": extract_decision_number(titre),
        "instance": instance,
        "region": entry.get("region") or None,
        "abstract": abstract or None,
        "keywords": entry.get("keywords") or [],
        "jurisdiction": "FR",
    }


def fetch_all(sample: bool = False, since: Optional[str] = None) -> Generator[dict, None, None]:
    """Yield all disciplinary decisions with full PDF text."""
    session = requests.Session()
    entries = fetch_data_array(session)
    print(f"Parsed {len(entries)} decisions from the jurisprudence database")

    target = 15 if sample else None
    count = 0
    seen = set()

    for entry in entries:
        if target and count >= target:
            break
        link = entry["link"]
        if link in seen:
            continue
        seen.add(link)

        iso = parse_date(entry.get("date", ""))
        if since and iso and iso < since:
            continue

        pdf_bytes = download_pdf(session, link)
        if not pdf_bytes:
            print(f"  [SKIP] {link}: PDF download failed")
            time.sleep(REQUEST_DELAY)
            continue
        full_text = extract_pdf_text(pdf_bytes)
        if len(full_text) < 200:
            print(f"  [SKIP] {link}: text too short ({len(full_text)} chars)")
            time.sleep(REQUEST_DELAY)
            continue

        record = normalize(entry, PDF_BASE + quote(link, safe="/"), full_text)
        count += 1
        print(
            f"  [{count}{'/' + str(target) if target else ''}] "
            f"{record['_id']}: {record['title'][:55]}... ({len(full_text)} chars)"
        )
        yield record
        time.sleep(REQUEST_DELAY)

    print(f"\nTotal records fetched: {count}")


def fetch_updates(since: str) -> Generator[dict, None, None]:
    yield from fetch_all(sample=False, since=since)


def save_samples(records: list):
    SAMPLE_DIR.mkdir(exist_ok=True)
    for i, record in enumerate(records, 1):
        path = SAMPLE_DIR / f"record_{i:03d}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        print(f"  Saved: {path.name}")


def main():
    parser = argparse.ArgumentParser(description="FR/OrdrePedicuresPodologues bootstrap")
    subparsers = parser.add_subparsers(dest="command")

    boot = subparsers.add_parser("bootstrap", help="Fetch records")
    boot.add_argument("--sample", action="store_true", help="Fetch 15 sample records only")

    fast = subparsers.add_parser("bootstrap-fast", help="Full fetch (VPS wrapper alias)")
    fast.add_argument("--sample", action="store_true", help="Fetch 15 sample records only")

    upd = subparsers.add_parser("updates", help="Fetch updates since a date")
    upd.add_argument("--since", required=True, help="ISO date YYYY-MM-DD")

    args = parser.parse_args()

    if args.command in ("bootstrap", "bootstrap-fast"):
        print(f"{SOURCE_ID} bootstrap ({'sample' if args.sample else 'full'} mode)")
        print(f"Source: {JURIS_URL}\n")
        if args.sample:
            records = list(fetch_all(sample=True))
            if records:
                save_samples(records)
                print(f"\n{len(records)} sample records saved to {SAMPLE_DIR}/")
        else:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            jsonl_path = DATA_DIR / "records.jsonl"
            count = 0
            with open(jsonl_path, "w", encoding="utf-8") as f:
                for r in fetch_all(sample=False):
                    f.write(json.dumps(r, ensure_ascii=False) + "\n")
                    count += 1
                    if count % 50 == 0:
                        print(f"Progress: {count} records written")
            print(f"\nFull bootstrap complete: {count} records -> {jsonl_path}")

    elif args.command == "updates":
        print(f"{SOURCE_ID} updates since {args.since}")
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        jsonl_path = DATA_DIR / "updates.jsonl"
        count = 0
        with open(jsonl_path, "w", encoding="utf-8") as f:
            for r in fetch_updates(args.since):
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
                count += 1
        print(f"\nUpdates complete: {count} records -> {jsonl_path}")
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
