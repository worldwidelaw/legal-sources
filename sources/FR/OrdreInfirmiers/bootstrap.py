#!/usr/bin/env python3
"""
FR/OrdreInfirmiers - Disciplinary jurisprudence of the French National Order of
Nurses (Ordre National des Infirmiers).

Fetches anonymised disciplinary decisions of the Chambre disciplinaire nationale
published on the Order's public jurisprudence pages, organised by year.

Data source: https://www.ordre-infirmiers.fr/jurisprudence-0

Each yearly page (`/decisions-de-l-annee-{YYYY}`) lists decisions as anchors
linking to the full anonymised decision PDF under `/system/files/inline-files/`.
The anchor text carries the decision number(s) and the decision date. Full text
is downloaded from the PDF and extracted with pdfplumber. Records are
de-duplicated by PDF URL.

Usage:
  python bootstrap.py bootstrap --sample          # 15 sample records
  python bootstrap.py bootstrap                    # full bootstrap -> data/records.jsonl
  python bootstrap.py bootstrap-fast               # VPS wrapper alias for full
  python bootstrap.py updates --since YYYY-MM-DD   # not supported
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
from urllib.parse import urljoin

import pdfplumber
import requests

SOURCE_ID = "FR/OrdreInfirmiers"
BASE_URL = "https://www.ordre-infirmiers.fr"
INDEX_URL = BASE_URL + "/jurisprudence-0"
YEAR_URL = BASE_URL + "/decisions-de-l-annee-{year}"

HEADERS = {
    "User-Agent": "Legal Data Hunter/1.0 (EU Legal Research; Open Data Collection)",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

SAMPLE_DIR = Path(__file__).parent / "sample"
DATA_DIR = Path(__file__).parent / "data"

REQUEST_DELAY = 1.5

FR_MONTHS = {
    "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4, "mai": 5,
    "juin": 6, "juillet": 7, "août": 8, "aout": 8, "septembre": 9,
    "octobre": 10, "novembre": 11, "décembre": 12, "decembre": 12,
}


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = unescape(text.replace("\xa0", " "))
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def get_year_urls(session: requests.Session) -> list:
    """Return the list of yearly-decision page URLs from the jurisprudence index."""
    try:
        resp = session.get(INDEX_URL, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        years = re.findall(r'href="(/decisions-de-l-annee-(\d{4}))"', resp.text)
    except requests.RequestException:
        years = []
    urls = []
    seen = set()
    for path, yr in years:
        if yr not in seen:
            seen.add(yr)
            urls.append((yr, urljoin(BASE_URL, path)))
    # Sort most-recent first
    urls.sort(key=lambda t: t[0], reverse=True)
    return urls


def parse_year_page(html: str) -> list:
    """Parse a yearly page into a list of {title, pdf_url, decision_no, date} dicts."""
    out = []
    for m in re.finditer(r'<a[^>]+href="([^"]*\.pdf[^"]*)"[^>]*>(.*?)</a>', html,
                         re.DOTALL | re.IGNORECASE):
        href = m.group(1)
        title = clean_text(m.group(2))
        if not title:
            continue
        out.append({
            "pdf_url": urljoin(BASE_URL, href),
            "title": title,
            "decision_no": parse_decision_no(title),
            "date": parse_fr_date(title),
        })
    return out


def parse_decision_no(title: str) -> str:
    """Extract decision number(s) from the anchor title text."""
    nums = re.findall(r'[Nn][°º]\s*([0-9][0-9A-Za-z\-/]*)', title)
    if nums:
        return " / ".join(nums)
    return ""


def parse_fr_date(title: str) -> Optional[str]:
    """Extract an ISO date from a French 'du DD month YYYY' phrase."""
    m = re.search(r'(\d{1,2})\s+([A-Za-zéûô]+)\s+(\d{4})', title)
    if not m:
        return None
    day, month_name, year = m.group(1), m.group(2).lower(), m.group(3)
    month = FR_MONTHS.get(month_name)
    if not month:
        return None
    try:
        return datetime(int(year), month, int(day)).strftime("%Y-%m-%d")
    except ValueError:
        return None


def extract_pdf_text(pdf_bytes: bytes) -> str:
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


def download_pdf(session: requests.Session, url: str) -> Optional[bytes]:
    try:
        resp = session.get(url, headers=HEADERS, timeout=60)
        if resp.status_code == 200 and resp.content[:4] == b"%PDF":
            return resp.content
    except requests.RequestException:
        pass
    return None


def normalize(item: dict, full_text: str) -> dict:
    decision_no = item.get("decision_no") or ""
    date = item.get("date")
    raw_title = item.get("title") or "Décision"

    title = f"Chambre disciplinaire nationale — {raw_title}"

    if decision_no:
        slug = re.sub(r'[^0-9A-Za-z\-]+', '-', decision_no).strip('-')
    else:
        slug = re.sub(r'[^0-9A-Za-z]+', '-',
                      item["pdf_url"].rsplit("/", 1)[-1]).strip('-')[:60]

    return {
        "_id": f"oni-{slug}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": full_text,
        "date": date,
        "url": item["pdf_url"],
        "decision_number": decision_no,
        "instance": "Chambre disciplinaire nationale",
        "jurisdiction": "FR",
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    session = requests.Session()
    year_urls = get_year_urls(session)
    print(f"Found {len(year_urls)} yearly pages: {[y for y, _ in year_urls]}")

    target = 15 if sample else None
    seen_pdfs = set()
    items = []

    for yr, url in year_urls:
        try:
            resp = session.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"  [WARN] year {yr} failed: {e}")
            continue
        page_items = parse_year_page(resp.text)
        for it in page_items:
            if it["pdf_url"] in seen_pdfs:
                continue
            seen_pdfs.add(it["pdf_url"])
            items.append(it)
        print(f"  {yr}: {len(page_items)} decisions")
        time.sleep(REQUEST_DELAY)
        if sample and len(items) >= target * 2:
            break

    print(f"Discovered {len(items)} unique decisions")

    count = 0
    for it in items:
        if target and count >= target:
            break
        pdf_bytes = download_pdf(session, it["pdf_url"])
        if not pdf_bytes:
            print(f"  [SKIP] {it['pdf_url'].rsplit('/',1)[-1]}: PDF download failed")
            continue
        full_text = extract_pdf_text(pdf_bytes)
        if len(full_text) < 200:
            print(f"  [SKIP] {it.get('decision_no')}: text too short ({len(full_text)} chars)")
            continue
        record = normalize(it, full_text)
        count += 1
        print(f"  [{count}{'/' + str(target) if target else ''}] "
              f"{record['title'][:60]}... ({len(full_text)} chars)")
        yield record
        time.sleep(REQUEST_DELAY)

    print(f"\nTotal records fetched: {count}")


def save_samples(records: list):
    SAMPLE_DIR.mkdir(exist_ok=True)
    for i, record in enumerate(records, 1):
        path = SAMPLE_DIR / f"record_{i:03d}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        print(f"  Saved: {path.name}")


def main():
    parser = argparse.ArgumentParser(description="FR/OrdreInfirmiers bootstrap")
    subparsers = parser.add_subparsers(dest="command")

    boot = subparsers.add_parser("bootstrap", help="Fetch records")
    boot.add_argument("--sample", action="store_true", help="Fetch 15 sample records only")

    fast = subparsers.add_parser("bootstrap-fast", help="Full fetch (VPS wrapper alias)")
    fast.add_argument("--sample", action="store_true", help="Fetch 15 sample records only")

    upd = subparsers.add_parser("updates", help="Fetch updates (not supported)")
    upd.add_argument("--since", required=True, help="Date (not used)")

    args = parser.parse_args()

    if args.command in ("bootstrap", "bootstrap-fast"):
        print(f"{SOURCE_ID} bootstrap ({'sample' if args.sample else 'full'} mode)")
        print(f"Source: {INDEX_URL}\n")
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
        print("Updates not supported for this source (no date-filtered API).")
        sys.exit(1)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
