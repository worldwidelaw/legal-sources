#!/usr/bin/env python3
"""
FR/OrdreMasseursKine - Disciplinary jurisprudence of the French National Order
of Massage Therapists / Physiotherapists (Ordre National des
Masseurs-Kinésithérapeutes).

Fetches anonymised disciplinary decisions published on the Order's public
jurisprudence database. Covers the Chambre disciplinaire nationale, the
regional chambres disciplinaires de première instance, the sections des
assurances sociales, and reproduced higher-court decisions (Conseil d'État,
Cour de cassation, cours administratives d'appel) involving the profession.

Data source: https://jurisprudence.ordremk.fr

The site is a WordPress install that exposes the standard REST API:

  * /wp-json/wp/v2/posts        -> one post per decision (title, date, category)
  * /wp-json/wp/v2/media?parent -> the attached PDF holding the FULL decision text
  * /wp-json/wp/v2/categories   -> instance/chamber taxonomy

The post `content` is empty; the full anonymised decision is the attached PDF
under wp-content/uploads/. Full text is obtained by downloading that PDF and
extracting it with pdfplumber (per-page cache flush to bound memory).

Records are de-duplicated by post id.

Usage:
  python bootstrap.py bootstrap --sample          # 15 sample records
  python bootstrap.py bootstrap                    # full bootstrap -> data/records.jsonl
  python bootstrap.py bootstrap-fast               # VPS wrapper alias for full
  python bootstrap.py updates --since YYYY-MM-DD   # posts published since a date
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

SOURCE_ID = "FR/OrdreMasseursKine"
BASE_URL = "https://jurisprudence.ordremk.fr"
API = BASE_URL + "/wp-json/wp/v2"

HEADERS = {
    "User-Agent": "Legal Data Hunter/1.0 (EU Legal Research; Open Data Collection)",
    "Accept": "application/json,text/html,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

SAMPLE_DIR = Path(__file__).parent / "sample"
DATA_DIR = Path(__file__).parent / "data"

REQUEST_DELAY = 1.2
PER_PAGE = 100

FRENCH_MONTHS = {
    "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4,
    "mai": 5, "juin": 6, "juillet": 7, "août": 8, "aout": 8,
    "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12,
    "decembre": 12,
}


def clean_text(text: str) -> str:
    """Strip tags, decode entities, collapse whitespace."""
    if not text:
        return ""
    text = re.sub(r'<[^>]+>', ' ', text)
    text = unescape(text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def get_category_map(session: requests.Session) -> dict:
    """Return {category_id: name} for instance/chamber labelling."""
    out = {}
    page = 1
    while True:
        url = f"{API}/categories?per_page=100&page={page}"
        try:
            resp = session.get(url, headers=HEADERS, timeout=30)
            if resp.status_code != 200:
                break
            cats = resp.json()
        except (requests.RequestException, ValueError):
            break
        if not cats:
            break
        for c in cats:
            out[c["id"]] = clean_text(c.get("name", ""))
        if len(cats) < 100:
            break
        page += 1
        time.sleep(0.5)
    return out


def parse_decision_date(title: str) -> Optional[str]:
    """Parse a French 'décision du 11 juin 2026' style date from the title."""
    m = re.search(r'(\d{1,2})\s+([A-Za-zéûôîè]+)\s+(\d{4})', title)
    if not m:
        return None
    day = int(m.group(1))
    month = FRENCH_MONTHS.get(m.group(2).lower())
    year = int(m.group(3))
    if not month:
        return None
    try:
        return datetime(year, month, day).strftime("%Y-%m-%d")
    except ValueError:
        return None


def extract_decision_numbers(title: str) -> list:
    """Pull out decision numbers like n°094-2022 from the title."""
    nums = re.findall(r'n[°ºo]\s*([0-9]+(?:-[0-9]+)?)', title, re.IGNORECASE)
    return [n.strip() for n in nums]


def get_pdf_url(session: requests.Session, post_id: int) -> Optional[str]:
    """Return the source_url of the PDF attached to a post."""
    url = f"{API}/media?parent={post_id}&per_page=20"
    try:
        resp = session.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        media = resp.json()
    except (requests.RequestException, ValueError):
        return None
    for m in media:
        if m.get("mime_type") == "application/pdf":
            return m.get("source_url")
    # fallback: any attachment whose source_url ends in .pdf
    for m in media:
        src = m.get("source_url", "")
        if src.lower().endswith(".pdf"):
            return src
    return None


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


def download_pdf(session: requests.Session, pdf_url: str) -> Optional[bytes]:
    """Download a PDF, percent-encoding any non-ASCII path characters."""
    enc = quote(pdf_url, safe=":/?&=%")
    try:
        resp = session.get(enc, headers=HEADERS, timeout=60)
        if resp.status_code == 200 and resp.content[:4] == b"%PDF":
            return resp.content
    except requests.RequestException:
        pass
    return None


def normalize(post: dict, cat_map: dict, pdf_url: str, full_text: str) -> dict:
    """Normalize a WP post + extracted PDF text into the standard schema."""
    title = clean_text(post.get("title", {}).get("rendered", ""))
    date = parse_decision_date(title)
    if not date:
        # fall back to publication date
        pub = post.get("date", "")
        date = pub[:10] if pub else None

    cat_ids = post.get("categories", []) or []
    cat_names = [cat_map.get(cid, "") for cid in cat_ids if cat_map.get(cid)]
    instance = cat_names[0] if cat_names else "Chambre disciplinaire"

    numbers = extract_decision_numbers(title)

    return {
        "_id": f"omk-{post['id']}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": full_text,
        "date": date,
        "url": post.get("link", ""),
        "pdf_url": pdf_url,
        "decision_number": numbers[0] if numbers else None,
        "decision_numbers": numbers,
        "instance": instance,
        "chambers": cat_names,
        "jurisdiction": "FR",
    }


def iter_posts(session: requests.Session, since: Optional[str] = None) -> Generator[dict, None, None]:
    """Yield WordPress post objects, newest first, paginating the REST API."""
    page = 1
    while True:
        url = f"{API}/posts?per_page={PER_PAGE}&page={page}&orderby=date&order=desc"
        if since:
            url += f"&after={since}T00:00:00"
        try:
            resp = session.get(url, headers=HEADERS, timeout=40)
        except requests.RequestException as e:
            print(f"  [WARN] posts page {page} failed: {e}")
            break
        if resp.status_code == 400:
            # past the last page
            break
        resp.raise_for_status()
        posts = resp.json()
        if not posts:
            break
        for p in posts:
            yield p
        if len(posts) < PER_PAGE:
            break
        page += 1
        time.sleep(REQUEST_DELAY)


def fetch_all(sample: bool = False, since: Optional[str] = None) -> Generator[dict, None, None]:
    """Yield all disciplinary decisions with full PDF text."""
    session = requests.Session()
    cat_map = get_category_map(session)
    print(f"Loaded {len(cat_map)} categories")

    target = 15 if sample else None
    count = 0
    seen = set()

    for post in iter_posts(session, since=since):
        if target and count >= target:
            break
        pid = post.get("id")
        if pid in seen:
            continue
        seen.add(pid)

        pdf_url = get_pdf_url(session, pid)
        if not pdf_url:
            print(f"  [SKIP] post {pid}: no PDF attachment")
            time.sleep(REQUEST_DELAY)
            continue
        pdf_bytes = download_pdf(session, pdf_url)
        if not pdf_bytes:
            print(f"  [SKIP] post {pid}: PDF download failed ({pdf_url})")
            time.sleep(REQUEST_DELAY)
            continue
        full_text = extract_pdf_text(pdf_bytes)
        if len(full_text) < 200:
            print(f"  [SKIP] post {pid}: text too short ({len(full_text)} chars)")
            time.sleep(REQUEST_DELAY)
            continue

        record = normalize(post, cat_map, pdf_url, full_text)
        count += 1
        print(f"  [{count}{'/' + str(target) if target else ''}] "
              f"{record['_id']}: {record['title'][:55]}... ({len(full_text)} chars)")
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
    parser = argparse.ArgumentParser(description="FR/OrdreMasseursKine bootstrap")
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
        print(f"Source: {BASE_URL}\n")
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
