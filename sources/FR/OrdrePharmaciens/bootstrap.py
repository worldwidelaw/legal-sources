#!/usr/bin/env python3
"""
FR/OrdrePharmaciens - Disciplinary jurisprudence of the French National Order of
Pharmacists (Conseil National de l'Ordre des Pharmaciens, CNOP).

Fetches the anonymised disciplinary decisions published on the Order's public
jurisprudence database. Covers the chambres de discipline de première instance
(central/regional councils), the chambre de discipline du Conseil national
(appeal), the sections des assurances sociales, and reproduced Conseil d'État
cassation decisions involving the profession.

Data source: https://www.ordre.pharmacien.fr/l-ordre/jurisprudence

The listing is a Symfony POST-search form (`name="jurisprudence"`) with a CSRF
token. Submitting an empty keyword returns the full corpus, 15 decisions per
page, paginated by appending `?page=N` to the POST URL. Each result links to a
detail page `/jurisprudence/{id}-{slug}` that carries:

  - a structured summary (résumé), the relevant CSP code articles, the
    keywords (mots-clés), and a "Chronologie des décisions" table (decision
    date, plaignant, jurisdiction level);
  - one or more links to the FULL anonymised decision PDF(s) under
    `/mediatheque/fichiers/jurisprudence/decision.pdfNNN`.

Full text is obtained by downloading every decision PDF on the detail page and
extracting it with pdfplumber (per-page cache flush to bound memory). The page
summary, articles and keywords are kept as metadata.

Records are de-duplicated by jurisprudence page id.

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

import pdfplumber
import requests

SOURCE_ID = "FR/OrdrePharmaciens"
BASE_URL = "https://www.ordre.pharmacien.fr"
JURIS_URL = BASE_URL + "/l-ordre/jurisprudence"

HEADERS = {
    "User-Agent": "Legal Data Hunter/1.0 (EU Legal Research; Open Data Collection)",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

SAMPLE_DIR = Path(__file__).parent / "sample"
DATA_DIR = Path(__file__).parent / "data"

REQUEST_DELAY = 1.2
MAX_PAGES = 60  # generous safety cap; corpus is ~7 pages

FRENCH_MONTHS = {
    "janvier": 1, "février": 2, "fevrier": 2, "mars": 3, "avril": 4,
    "mai": 5, "juin": 6, "juillet": 7, "août": 8, "aout": 8,
    "septembre": 9, "octobre": 10, "novembre": 11, "décembre": 12, "decembre": 12,
}


def clean_text(text: str) -> str:
    """Strip tags, decode entities, collapse whitespace."""
    if not text:
        return ""
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\s*\n\s*", "\n", text)
    return text.strip()


def get_token(html: str) -> Optional[str]:
    m = re.search(r'jurisprudence\[_token\]"\s+value="([^"]+)"', html)
    return m.group(1) if m else None


def fetch_decision_urls(session: requests.Session) -> list:
    """POST the empty-keyword search and page through all results.

    Returns an ordered list of unique detail-page paths (/jurisprudence/{id}-{slug}).
    """
    # Prime the session: fetch the form to obtain a cookie + CSRF token.
    resp = session.get(JURIS_URL, headers=HEADERS, timeout=60)
    resp.raise_for_status()
    token = get_token(resp.text)
    if not token:
        raise RuntimeError("could not locate jurisprudence CSRF token on the form page")

    seen = set()
    ordered = []
    for page in range(1, MAX_PAGES + 1):
        url = f"{JURIS_URL}?page={page}"
        r = session.post(
            url,
            headers=HEADERS,
            data={"jurisprudence[keyword]": "", "jurisprudence[_token]": token},
            timeout=60,
        )
        if r.status_code == 404:
            # Paging past the last page returns 404 — normal end of results.
            break
        r.raise_for_status()
        links = re.findall(r'href="(/jurisprudence/\d+-[^"#]*)"', r.text)
        new = [l for l in dict.fromkeys(links) if l not in seen]
        if not new:
            break
        for l in new:
            seen.add(l)
            ordered.append(l)
        print(f"  page {page}: {len(new)} decisions (total {len(ordered)})")
        time.sleep(REQUEST_DELAY)
    return ordered


def parse_french_date(text: str) -> Optional[str]:
    """Parse 'lundi 16 novembre 2009' (or '16 novembre 2009') to ISO YYYY-MM-DD."""
    if not text:
        return None
    m = re.search(r"(\d{1,2})\s+([A-Za-zéûôèàç]+)\s+(\d{4})", text.strip())
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


def parse_detail(html: str) -> dict:
    """Extract structured metadata from a jurisprudence detail page."""
    info = {}

    h1 = re.search(r"<h1[^>]*>(.*?)</h1>", html, re.DOTALL)
    raw_title = clean_text(h1.group(1)) if h1 else ""
    # Drop a leading numeric id prefix like "69 - " if present.
    info["title"] = re.sub(r"^\d+\s*[-–]\s*", "", raw_title).strip()

    # PDF links to the full anonymised decisions.
    info["pdf_paths"] = list(
        dict.fromkeys(
            re.findall(r'href="(/mediatheque/fichiers/jurisprudence/[^"]+)"', html)
        )
    )

    # Summary / résumé: the main article text between the share toolbar and the
    # "Mots-clés" block. Work on the <main> region only.
    main_m = re.search(r"<main[^>]*>(.*?)</main>", html, re.DOTALL)
    main = main_m.group(1) if main_m else html
    main_text = clean_text(main)
    summary = ""
    # Anchor after the LinkedIn share label that ends the toolbar, take up to Mots-clés.
    lo = main_text.find("Partager sur LinkedIn")
    lo = (lo + len("Partager sur LinkedIn")) if lo != -1 else 0
    hi = main_text.find("Mots-clés", lo)
    if hi == -1:
        hi = main_text.find("Article", lo)
    if hi == -1:
        hi = lo + 4000
    summary = main_text[lo:hi].strip()
    info["summary"] = summary

    # Category label (e.g. "Procédure disciplinaire") sits at the start of the
    # summary block; keep the leading short line if it looks like a label.
    info["category"] = None
    first_line = summary.split("\n", 1)[0].strip() if summary else ""
    if 0 < len(first_line) <= 60 and first_line[:1].isupper():
        info["category"] = first_line

    # Keywords (Mots-clés ... up to "Article" or "Chronologie").
    kw_block = ""
    km = re.search(r"Mots-clés(.*?)(?:Article|Chronologie|$)", main_text, re.DOTALL)
    if km:
        kw_block = km.group(1)
    # Split heuristically on capitalised phrase boundaries is unreliable; instead
    # pull anchor text of the keyword links if present.
    kw_links = re.findall(r'<a[^>]*>([^<]{3,80})</a>', html)
    info["keywords"] = []

    # Code articles: "Article CSP R4235-13 L4241-1 ..."
    art_m = re.search(r"Article\s+CSP\s+([A-Z0-9\s\-\.]+?)(?:Chronologie|Mots-clés|$)", main_text)
    if art_m:
        arts = re.findall(r"[LRD]\.?\s?\d+[\-\d\.]*", art_m.group(1))
        info["articles"] = [a.replace(" ", "") for a in arts]
    else:
        info["articles"] = []

    # Chronologie table -> decision date(s), plaignant, levels.
    chrono = []
    chrono_idx = main_text.find("Chronologie")
    chrono_text = main_text[chrono_idx:] if chrono_idx != -1 else ""
    dates = []
    for cell in re.findall(r"Date de la décision\s*</td>\s*<td[^>]*>(.*?)</td>", html, re.DOTALL):
        iso = parse_french_date(clean_text(cell))
        if iso:
            dates.append(iso)
    if not dates:
        # fallback: any french date in the chronology text
        for m in re.finditer(r"(\d{1,2}\s+[A-Za-zéûôèàç]+\s+\d{4})", chrono_text):
            iso = parse_french_date(m.group(1))
            if iso:
                dates.append(iso)
    info["dates"] = dates

    pl = re.search(r'data-th="Plaignant"[^>]*>(.*?)</td>', html, re.DOTALL)
    info["plaignant"] = clean_text(pl.group(1)) if pl else None

    # Jurisdiction levels present in the chronology (Première instance / Appel / Cassation).
    levels = []
    for lvl in ("Première instance", "Appel", "Cassation", "Conseil d'État", "Conseil d'Etat"):
        if lvl.lower() in chrono_text.lower():
            levels.append(lvl)
    info["instances"] = levels

    return info


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


def download(session: requests.Session, path: str) -> Optional[bytes]:
    url = path if path.startswith("http") else BASE_URL + path
    try:
        resp = session.get(url, headers=HEADERS, timeout=90)
        if resp.status_code == 200:
            return resp.content
    except requests.RequestException:
        pass
    return None


def make_id(detail_path: str) -> str:
    m = re.search(r"/jurisprudence/(\d+)-", detail_path)
    num = m.group(1) if m else re.sub(r"[^0-9]", "", detail_path)
    return f"cnop-juris-{num}"


def normalize(detail_path: str, info: dict, full_text: str) -> dict:
    detail_url = BASE_URL + detail_path
    dates = info.get("dates") or []
    return {
        "_id": make_id(detail_path),
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": info.get("title") or "Décision disciplinaire — Ordre des pharmaciens",
        "text": full_text,
        "date": dates[0] if dates else None,
        "url": detail_url,
        "pdf_urls": [BASE_URL + p for p in info.get("pdf_paths", [])],
        "category": info.get("category"),
        "instances": info.get("instances") or [],
        "decision_dates": dates,
        "plaignant": info.get("plaignant"),
        "articles": info.get("articles") or [],
        "abstract": info.get("summary") or None,
        "jurisdiction": "FR",
    }


def fetch_all(sample: bool = False, since: Optional[str] = None) -> Generator[dict, None, None]:
    """Yield all disciplinary decisions with full PDF text."""
    session = requests.Session()
    paths = fetch_decision_urls(session)
    print(f"Discovered {len(paths)} decision pages in the jurisprudence database")

    target = 15 if sample else None
    count = 0
    seen_ids = set()

    for path in paths:
        if target and count >= target:
            break
        rid = make_id(path)
        if rid in seen_ids:
            continue
        seen_ids.add(rid)

        page = download(session, path)
        if not page:
            print(f"  [SKIP] {path}: detail page fetch failed")
            time.sleep(REQUEST_DELAY)
            continue
        info = parse_detail(page.decode("utf-8", errors="replace"))

        iso = info["dates"][0] if info.get("dates") else None
        if since and iso and iso < since:
            continue

        # Download every decision PDF and concatenate the full text.
        chunks = []
        for pdf_path in info.get("pdf_paths", []):
            pdf_bytes = download(session, pdf_path)
            if pdf_bytes and pdf_bytes[:4] == b"%PDF":
                t = extract_pdf_text(pdf_bytes)
                if t:
                    chunks.append(t)
            time.sleep(0.5)
        full_text = "\n\n----------\n\n".join(chunks).strip()

        if len(full_text) < 200:
            print(f"  [SKIP] {path}: insufficient full text ({len(full_text)} chars)")
            time.sleep(REQUEST_DELAY)
            continue

        record = normalize(path, info, full_text)
        count += 1
        print(
            f"  [{count}{'/' + str(target) if target else ''}] "
            f"{record['_id']}: {record['title'][:50]}... "
            f"({len(full_text)} chars, {len(info.get('pdf_paths', []))} pdf)"
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
    parser = argparse.ArgumentParser(description="FR/OrdrePharmaciens bootstrap")
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
                    if count % 25 == 0:
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
