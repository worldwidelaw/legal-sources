#!/usr/bin/env python3
"""
FR/OrdreChirurgiensDentistes - Disciplinary jurisprudence of the French
National Order of Dental Surgeons (Ordre National des Chirurgiens-Dentistes).

Fetches anonymised disciplinary decisions (Chambre disciplinaire nationale and
first-instance chambers) published on the Order's public jurisprudence database.

Data source: https://www.ordre-chirurgiens-dentistes.fr/decisions/

The decisions portal is a plain PHP site. Decisions are browsed by theme via
`decision.php?categorie={theme}`. Each decision "card" exposes its dossier
number, decision date, instance type, and a relative link to the FULL anonymised
decision PDF under `decisions/Fichiers/{name}.pdf`. Full text is obtained by
downloading that PDF and extracting it with pdfplumber.

The same decision can appear under several themes, so records are de-duplicated
by dossier number (themes are merged into the `keywords` field).

Usage:
  python bootstrap.py bootstrap --sample          # 15 sample records
  python bootstrap.py bootstrap                    # full bootstrap -> data/records.jsonl
  python bootstrap.py bootstrap-fast               # VPS wrapper alias for full
  python bootstrap.py updates --since YYYY-MM-DD   # not supported (no date API)
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
from urllib.parse import quote, urljoin

import pdfplumber
import requests

SOURCE_ID = "FR/OrdreChirurgiensDentistes"
BASE_URL = "https://www.ordre-chirurgiens-dentistes.fr/decisions/"
DECISIONS_INDEX = BASE_URL  # the index page lists all themes
CATEGORY_URL = BASE_URL + "decision.php?categorie={categorie}"

HEADERS = {
    "User-Agent": "Legal Data Hunter/1.0 (EU Legal Research; Open Data Collection)",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

SAMPLE_DIR = Path(__file__).parent / "sample"
DATA_DIR = Path(__file__).parent / "data"

REQUEST_DELAY = 1.5


def clean_text(text: str) -> str:
    """Collapse whitespace and decode entities."""
    if not text:
        return ""
    text = unescape(text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def get_categories(session: requests.Session) -> list:
    """Return the list of theme/category names from the decisions index page."""
    resp = session.get(DECISIONS_INDEX, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    cats = re.findall(r"decision\.php\?categorie=([^`]+)`", resp.text)
    # De-duplicate while preserving order
    seen = set()
    out = []
    for c in cats:
        c = unescape(c).strip()
        if c and c not in seen:
            seen.add(c)
            out.append(c)
    return out


def parse_category_page(html: str) -> list:
    """Parse a category page into a list of decision-card dicts."""
    cards = []
    pattern = re.compile(
        r"<div id='(\d+)' class=\"décisionTest\">(.*?)"
        r"(?=<div id='\d+' class=\"décisionTest\">|<footer|</body>)",
        re.DOTALL,
    )
    for m in pattern.finditer(html):
        block = m.group(2)
        doss = re.search(r"N° dossier\s*:\s*<span[^>]*>([^<]+)</span>", block)
        date = re.search(r"Date de la décision\s*:\s*<span[^>]*>([^<]+)</span>", block)
        inst = re.search(r"Type d'instance\s*:\s*<span[^>]*>([^<]+)</span>", block)
        pdf = re.search(r"download\(`([^`]+)`", block)
        if not doss or not pdf:
            continue
        cards.append({
            "dossier": clean_text(doss.group(1)),
            "date_raw": clean_text(date.group(1)) if date else "",
            "instance": clean_text(inst.group(1)) if inst else "",
            "pdf_rel": pdf.group(1).strip(),
        })
    return cards


def parse_date(date_raw: str) -> Optional[str]:
    """Convert DD/MM/YYYY -> ISO YYYY-MM-DD."""
    if not date_raw:
        return None
    m = re.search(r'(\d{2})/(\d{2})/(\d{4})', date_raw)
    if not m:
        return None
    try:
        dt = datetime.strptime(m.group(0), "%d/%m/%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber (with per-page cache flush)."""
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


def download_pdf(session: requests.Session, pdf_rel: str) -> Optional[bytes]:
    """Resolve a relative PDF path against the decisions base and download it."""
    # pdf_rel looks like '../decisions/Fichiers/2999 - Anonymisée.pdf'
    abs_url = urljoin(CATEGORY_URL, pdf_rel)
    # Percent-encode spaces and accented chars in the path component
    abs_url = quote(abs_url, safe=":/?&=%")
    try:
        resp = session.get(abs_url, headers=HEADERS, timeout=60)
        if resp.status_code == 200 and resp.content[:4] == b"%PDF":
            return resp.content
    except requests.RequestException:
        pass
    return None


def normalize(card: dict, full_text: str) -> dict:
    """Normalize a decision card + extracted text into the standard schema."""
    dossier = card["dossier"]
    instance = card.get("instance") or "Chambre disciplinaire"
    date = parse_date(card.get("date_raw", ""))

    title = f"{instance} — Dossier n° {dossier}"
    if date:
        title += f" — {date}"

    safe_doss = re.sub(r'[^0-9A-Za-z-]+', '-', dossier).strip('-')
    return {
        "_id": f"ocd-{safe_doss}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": full_text,
        "date": date,
        "url": CATEGORY_URL.format(categorie=quote(card.get("_categorie", ""))),
        "dossier_number": dossier,
        "instance": instance,
        "keywords": sorted(card.get("themes", [])),
        "jurisdiction": "FR",
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Yield all disciplinary decisions, browsing every theme and de-duplicating."""
    session = requests.Session()
    categories = get_categories(session)
    print(f"Found {len(categories)} themes")

    target = 15 if sample else None
    seen_dossiers = {}  # dossier -> card (for theme accumulation)
    ordered = []        # dossiers in discovery order

    # Pass 1: collect cards across themes, merging themes per dossier
    for cat in categories:
        url = CATEGORY_URL.format(categorie=quote(cat))
        try:
            resp = session.get(url, headers=HEADERS, timeout=30)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"  [WARN] theme '{cat}' failed: {e}")
            continue
        cards = parse_category_page(resp.text)
        for card in cards:
            doss = card["dossier"]
            if doss in seen_dossiers:
                seen_dossiers[doss]["themes"].add(cat)
                continue
            card["themes"] = {cat}
            card["_categorie"] = cat
            seen_dossiers[doss] = card
            ordered.append(doss)
        time.sleep(REQUEST_DELAY)
        # In sample mode, stop crawling themes once we have plenty of candidates
        if sample and len(ordered) >= target * 3:
            break

    print(f"Discovered {len(ordered)} unique decisions")

    # Pass 2: download PDFs + yield
    count = 0
    for doss in ordered:
        if target and count >= target:
            break
        card = seen_dossiers[doss]
        pdf_bytes = download_pdf(session, card["pdf_rel"])
        if not pdf_bytes:
            print(f"  [SKIP] dossier {doss}: PDF download failed")
            continue
        full_text = extract_pdf_text(pdf_bytes)
        if len(full_text) < 200:
            print(f"  [SKIP] dossier {doss}: text too short ({len(full_text)} chars)")
            continue
        record = normalize(card, full_text)
        count += 1
        print(f"  [{count}{'/' + str(target) if target else ''}] "
              f"dossier {doss}: {record['title'][:55]}... ({len(full_text)} chars)")
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
    parser = argparse.ArgumentParser(description="FR/OrdreChirurgiensDentistes bootstrap")
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
        print("Updates not supported for this source (no date-filtered API).")
        sys.exit(1)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
