#!/usr/bin/env python3
"""
FR/OrdreMedecins - Ordre des Médecins Disciplinary Decisions Fetcher

Fetches disciplinary decisions from the French Medical Order's jurisprudence
database (Chambre disciplinaire nationale and regional chambers).

Data source: https://www.jurisprudence.ordre.medecin.fr/
License: Licence Ouverte 2.0

The site uses Struts 2 with server-side conversation state. Each detail page
contains metadata + abstract. Full decision text is obtained via PDF export
(requires posting CTX token back to the form).

Usage:
  python bootstrap.py bootstrap --sample  # Fetch 15 sample records
  python bootstrap.py bootstrap            # Full bootstrap (all ~20K+ decisions)
  python bootstrap.py updates --since YYYY-MM-DD  # Decisions dated on/after a date
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

SOURCE_ID = "FR/OrdreMedecins"
BASE_URL = "https://jurisprudence.ordre.medecin.fr"
DETAIL_URL = BASE_URL + "/FicheDetailConsultation.do?ficId={fic_id}&isFromRecherche=listeResultats"

HEADERS = {
    "User-Agent": "Legal Data Hunter/1.0 (EU Legal Research; Open Data Collection)",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
}

SAMPLE_DIR = Path(__file__).parent / "sample"
DATA_DIR = Path(__file__).parent / "data"

# Known valid ficId range (non-sequential, with gaps).
# MAX_FIC_ID is only a starting hint — the real ceiling is probed at runtime by
# find_max_fic_id(), otherwise the corpus silently freezes once the site issues
# ids beyond the hint.
MIN_FIC_ID = 1
MAX_FIC_ID = 23000

# Upward probing is bounded so an always-answering site can never loop forever.
FIC_ID_PROBE_STEP = 500
FIC_ID_PROBE_MAX_STEPS = 40

# ficIds run roughly, but not strictly, in date order: sampling the live site
# showed neighbours up to ~18 months apart (22824 = 2025-12-18 sits next to
# 22823 = 2025-04-10). fetch_updates therefore keeps walking until this many
# consecutive decisions are all older than `since`.
UPDATE_STOP_AFTER_OLDER = 300


def clean_html(html_text: str) -> str:
    """Remove HTML tags and clean text."""
    if not html_text:
        return ""
    text = unescape(html_text)
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def parse_detail_page(html: str, fic_id: int) -> Optional[dict]:
    """Parse the detail page HTML to extract metadata."""
    if 'ABSTRACT' not in html:
        return None

    result = {"fic_id": fic_id}

    # Extract instance/jurisdiction
    m = re.search(
        r'fiche_juridictionLibelleHighlight.*?<td[^>]*>\s*<div>\s*(.*?)\s*</div>',
        html, re.DOTALL
    )
    if m:
        result["instance"] = clean_html(m.group(1)).strip()

    # Extract date
    m = re.search(r'datepicker_\d+["\'][^>]*>(\d{2}/\d{2}/\d{4})', html)
    if m:
        try:
            dt = datetime.strptime(m.group(1), "%d/%m/%Y")
            result["date"] = dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    # Extract document type
    m = re.search(
        r'typeDocumentHighlight.*?<td[^>]*>\s*<div>\s*(.*?)\s*</div>',
        html, re.DOTALL
    )
    if m:
        result["doc_type"] = clean_html(m.group(1)).strip()

    # Extract dossier number
    m = re.search(
        r'numeroDossierHighlight.*?<td[^>]*>\s*<div>\s*(.*?)\s*</div>',
        html, re.DOTALL
    )
    if m:
        result["dossier_number"] = clean_html(m.group(1)).strip()

    # Extract keywords
    keywords = re.findall(r'<hlfrag>(.*?)</hlfrag>', html)
    if keywords:
        result["keywords"] = [clean_html(k) for k in keywords
                              if k not in ("Plaignant", "Requérant", "Poursuivi",
                                           "Partie dans l'affaire")]

    # Extract abstract
    m = re.search(
        r'<h2[^>]*>ABSTRACT</h2>\s*<div[^>]*>(.*?)</div>',
        html, re.DOTALL
    )
    if m:
        result["abstract"] = clean_html(m.group(1)).strip()

    # Extract articles referenced
    m = re.search(
        r'code sant.*?publique.*?<td[^>]*>\s*<div>\s*(.*?)\s*</div>',
        html, re.DOTALL | re.IGNORECASE
    )
    if m:
        result["articles_csp"] = clean_html(m.group(1)).strip()

    # Extract CTX token for PDF export
    m = re.search(r'name="CTX"\s+value="([^"]+)"', html)
    if m:
        result["ctx"] = m.group(1)

    # Extract jsessionid
    m = re.search(r'jsessionid=([A-Z0-9.]+)', html)
    if m:
        result["jsessionid"] = m.group(1)

    return result


def export_decision_pdf(session: requests.Session, metadata: dict) -> Optional[bytes]:
    """Export the full decision PDF using the Struts 2 conversation context."""
    ctx = metadata.get("ctx")
    jsessionid = metadata.get("jsessionid")
    if not ctx or not jsessionid:
        return None

    export_url = f"{BASE_URL}/FicheDetailConsultation.do;jsessionid={jsessionid}"
    data = {
        "CTX": ctx,
        "action:FicheDetailConsultationExportDecision": "",
    }
    headers = {
        **HEADERS,
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": DETAIL_URL.format(fic_id=metadata["fic_id"]),
    }

    try:
        resp = session.post(export_url, data=data, headers=headers, timeout=30)
        if resp.status_code == 200 and resp.headers.get("Content-Disposition"):
            return resp.content
    except requests.RequestException:
        pass
    return None


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber."""
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages.append(text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
            return "\n\n".join(pages)
    except Exception:
        return ""


def normalize(metadata: dict, full_text: str) -> dict:
    """Normalize a decision into the standard schema."""
    fic_id = metadata["fic_id"]
    dossier = metadata.get("dossier_number", str(fic_id))
    instance = metadata.get("instance", "Chambre disciplinaire")
    date = metadata.get("date", "")

    title = f"{instance} — Dossier n° {dossier}"
    if date:
        title += f" — {date}"

    return {
        "_id": f"ordre-medecins-{fic_id}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": full_text,
        "date": date or None,
        "url": DETAIL_URL.format(fic_id=fic_id),
        "dossier_number": dossier,
        "instance": instance,
        "doc_type": metadata.get("doc_type", ""),
        "keywords": metadata.get("keywords", []),
        "abstract": metadata.get("abstract", ""),
        "articles_csp": metadata.get("articles_csp", ""),
    }


def fetch_decision(session: requests.Session, fic_id: int) -> Optional[dict]:
    """Fetch a single decision by ficId: metadata + full text PDF.

    Uses a fresh session per request because Struts 2 conversation state
    conflicts when the same session visits multiple ficId pages.
    """
    # Fresh session to avoid Struts 2 CTX token conflicts
    sess = requests.Session()
    url = DETAIL_URL.format(fic_id=fic_id)
    try:
        resp = sess.get(url, headers=HEADERS, timeout=30)
        if resp.status_code != 200:
            return None
    except requests.RequestException:
        return None

    metadata = parse_detail_page(resp.text, fic_id)
    if not metadata:
        return None

    # Export and extract PDF using same session (preserves CTX state)
    pdf_bytes = export_decision_pdf(sess, metadata)
    if not pdf_bytes:
        # Fall back to abstract only if PDF export fails
        full_text = metadata.get("abstract", "")
        if not full_text:
            return None
    else:
        full_text = extract_pdf_text(pdf_bytes)
        if not full_text:
            full_text = metadata.get("abstract", "")

    return normalize(metadata, full_text)


def fic_id_exists(session: requests.Session, fic_id: int) -> bool:
    """True if the site serves a real decision page for this ficId.

    Missing ids answer with HTTP 500 and a short error page, but the check is on
    parsed content rather than status so a soft-200 error page cannot be mistaken
    for a decision.
    """
    try:
        resp = session.get(DETAIL_URL.format(fic_id=fic_id), headers=HEADERS, timeout=30)
    except requests.RequestException:
        return False
    if resp.status_code != 200:
        return False
    return parse_detail_page(resp.text, fic_id) is not None


def find_max_fic_id(session: requests.Session, hint: int = MAX_FIC_ID) -> int:
    """Probe for the highest ficId the site currently serves.

    Walks up from `hint` in fixed steps until a whole step lands in empty space,
    then binary-searches the boundary. Bounded by FIC_ID_PROBE_MAX_STEPS so the
    loop terminates even if the site starts answering every id.
    """
    print(f"Probing for the highest ficId (hint {hint})...")

    # Find a known-good floor at or below the hint.
    lo = None
    probe = hint
    for _ in range(FIC_ID_PROBE_MAX_STEPS):
        if fic_id_exists(session, probe):
            lo = probe
            break
        probe -= FIC_ID_PROBE_STEP
        if probe < MIN_FIC_ID:
            break
    if lo is None:
        print(f"  No live ficId found near {hint}; falling back to {hint}")
        return hint

    # Walk up until a step lands past the end.
    hi = lo + FIC_ID_PROBE_STEP
    for _ in range(FIC_ID_PROBE_MAX_STEPS):
        if not fic_id_exists(session, hi):
            break
        lo, hi = hi, hi + FIC_ID_PROBE_STEP
    else:
        print(f"  Upward probe hit its step limit at {lo}; using it as the ceiling")
        return lo

    while hi - lo > 1:
        mid = (lo + hi) // 2
        if fic_id_exists(session, mid):
            lo = mid
        else:
            hi = mid

    print(f"  Highest ficId: {lo}")
    return lo


def _coerce_since(since) -> str:
    """Accept a datetime or a YYYY-MM-DD string — the refresh runner passes both."""
    if isinstance(since, datetime):
        return since.strftime("%Y-%m-%d")
    return str(since)[:10]


def fetch_updates(since) -> Generator[dict, None, None]:
    """Yield decisions dated on or after `since`.

    The site has no date-filtered query, so this walks the ficId space downward
    from the live ceiling — new decisions get new, higher ids. Because ids are
    only roughly date-ordered, the walk does not stop at the first old decision
    but after UPDATE_STOP_AFTER_OLDER consecutive ones.
    """
    since_str = _coerce_since(since)
    session = requests.Session()
    max_fic_id = find_max_fic_id(session)

    print(f"Fetching decisions dated on or after {since_str} "
          f"(walking down from ficId {max_fic_id})...")

    yielded = 0
    consecutive_older = 0

    for fic_id in range(max_fic_id, MIN_FIC_ID - 1, -1):
        record = fetch_decision(session, fic_id)
        if record is None:
            continue

        date = record.get("date")
        if date and date < since_str:
            consecutive_older += 1
            if consecutive_older >= UPDATE_STOP_AFTER_OLDER:
                print(f"  Stopping at ficId={fic_id}: {consecutive_older} consecutive "
                      f"decisions older than {since_str}")
                break
            time.sleep(2.0)
            continue

        consecutive_older = 0

        if len(record.get("text", "")) < 100:
            print(f"  [SKIP] ficId={fic_id}: text too short ({len(record.get('text', ''))} chars)")
            time.sleep(2.0)
            continue

        yielded += 1
        print(f"  [{yielded}] ficId={fic_id} ({date}): {record['title'][:60]}... "
              f"({len(record['text'])} chars)")
        yield record
        time.sleep(2.0)

    print(f"\nTotal updated records: {yielded}")


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Yield all decisions, iterating through ficId values."""
    session = requests.Session()
    count = 0
    target = 15 if sample else 999999
    errors_in_row = 0

    # For sample mode, use known-good recent ficIds to save time
    if sample:
        # Scan from recent IDs backward to find valid ones quickly
        fic_ids = range(22500, 19000, -1)
    else:
        fic_ids = range(find_max_fic_id(session), MIN_FIC_ID - 1, -1)

    for fic_id in fic_ids:
        if count >= target:
            break

        record = fetch_decision(session, fic_id)
        if record is None:
            errors_in_row += 1
            if errors_in_row > 50 and sample:
                # Too many misses, skip ahead
                break
            continue

        errors_in_row = 0

        if len(record.get("text", "")) < 100:
            print(f"  [SKIP] ficId={fic_id}: text too short ({len(record.get('text', ''))} chars)")
            continue

        count += 1
        print(f"  [{count}/{target}] ficId={fic_id}: {record['title'][:60]}... ({len(record['text'])} chars)")
        yield record

        # Rate limiting
        time.sleep(2.0)

    print(f"\nTotal records fetched: {count}")


def save_samples(records: list):
    """Save sample records to the sample/ directory."""
    SAMPLE_DIR.mkdir(exist_ok=True)
    for i, record in enumerate(records, 1):
        path = SAMPLE_DIR / f"record_{i:03d}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        print(f"  Saved: {path.name}")


def main():
    parser = argparse.ArgumentParser(description="FR/OrdreMedecins bootstrap")
    subparsers = parser.add_subparsers(dest="command")

    boot_parser = subparsers.add_parser("bootstrap", help="Fetch records")
    boot_parser.add_argument("--sample", action="store_true", help="Fetch 15 sample records only")

    # VPS wrapper compatibility: bootstrap-fast == full bootstrap
    fast_parser = subparsers.add_parser("bootstrap-fast", help="Full fetch (VPS wrapper alias)")
    fast_parser.add_argument("--sample", action="store_true", help="Fetch 15 sample records only")

    updates_parser = subparsers.add_parser("updates", help="Fetch decisions since a date")
    updates_parser.add_argument("--since", required=True, help="Date (YYYY-MM-DD)")

    args = parser.parse_args()

    if args.command in ("bootstrap", "bootstrap-fast"):
        print(f"FR/OrdreMedecins bootstrap ({'sample' if args.sample else 'full'} mode)")
        print(f"Source: {BASE_URL}")
        print()

        if args.sample:
            records = list(fetch_all(sample=True))
            if records:
                save_samples(records)
                print(f"\n{len(records)} sample records saved to {SAMPLE_DIR}/")
        else:
            # Full mode: stream to data/records.jsonl so the VPS pipeline ingests them.
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            jsonl_path = DATA_DIR / "records.jsonl"
            count = 0
            with open(jsonl_path, "w", encoding="utf-8") as f:
                for r in fetch_all(sample=False):
                    f.write(json.dumps(r, ensure_ascii=False) + "\n")
                    count += 1
                    if count % 100 == 0:
                        print(f"Progress: {count} records written")
            print(f"\nFull bootstrap complete: {count} records -> {jsonl_path}")

    elif args.command == "updates":
        since = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        print(f"FR/OrdreMedecins updates since {since.date()}")
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        jsonl_path = DATA_DIR / "records.jsonl"
        count = 0
        with open(jsonl_path, "w", encoding="utf-8") as f:
            for record in fetch_updates(since):
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
                count += 1
                if count % 25 == 0:
                    print(f"Progress: {count} records written")
        print(f"\nUpdates complete: {count} records -> {jsonl_path}")
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
