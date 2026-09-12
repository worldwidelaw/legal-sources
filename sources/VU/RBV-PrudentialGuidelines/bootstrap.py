#!/usr/bin/env python3
"""VU/RBV-PrudentialGuidelines — Reserve Bank of Vanuatu.

Fetches the RBV prudential framework: prudential guidelines for domestic and
international banks, insurance prudential guidelines, and the underlying banking
and insurance legislation hosted by the Reserve Bank of Vanuatu. PDFs are
downloaded and their text extracted via pdfminer.

Each category page under /financial-stability/prudential-framework/ lists the
PDFs. Guidelines/policies are classified as `doctrine` (regulatory guidance
issued by the central bank); the Acts and Regulations are `legislation`.
"""

import argparse
import io
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse, unquote

import requests

try:
    from pdfminer.high_level import extract_text as _pdf_extract
except ImportError:
    _pdf_extract = None

SOURCE_ID = "VU/RBV-PrudentialGuidelines"
BASE_URL = "https://www.rbv.gov.vu"
PF = f"{BASE_URL}/index.php/en/financial-stability/prudential-framework"

# (category page slug, document type)
CATEGORIES = [
    ("66-legislations-for-insurance", "legislation"),
    ("67-prudential-guidelines-for-insurance", "doctrine"),
    ("69-prudential-guidelines-for-domestic-banks", "doctrine"),
    ("70-legislation-for-banking-industry", "legislation"),
    ("71-prudential-guidelines-for-international-banks", "doctrine"),
]

SAMPLE_DIR = Path(__file__).parent / "sample"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (legal-data-research)",
}

session = requests.Session()
session.headers.update(HEADERS)

ANCHOR_RE = re.compile(
    r'<a[^>]+href=["\']([^"\']*\.pdf)["\'][^>]*>(.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)
# Generic anchor text such as "Policy 8" or "Prudential Guideline 12"
GENERIC_RE = re.compile(r'^(prudential\s+guideline|policy|pg)\s*\.?\s*(no\.?)?\s*\d+$', re.I)


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    if _pdf_extract is None:
        raise ImportError("pdfminer.six is required: pip install pdfminer.six")
    return _pdf_extract(io.BytesIO(pdf_bytes))


def _titlecase(s: str) -> str:
    small = {"of", "and", "for", "the", "to", "in", "on", "a", "an", "by", "with"}
    words = s.lower().split()
    out = []
    for i, w in enumerate(words):
        if w not in small or i == 0:
            out.append(w.capitalize())
        else:
            out.append(w)
    return " ".join(out)


def extract_subject(text: str) -> str | None:
    """Pull the document subject from the PDF header.

    RBV guidelines/policies open with a line like "PRUDENTIAL GUIDELINE NO. 1"
    followed by the subject in caps, then a section marker ("A. PURPOSE").
    """
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    idx = None
    for i, l in enumerate(lines[:25]):
        # Header line ends in a number, e.g. "PRUDENTIAL GUIDELINE NO: 1",
        # "Policy Guideline 8", "POLICY NUMBER 3".
        if re.search(r'(guideline|policy)\s*(?:no\.?:?|number)?[\s:.]*\d+\s*$', l, re.I):
            idx = i
            break
    if idx is None:
        return None
    subj = []
    for l in lines[idx + 1: idx + 6]:
        if re.match(r'^[A-Z]\.\s', l) or re.match(r'^\d+[\.\)]\s', l):
            break
        if re.fullmatch(r'(?i)purpose|introduction', l) and subj:
            break
        letters = [c for c in l if c.isalpha()]
        if letters and sum(c.isupper() for c in letters) / len(letters) > 0.6:
            subj.append(l)
        else:
            break
    if not subj:
        return None
    return _titlecase(" ".join(subj))


def _year_from(*candidates) -> str | None:
    for c in candidates:
        if not c:
            continue
        m = re.search(r'\b(19\d{2}|20\d{2})\b', c)
        if m:
            return m.group(1)
    return None


def discover_documents():
    seen = set()
    for slug, dtype in CATEGORIES:
        page_url = f"{PF}/{slug}"
        try:
            resp = session.get(page_url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            print(f"  [ERROR] Failed to fetch {page_url}: {e}", file=sys.stderr)
            continue

        for m in ANCHOR_RE.finditer(resp.text):
            href = m.group(1).strip()
            anchor = re.sub(r'<[^>]+>', '', m.group(2))
            anchor = re.sub(r'\s+', ' ', anchor).strip()
            if not href:
                continue

            pdf_url = urljoin(page_url, href)
            if pdf_url in seen:
                continue
            seen.add(pdf_url)

            path = unquote(urlparse(pdf_url).path)
            parts = path.strip("/").split("/")
            key = (parts[-2] + "_" + Path(parts[-1]).stem) if len(parts) >= 2 else Path(parts[-1]).stem
            doc_id = re.sub(r'[^a-z0-9]+', '-', key.lower()).strip('-')

            yield {
                "doc_id": doc_id,
                "anchor": anchor,
                "pdf_url": pdf_url,
                "type": dtype,
                "category": slug,
            }


def normalize(raw: dict) -> dict:
    now = datetime.now(timezone.utc).isoformat()
    year = raw.get("year")
    date = f"{year}-01-01" if year else None
    return {
        "_id": f"vu-rbv-{raw['doc_id']}",
        "_source": SOURCE_ID,
        "_type": raw.get("type", "doctrine"),
        "_fetched_at": now,
        "title": raw.get("title", ""),
        "text": raw.get("text", ""),
        "date": date,
        "url": raw.get("pdf_url", ""),
        "doc_id": f"vu-rbv-{raw['doc_id']}",
        "category": raw.get("category", ""),
        "language": "en",
        "pdf_url": raw.get("pdf_url", ""),
    }


def fetch_all():
    for doc in discover_documents():
        try:
            print(f"  Fetching: {doc['anchor'][:55]}...", file=sys.stderr)
            resp = session.get(doc["pdf_url"], timeout=90)
            resp.raise_for_status()

            text = extract_text_from_pdf(resp.content).strip()
            if len(text) < 50:
                print(f"  [SKIP] Insufficient text for {doc['anchor']}", file=sys.stderr)
                continue

            # Build the best available title.
            title = doc["anchor"]
            if GENERIC_RE.match(title):
                subject = extract_subject(text)
                if subject:
                    title = f"{title} — {subject}"
            doc["title"] = title
            doc["year"] = _year_from(doc["anchor"], doc["doc_id"], "\n".join(text.splitlines()[:8]))
            doc["text"] = text

            yield normalize(doc)
            time.sleep(1.5)
        except Exception as e:
            print(f"  [ERROR] {doc['anchor']}: {e}", file=sys.stderr)
            time.sleep(2)


def fetch_updates(since=None):
    # No per-document timestamps available; full re-fetch on schedule.
    yield from fetch_all()


def bootstrap_sample(limit: int = 15):
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    count = 0
    for record in fetch_all():
        if count >= limit:
            break
        fname = SAMPLE_DIR / f"record_{count:04d}.json"
        with open(fname, "w", encoding="utf-8") as f:
            json.dump(record, f, indent=2, ensure_ascii=False)
        text_len = len(record.get("text", ""))
        print(f"  [{count+1}/{limit}] {record['title'][:60]} ({text_len} chars)")
        count += 1

    print(f"\nSaved {count} samples to {SAMPLE_DIR}")
    return count


def main():
    parser = argparse.ArgumentParser(description="VU/RBV-PrudentialGuidelines bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Run bootstrapper")
    boot.add_argument("--sample", action="store_true", help="Fetch sample only")
    boot.add_argument("--limit", type=int, default=15, help="Sample limit")
    boot.add_argument("--full", action="store_true", help="Full fetch")

    args = parser.parse_args()

    if args.command == "bootstrap":
        if args.sample or not args.full:
            count = bootstrap_sample(args.limit)
            if count < 10:
                print(f"WARNING: Only {count} samples collected", file=sys.stderr)
                sys.exit(1)
        else:
            for record in fetch_all():
                print(json.dumps(record, ensure_ascii=False))
    else:
        parser.print_help()


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
