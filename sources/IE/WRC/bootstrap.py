#!/usr/bin/env python3
"""
Ireland — Workplace Relations Commission (WRC) Adjudication Decisions Fetcher

The WRC publishes the full text of Adjudication Officer decisions (employment,
equal-status and equality complaints under the Workplace Relations Act 2015 and
the many employment/equality Acts it consolidates), plus the legacy Equality
Tribunal decisions (DEC-E / DEC-S / DEC-P series). All decisions are published as
server-rendered HTML pages at:

    https://www.workplacerelations.ie/en/cases/{YYYY}/{month}/{ref}.html

and are enumerable via the decisions search listing:

    https://www.workplacerelations.ie/en/search/?query=&decisions=1&pageNumber={N}

10 decisions per page, newest first, ~3,200 pages (~32,000 full-text decisions).

Data source: https://www.workplacerelations.ie/en/search/?decisions=1
License: PSI Licence / CC BY 4.0 (Irish public sector information — commercial use permitted)

Notes:
- The host multiplexes badly over HTTP/2 (stream CANCEL errors); we force HTTP/1.1.
- Full bootstrap streams to data/records.jsonl (fleet-friendly); --sample writes to sample/.
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Iterator, Optional, Dict, List

SOURCE_ID = "IE/WRC"
BASE_URL = "https://www.workplacerelations.ie"
SEARCH_URL = BASE_URL + "/en/search/?query=&decisions=1&pageNumber={page}"
PER_PAGE = 10
REQUEST_DELAY = 1.5
UA = "Mozilla/5.0 (compatible; LegalDataHunter/1.0)"

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}

CASE_LINK_RE = re.compile(
    r'href="(/en/cases/(\d{4})/[a-z]+/([a-z0-9\-]+)\.html)"', re.I
)
FOOTER_MARKERS = (
    "Add To My Documents",
    "The Labour Court Cookie Management",
    "Cookie Management Accessibility",
)


def curl_get(url: str, timeout: int = 45, retries: int = 3) -> Optional[str]:
    """Fetch a URL with curl over HTTP/1.1 (the host breaks on HTTP/2)."""
    for attempt in range(retries):
        try:
            result = subprocess.run(
                ["curl", "-sS", "--http1.1", "--compressed", "--max-time",
                 str(timeout), "-H", f"User-Agent: {UA}", url],
                capture_output=True, text=True, timeout=timeout + 10,
            )
            if result.returncode == 0 and result.stdout:
                return result.stdout
        except Exception as e:
            print(f"  curl error ({attempt + 1}/{retries}): {e}", file=sys.stderr)
        time.sleep(2 * (attempt + 1))
    return None


def _extract_content(html_text: str) -> str:
    """Pull the main decision body out of the page's <div class="content"> region."""
    i = html_text.find('class="content"')
    if i == -1:
        return ""
    j = html_text.find(">", i)
    if j == -1:
        return ""
    seg = html_text[j + 1:]
    seg = re.sub(r"(?is)<(script|style|nav|footer)[^>]*>.*?</\1>", " ", seg)
    seg = re.sub(r"(?is)<br\s*/?>", "\n", seg)
    seg = re.sub(r"(?is)</(p|div|tr|h[1-6]|li)>", "\n", seg)
    text = unescape(re.sub(r"(?s)<[^>]+>", " ", seg))
    text = re.sub(r"[ \t ]+", " ", text)
    text = re.sub(r"\n[ \t]*", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    # Trim trailing site chrome / footer navigation.
    cut = len(text)
    for mark in FOOTER_MARKERS:
        k = text.find(mark)
        if 0 <= k < cut:
            cut = k
    return text[:cut].strip()


def _parse_ref(slug: str) -> str:
    """adj-00064271 -> ADJ-00064271 ; dec-e2015-135 -> DEC-E2015-135."""
    ref = slug.upper()
    # keep the standard hyphenation; e.g. DEC-E2015-135, ADJ-00064271
    return ref


def _parse_date(text: str, url_year: str) -> Optional[str]:
    """Prefer the 'Dated:' decision date, else fall back to the URL year."""
    m = re.search(r"Dated:\s*(\d{1,2})(?:st|nd|rd|th)?\s+(?:of\s+)?([A-Za-z]+),?\s+(\d{4})", text)
    if not m:
        m = re.search(r"Date of (?:Decision|Determination):\s*(\d{1,2})(?:st|nd|rd|th)?\s+(?:of\s+)?([A-Za-z]+),?\s+(\d{4})", text)
    if m:
        day, mon, year = m.group(1), m.group(2).lower(), m.group(3)
        if mon in MONTHS:
            return f"{year}-{MONTHS[mon]:02d}-{int(day):02d}"
    # Labour Court determinations sign with a numeric date before "Chairman".
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})\s+(?:Deputy\s+)?Chair", text)
    if m:
        return f"{m.group(3)}-{int(m.group(2)):02d}-{int(m.group(1)):02d}"
    if url_year and url_year.isdigit():
        return f"{url_year}-01-01"
    return None


def _parse_title(ref: str, text: str) -> str:
    """Compose a human title: reference plus the primary Act, if detectable."""
    act = None
    m = re.search(r"under (?:section|s\.?)\s*[\w()]+\s+of the ([A-Z][A-Za-z ,'&\-]+?Acts?,?\s*\d{4})", text)
    if not m:
        m = re.search(r"of the ([A-Z][A-Za-z ,'&\-]+?Acts?,?\s*\d{4})", text)
    if m:
        act = re.sub(r"\s+", " ", m.group(1)).strip().rstrip(",")
    return f"{ref} — {act}" if act else ref


def normalize(raw: Dict) -> Dict:
    """Transform a fetched decision into the standard schema."""
    text = raw["text"]
    ref = raw["ref"]
    return {
        "_id": f"IE_WRC_{ref}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": _parse_title(ref, text),
        "text": text,
        "date": _parse_date(text, raw.get("year", "")),
        "url": raw["url"],
        "reference": ref,
        "tribunal": "Workplace Relations Commission",
        "language": "en",
    }


def _list_page(page: int) -> List[Dict]:
    """Return the case-link dicts on one search-listing page."""
    html_text = curl_get(SEARCH_URL.format(page=page))
    if not html_text:
        return []
    out, seen = [], set()
    for m in CASE_LINK_RE.finditer(html_text):
        path, year, slug = m.group(1), m.group(2), m.group(3)
        if slug in seen:
            continue
        seen.add(slug)
        out.append({"url": BASE_URL + path, "year": year, "slug": slug})
    return out


def _fetch_decision(item: Dict) -> Optional[Dict]:
    """Fetch and extract one decision's full text."""
    html_text = curl_get(item["url"])
    if not html_text:
        return None
    text = _extract_content(html_text)
    if not text or len(text) < 200:
        return None
    return normalize({
        "text": text,
        "ref": _parse_ref(item["slug"]),
        "year": item["year"],
        "url": item["url"],
    })


def fetch_all(sample: bool = False) -> Iterator[Dict]:
    """Yield every WRC decision (newest first) with full text."""
    page = 1
    empty_streak = 0
    emitted = 0
    seen_ids = set()
    while True:
        items = _list_page(page)
        if not items:
            empty_streak += 1
            if empty_streak >= 2:
                print(f"No results on page {page} (x{empty_streak}) — stopping.", file=sys.stderr)
                break
            page += 1
            continue
        empty_streak = 0
        print(f"Listing page {page}: {len(items)} decisions", file=sys.stderr)
        for item in items:
            rec = _fetch_decision(item)
            time.sleep(REQUEST_DELAY)
            if not rec or rec["_id"] in seen_ids:
                continue
            seen_ids.add(rec["_id"])
            yield rec
            emitted += 1
            if sample and emitted >= 12:
                return
        page += 1
        time.sleep(REQUEST_DELAY)


def fetch_updates(since: str) -> Iterator[Dict]:
    """Yield decisions with a decision date on/after `since` (ISO YYYY-MM-DD).

    Decisions are listed newest-first, so we walk pages until we pass the cutoff.
    """
    for rec in fetch_all(sample=False):
        d = rec.get("date")
        if d and d >= since:
            yield rec
        elif d and d < since:
            # newest-first: once we're comfortably past the cutoff, stop.
            break


def save_sample(records: List[Dict], sample_dir: Path) -> None:
    sample_dir.mkdir(parents=True, exist_ok=True)
    for rec in records:
        fn = re.sub(r"[^\w-]", "_", rec["_id"])[:100] + ".json"
        with open(sample_dir / fn, "w", encoding="utf-8") as f:
            json.dump(rec, f, ensure_ascii=False, indent=2)
    if records:
        texts = [r["text"] for r in records if r.get("text")]
        avg = sum(len(t) for t in texts) / len(texts) if texts else 0
        print(f"Saved {len(records)} samples | avg text {avg:.0f} chars | "
              f"all have text: {all(r.get('text') for r in records)}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Fetch IE/WRC adjudication decisions")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("bootstrap", "bootstrap-fast"):
        p = sub.add_parser(name)
        p.add_argument("--sample", action="store_true", help="Fetch only sample records")
    up = sub.add_parser("updates")
    up.add_argument("--since", required=True, help="ISO date (e.g. 2026-01-01)")

    args = parser.parse_args()
    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"
    data_dir = script_dir / "data"

    if args.command in ("bootstrap", "bootstrap-fast"):
        if args.sample:
            records = list(fetch_all(sample=True))
            print(f"\nFetched {len(records)} sample records", file=sys.stderr)
            save_sample(records, sample_dir)
        else:
            data_dir.mkdir(parents=True, exist_ok=True)
            jsonl_path = data_dir / "records.jsonl"
            count = 0
            with open(jsonl_path, "w", encoding="utf-8") as f:
                for record in fetch_all(sample=False):
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    count += 1
                    if count % 50 == 0:
                        print(f"Progress: {count} records written", file=sys.stderr)
            print(f"Full bootstrap complete: {count} records -> {jsonl_path}", file=sys.stderr)

    elif args.command == "updates":
        for record in fetch_updates(args.since):
            print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
