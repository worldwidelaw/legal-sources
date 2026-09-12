#!/usr/bin/env python3
"""
INTL/UNODC-TRACK - UNODC TRACK Anti-Corruption Legal Library.

Fetches anti-corruption legislation from 180+ States parties to UNCAC.
Records are organised by UNCAC article (Chapter II-V) and represent national
law provisions implementing the Convention.

Data source: https://track.unodc.org/track/en/legal-library/index.html
            (backed by https://www.unodc.org/cld/en/trackview/data.json)
Method:      JSON search API for paging + x2j.jspx for per-article full text
License:     United Nations / UNODC (open government data)

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample records
  python bootstrap.py bootstrap            # Full bootstrap
  python bootstrap.py test                 # Test connectivity
"""

import argparse
import hashlib
import json
import re
import sys
import time
import urllib.parse
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

import requests

SOURCE_ID = "INTL/UNODC-TRACK"
SAMPLE_DIR = Path(__file__).parent / "sample"

BASE_URL = "https://www.unodc.org"
SEARCH_ENDPOINT = "/cld/en/trackview/data.json"
DETAIL_ENDPOINT = "/cld/trackview/x2j.jspx"

PAGE_SIZE = 100  # fixed server-side
DELAY = 1.5

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/html, */*",
}


def strip_html(html_text: str) -> str:
    if not html_text:
        return ""
    text = re.sub(r"<br\s*/?>", "\n", html_text)
    text = re.sub(r"</?p[^>]*>", "\n", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


class UpstreamGone(RuntimeError):
    """The CLD backend answered, but with nothing in it.

    www.unodc.org/cld/* redirects to sherloc.unodc.org. On the fleet the
    redirect target returned HTTP 200 with a zero-length body; the old code
    read that as 'no more results' and stopped, so an empty reply was
    indistinguishable from a finished crawl and the run exited with
    'No records saved' and no reason (#1449). Raising makes it loud.

    That empty body is NOT the backend being decommissioned — re-verified
    2026-09-01 from a non-datacenter vantage, where the same redirect serves
    23,103 records and `startAt` paginates correctly (offsets 0, 100 and
    20,000 return disjoint result sets, and a --sample run returns 15
    full-text records). So a zero-length body means THIS VANTAGE is being
    served an empty response: the fix is a different vantage/proxy, NOT
    blocking a live 23K-document corpus.
    """


def fetch_json(url: str, session: requests.Session) -> Optional[dict]:
    for attempt in range(3):
        try:
            resp = session.get(url, headers=HEADERS, timeout=30)
            if resp.status_code == 200 and not resp.content:
                raise UpstreamGone(
                    f"{url} returned HTTP 200 with an empty body "
                    f"(redirected to {resp.url}). The corpus is NOT gone: the "
                    f"same URL served 23,103 records from a non-datacenter "
                    f"vantage on 2026-09-01, so this vantage is being fed an "
                    f"empty response and needs a proxy. Refusing to report "
                    f"this as an empty corpus (#1449)."
                )
            if resp.status_code == 200 and resp.content:
                try:
                    return resp.json()
                except ValueError:
                    return None
            if resp.status_code >= 500:
                print(f"  HTTP {resp.status_code}, retrying...")
                time.sleep(DELAY * 2)
                continue
            return None
        except requests.RequestException as e:
            print(f"  Request error (attempt {attempt + 1}): {e}")
            time.sleep(DELAY)
    return None


def build_search_url(start_at: int) -> str:
    criteria = json.dumps({"startAt": start_at}, separators=(",", ":"))
    return (
        f"{BASE_URL}{SEARCH_ENDPOINT}?lng=en"
        f"&criteria={urllib.parse.quote(criteria)}"
    )


def build_detail_url(uri: str) -> str:
    return f"{BASE_URL}{DETAIL_ENDPOINT}?uri={urllib.parse.quote(uri)}"


def build_human_url(uri: str) -> str:
    return f"{BASE_URL}/cld{uri}?lng=en&tmpl=track"


def extract_text(detail: dict) -> str:
    """Extract the originalText html body, stripped of tags."""
    if not detail:
        return ""
    ot = detail.get("originalText") or {}
    html_items = ot.get("html") or []
    if not isinstance(html_items, list):
        html_items = [html_items]
    parts = []
    for item in html_items:
        if isinstance(item, dict):
            v = item.get("_value")
        else:
            v = item
        if v:
            parts.append(strip_html(str(v)))
    return "\n\n".join(p for p in parts if p)


def extract_uncac_provisions(detail: dict) -> list:
    prov = (detail or {}).get("uncacProvisions") or {}
    items = prov.get("uncacProvision") or []
    if isinstance(items, dict):
        items = [items]
    refs = []
    for it in items:
        ref = it.get("ref") if isinstance(it, dict) else None
        if isinstance(ref, list):
            parts = []
            for r in ref:
                if isinstance(r, dict):
                    v = r.get("_value")
                    if v:
                        parts.append(v)
            if parts:
                refs.append("/".join(parts))
    return refs


def normalize(result: dict, detail: Optional[dict]) -> dict:
    values = result.get("values", {}) or {}
    uri = result.get("uri", "") or ""

    law_title = values.get("legislation.nationalLawArticle@title_s1", "") or ""
    article = values.get("legislation.nationalLawArticle@article_s1", "") or ""
    chapter = values.get("legislation.nationalLawArticle@chapter_s1", "") or ""

    title_parts = [law_title]
    if chapter:
        title_parts.append(chapter)
    if article:
        title_parts.append(article)
    title = " — ".join(p for p in title_parts if p)

    text = extract_text(detail) if detail else ""

    country_label = values.get("en#legislation@country_label_s1", "") or ""
    country_code = values.get("legislation@country_s1", "") or ""

    law_identifier = ""
    if detail:
        law_identifier = (detail.get("@lawIdentifier") or {}).get("_value") or ""

    raw_id = result.get("id") or uri
    digest = hashlib.md5(raw_id.encode("utf-8")).hexdigest()[:12]
    doc_id = law_identifier or digest

    uncac_refs = extract_uncac_provisions(detail) if detail else []

    return {
        "_id": f"TRACK-{doc_id}-{digest}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": None,
        "url": build_human_url(uri) if uri else "",
        "country": country_label,
        "country_code": country_code,
        "law_title": law_title,
        "chapter": chapter,
        "article": article,
        "law_identifier": law_identifier,
        "uncac_provisions": uncac_refs,
    }


def fetch_records(
    session: requests.Session, sample: bool = False
) -> Generator[dict, None, None]:
    start_at = 0
    total = None
    fetched = 0
    skipped = 0
    limit = 15 if sample else None
    seen_ids: set = set()

    while True:
        url = build_search_url(start_at)
        print(f"  Fetching page at offset {start_at}...")
        data = fetch_json(url, session)
        if not data:
            print("  Failed to fetch page, stopping.")
            break

        if total is None:
            total = data.get("found", 0)
            print(f"  Total records reported: {total}")

        results = data.get("results", []) or []
        if not results:
            break

        # Server ignores startAt sometimes — bail out if we see only seen ids
        new_ids = [r.get("id") for r in results if r.get("id") not in seen_ids]
        if not new_ids:
            print("  No new ids on page, stopping (server paging quirk).")
            break

        for result in results:
            rid = result.get("id")
            if rid in seen_ids:
                continue
            seen_ids.add(rid)

            uri = result.get("uri", "")
            if not uri:
                continue

            detail_url = build_detail_url(uri)
            time.sleep(DELAY)
            detail = fetch_json(detail_url, session)

            record = normalize(result, detail)
            if record["text"]:
                yield record
                fetched += 1
                if limit and fetched >= limit:
                    print(f"  Sample: fetched {fetched}, skipped {skipped}")
                    return
            else:
                skipped += 1
                if skipped <= 5:
                    print(f"  No text for: {record['title'][:80]}")

        start_at += len(results)
        if total and start_at >= total:
            break
        time.sleep(DELAY)

    print(f"  Done: fetched {fetched}, skipped {skipped} (no text)")


def save_record(record: dict, sample_dir: Path) -> None:
    sample_dir.mkdir(parents=True, exist_ok=True)
    safe_id = re.sub(r"[^\w\-]", "_", record["_id"])
    if len(safe_id) > 80:
        h = hashlib.md5(record["_id"].encode()).hexdigest()[:8]
        safe_id = safe_id[:70] + "_" + h
    filepath = sample_dir / (safe_id + ".json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False, indent=2)


def test_connectivity() -> bool:
    session = requests.Session()
    print("Testing TRACK Legal Library API...")
    url = build_search_url(0)
    data = fetch_json(url, session)
    if not data:
        print("FAIL: search API unreachable")
        return False
    print(f"  Search API: OK ({data.get('found', 0)} records)")

    results = data.get("results", [])
    if not results:
        print("FAIL: no results returned")
        return False

    sample_uri = results[0].get("uri", "")
    detail = fetch_json(build_detail_url(sample_uri), session)
    if not detail:
        print("FAIL: detail endpoint unreachable")
        return False
    text = extract_text(detail)
    if not text:
        print("WARN: detail returned no originalText")
    else:
        print(f"  Detail API: OK ({len(text)} chars in first article)")
    return True


def bootstrap(sample: bool = False) -> None:
    session = requests.Session()
    sample_dir = SAMPLE_DIR

    print(f"{'Sample' if sample else 'Full'} bootstrap starting...")
    records_saved = 0
    for record in fetch_records(session, sample=sample):
        if sample and records_saved == 0 and sample_dir.exists():
            # Clear the old samples only once a replacement is actually in
            # hand. Clearing up-front meant any failed run (an upstream 403,
            # or the dead-backend case in #1449) destroyed the committed
            # samples — which is exactly what the fleet wrapper falls back to
            # ingesting when a crawl yields nothing.
            for f in sample_dir.glob("*.json"):
                f.unlink()
        save_record(record, sample_dir)
        records_saved += 1
        if records_saved % 100 == 0:
            print(f"  Saved {records_saved} records...")

    print(f"\nBootstrap complete: {records_saved} records saved")
    print(f"  Output: {sample_dir}")

    if records_saved == 0:
        print("ERROR: No records saved!")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="INTL/UNODC-TRACK bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test"])
    parser.add_argument("--sample", action="store_true",
                        help="Fetch only ~15 records")
    parser.add_argument("--full", action="store_true",
                        help="Fetch all records")
    args = parser.parse_args()

    if args.command == "test":
        sys.exit(0 if test_connectivity() else 1)
    bootstrap(sample=args.sample)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
