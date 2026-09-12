#!/usr/bin/env python3
"""
CH/BS-Legislation -- Kanton Basel-Stadt Gesetzessammlung (SG)

Fetches Basel-Stadt cantonal legislation from the LexWork REST API (Basel-Stadt SG).
Full text is returned as structured JSON — no PDF extraction needed.

Data source: https://www.gesetzessammlung.bs.ch/
License: Public domain (Swiss cantonal legislation)

Strategy:
  - GET /api/de/texts_of_law/lightweight_index for list of all laws
  - GET /api/de/texts_of_law/{systematic_number}/show_as_json for full text
  - Extract text by recursively walking the JSON content tree
"""

import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

API_BASE = "https://www.gesetzessammlung.bs.ch/api/de"
RATE_LIMIT_DELAY = 1.5
CURL_TIMEOUT = 120

SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"
DATA_DIR = SOURCE_DIR / "data"
RECORDS_PATH = DATA_DIR / "records.jsonl"


def fetch_json(url: str, retries: int = 2):
    """Fetch JSON from the API using curl."""
    for attempt in range(retries + 1):
        try:
            result = subprocess.run(
                ["curl", "-sL", "-m", str(CURL_TIMEOUT),
                 "-H", "Accept: application/json",
                 "-w", "\n%{http_code}", url],
                capture_output=True, text=True, timeout=CURL_TIMEOUT + 10
            )
            parts = result.stdout.rsplit("\n", 1)
            if len(parts) == 2:
                body, status = parts[0], parts[1].strip()
            else:
                body, status = result.stdout, "000"

            if not status.startswith("2"):
                if attempt == retries:
                    print("HTTP %s for %s" % (status, url), file=sys.stderr)
                    return None
                time.sleep(3)
                continue
            if body:
                return json.loads(body)
            if attempt == retries:
                return None
            time.sleep(3)
        except Exception as e:
            if attempt == retries:
                print("Failed to fetch %s: %s" % (url, e), file=sys.stderr)
                return None
            time.sleep(3)
    return None


def strip_html(html):
    """Strip HTML tags and decode entities."""
    if not html:
        return ""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
    text = text.replace('&quot;', '"').replace('&#39;', "'").replace('&nbsp;', ' ')
    text = text.replace('&ouml;', 'ö').replace('&uuml;', 'ü').replace('&auml;', 'ä')
    text = text.replace('&Ouml;', 'Ö').replace('&Uuml;', 'Ü').replace('&Auml;', 'Ä')
    text = re.sub(r'&#(\d+);', lambda m: chr(int(m.group(1))), text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def dget(obj, key, default=None):
    """`obj[key]`, treating an explicit JSON `null` like a missing key.

    LexWork emits the key with a `null` value rather than omitting it — e.g.
    `text_of_law_type` is null on ~600 of the 915 laws — so plain
    `.get(key, {})` hands back None and the next `.get()` in the chain raises
    AttributeError (issue #1318).
    """
    if not isinstance(obj, dict):
        return default
    value = obj.get(key)
    return default if value is None else value


def extract_text_from_tree(node):
    """Recursively extract text from the LexWork JSON content tree."""
    if not isinstance(node, dict):
        return ""

    parts = []

    # Get title/heading text
    text_dict = dget(node, "text", {})
    if isinstance(text_dict, dict):
        title_text = dget(text_dict, "de", "")
        if title_text:
            parts.append(title_text)

    # Get paragraph content from html_content
    html_content = dget(node, "html_content", {})
    if isinstance(html_content, dict):
        html = dget(html_content, "de", "")
    else:
        html = ""
    if html:
        cleaned = strip_html(html)
        if cleaned:
            parts.append(cleaned)

    # Recurse into children
    for child in dget(node, "children", []) or []:
        child_text = extract_text_from_tree(child)
        if child_text:
            parts.append(child_text)

    return "\n".join(parts)


def get_law_index():
    """Get the lightweight index of all in-force laws."""
    data = fetch_json("%s/texts_of_law/lightweight_index" % API_BASE)
    if not data:
        return []

    laws = []
    for cat_id, items in data.items():
        for item in items or []:
            if not isinstance(item, dict) or not item.get("systematic_number"):
                continue
            laws.append({
                "id": item.get("id"),
                "systematic_number": item["systematic_number"],
                "title": dget(item, "title", ""),
                "abrogated": bool(item.get("abrogated", False)),
            })
    return laws


def fetch_law_text(systematic_number):
    """Fetch the full text of a law as JSON and extract text."""
    data = fetch_json("%s/texts_of_law/%s/show_as_json" % (API_BASE, systematic_number))
    if not data:
        return None, ""

    tol = dget(data, "text_of_law", {})
    sv = dget(tol, "selected_version", {})
    jc = dget(sv, "json_content", {})
    doc = dget(jc, "document", {})
    content = dget(doc, "content", {})

    text = ""
    if isinstance(content, dict):
        text = extract_text_from_tree(content)
    elif isinstance(content, list):
        parts = [extract_text_from_tree(item) for item in content]
        text = "\n".join(p for p in parts if p)

    # Extract date — prefer enactment (in-force date), fall back to date_of_decision
    date = dget(tol, "enactment", "")
    if not date:
        date = dget(tol, "date_of_decision", "")
    if not date:
        vds = dget(sv, "version_dates_str", "")
        if vds:
            m = re.search(r"in Kraft seit[:\s]*(\d{2})\.(\d{2})\.(\d{4})", vds)
            if m:
                date = "%s-%s-%s" % (m.group(3), m.group(2), m.group(1))

    law_type = dget(dget(tol, "text_of_law_type", {}), "description", "")

    return {
        "date": date,
        "law_type": law_type,
        "abbreviation": dget(tol, "abbreviation", ""),
    }, text


def normalize(law_info, text, meta):
    """Normalize a law record."""
    sn = law_info["systematic_number"]
    return {
        "_id": "CH/BS-Legislation/%s" % sn,
        "_source": "CH/BS-Legislation",
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": law_info["title"],
        "text": text,
        "date": dget(meta, "date", ""),
        "url": "https://www.gesetzessammlung.bs.ch/app/de/texts_of_law/%s" % sn,
        "doc_type": dget(meta, "law_type", "") or "legislation",
        "systematic_number": sn,
        "abbreviation": dget(meta, "abbreviation", ""),
        "jurisdiction": "CH-BS",
    }


def fetch_all(sample=False):
    """Fetch all Basel-Stadt cantonal legislation."""
    laws = get_law_index()
    if not laws:
        print("ERROR: Could not fetch law index", file=sys.stderr)
        return

    active = [l for l in laws if not l.get("abrogated")]
    print("Found %d in-force laws (of %d total)" % (len(active), len(laws)), file=sys.stderr)

    count = 0
    for law_info in active:
        sn = law_info["systematic_number"]
        meta, text = fetch_law_text(sn)

        if not text or len(text) < 50:
            print("  Skipping %s: insufficient text (%d chars)" % (sn, len(text) if text else 0), file=sys.stderr)
            time.sleep(RATE_LIMIT_DELAY)
            continue

        record = normalize(law_info, text, meta)
        yield record
        count += 1

        if count % 25 == 0:
            print("  Fetched %d records..." % count, file=sys.stderr)
        if sample and count >= 15:
            break

        time.sleep(RATE_LIMIT_DELAY)

    print("Total records: %d" % count, file=sys.stderr)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Basel-Stadt cantonal legislation fetcher")
    sub = parser.add_subparsers(dest="command")

    for name in ("bootstrap", "bootstrap-fast"):
        boot = sub.add_parser(name, help="Fetch documents")
        boot.add_argument("--sample", action="store_true", help="Fetch ~15 sample records")
        boot.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command in ("bootstrap", "bootstrap-fast"):
        # A sample run writes the curated sample/ files; a full run streams to
        # data/records.jsonl, which is what the pipeline ingests. The full path
        # used to write into sample/ as well, so a completed crawl was
        # indistinguishable from a sample fallback (the #798 class).
        count = 0
        if args.sample:
            SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
            for record in fetch_all(sample=True):
                (SAMPLE_DIR / ("%04d.json" % count)).write_text(
                    json.dumps(record, ensure_ascii=False, indent=2)
                )
                count += 1
            print("Done. Saved %d records to %s/" % (count, SAMPLE_DIR), file=sys.stderr)
        else:
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            with RECORDS_PATH.open("w", encoding="utf-8") as stream:
                for record in fetch_all(sample=False):
                    stream.write(json.dumps(record, ensure_ascii=False) + "\n")
                    stream.flush()
                    count += 1
            print("%s complete: %d records written to %s"
                  % (args.command, count, RECORDS_PATH), file=sys.stderr)
        sys.exit(0 if count >= (10 if args.sample else 1) else 1)


if __name__ == "__main__":
    main()
