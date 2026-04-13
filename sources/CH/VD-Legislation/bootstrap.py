#!/usr/bin/env python3
"""
CH/VD-Legislation -- Canton de Vaud Recueil systématique (BLV)

Fetches Vaud cantonal legislation from the BLV publication REST API.
Full text is extracted from HTML endpoints.

Data source: https://prestations.vd.ch/pub/blv-publication/
License: Public domain (Swiss cantonal legislation)

Strategy:
  - GET /api/recueil-systematique?code={vol} for each volume (1-9)
  - Recursively walk tree to find all ACTE nodes
  - GET /api/actes/CONSOLIDE?id={atelierId}&cote={code} for current version
  - GET /api/actes/{htmlId}/html for full text
"""

import html as html_lib
import json
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

API_BASE = "https://prestations.vd.ch/pub/blv-publication/api"
RATE_LIMIT_DELAY = 1.5
CURL_TIMEOUT = 120

SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"


def fetch_json(url, retries=2):
    """Fetch JSON from URL using curl."""
    for attempt in range(retries + 1):
        try:
            result = subprocess.run(
                ["curl", "-sL", "-m", str(CURL_TIMEOUT),
                 "-H", "Accept: application/json",
                 "-w", "\n%{http_code}", url],
                capture_output=True, timeout=CURL_TIMEOUT + 10
            )
            raw = result.stdout
            last_nl = raw.rfind(b"\n")
            if last_nl >= 0:
                body = raw[:last_nl]
                status = raw[last_nl + 1:].decode("ascii", errors="replace").strip()
            else:
                body = raw
                status = "000"

            if not status.startswith("2"):
                if attempt == retries:
                    print("HTTP %s for %s" % (status, url), file=sys.stderr)
                    return None
                time.sleep(3)
                continue

            return json.loads(body)
        except Exception as e:
            if attempt == retries:
                print("Failed to fetch %s: %s" % (url, e), file=sys.stderr)
                return None
            time.sleep(3)
    return None


def fetch_html(url, retries=2):
    """Fetch HTML from URL using curl."""
    for attempt in range(retries + 1):
        try:
            result = subprocess.run(
                ["curl", "-sL", "-m", str(CURL_TIMEOUT),
                 "-H", "Accept: text/html",
                 "-w", "\n%{http_code}", url],
                capture_output=True, timeout=CURL_TIMEOUT + 10
            )
            raw = result.stdout
            last_nl = raw.rfind(b"\n")
            if last_nl >= 0:
                body = raw[:last_nl]
                status = raw[last_nl + 1:].decode("ascii", errors="replace").strip()
            else:
                body = raw
                status = "000"

            if not status.startswith("2"):
                if attempt == retries:
                    print("HTTP %s for %s" % (status, url), file=sys.stderr)
                    return None
                time.sleep(3)
                continue

            return body.decode("utf-8", errors="replace")
        except Exception as e:
            if attempt == retries:
                print("Failed to fetch %s: %s" % (url, e), file=sys.stderr)
                return None
            time.sleep(3)
    return None


def strip_html(raw):
    """Strip HTML tags and decode entities."""
    if not raw:
        return ""
    text = re.sub(r'<script[^>]*>.*?</script>', '', raw, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</(?:p|div|h[1-6]|tr|li)>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = html_lib.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def find_actes(node, actes):
    """Recursively walk the tree to find all ACTE nodes."""
    if node.get("typeElement") == "ACTE":
        actes.append({
            "code": node["code"],
            "atelierId": node.get("atelierId"),
            "libelle": node.get("libelle", ""),
        })
    for child in node.get("children", []):
        find_actes(child, actes)


def get_all_actes():
    """Fetch all actes across all 9 volumes."""
    actes = []
    for vol in range(1, 10):
        url = "%s/recueil-systematique?code=%d" % (API_BASE, vol)
        data = fetch_json(url)
        if not data:
            print("WARNING: Could not fetch volume %d" % vol, file=sys.stderr)
            continue
        for node in data:
            find_actes(node, actes)
        print("Volume %d: %d actes so far" % (vol, len(actes)), file=sys.stderr)
        time.sleep(RATE_LIMIT_DELAY)
    return actes


def get_current_version(acte):
    """Get the current (ACTUELLE) version of an acte."""
    url = "%s/actes/CONSOLIDE?id=%s&cote=%s" % (
        API_BASE, acte["atelierId"], acte["code"])
    versions = fetch_json(url)
    if not versions:
        return None

    # Find the ACTUELLE version
    for v in versions:
        if v.get("versionType") == "ACTUELLE":
            return v

    # Fallback: first version
    if versions:
        return versions[0]
    return None


def get_full_text(html_id):
    """Fetch and extract full text from the HTML endpoint."""
    url = "%s/actes/%s/html" % (API_BASE, html_id)
    raw = fetch_html(url)
    if not raw:
        return ""

    # Extract body content
    m = re.search(r'<body[^>]*>(.*)</body>', raw, re.DOTALL | re.IGNORECASE)
    if m:
        return strip_html(m.group(1))
    return strip_html(raw)


def parse_date(date_str):
    """Parse VD date format (DD.MM.YYYY) to ISO 8601."""
    if not date_str:
        return ""
    m = re.match(r'(\d{2})\.(\d{2})\.(\d{4})', date_str)
    if m:
        return "%s-%s-%s" % (m.group(3), m.group(2), m.group(1))
    return ""


def extract_title_and_abbr(libelle):
    """Extract clean title and abbreviation from libelle field."""
    # libelle format: "TYPE du DD.MM.YYYY Titre (ABBR; BLV code)"
    title = libelle

    # Extract abbreviation from (ABBR; BLV xxx) pattern
    abbr = ""
    m = re.search(r'\(([A-ZÀÂÄÉÈÊËÏÎÔÙÛÜŸÆŒÇ][A-ZÀÂÄÉÈÊËÏÎÔÙÛÜŸÆŒÇa-zàâäéèêëïîôùûüÿæœç/-]+);\s*BLV\s+[\d.]+\)\s*$', title)
    if m:
        abbr = m.group(1)

    return title, abbr


def normalize(acte, version, text):
    """Normalize a law record."""
    title = version.get("titre", "") if version else acte["libelle"]
    date = parse_date(version.get("dateAdoption", "")) if version else ""

    return {
        "_id": "CH/VD-Legislation/%s" % acte["code"],
        "_source": "CH/VD-Legislation",
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date,
        "url": "https://prestations.vd.ch/pub/blv-publication/actes/consolide/%s" % acte["code"],
        "systematic_number": acte["code"],
        "jurisdiction": "CH-VD",
    }


def fetch_all(sample=False):
    """Fetch all Vaud cantonal legislation."""
    actes = get_all_actes()
    if not actes:
        print("ERROR: Could not fetch actes", file=sys.stderr)
        return

    print("Found %d actes total" % len(actes), file=sys.stderr)

    count = 0
    for acte in actes:
        if not acte.get("atelierId"):
            continue

        version = get_current_version(acte)
        time.sleep(RATE_LIMIT_DELAY)

        if not version or not version.get("htmlId"):
            print("  Skipping %s: no current version" % acte["code"], file=sys.stderr)
            continue

        text = get_full_text(version["htmlId"])
        time.sleep(RATE_LIMIT_DELAY)

        if not text or len(text) < 50:
            print("  Skipping %s: insufficient text (%d chars)" % (
                acte["code"], len(text) if text else 0), file=sys.stderr)
            continue

        record = normalize(acte, version, text)
        yield record
        count += 1

        if count % 25 == 0:
            print("  Fetched %d records..." % count, file=sys.stderr)
        if sample and count >= 15:
            break

    print("Total records: %d" % count, file=sys.stderr)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Vaud cantonal legislation fetcher")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Fetch documents")
    boot.add_argument("--sample", action="store_true", help="Fetch ~15 sample records")
    boot.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "bootstrap":
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        count = 0
        for record in fetch_all(sample=args.sample):
            out_path = SAMPLE_DIR / ("%04d.json" % count)
            out_path.write_text(json.dumps(record, ensure_ascii=False, indent=2))
            count += 1
        print("Done. Saved %d records to %s/" % (count, SAMPLE_DIR), file=sys.stderr)


if __name__ == "__main__":
    main()
