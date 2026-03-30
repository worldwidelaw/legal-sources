#!/usr/bin/env python3
"""
SI/DrzavniZbor Bootstrap
Slovenian National Assembly Verbatim Session Transcripts

Fetches plenary session transcripts from the Slovenian Parliament.

Data sources:
- Metadata: Parliament open data XML (fotogalerija.dz-rs.si/datoteke/opendata/)
- Full text: Parliament website transcript pages

Usage:
    python bootstrap.py bootstrap --sample   # Fetch 15 sample records
    python bootstrap.py bootstrap --full     # Fetch all records
"""

import argparse
import json
import os
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# Configuration
METADATA_BASE = "https://fotogalerija.dz-rs.si/datoteke/opendata/"
TRANSCRIPT_BASE = "https://www.dz-rs.si/wps/portal/Home/seje/evidenca"
RATE_LIMIT = 2  # seconds between transcript fetches

# SDZ XML files: filename -> mandate number
SDZ_FILES = {
    "SDZ2.XML": 2,
    "SDZ3.XML": 3,
    "SDZ4.XML": 4,
    "SDZ5.XML": 5,
    "SDZ6.XML": 6,
    "SDZ7.XML": 7,
    "SDZ8.XML": 8,
    "SDZ.XML": 9,
}

# Mandate number -> Roman numeral for URL
MANDATE_ROMAN = {
    1: "I", 2: "II", 3: "III", 4: "IV", 5: "V",
    6: "VI", 7: "VII", 8: "VIII", 9: "IX",
}

SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://github.com/legal-data-hunter)"
})


def fetch_sdz_xml(filename):
    """Download and parse an SDZ XML file."""
    url = METADATA_BASE + filename
    print(f"  Fetching {url}...")
    response = session.get(url, timeout=120)
    response.raise_for_status()
    return ET.fromstring(response.content)


def parse_sessions(root, mandate):
    """Parse SEJA elements from an SDZ XML file."""
    sessions = []
    for seja in root.findall("SEJA"):
        kartica = seja.find("KARTICA_SEJE")
        if kartica is None:
            continue

        unid = kartica.findtext("UNID", "").strip()
        oznaka = kartica.findtext("KARTICA_OZNAKA", "").strip()
        vrsta = kartica.findtext("KARTICA_VRSTA", "").strip()

        # Collect transcript UNIDs
        transcript_uids = []
        for dz in seja.findall("DOBESEDNI_ZAPISI_SEJE"):
            for uid_el in dz.findall("UNID"):
                raw = uid_el.text.strip() if uid_el.text else ""
                # Extract UID after the pipe separator (e.g., "SZA|ABC123..." -> "ABC123...")
                if "|" in raw:
                    transcript_uids.append(raw.split("|", 1)[1])
                elif raw:
                    transcript_uids.append(raw)

        if not transcript_uids:
            continue

        sessions.append({
            "session_number": oznaka.lstrip("0") or "0",
            "mandate": mandate,
            "session_type": vrsta,
            "session_unid": unid.split("|", 1)[1] if "|" in unid else unid,
            "transcript_uids": transcript_uids,
        })

    return sessions


def fetch_transcript(uid, mandate):
    """Fetch the full text of a session transcript."""
    roman = MANDATE_ROMAN.get(mandate, "IX")
    url = f"{TRANSCRIPT_BASE}?mandat={roman}&type=mag&uid={uid}"

    try:
        response = session.get(url, timeout=60)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"    Error fetching transcript {uid}: {e}")
        return None, None, None

    soup = BeautifulSoup(response.text, "html.parser")

    # Find the main content area
    body = soup.find(attrs={"class": lambda x: x and "wpthemeControlBody" in str(x)})
    if not body:
        print(f"    No content body found for {uid}")
        return None, None, None

    text = body.get_text(separator="\n", strip=True)
    lines = text.split("\n")

    # Extract session date from header (e.g., "26. 2. 2026" or "(26. februar 2026)")
    date_str = None
    for line in lines[:10]:
        # Try "DD. M. YYYY" format
        m = re.search(r"(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})", line)
        if m:
            day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
            date_str = f"{year:04d}-{month:02d}-{day:02d}"
            break
        # Try "(DD. mesec YYYY)" format
        m = re.search(r"\((\d{1,2})\.\s+(\w+)\s+(\d{4})\)", line)
        if m:
            day = int(m.group(1))
            month_name = m.group(2).lower()
            year = int(m.group(3))
            month_map = {
                "januar": 1, "februar": 2, "marec": 3, "april": 4,
                "maj": 5, "junij": 6, "julij": 7, "avgust": 8,
                "september": 9, "oktober": 10, "november": 11, "december": 12,
            }
            month = month_map.get(month_name, 0)
            if month:
                date_str = f"{year:04d}-{month:02d}-{day:02d}"
                break

    # Extract transcript text starting from "REPUBLIKA SLOVENIJA"
    transcript_start = 0
    for i, line in enumerate(lines):
        if "REPUBLIKA SLOVENIJA" in line:
            transcript_start = i
            break

    transcript_text = "\n".join(lines[transcript_start:]).strip()

    # Extract session title from first few lines
    title_parts = []
    for line in lines[transcript_start:transcript_start + 4]:
        line = line.strip()
        if line:
            title_parts.append(line)
        if "seja" in line.lower():
            break
    title = " - ".join(title_parts[:3]) if title_parts else None

    return transcript_text, date_str, title


def normalize(raw, transcript_text, date_str, title):
    """Normalize a session record into standard schema."""
    mandate = raw["mandate"]
    session_num = raw["session_number"]
    session_type = raw["session_type"]

    if not title:
        type_label = "redna" if session_type == "Redna" else "izredna"
        title = f"{session_num}. {type_label} seja Državnega zbora ({mandate}. mandat)"

    source_url = f"https://www.dz-rs.si/wps/portal/Home/seje/izbranaSeja?uid={raw['session_unid']}&mandat={MANDATE_ROMAN.get(mandate, 'IX')}"

    return {
        "_id": f"SI-DZ-M{mandate}-S{session_num}-{session_type[:3]}",
        "_source": "SI/DrzavniZbor",
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": transcript_text,
        "date": date_str,
        "url": source_url,
        "session_number": session_num,
        "mandate": mandate,
        "session_type": session_type,
    }


def fetch_all():
    """Yield all session transcripts across all mandates."""
    for filename, mandate in sorted(SDZ_FILES.items(), key=lambda x: x[1]):
        print(f"Processing {filename} (mandate {mandate})...")
        root = fetch_sdz_xml(filename)
        sessions = parse_sessions(root, mandate)
        print(f"  Found {len(sessions)} sessions with transcripts")

        for sess in sessions:
            for uid in sess["transcript_uids"]:
                print(f"  Fetching transcript {uid}...")
                transcript_text, date_str, title = fetch_transcript(uid, mandate)
                if not transcript_text or len(transcript_text) < 100:
                    print(f"    Skipping - no text or too short")
                    continue

                record = normalize(sess, transcript_text, date_str, title)
                yield record
                time.sleep(RATE_LIMIT)


def fetch_updates(since):
    """Yield transcripts modified since a given date (current mandate only)."""
    root = fetch_sdz_xml("SDZ.XML")
    sessions = parse_sessions(root, 9)

    for sess in sessions:
        for uid in sess["transcript_uids"]:
            transcript_text, date_str, title = fetch_transcript(uid, 9)
            if not transcript_text or len(transcript_text) < 100:
                continue

            if date_str and date_str >= since:
                record = normalize(sess, transcript_text, date_str, title)
                yield record
                time.sleep(RATE_LIMIT)


def bootstrap_sample():
    """Fetch a sample of 15 records from the current mandate."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print("Fetching sample records from current mandate (9)...")
    root = fetch_sdz_xml("SDZ.XML")
    sessions = parse_sessions(root, 9)
    print(f"Found {len(sessions)} sessions with transcripts")

    # Try recent sessions first, go further back if needed
    candidates = list(reversed(sessions))
    records = []

    for sess in candidates:
        if len(records) >= 15:
            break
        # Take only the first transcript UID per session
        uid = sess["transcript_uids"][0]
        print(f"  Fetching session {sess['session_number']} ({sess['session_type']}) transcript {uid}...")
        transcript_text, date_str, title = fetch_transcript(uid, 9)

        if not transcript_text or len(transcript_text) < 100:
            print(f"    Skipping - insufficient text ({len(transcript_text) if transcript_text else 0} chars)")
            continue

        record = normalize(sess, transcript_text, date_str, title)
        records.append(record)
        print(f"    OK: {len(transcript_text):,} chars, date={date_str}")
        time.sleep(RATE_LIMIT)

    # Save individual records
    for i, record in enumerate(records):
        path = SAMPLE_DIR / f"record_{i:04d}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    # Save aggregated sample
    with open(SAMPLE_DIR / "all_samples.json", "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"\nSaved {len(records)} sample records to {SAMPLE_DIR}")
    return records


def bootstrap_full():
    """Fetch all transcripts across all mandates."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    count = 0
    for record in fetch_all():
        path = SAMPLE_DIR / f"record_{count:04d}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        count += 1
        print(f"  Saved record {count}: {record['title'][:80]}")
    print(f"\nTotal: {count} records saved")


def main():
    parser = argparse.ArgumentParser(description="SI/DrzavniZbor Bootstrap")
    parser.add_argument("command", choices=["bootstrap"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample records only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    if args.command == "bootstrap":
        if args.sample:
            records = bootstrap_sample()
            if records:
                print(f"\nValidation:")
                print(f"  Records: {len(records)}")
                print(f"  All have text: {all(r.get('text') for r in records)}")
                print(f"  Min text length: {min(len(r['text']) for r in records):,}")
                print(f"  Max text length: {max(len(r['text']) for r in records):,}")
        elif args.full:
            bootstrap_full()
        else:
            print("Specify --sample or --full")
            sys.exit(1)


if __name__ == "__main__":
    main()
