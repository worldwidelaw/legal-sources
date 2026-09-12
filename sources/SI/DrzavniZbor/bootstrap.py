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
RATE_LIMIT = 1  # seconds between transcript fetches

# SDZ XML files, oldest first. The mandate is NOT implied by the filename —
# it is read per session from KARTICA_MANDAT (SDZ.XML is the current mandate,
# which is 10, not 9; SDZ9.XML holds mandate 9 and used to be missing here).
SDZ_FILES = [
    "SDZ2.XML",
    "SDZ3.XML",
    "SDZ4.XML",
    "SDZ5.XML",
    "SDZ6.XML",
    "SDZ7.XML",
    "SDZ8.XML",
    "SDZ9.XML",
    "SDZ.XML",
]

# Mandate number -> Roman numeral for URL
MANDATE_ROMAN = {
    1: "I", 2: "II", 3: "III", 4: "IV", 5: "V",
    6: "VI", 7: "VII", 8: "VIII", 9: "IX", 10: "X",
}

# The UNID prefix in DOBESEDNI_ZAPISI_SEJE selects the `type` URL parameter.
# SZA (sejni zapis) -> type=sz, MDZ (magnetogram) -> type=mag. Serving the
# wrong one returns a 200 page reading "Podatki dokumenta so nedostopni".
DOC_TYPE_BY_PREFIX = {
    "SZA": "sz",
    "SZX": "sz",
    "MDZ": "mag",
}
DOC_TYPE_FALLBACKS = ["sz", "mag"]

UNAVAILABLE_MARKER = "Podatki dokumenta so nedostopni"

SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"
DATA_DIR = SOURCE_DIR / "data"

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; +https://github.com/legal-data-hunter)"
})

MONTH_MAP = {
    "januar": 1, "februar": 2, "marec": 3, "april": 4,
    "maj": 5, "junij": 6, "julij": 7, "avgust": 8,
    "september": 9, "oktober": 10, "november": 11, "december": 12,
}


def fetch_sdz_xml(filename):
    """Download and parse an SDZ XML file."""
    url = METADATA_BASE + filename
    print(f"  Fetching {url}...")
    response = session.get(url, timeout=120)
    response.raise_for_status()
    return ET.fromstring(response.content)


def parse_sessions(root):
    """Parse SEJA elements into one entry per verbatim transcript document."""
    sessions = []
    for seja in root.findall("SEJA"):
        kartica = seja.find("KARTICA_SEJE")
        if kartica is None:
            continue

        unid = kartica.findtext("UNID", "").strip()
        oznaka = kartica.findtext("KARTICA_OZNAKA", "").strip()
        vrsta = kartica.findtext("KARTICA_VRSTA", "").strip()
        mandate_raw = kartica.findtext("KARTICA_MANDAT", "").strip()
        try:
            mandate = int(mandate_raw)
        except ValueError:
            continue

        transcripts = []
        for dz in seja.findall("DOBESEDNI_ZAPISI_SEJE"):
            for uid_el in dz.findall("UNID"):
                raw = uid_el.text.strip() if uid_el.text else ""
                if "|" in raw:
                    prefix, uid = raw.split("|", 1)
                else:
                    prefix, uid = "", raw
                if uid:
                    transcripts.append({"prefix": prefix, "uid": uid})

        if not transcripts:
            continue

        sessions.append({
            "session_number": oznaka.lstrip("0") or "0",
            "mandate": mandate,
            "session_type": vrsta,
            "session_unid": unid.split("|", 1)[1] if "|" in unid else unid,
            "transcripts": transcripts,
        })

    return sessions


def _parse_date(lines):
    """Extract the session date from the first lines of a transcript page."""
    for line in lines[:10]:
        m = re.search(r"(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})", line)
        if m:
            day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if 1 <= month <= 12 and 1 <= day <= 31:
                return f"{year:04d}-{month:02d}-{day:02d}"
        m = re.search(r"\((\d{1,2})\.\s+(\w+)\s+(\d{4})\)", line)
        if m:
            month = MONTH_MAP.get(m.group(2).lower(), 0)
            if month:
                return f"{int(m.group(3)):04d}-{month:02d}-{int(m.group(1)):02d}"
    return None


def _fetch_transcript_page(uid, mandate, doc_type):
    """Fetch one transcript rendering; returns (text, date, title) or Nones."""
    roman = MANDATE_ROMAN.get(mandate)
    if not roman:
        return None, None, None
    url = f"{TRANSCRIPT_BASE}?mandat={roman}&type={doc_type}&uid={uid}"

    try:
        response = session.get(url, timeout=60)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"    Error fetching transcript {uid} (type={doc_type}): {e}")
        return None, None, None

    soup = BeautifulSoup(response.text, "html.parser")

    body = soup.find(attrs={"class": lambda x: x and "wpthemeControlBody" in str(x)})
    if not body:
        return None, None, None

    text = body.get_text(separator="\n", strip=True)
    if UNAVAILABLE_MARKER in text:
        return None, None, None

    lines = text.split("\n")
    date_str = _parse_date(lines)

    # Transcript body starts at the letterhead line when present.
    transcript_start = 0
    for i, line in enumerate(lines):
        if "REPUBLIKA SLOVENIJA" in line:
            transcript_start = i
            break

    transcript_text = "\n".join(lines[transcript_start:]).strip()

    title_parts = []
    for line in lines[transcript_start:transcript_start + 4]:
        line = line.strip()
        if line:
            title_parts.append(line)
        if "seja" in line.lower():
            break
    title = " - ".join(title_parts[:3]) if title_parts else None

    return transcript_text, date_str, title


def fetch_transcript(uid, mandate, prefix=""):
    """Fetch a transcript, picking the `type` parameter from the UNID prefix."""
    preferred = DOC_TYPE_BY_PREFIX.get(prefix.upper())
    order = [preferred] if preferred else []
    order += [t for t in DOC_TYPE_FALLBACKS if t != preferred]

    for doc_type in order:
        text, date_str, title = _fetch_transcript_page(uid, mandate, doc_type)
        if text and len(text) >= 100:
            return text, date_str, title
    return None, None, None


def normalize(raw, transcript, transcript_text, date_str, title):
    """Normalize a session transcript record into standard schema."""
    mandate = raw["mandate"]
    session_num = raw["session_number"]
    session_type = raw["session_type"]

    if not title:
        type_label = "redna" if session_type == "Redna" else "izredna"
        title = f"{session_num}. {type_label} seja Državnega zbora ({mandate}. mandat)"
    if date_str:
        title = f"{title} ({date_str})"

    source_url = (
        f"{TRANSCRIPT_BASE}?mandat={MANDATE_ROMAN.get(mandate, 'X')}"
        f"&type={DOC_TYPE_BY_PREFIX.get(transcript['prefix'].upper(), 'sz')}"
        f"&uid={transcript['uid']}"
    )

    # A session spans several sitting days, each with its own verbatim record,
    # so the transcript UID — not the session number — is what is unique.
    return {
        "_id": f"SI-DZ-{transcript['uid']}",
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
        "session_url": (
            "https://www.dz-rs.si/wps/portal/Home/seje/izbranaSeja"
            f"?uid={raw['session_unid']}&mandat={MANDATE_ROMAN.get(mandate, 'X')}"
        ),
    }


def _iter_transcripts(files):
    """Yield (session, transcript) pairs across the given SDZ XML files."""
    for filename in files:
        print(f"Processing {filename}...")
        try:
            root = fetch_sdz_xml(filename)
        except (requests.RequestException, ET.ParseError) as e:
            print(f"  Error reading {filename}: {e}")
            continue
        sessions = parse_sessions(root)
        total = sum(len(s["transcripts"]) for s in sessions)
        print(f"  Found {len(sessions)} sessions / {total} transcripts")
        for sess in sessions:
            for transcript in sess["transcripts"]:
                yield sess, transcript


def fetch_all():
    """Yield all session transcripts across all mandates."""
    seen = set()
    for sess, transcript in _iter_transcripts(SDZ_FILES):
        uid = transcript["uid"]
        if uid in seen:
            continue
        seen.add(uid)

        text, date_str, title = fetch_transcript(uid, sess["mandate"], transcript["prefix"])
        if not text or len(text) < 100:
            print(f"    Skipping {uid} - no text")
            continue

        yield normalize(sess, transcript, text, date_str, title)
        time.sleep(RATE_LIMIT)


def fetch_updates(since):
    """Yield transcripts of the current mandate dated on/after `since`."""
    if isinstance(since, datetime):
        since = since.date().isoformat()
    since = str(since)[:10]

    for sess, transcript in _iter_transcripts(["SDZ.XML"]):
        text, date_str, title = fetch_transcript(
            transcript["uid"], sess["mandate"], transcript["prefix"]
        )
        if not text or len(text) < 100:
            continue
        if date_str and date_str < since:
            continue
        yield normalize(sess, transcript, text, date_str, title)
        time.sleep(RATE_LIMIT)


def bootstrap_sample():
    """Fetch a sample of 15 records from the most recent mandates."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    print("Fetching sample records from the most recent mandates...")
    pairs = list(_iter_transcripts(["SDZ.XML", "SDZ9.XML"]))
    pairs.reverse()

    records = []
    for sess, transcript in pairs:
        if len(records) >= 15:
            break
        uid = transcript["uid"]
        print(f"  Fetching mandate {sess['mandate']} session {sess['session_number']} "
              f"({sess['session_type']}) transcript {uid}...")
        text, date_str, title = fetch_transcript(uid, sess["mandate"], transcript["prefix"])

        if not text or len(text) < 100:
            print(f"    Skipping - insufficient text ({len(text) if text else 0} chars)")
            continue

        records.append(normalize(sess, transcript, text, date_str, title))
        print(f"    OK: {len(text):,} chars, date={date_str}")
        time.sleep(RATE_LIMIT)

    for i, record in enumerate(records):
        path = SAMPLE_DIR / f"record_{i:04d}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    with open(SAMPLE_DIR / "all_samples.json", "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"\nSaved {len(records)} sample records to {SAMPLE_DIR}")
    return records


def bootstrap_full():
    """Stream all transcripts to data/records.jsonl for pipeline ingest."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path = DATA_DIR / "records.jsonl"
    count = 0
    with open(out_path, "w", encoding="utf-8") as f:
        for record in fetch_all():
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            f.flush()
            count += 1
            print(f"  [{count}] {record['title'][:80]}")
    print(f"\nTotal: {count} records written to {out_path}")


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
                print(f"  Distinct _id: {len({r['_id'] for r in records})}")
                print(f"  Min text length: {min(len(r['text']) for r in records):,}")
                print(f"  Max text length: {max(len(r['text']) for r in records):,}")
        else:
            bootstrap_full()


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
