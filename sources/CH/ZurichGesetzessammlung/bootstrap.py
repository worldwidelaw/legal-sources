#!/usr/bin/env python3
"""
CH/ZurichGesetzessammlung - Zurich Cantonal Law Collection (1803-1998)
Fetches TEI-XML legislation from Zenodo bulk download.
"""

import argparse
import io
import json
import os
import re
import sys
import time
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Generator, Optional
from xml.etree import ElementTree as ET

import requests

# Zenodo record for the dataset
ZENODO_RECORD_ID = "13347459"
ZENODO_API_URL = f"https://zenodo.org/api/records/{ZENODO_RECORD_ID}"
ZIP_FILENAME = "STAZH_OGD_eOSZH_V4_NER.zip"
DOWNLOAD_URL = f"https://zenodo.org/api/records/{ZENODO_RECORD_ID}/files/{ZIP_FILENAME}/content"

# TEI namespace
TEI_NS = "http://www.tei-c.org/ns/1.0"
NS = {"tei": TEI_NS}

SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"


def download_zip() -> zipfile.ZipFile:
    """Download the ZIP archive into memory."""
    print(f"Downloading {ZIP_FILENAME} from Zenodo...", file=sys.stderr)
    resp = requests.get(DOWNLOAD_URL, stream=True, timeout=300,
                        headers={"User-Agent": "LegalDataHunter/1.0 (research project)"})
    resp.raise_for_status()
    total = int(resp.headers.get("content-length", 0))
    buf = io.BytesIO()
    downloaded = 0
    for chunk in resp.iter_content(chunk_size=1024 * 1024):
        buf.write(chunk)
        downloaded += len(chunk)
        if total:
            pct = downloaded * 100 // total
            print(f"\r  {downloaded // (1024*1024)} / {total // (1024*1024)} MB ({pct}%)", end="", file=sys.stderr)
    print(file=sys.stderr)
    buf.seek(0)
    return zipfile.ZipFile(buf)


def extract_text(elem) -> str:
    """Recursively extract text from a TEI element, stripping NER tags."""
    parts = []
    if elem.text:
        parts.append(elem.text)
    for child in elem:
        parts.append(extract_text(child))
        if child.tail:
            parts.append(child.tail)
    return "".join(parts)


def parse_tei_xml(content: bytes, filename: str) -> Optional[dict]:
    """Parse a TEI-XML file and extract metadata + full text."""
    try:
        root = ET.fromstring(content)
    except ET.ParseError as e:
        print(f"  XML parse error in {filename}: {e}", file=sys.stderr)
        return None

    header = root.find(f"{{{TEI_NS}}}teiHeader")
    if header is None:
        return None

    # Title
    title_elem = header.find(f".//{{{TEI_NS}}}title")
    title = title_elem.text.strip() if title_elem is not None and title_elem.text else ""

    # Date
    date_str = None
    date_elem = header.find(f".//{{{TEI_NS}}}sourceDesc//{{{TEI_NS}}}date")
    if date_elem is not None:
        date_str = date_elem.get("when") or date_elem.get("notBefore") or ""
        if not date_str and date_elem.text:
            date_str = date_elem.text.strip()

    # Signature / identifier
    signature = ""
    ident_elem = header.find(f".//{{{TEI_NS}}}sourceDesc//{{{TEI_NS}}}ident")
    if ident_elem is not None and ident_elem.text:
        signature = ident_elem.text.strip()

    # Reference URL
    url = ""
    ref_elem = header.find(f".//{{{TEI_NS}}}sourceDesc//{{{TEI_NS}}}ref")
    if ref_elem is not None:
        url = ref_elem.get("target", "")

    # Classification number (idno)
    idno = ""
    idno_elem = header.find(f".//{{{TEI_NS}}}sourceDesc//{{{TEI_NS}}}idno")
    if idno_elem is not None and idno_elem.text:
        idno = idno_elem.text.strip()

    # Full text from body
    body = root.find(f".//{{{TEI_NS}}}body")
    if body is None:
        body = root.find(f".//{{{TEI_NS}}}text/{{{TEI_NS}}}body")

    text = ""
    if body is not None:
        text = extract_text(body).strip()
        # Clean up excessive whitespace
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r" {2,}", " ", text)

    if not text:
        return None

    return {
        "title": title,
        "date": date_str,
        "signature": signature,
        "idno": idno,
        "url": url,
        "text": text,
        "filename": filename,
    }


def normalize(raw: dict) -> dict:
    """Normalize a parsed TEI record into standard schema."""
    doc_id = raw.get("signature") or raw.get("filename", "unknown")
    doc_id = re.sub(r"[^a-zA-Z0-9_.-]", "_", doc_id)

    return {
        "_id": doc_id,
        "_source": "CH/ZurichGesetzessammlung",
        "_type": "legislation",
        "_fetched_at": datetime.utcnow().isoformat() + "Z",
        "title": raw.get("title", ""),
        "text": raw.get("text", ""),
        "date": raw.get("date"),
        "url": raw.get("url", ""),
        "signature": raw.get("signature", ""),
        "idno": raw.get("idno", ""),
        "filename": raw.get("filename", ""),
    }


def fetch_all(sample: bool = False) -> Generator[dict, None, None]:
    """Fetch all documents from the ZIP archive."""
    zf = download_zip()
    count = 0
    xml_files = sorted([n for n in zf.namelist() if n.endswith(".xml")])
    print(f"Found {len(xml_files)} XML files in archive", file=sys.stderr)

    for name in xml_files:
        try:
            content = zf.read(name)
        except Exception as e:
            print(f"  Error reading {name}: {e}", file=sys.stderr)
            continue

        raw = parse_tei_xml(content, os.path.basename(name))
        if raw is None:
            continue

        record = normalize(raw)
        yield record
        count += 1

        if sample and count >= 20:
            break

    print(f"Total records yielded: {count}", file=sys.stderr)


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """No incremental updates available - this is a static dataset."""
    print("Dataset is a static Zenodo archive; use fetch_all for updates.", file=sys.stderr)
    yield from fetch_all()


def main():
    parser = argparse.ArgumentParser(description="CH/ZurichGesetzessammlung bootstrap")
    parser.add_argument("action", choices=["bootstrap", "update"],
                        help="Action to perform")
    parser.add_argument("--sample", action="store_true",
                        help="Only fetch a sample of records")
    parser.add_argument("--since", type=str, default=None,
                        help="Fetch updates since date (ISO 8601)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    SAMPLE_DIR.mkdir(exist_ok=True)

    if args.action == "bootstrap":
        count = 0
        for record in fetch_all(sample=args.sample):
            outpath = SAMPLE_DIR / f"{record['_id'][:80]}.json"
            with open(outpath, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            if count % 100 == 0:
                print(f"  Saved {count} records...", file=sys.stderr)
        print(f"Done. Saved {count} records to {SAMPLE_DIR}", file=sys.stderr)

    elif args.action == "update":
        since = args.since or "2020-01-01"
        count = 0
        for record in fetch_updates(since):
            outpath = SAMPLE_DIR / f"{record['_id'][:80]}.json"
            with open(outpath, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        print(f"Done. Saved {count} records to {SAMPLE_DIR}", file=sys.stderr)


if __name__ == "__main__":
    main()
