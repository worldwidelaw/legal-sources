#!/usr/bin/env python3
"""
Service-Public.fr - Fiches pratiques Data Fetcher

Fetches practical guides (administrative doctrine) from Service-Public.fr.
Covers three audiences: Particuliers (individuals), Entreprises (businesses),
and Associations (included in Particuliers dataset).

Data sources:
- Particuliers + Associations: https://lecomarquage.service-public.gouv.fr/vdd/3.4/part/zip/vosdroits-latest.zip
- Entreprises: https://lecomarquage.service-public.gouv.fr/vdd/3.4/pro/zip/vosdroits-latest.zip

License: Licence Ouverte v2.0 (Etalab)
"""

import io
import json
import re
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional
from xml.etree import ElementTree as ET

import requests

# Constants
PARTICULIERS_ZIP = "https://lecomarquage.service-public.gouv.fr/vdd/3.4/part/zip/vosdroits-latest.zip"
ENTREPRISES_ZIP = "https://lecomarquage.service-public.gouv.fr/vdd/3.4/pro/zip/vosdroits-latest.zip"
REQUEST_TIMEOUT = 120


def download_zip(url: str) -> zipfile.ZipFile:
    """Download a ZIP file and return a ZipFile object."""
    print(f"Downloading {url}...", file=sys.stderr)
    response = requests.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return zipfile.ZipFile(io.BytesIO(response.content))


def extract_text_from_element(elem: ET.Element) -> str:
    """
    Recursively extract text from XML element, handling mixed content
    and Service-Public.fr specific tags.
    """
    text_parts = []

    if elem.text:
        text_parts.append(elem.text.strip())

    for child in elem:
        # Handle specific tag types
        tag = child.tag

        if tag in ("Paragraphe", "Texte", "Item", "Cellule"):
            # Content elements
            child_text = extract_text_from_element(child)
            if child_text:
                text_parts.append(child_text)
        elif tag == "Titre":
            # Titles get emphasized
            child_text = extract_text_from_element(child)
            if child_text:
                text_parts.append(f"\n## {child_text}\n")
        elif tag in ("Liste", "Tableau"):
            # Lists and tables
            child_text = extract_text_from_element(child)
            if child_text:
                text_parts.append(child_text)
        elif tag == "Rangee":
            # Table rows
            child_text = extract_text_from_element(child)
            if child_text:
                text_parts.append(child_text + "\n")
        elif tag in ("LienInterne", "LienIntra", "LienExterne"):
            # Links - extract text content
            child_text = extract_text_from_element(child)
            if child_text:
                text_parts.append(child_text)
        elif tag in ("MiseEnEvidence", "Expression", "Exposant"):
            # Emphasized text
            child_text = extract_text_from_element(child)
            if child_text:
                text_parts.append(child_text)
        elif tag in ("ASavoir", "Attention", "ANoter", "Rappel"):
            # Note boxes
            child_text = extract_text_from_element(child)
            if child_text:
                text_parts.append(f"\n{child_text}\n")
        elif tag == "SousDossier":
            # Sub-dossiers
            child_text = extract_text_from_element(child)
            if child_text:
                text_parts.append(child_text)
        elif tag == "Fiche":
            # Fiche references within dossiers
            child_text = extract_text_from_element(child)
            if child_text:
                text_parts.append(f"- {child_text}\n")
        else:
            # Default: extract any text content
            child_text = extract_text_from_element(child)
            if child_text:
                text_parts.append(child_text)

        if child.tail:
            text_parts.append(child.tail.strip())

    # Join and clean up
    result = " ".join(text_parts)
    # Clean up multiple spaces and newlines
    result = re.sub(r'\s+', ' ', result)
    result = re.sub(r'\n\s+\n', '\n\n', result)
    return result.strip()


def parse_xml_file(xml_content: bytes) -> Optional[dict]:
    """
    Parse a Service-Public.fr XML file and extract relevant content.
    Returns None if the file should be skipped.
    """
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        print(f"XML parse error: {e}", file=sys.stderr)
        return None

    # Get document ID and type
    doc_id = root.get("ID", "")
    doc_type = root.get("type", "")
    sp_url = root.get("spUrl", "")

    # Skip certain document types that don't have meaningful content
    # We want Fiches (F*), Dossiers (N* with content), Question-Reponse, CommentFaireSi
    # Skip: R (resources/references only - Teleservices, Formulaires, etc. have minimal text)
    if not doc_id:
        return None

    # R* files are resource references (teleservices, forms, etc.) with minimal text
    # Focus on F* (fiches) which have substantive doctrinal content
    if doc_id.startswith("R"):
        return None

    # Extract Dublin Core metadata
    ns = {"dc": "http://purl.org/dc/elements/1.1/"}

    title = root.findtext("dc:title", "", ns)
    description = root.findtext("dc:description", "", ns)
    subject = root.findtext("dc:subject", "", ns)
    date_modified = root.findtext("dc:date", "", ns)
    coverage = root.findtext("dc:coverage", "", ns)

    # Parse date (format: "modified YYYY-MM-DD")
    date = None
    if date_modified:
        match = re.search(r'(\d{4}-\d{2}-\d{2})', date_modified)
        if match:
            date = match.group(1)

    # Get audience
    audience = root.findtext("Audience", "")

    # Get theme from hierarchy
    theme_elem = root.find("Theme")
    theme = theme_elem.findtext("Titre", "") if theme_elem is not None else ""

    # Extract main text content
    text_parts = []

    # Introduction
    intro = root.find("Introduction")
    if intro is not None:
        intro_text = extract_text_from_element(intro)
        if intro_text:
            text_parts.append(intro_text)

    # Main Texte elements
    for texte in root.findall(".//Texte"):
        texte_content = extract_text_from_element(texte)
        if texte_content:
            text_parts.append(texte_content)

    # DossierPere (parent dossier) - extract structure info
    dossier_pere = root.find("DossierPere")
    if dossier_pere is not None:
        dossier_text = extract_text_from_element(dossier_pere)
        if dossier_text and len(dossier_text) > 20:
            text_parts.append(f"\nContenu du dossier:\n{dossier_text}")

    # Combine text
    full_text = "\n\n".join(text_parts)

    # If no text content, try description as fallback for some doc types
    if not full_text.strip() and description:
        full_text = description

    # Skip documents with minimal/no text
    # F* fiches should have substantial content; N* dossiers are often just indexes
    min_text_length = 500 if doc_id.startswith("N") else 100
    if len(full_text.strip()) < min_text_length:
        return None

    # Build URL
    url = sp_url if sp_url else f"https://www.service-public.fr/particuliers/vosdroits/{doc_id}"

    return {
        "doc_id": doc_id,
        "title": title,
        "description": description,
        "text": full_text.strip(),
        "date": date,
        "url": url,
        "audience": audience,
        "theme": theme,
        "subject": subject,
        "doc_type": doc_type,
        "coverage": coverage
    }


def fetch_all(max_docs: Optional[int] = None, audiences: list = None) -> Generator[dict, None, None]:
    """
    Fetch all practical guides from Service-Public.fr.

    Args:
        max_docs: Maximum number of documents to fetch (None for all)
        audiences: List of audiences to fetch ("particuliers", "entreprises")
                   Default: both
    """
    if audiences is None:
        audiences = ["particuliers", "entreprises"]

    doc_count = 0
    seen_ids = set()  # Track seen IDs to avoid duplicates

    for audience in audiences:
        if audience == "particuliers":
            zip_url = PARTICULIERS_ZIP
        elif audience == "entreprises":
            zip_url = ENTREPRISES_ZIP
        else:
            continue

        try:
            zf = download_zip(zip_url)
        except Exception as e:
            print(f"Error downloading {audience} ZIP: {e}", file=sys.stderr)
            continue

        print(f"Processing {audience} archive ({len(zf.namelist())} files)...", file=sys.stderr)

        for filename in zf.namelist():
            if not filename.endswith(".xml"):
                continue

            try:
                xml_content = zf.read(filename)
                parsed = parse_xml_file(xml_content)

                if parsed is None:
                    continue

                # Skip duplicates
                if parsed["doc_id"] in seen_ids:
                    continue
                seen_ids.add(parsed["doc_id"])

                # Add audience if not set
                if not parsed["audience"]:
                    parsed["audience"] = audience.capitalize()

                yield parsed
                doc_count += 1

                if max_docs and doc_count >= max_docs:
                    return

            except Exception as e:
                print(f"Error processing {filename}: {e}", file=sys.stderr)
                continue

        zf.close()

    print(f"Total documents fetched: {doc_count}", file=sys.stderr)


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """
    Fetch documents updated since a given date.

    Note: The ZIP files contain all current documents.
    We filter by modification date.
    """
    for doc in fetch_all():
        if doc.get("date"):
            try:
                doc_date = datetime.strptime(doc["date"], "%Y-%m-%d")
                if doc_date.replace(tzinfo=timezone.utc) >= since.replace(tzinfo=timezone.utc):
                    yield doc
            except ValueError:
                # Can't parse date, include document
                yield doc


def normalize(raw: dict) -> dict:
    """Transform parsed XML into normalized schema."""
    now = datetime.now(timezone.utc).isoformat()

    return {
        "_id": raw["doc_id"],
        "_source": "FR/ServicePublic",
        "_type": "doctrine",
        "_fetched_at": now,
        "title": raw["title"],
        "text": raw["text"],
        "date": raw["date"],
        "url": raw["url"],
        "audience": raw.get("audience", ""),
        "theme": raw.get("theme", ""),
        "subject": raw.get("subject", ""),
        "description": raw.get("description", ""),
        "doc_type": raw.get("doc_type", ""),
        "coverage": raw.get("coverage", ""),
        "language": "fr"
    }


def bootstrap_sample(sample_dir: Path, count: int = 15) -> None:
    """Generate sample data files."""
    sample_dir.mkdir(parents=True, exist_ok=True)

    samples = []
    for raw in fetch_all(max_docs=count + 20):  # Fetch extra in case some are skipped
        record = normalize(raw)

        # Skip records without meaningful text
        if not record["text"] or len(record["text"]) < 100:
            print(f"Skipping {record['_id']}: insufficient text content", file=sys.stderr)
            continue

        samples.append(record)

        # Save individual sample
        safe_id = record["_id"].replace("/", "_").replace(":", "_")
        filename = f"{safe_id}.json"
        with open(sample_dir / filename, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

        print(f"Saved: {filename} ({len(record['text']):,} chars)", file=sys.stderr)

        if len(samples) >= count:
            break

    # Save combined samples
    if samples:
        with open(sample_dir / "all_samples.json", "w", encoding="utf-8") as f:
            json.dump(samples, f, ensure_ascii=False, indent=2)

        # Calculate statistics
        text_lengths = [len(s["text"]) for s in samples]
        avg_length = sum(text_lengths) / len(text_lengths)

        print(f"\n=== Sample Statistics ===", file=sys.stderr)
        print(f"Total samples: {len(samples)}", file=sys.stderr)
        print(f"Avg text length: {avg_length:,.0f} chars", file=sys.stderr)
        print(f"Min text length: {min(text_lengths):,} chars", file=sys.stderr)
        print(f"Max text length: {max(text_lengths):,} chars", file=sys.stderr)

        # Count by audience
        by_audience = {}
        for s in samples:
            audience = s.get("audience") or "Unknown"
            by_audience[audience] = by_audience.get(audience, 0) + 1

        print(f"\nBy audience:", file=sys.stderr)
        for audience, cnt in sorted(by_audience.items(), key=lambda x: -x[1]):
            print(f"  {audience}: {cnt}", file=sys.stderr)

        # Count by doc_type
        by_type = {}
        for s in samples:
            dtype = s.get("doc_type") or "Unknown"
            by_type[dtype] = by_type.get(dtype, 0) + 1

        print(f"\nBy document type:", file=sys.stderr)
        for dtype, cnt in sorted(by_type.items(), key=lambda x: -x[1]):
            print(f"  {dtype}: {cnt}", file=sys.stderr)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Service-Public.fr fiches pratiques fetcher")
    parser.add_argument("command", choices=["bootstrap", "fetch", "updates"],
                       help="Command to run")
    parser.add_argument("--sample", action="store_true",
                       help="Generate sample data only")
    parser.add_argument("--count", type=int, default=15,
                       help="Number of samples to generate")
    parser.add_argument("--since", type=str,
                       help="Fetch updates since date (YYYY-MM-DD)")
    parser.add_argument("--audience", type=str, choices=["particuliers", "entreprises", "all"],
                       default="all", help="Which audience(s) to fetch")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    # Determine audiences
    if args.audience == "all":
        audiences = ["particuliers", "entreprises"]
    else:
        audiences = [args.audience]

    if args.command == "bootstrap":
        if args.sample:
            bootstrap_sample(sample_dir, args.count)
        else:
            # Full bootstrap
            for raw in fetch_all(audiences=audiences):
                record = normalize(raw)
                if record["text"]:
                    print(json.dumps(record, ensure_ascii=False))

    elif args.command == "fetch":
        for raw in fetch_all(max_docs=args.count if args.sample else None, audiences=audiences):
            record = normalize(raw)
            if record["text"]:
                print(json.dumps(record, ensure_ascii=False))

    elif args.command == "updates":
        if not args.since:
            print("Error: --since is required for updates command", file=sys.stderr)
            sys.exit(1)
        since = datetime.fromisoformat(args.since)
        for raw in fetch_updates(since):
            record = normalize(raw)
            if record["text"]:
                print(json.dumps(record, ensure_ascii=False))


if __name__ == "__main__":
    main()
