#!/usr/bin/env python3
"""
FR/CouncilState -- French Administrative Courts Case Law

Fetches administrative court case law decisions with full text from the
Open Data platform of the French administrative justice system.

Covers all three levels of French administrative justice:
- CE (Conseil d'État) - Supreme Administrative Court
- CAA (Cours Administratives d'Appel) - Administrative Courts of Appeal
- TA (Tribunaux Administratifs) - Administrative Tribunals

Data sources:
- CE archives: opendata.justice-administrative.fr/DCE/{year}/{month}/CE_{yearmonth}.zip
- CAA archives: opendata.justice-administrative.fr/DCA/{year}/{month}/CAA_{yearmonth}.zip
- TA archives: opendata.justice-administrative.fr/DTA/{year}/{month}/TA_{yearmonth}.zip

Usage:
    python bootstrap.py bootstrap --sample   # Fetch samples from all courts
    python bootstrap.py bootstrap --sample --court CE  # Conseil d'État only
    python bootstrap.py bootstrap --sample --court CAA  # Courts of Appeal only
    python bootstrap.py bootstrap --sample --court TA  # Administrative Tribunals only
    python bootstrap.py bootstrap --full     # Full fetch from all courts
    python bootstrap.py updates --since YYYY-MM-DD  # Incremental updates
"""

import argparse
import io
import json
import os
import re
import sys
import time
import zipfile
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, List, Optional, Tuple

import requests
import yaml

# Configuration
SOURCE_ID = "FR/CouncilState"
BASE_URL = "https://opendata.justice-administrative.fr"
USER_AGENT = "WorldWideLaw/1.0 (Open Data Research)"
REQUEST_DELAY = 1.0  # seconds between requests

# Court configurations
COURTS = {
    "CE": {
        "name": "Conseil d'État",
        "name_full": "Conseil d'État (Supreme Administrative Court)",
        "dir": "DCE",
        "prefix": "CE",
    },
    "CAA": {
        "name": "Cours Administratives d'Appel",
        "name_full": "Cours Administratives d'Appel (Administrative Courts of Appeal)",
        "dir": "DCA",
        "prefix": "CAA",
    },
    "TA": {
        "name": "Tribunaux Administratifs",
        "name_full": "Tribunaux Administratifs (Administrative Tribunals)",
        "dir": "DTA",
        "prefix": "TA",
    },
}

# Paths
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "data"
SAMPLE_DIR = SCRIPT_DIR / "sample"
STATUS_FILE = SCRIPT_DIR / "status.yaml"


def get_session() -> requests.Session:
    """Create a requests session with appropriate headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "application/xml,application/json,*/*",
        "Accept-Language": "fr,en;q=0.5",
    })
    return session


def clean_html_text(text: str) -> str:
    """Clean HTML tags from text while preserving structure."""
    if not text:
        return ""

    # Replace <br/> and <br> with newlines
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)

    # Replace paragraph tags with double newlines
    text = re.sub(r'</?p[^>]*>', '\n\n', text, flags=re.IGNORECASE)

    # Remove other HTML tags
    text = re.sub(r'<[^>]+>', '', text)

    # Decode HTML entities
    import html
    text = html.unescape(text)

    # Normalize whitespace
    text = re.sub(r'\n\s*\n', '\n\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n ', '\n', text)

    return text.strip()


def parse_xml_decision(xml_content: str, court_code: str) -> Optional[dict]:
    """Parse an XML decision document and extract all fields."""
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        print(f"  XML parse error: {e}", file=sys.stderr)
        return None

    def get_text(parent: ET.Element, path: str) -> Optional[str]:
        """Get text content from an element by path."""
        elem = parent.find(path)
        return elem.text.strip() if elem is not None and elem.text else None

    def get_all_text(elem: ET.Element) -> str:
        """Get all text content including nested elements."""
        if elem is None:
            return ""
        # Get text and tail of all descendants
        parts = []
        for e in elem.iter():
            if e.text:
                parts.append(e.text)
            if e.tail:
                parts.append(e.tail)
        return ' '.join(parts).strip()

    # Technical metadata
    donnees = root.find('Donnees_Techniques')
    identification = get_text(donnees, 'Identification') if donnees is not None else None
    date_maj = get_text(donnees, 'Date_Mise_Jour') if donnees is not None else None

    # Case information
    dossier = root.find('Dossier')
    if dossier is None:
        return None

    code_juridiction = get_text(dossier, 'Code_Juridiction')
    nom_juridiction = get_text(dossier, 'Nom_Juridiction')
    numero_dossier = get_text(dossier, 'Numero_Dossier')
    date_lecture = get_text(dossier, 'Date_Lecture')
    numero_ecli = get_text(dossier, 'Numero_ECLI')
    avocat_requerant = get_text(dossier, 'Avocat_Requerant')
    type_decision = get_text(dossier, 'Type_Decision')
    type_recours = get_text(dossier, 'Type_Recours')
    code_publication = get_text(dossier, 'Code_Publication')
    solution = get_text(dossier, 'Solution')

    # Hearing information
    audience = root.find('Audience')
    date_audience = get_text(audience, 'Date_Audience') if audience is not None else None
    numero_role = get_text(audience, 'Numero_Role') if audience is not None else None
    formation_jugement = get_text(audience, 'Formation_Jugement') if audience is not None else None

    # Full text
    decision = root.find('Decision')
    texte_elem = decision.find('Texte_Integral') if decision is not None else None

    # Get raw text including HTML tags, then clean
    if texte_elem is not None:
        # Get the raw inner content with tags
        raw_text = ET.tostring(texte_elem, encoding='unicode', method='html')
        # Remove the outer Texte_Integral tags
        raw_text = re.sub(r'^<Texte_Integral[^>]*>', '', raw_text)
        raw_text = re.sub(r'</Texte_Integral>$', '', raw_text)
        full_text = clean_html_text(raw_text)
    else:
        full_text = ""

    return {
        'identification': identification,
        'date_mise_jour': date_maj,
        'code_juridiction': code_juridiction,
        'nom_juridiction': nom_juridiction,
        'numero_dossier': numero_dossier,
        'date_lecture': date_lecture,
        'numero_ecli': numero_ecli,
        'avocat_requerant': avocat_requerant,
        'type_decision': type_decision,
        'type_recours': type_recours,
        'code_publication': code_publication,
        'solution': solution,
        'date_audience': date_audience,
        'numero_role': numero_role,
        'formation_jugement': formation_jugement,
        'full_text': full_text,
        'court_type': court_code,  # Track which court type this came from
    }


def normalize(raw: dict) -> dict:
    """Transform raw document data into normalized schema."""

    ecli = raw.get('numero_ecli', '')
    dossier = raw.get('numero_dossier', '')
    court_type = raw.get('court_type', 'CE')

    # Create unique ID
    doc_id = ecli if ecli else f"{court_type}_{dossier}_{raw.get('date_lecture', '')}"

    # Build title from available info
    title_parts = []
    if raw.get('nom_juridiction'):
        title_parts.append(raw['nom_juridiction'])
    elif court_type in COURTS:
        title_parts.append(COURTS[court_type]['name'])
    if raw.get('type_decision'):
        title_parts.append(raw['type_decision'])
    if raw.get('formation_jugement'):
        title_parts.append(raw['formation_jugement'])
    if dossier:
        title_parts.append(f"n° {dossier}")
    if raw.get('date_lecture'):
        title_parts.append(f"du {raw['date_lecture']}")

    title = " - ".join(title_parts) if title_parts else f"Décision {dossier}"

    # Build URL based on court type
    if ecli:
        if court_type == "CE":
            url = f"https://www.conseil-etat.fr/arianeweb/CE/decision/{raw.get('date_lecture', '').replace('-', '')}/{dossier}"
        else:
            url = f"https://opendata.justice-administrative.fr/recherche/"
    else:
        url = f"https://opendata.justice-administrative.fr/recherche/"

    # Determine court tier based on court type
    # CE (Conseil d'État) = tier 1 (Supreme)
    # CAA (Cours Administratives d'Appel) = tier 2 (Appeals)
    # TA (Tribunaux Administratifs) = tier 3 (First Instance)
    court_tier = {"CE": 1, "CAA": 2, "TA": 3}.get(court_type, 2)

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": raw.get('full_text', ''),
        "date": raw.get('date_lecture'),
        "url": url,
        "ecli": ecli,
        "case_number": dossier,
        "court": raw.get('nom_juridiction'),
        "court_code": raw.get('code_juridiction'),
        "court_type": court_type,  # CE, CAA, or TA
        "court_tier": court_tier,  # 1=Supreme, 2=Appeals, 3=First Instance
        "decision_type": raw.get('type_decision'),
        "appeal_type": raw.get('type_recours'),
        "publication_code": raw.get('code_publication'),
        "formation": raw.get('formation_jugement'),
        "solution": raw.get('solution'),
        "hearing_date": raw.get('date_audience'),
        "lawyer": raw.get('avocat_requerant'),
    }


def get_available_archives(session: requests.Session, court_code: str) -> List[Tuple[int, int]]:
    """Get list of available (year, month) archives for a court type."""
    archives = []

    court = COURTS[court_code]
    dir_name = court['dir']
    prefix = court['prefix']

    # Parse the directory listing
    try:
        response = session.get(f"{BASE_URL}/{dir_name}/", timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"  Error fetching archive list for {court_code}: {e}", file=sys.stderr)
        return []

    # Extract year/month from links like /DCE/2024/01/CE_202401.zip
    pattern = rf'/{dir_name}/(\d{{4}})/(\d{{2}})/{prefix}_\d+\.zip'
    for match in re.finditer(pattern, response.text):
        year = int(match.group(1))
        month = int(match.group(2))
        archives.append((year, month))

    return sorted(archives, reverse=True)  # Most recent first


def fetch_archive(session: requests.Session, court_code: str, year: int, month: int) -> Generator[dict, None, None]:
    """Download and parse an archive, yielding normalized records."""

    court = COURTS[court_code]
    dir_name = court['dir']
    prefix = court['prefix']

    url = f"{BASE_URL}/{dir_name}/{year}/{month:02d}/{prefix}_{year}{month:02d}.zip"
    print(f"  Downloading {url}...")

    try:
        response = session.get(url, timeout=120)
        if response.status_code == 404:
            print(f"    Archive not found: {url}")
            return
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"    Error downloading archive: {e}", file=sys.stderr)
        return

    # Parse ZIP in memory
    try:
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            for name in zf.namelist():
                if not name.endswith('.xml'):
                    continue

                try:
                    xml_content = zf.read(name).decode('utf-8')
                    raw = parse_xml_decision(xml_content, court_code)

                    if raw and raw.get('full_text'):
                        yield normalize(raw)

                except Exception as e:
                    print(f"    Error parsing {name}: {e}", file=sys.stderr)
                    continue

    except zipfile.BadZipFile as e:
        print(f"    Invalid ZIP file: {e}", file=sys.stderr)
        return


def fetch_sample(session: requests.Session, count: int = 15, court_filter: Optional[str] = None) -> List[dict]:
    """Fetch a sample of records with full text.

    Args:
        session: requests session
        count: number of samples to fetch
        court_filter: if specified, only fetch from this court (CE, CAA, or TA)
    """

    records = []
    courts_to_fetch = [court_filter] if court_filter else list(COURTS.keys())

    # Calculate samples per court
    samples_per_court = count // len(courts_to_fetch)
    remainder = count % len(courts_to_fetch)

    for court_code in courts_to_fetch:
        # Add any remainder to the first court
        court_count = samples_per_court + (1 if remainder > 0 else 0)
        remainder = max(0, remainder - 1)

        print(f"\n=== {COURTS[court_code]['name_full']} ===")

        archives = get_available_archives(session, court_code)

        if not archives:
            print(f"No archives found for {court_code}!", file=sys.stderr)
            continue

        print(f"Found {len(archives)} available archives for {court_code}.")

        court_records = []
        for year, month in archives[:3]:  # Check up to 3 most recent months
            if len(court_records) >= court_count:
                break

            print(f"\nProcessing {court_code} {year}-{month:02d}...")

            for record in fetch_archive(session, court_code, year, month):
                if len(court_records) >= court_count:
                    break

                # Only include records with substantial text
                if len(record.get('text', '')) > 500:
                    court_records.append(record)
                    print(f"  [{len(court_records)}/{court_count}] {record['_id']}: {len(record['text'])} chars")

            time.sleep(REQUEST_DELAY)

        records.extend(court_records)

    return records


def fetch_all(session: requests.Session, court_filter: Optional[str] = None) -> Generator[dict, None, None]:
    """Fetch all available records with full text."""

    courts_to_fetch = [court_filter] if court_filter else list(COURTS.keys())

    for court_code in courts_to_fetch:
        print(f"\n=== Fetching all records from {COURTS[court_code]['name_full']} ===")

        archives = get_available_archives(session, court_code)

        for year, month in archives:
            print(f"Processing {court_code} {year}-{month:02d}...")

            for record in fetch_archive(session, court_code, year, month):
                if record.get('text'):
                    yield record

            time.sleep(REQUEST_DELAY)


def fetch_updates(session: requests.Session, since: datetime, court_filter: Optional[str] = None) -> Generator[dict, None, None]:
    """Fetch updates since a given date."""

    courts_to_fetch = [court_filter] if court_filter else list(COURTS.keys())
    since_year = since.year
    since_month = since.month

    for court_code in courts_to_fetch:
        print(f"\n=== Fetching updates from {COURTS[court_code]['name_full']} ===")

        archives = get_available_archives(session, court_code)

        for year, month in archives:
            # Skip archives older than since date
            if (year, month) < (since_year, since_month):
                continue

            print(f"Processing {court_code} {year}-{month:02d}...")

            for record in fetch_archive(session, court_code, year, month):
                # Additional date filter
                record_date = record.get('date')
                if record_date and record_date >= since.strftime("%Y-%m-%d"):
                    if record.get('text'):
                        yield record

            time.sleep(REQUEST_DELAY)


def save_samples(records: List[dict]) -> None:
    """Save sample records to the sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    for i, record in enumerate(records):
        filepath = SAMPLE_DIR / f"record_{i:04d}.json"
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    # Also save all samples in one file
    all_samples = SAMPLE_DIR / "all_samples.json"
    with open(all_samples, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"\nSaved {len(records)} samples to {SAMPLE_DIR}")


def update_status(records_fetched: int, errors: int, sample_count: int = 0) -> None:
    """Update the status.yaml file."""
    now = datetime.now(timezone.utc).isoformat()

    status = {
        "last_run": now,
        "last_bootstrap": now if sample_count > 0 else None,
        "last_error": None,
        "total_records": 0,
        "run_history": [{
            "started_at": now,
            "finished_at": now,
            "records_fetched": records_fetched,
            "records_new": 0,
            "records_updated": 0,
            "records_skipped": 0,
            "sample_records_saved": sample_count,
            "errors": errors,
        }]
    }

    # Load existing status if present
    if STATUS_FILE.exists():
        try:
            with open(STATUS_FILE) as f:
                existing = yaml.safe_load(f) or {}
            if "run_history" in existing:
                status["run_history"] = existing["run_history"][-9:] + status["run_history"]
        except Exception:
            pass

    with open(STATUS_FILE, 'w') as f:
        yaml.dump(status, f, default_flow_style=False)


def main():
    parser = argparse.ArgumentParser(description="FR/CouncilState data fetcher (CE, CAA, TA)")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Bootstrap command
    bootstrap_parser = subparsers.add_parser("bootstrap", help="Initial data fetch")
    bootstrap_parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    bootstrap_parser.add_argument("--full", action="store_true", help="Full fetch")
    bootstrap_parser.add_argument("--count", type=int, default=15, help="Number of samples")
    bootstrap_parser.add_argument("--court", choices=["CE", "CAA", "TA"],
                                  help="Fetch from specific court only (default: all)")

    # Updates command
    updates_parser = subparsers.add_parser("updates", help="Fetch updates")
    updates_parser.add_argument("--since", required=True, help="Date to fetch from (YYYY-MM-DD)")
    updates_parser.add_argument("--court", choices=["CE", "CAA", "TA"],
                               help="Fetch from specific court only (default: all)")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    session = get_session()

    if args.command == "bootstrap":
        if args.sample:
            court_desc = args.court if args.court else "all courts (CE, CAA, TA)"
            print(f"Fetching {args.count} sample records from {court_desc}...")
            records = fetch_sample(session, args.count, args.court)
            if records:
                save_samples(records)
                update_status(len(records), 0, len(records))

                # Print summary by court
                print(f"\n=== SUMMARY ===")
                by_court = {}
                for r in records:
                    ct = r.get('court_type', 'unknown')
                    if ct not in by_court:
                        by_court[ct] = []
                    by_court[ct].append(len(r.get('text', '')))

                for ct, lengths in sorted(by_court.items()):
                    avg_len = sum(lengths) / len(lengths) if lengths else 0
                    print(f"\n{ct} ({COURTS.get(ct, {}).get('name', ct)}):")
                    print(f"  Records: {len(lengths)}")
                    print(f"  Avg text length: {avg_len:.0f} chars")
                    print(f"  Min text length: {min(lengths)} chars")
                    print(f"  Max text length: {max(lengths)} chars")

                text_lengths = [len(r.get('text', '')) for r in records]
                avg_len = sum(text_lengths) / len(text_lengths) if text_lengths else 0
                print(f"\nTotal:")
                print(f"  Records: {len(records)}")
                print(f"  Avg text length: {avg_len:.0f} chars")
            else:
                print("No records fetched!", file=sys.stderr)
                update_status(0, 1)
                sys.exit(1)

        elif args.full:
            print("Starting full fetch...")
            count = 0
            for record in fetch_all(session, args.court):
                count += 1
                if count % 100 == 0:
                    print(f"  {count} records...")
            print(f"Fetched {count} records")
            update_status(count, 0)

    elif args.command == "updates":
        since = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        print(f"Fetching updates since {since.date()}...")
        count = 0
        for record in fetch_updates(session, since, args.court):
            count += 1
        print(f"Fetched {count} updated records")
        update_status(count, 0)


if __name__ == "__main__":
    main()
