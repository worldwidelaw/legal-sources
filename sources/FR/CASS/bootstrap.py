#!/usr/bin/env python3
"""
FR/CASS — French Court of Cassation Case Law (CASS Database)

Fetches French case law from DILA's open data bulk archives.
Uses Freemium_cass_global for full archive (518K+ decisions, ~248MB).
Uses weekly CASS_ increments for updates.

Usage:
    python bootstrap.py bootstrap --sample   # Fetch sample records
    python bootstrap.py bootstrap --full     # Full fetch with checkpoint
    python bootstrap.py updates --since YYYY-MM-DD  # Incremental updates
    python bootstrap.py status               # Show checkpoint status
    python bootstrap.py clear-checkpoint     # Reset checkpoint
"""

import argparse
import json
import os
import re
import sys
import tarfile
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Union
from xml.etree import ElementTree as ET
import html

import requests
import yaml

# Configuration
SOURCE_ID = "FR/CASS"
BASE_URL = "https://echanges.dila.gouv.fr/OPENDATA/CASS/"
USER_AGENT = "WorldWideLaw/1.0 (Open Data Research)"
REQUEST_DELAY = 1.0  # seconds between requests

# Global dump pattern (contains full archive - 500K+ records, ~248MB)
GLOBAL_DUMP_PATTERN = r'Freemium_cass_global_\d+-\d+\.tar\.gz'

# Checkpoint: save progress every N records
CHECKPOINT_INTERVAL = 1000

# Paths
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "data"
SAMPLE_DIR = SCRIPT_DIR / "sample"
STATUS_FILE = SCRIPT_DIR / "status.yaml"
CHECKPOINT_FILE = SCRIPT_DIR / "checkpoint.json"


def get_session() -> requests.Session:
    """Create a requests session with appropriate headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "*/*",
    })
    return session


def load_checkpoint() -> Dict:
    """Load checkpoint data from file."""
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                return json.load(f)
        except Exception:
            pass
    return {"processed_ids": set(), "total_processed": 0, "archive_name": None}


def save_checkpoint(checkpoint: Dict) -> None:
    """Save checkpoint data to file."""
    # Convert set to list for JSON serialization
    data = {
        "processed_ids": list(checkpoint.get("processed_ids", set())),
        "total_processed": checkpoint.get("total_processed", 0),
        "archive_name": checkpoint.get("archive_name"),
        "last_updated": datetime.now(timezone.utc).isoformat()
    }
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(data, f)


def clear_checkpoint() -> None:
    """Clear checkpoint file."""
    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()
        print("Checkpoint cleared.")
    else:
        print("No checkpoint file found.")


def find_global_dump(session: requests.Session) -> Optional[Dict]:
    """Find the global CASS dump file (contains full 500K+ record archive)."""
    response = session.get(BASE_URL, timeout=30)
    response.raise_for_status()

    # Look for global dump file: Freemium_cass_global_YYYYMMDD-HHMMSS.tar.gz
    match = re.search(r'<a href="(Freemium_cass_global_(\d{8})-\d+\.tar\.gz)">', response.text)
    if match:
        filename = match.group(1)
        date_str = match.group(2)
        try:
            date = datetime.strptime(date_str, "%Y%m%d").date()
            return {
                "filename": filename,
                "date": date,
                "url": BASE_URL + filename,
                "type": "global"
            }
        except ValueError:
            pass

    return None


def list_available_archives(session: requests.Session) -> List[Dict]:
    """List available CASS incremental archives from the DILA server."""
    response = session.get(BASE_URL, timeout=30)
    response.raise_for_status()

    archives = []
    # Parse the directory listing HTML for weekly increments
    for match in re.finditer(r'<a href="(CASS_(\d{8})-\d+\.tar\.gz)">', response.text):
        filename = match.group(1)
        date_str = match.group(2)
        try:
            date = datetime.strptime(date_str, "%Y%m%d").date()
            archives.append({
                "filename": filename,
                "date": date,
                "url": BASE_URL + filename,
                "type": "incremental"
            })
        except ValueError:
            continue

    # Sort by date descending (most recent first)
    archives.sort(key=lambda x: x["date"], reverse=True)
    return archives


def download_archive(session: requests.Session, url: str, dest_path: Path,
                     expected_size_mb: float = 0) -> bool:
    """Download a tar.gz archive with progress reporting.

    Args:
        session: requests Session
        url: URL to download
        dest_path: destination file path
        expected_size_mb: expected file size in MB for progress calculation
    """
    try:
        # Use longer timeout for large files (connect timeout, read timeout)
        # Read timeout per chunk, not total download time
        response = session.get(url, timeout=(30, 60), stream=True)
        response.raise_for_status()

        total_size = int(response.headers.get('content-length', 0))
        downloaded = 0
        chunk_size = 65536  # 64KB chunks for faster download
        last_progress = 0

        with open(dest_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                f.write(chunk)
                downloaded += len(chunk)

                # Progress reporting for large files
                if total_size > 10_000_000:  # >10MB
                    progress = int(downloaded * 100 / total_size)
                    if progress >= last_progress + 10:
                        print(f"  Downloaded {downloaded / (1024*1024):.1f} MB / {total_size / (1024*1024):.1f} MB ({progress}%)")
                        last_progress = progress

        return True
    except Exception as e:
        print(f"Error downloading {url}: {e}", file=sys.stderr)
        return False


def extract_text_from_html(html_content: str) -> str:
    """Extract clean text from HTML content."""
    if not html_content:
        return ""

    # Decode HTML entities
    text = html.unescape(html_content)

    # Convert line breaks
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<p[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)

    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)

    # Clean up whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = text.strip()

    return text


def get_element_text(element: Optional[ET.Element], default: str = "") -> str:
    """Safely get text content from an XML element."""
    if element is None:
        return default
    return element.text or default


def get_all_text(element: Optional[ET.Element]) -> str:
    """Get all text content including nested elements."""
    if element is None:
        return ""
    return ''.join(element.itertext())


def parse_juritext(xml_content: bytes) -> Optional[Dict]:
    """Parse a JURITEXT XML file (case law decision)."""
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        print(f"XML parse error: {e}", file=sys.stderr)
        return None

    # Extract metadata
    meta = root.find(".//META")
    if meta is None:
        return None

    # Common metadata
    meta_commun = meta.find("META_COMMUN")
    doc_id = get_element_text(meta_commun.find("ID")) if meta_commun is not None else ""
    if not doc_id:
        return None

    nature = get_element_text(meta_commun.find("NATURE")) if meta_commun is not None else "ARRET"
    url = get_element_text(meta_commun.find("URL")) if meta_commun is not None else ""

    # Jurisprudence specific metadata
    meta_juri = meta.find(".//META_JURI")
    title = get_element_text(meta_juri.find("TITRE")) if meta_juri is not None else ""
    date_dec = get_element_text(meta_juri.find("DATE_DEC")) if meta_juri is not None else ""
    juridiction = get_element_text(meta_juri.find("JURIDICTION")) if meta_juri is not None else ""
    numero = get_element_text(meta_juri.find("NUMERO")) if meta_juri is not None else ""
    solution = get_element_text(meta_juri.find("SOLUTION")) if meta_juri is not None else ""

    # Judicial metadata (specific to CASS)
    meta_juri_judi = meta.find(".//META_JURI_JUDI")

    formation = ""
    numeros_affaires = []
    ecli = ""
    president = ""
    avocats = ""
    form_dec_att = ""
    date_dec_att = ""
    publi_bull = False

    if meta_juri_judi is not None:
        formation = get_element_text(meta_juri_judi.find("FORMATION"))

        # Get case numbers
        nums = meta_juri_judi.find("NUMEROS_AFFAIRES")
        if nums is not None:
            for num in nums.findall("NUMERO_AFFAIRE"):
                if num.text:
                    numeros_affaires.append(num.text.strip())

        ecli = get_element_text(meta_juri_judi.find("ECLI"))
        president = get_element_text(meta_juri_judi.find("PRESIDENT"))
        avocats = get_element_text(meta_juri_judi.find("AVOCATS"))
        form_dec_att = get_element_text(meta_juri_judi.find("FORM_DEC_ATT"))
        date_dec_att = get_element_text(meta_juri_judi.find("DATE_DEC_ATT"))

        publi = meta_juri_judi.find("PUBLI_BULL")
        if publi is not None:
            publi_bull = publi.get("publie") == "oui"

    # Extract decision text
    texte = root.find(".//TEXTE")
    decision_text = ""
    sommaire = ""
    analyse = ""

    if texte is not None:
        # Main decision text
        contenu = texte.find("BLOC_TEXTUEL/CONTENU")
        if contenu is not None:
            decision_text = extract_text_from_html(get_all_text(contenu))

        # Summary (SOMMAIRE)
        som = texte.find("SOMMAIRE")
        if som is not None:
            som_parts = []
            for sct in som.findall("SCT"):
                sct_type = sct.get("TYPE", "")
                if sct.text:
                    som_parts.append(f"[{sct_type}] {sct.text.strip()}")

            # Analyse
            ana = som.find("ANA")
            if ana is not None and ana.text:
                analyse = ana.text.strip()
                som_parts.append(f"[ANALYSE] {analyse}")

            sommaire = "\n".join(som_parts)

    # Build full text combining all parts
    full_text_parts = []
    if decision_text:
        full_text_parts.append(decision_text)
    if sommaire:
        full_text_parts.append(f"\n\nSOMMAIRE:\n{sommaire}")

    full_text = "\n".join(full_text_parts)

    # Extract legal citations (LIENS)
    citations = []
    liens = root.find(".//LIENS")
    if liens is not None:
        for lien in liens.findall("LIEN"):
            if lien.text:
                citations.append(lien.text.strip())

    return {
        "id": doc_id,
        "title": title,
        "nature": nature,
        "date_dec": date_dec,
        "juridiction": juridiction,
        "numero": numero,
        "solution": solution,
        "formation": formation,
        "numeros_affaires": numeros_affaires,
        "ecli": ecli,
        "president": president,
        "avocats": avocats,
        "form_dec_att": form_dec_att,
        "date_dec_att": date_dec_att,
        "publi_bull": publi_bull,
        "decision_text": decision_text,
        "sommaire": sommaire,
        "analyse": analyse,
        "citations": citations,
        "url": url,
        "full_text": full_text,
    }


def process_archive(archive_path: Path) -> Generator[Dict, None, None]:
    """Process a CASS tar.gz archive and yield normalized records."""

    with tarfile.open(archive_path, 'r:gz') as tar:
        for member in tar.getmembers():
            if not member.isfile():
                continue

            # Only process XML files
            if not member.name.endswith('.xml'):
                continue

            # Only process JURITEXT files (case law decisions)
            filename = os.path.basename(member.name)
            if not filename.startswith('JURITEXT'):
                continue

            try:
                f = tar.extractfile(member)
                if f is None:
                    continue
                content = f.read()
            except Exception:
                continue

            data = parse_juritext(content)
            if data and data.get('full_text'):
                # Skip if text is too short (likely parsing issue)
                if len(data['full_text']) < 200:
                    continue

                record = normalize(data)
                yield record


def normalize(raw: Dict) -> Dict:
    """Transform raw CASS data into normalized schema."""

    doc_id = raw.get('id', '')
    ecli = raw.get('ecli', '')

    # Build Légifrance URL
    if doc_id:
        url = f"https://www.legifrance.gouv.fr/juri/id/{doc_id}"
    else:
        url = "https://www.legifrance.gouv.fr"

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": raw.get('title', ''),
        "text": raw.get('full_text', ''),
        "date": raw.get('date_dec', ''),
        "url": url,

        # Case law specific fields
        "ecli": ecli,
        "juridiction": raw.get('juridiction', ''),
        "formation": raw.get('formation', ''),
        "solution": raw.get('solution', ''),
        "numero": raw.get('numero', ''),
        "numeros_affaires": raw.get('numeros_affaires', []),
        "president": raw.get('president', ''),
        "avocats": raw.get('avocats', ''),

        # Lower court info
        "form_dec_att": raw.get('form_dec_att', ''),
        "date_dec_att": raw.get('date_dec_att', ''),

        # Additional metadata
        "nature": raw.get('nature', ''),
        "publi_bull": raw.get('publi_bull', False),
        "sommaire": raw.get('sommaire', ''),
        "analyse": raw.get('analyse', ''),
        "citations": raw.get('citations', []),
    }


def fetch_sample(session: requests.Session, count: int = 15) -> List[Dict]:
    """Fetch a sample of records by downloading a recent archive."""

    print("Listing available archives...")
    archives = list_available_archives(session)

    if not archives:
        print("No archives found!", file=sys.stderr)
        return []

    # Pick a recent archive with reasonable size
    target_archive = archives[0]

    print(f"Downloading {target_archive['filename']}...")

    with tempfile.TemporaryDirectory() as tmpdir:
        archive_path = Path(tmpdir) / target_archive['filename']

        if not download_archive(session, target_archive['url'], archive_path):
            return []

        print(f"Processing archive ({archive_path.stat().st_size / 1024:.1f} KB)...")

        records = []
        for record in process_archive(archive_path):
            records.append(record)
            if len(records) >= count:
                break
            if len(records) % 5 == 0:
                print(f"  Processed {len(records)} records...")

    return records


def fetch_all(session: requests.Session, use_checkpoint: bool = True) -> Generator[Dict, None, None]:
    """Fetch all records from the global CASS dump with checkpoint support.

    Downloads the Freemium_cass_global archive (248MB, 500K+ records).
    Uses checkpoint file to resume interrupted downloads.

    Args:
        use_checkpoint: Whether to use checkpoint for resume capability.
    """

    # Find the global dump
    global_dump = find_global_dump(session)

    if not global_dump:
        print("ERROR: Global dump (Freemium_cass_global_*.tar.gz) not found!", file=sys.stderr)
        print("Falling back to incremental archives...", file=sys.stderr)
        # Fallback to incremental archives
        archives = list_available_archives(session)
        if not archives:
            print("No archives found!", file=sys.stderr)
            return

        print(f"Processing {len(archives)} incremental archives...")
        with tempfile.TemporaryDirectory() as tmpdir:
            for i, arch in enumerate(archives):
                print(f"Processing {arch['filename']} ({i+1}/{len(archives)})...")
                archive_path = Path(tmpdir) / arch['filename']
                if not download_archive(session, arch['url'], archive_path):
                    continue
                for record in process_archive(archive_path):
                    yield record
                archive_path.unlink()
                time.sleep(REQUEST_DELAY)
        return

    print(f"Found global dump: {global_dump['filename']} (dated {global_dump['date']})")
    print(f"This archive contains 500K+ Court of Cassation decisions.")

    # Load checkpoint
    checkpoint = {"processed_ids": set(), "total_processed": 0, "archive_name": None}
    if use_checkpoint:
        checkpoint_data = load_checkpoint()
        # Convert list back to set
        checkpoint["processed_ids"] = set(checkpoint_data.get("processed_ids", []))
        checkpoint["total_processed"] = checkpoint_data.get("total_processed", 0)
        checkpoint["archive_name"] = checkpoint_data.get("archive_name")

        if checkpoint["archive_name"] == global_dump['filename'] and checkpoint["total_processed"] > 0:
            print(f"Resuming from checkpoint: {checkpoint['total_processed']} records already processed")
        else:
            # Different archive or no checkpoint - start fresh
            checkpoint = {"processed_ids": set(), "total_processed": 0, "archive_name": global_dump['filename']}

    with tempfile.TemporaryDirectory() as tmpdir:
        archive_path = Path(tmpdir) / global_dump['filename']

        print(f"Downloading {global_dump['filename']} (~248MB)...")
        if not download_archive(session, global_dump['url'], archive_path):
            print("Failed to download global dump!", file=sys.stderr)
            return

        print(f"Downloaded {archive_path.stat().st_size / (1024*1024):.1f} MB")
        print("Processing archive (this may take a while)...")

        processed = 0
        skipped = 0
        new_this_session = 0

        with tarfile.open(archive_path, 'r:gz') as tar:
            members = [m for m in tar.getmembers() if m.isfile() and m.name.endswith('.xml') and 'JURITEXT' in m.name]
            total_members = len(members)
            print(f"Found {total_members} JURITEXT XML files to process")

            for member in members:
                filename = os.path.basename(member.name)
                doc_id = filename.replace('.xml', '')

                # Skip if already processed (checkpoint)
                if doc_id in checkpoint["processed_ids"]:
                    skipped += 1
                    continue

                try:
                    f = tar.extractfile(member)
                    if f is None:
                        continue
                    content = f.read()
                except Exception:
                    continue

                data = parse_juritext(content)
                if data and data.get('full_text'):
                    # Skip if text is too short (likely parsing issue)
                    if len(data['full_text']) < 200:
                        continue

                    record = normalize(data)
                    yield record

                    # Update checkpoint
                    checkpoint["processed_ids"].add(doc_id)
                    checkpoint["total_processed"] += 1
                    new_this_session += 1
                    processed += 1

                    # Save checkpoint periodically
                    if processed % CHECKPOINT_INTERVAL == 0:
                        checkpoint["archive_name"] = global_dump['filename']
                        save_checkpoint(checkpoint)
                        print(f"  Checkpoint saved: {checkpoint['total_processed']} total ({new_this_session} new this session)")

                    # Progress update
                    if processed % 5000 == 0:
                        print(f"  Processed {processed} records...")

        # Final checkpoint save for global dump
        checkpoint["archive_name"] = global_dump['filename']
        save_checkpoint(checkpoint)

        print(f"\nGlobal dump complete:")
        print(f"  Total processed: {checkpoint['total_processed']}")
        print(f"  New this session: {new_this_session}")
        print(f"  Skipped (from checkpoint): {skipped}")

    # Also process incremental archives newer than global dump
    archives = list_available_archives(session)
    newer_archives = [a for a in archives if a['date'] > global_dump['date']]

    if newer_archives:
        print(f"\nProcessing {len(newer_archives)} incremental archives newer than global dump...")
        incremental_count = 0
        with tempfile.TemporaryDirectory() as tmpdir:
            for i, arch in enumerate(newer_archives):
                print(f"  Processing {arch['filename']} ({i+1}/{len(newer_archives)})...")
                archive_path = Path(tmpdir) / arch['filename']
                if not download_archive(session, arch['url'], archive_path):
                    continue
                for record in process_archive(archive_path):
                    # Skip if already in checkpoint from global dump
                    if record['_id'] not in checkpoint["processed_ids"]:
                        yield record
                        incremental_count += 1
                archive_path.unlink()
                time.sleep(REQUEST_DELAY)
        print(f"  Incremental archives added: {incremental_count} new records")


def fetch_updates(session: requests.Session, since: datetime) -> Generator[Dict, None, None]:
    """Fetch updates since a given date."""

    archives = list_available_archives(session)

    # Filter to archives since the given date
    since_date = since.date()
    relevant = [a for a in archives if a['date'] >= since_date]

    if not relevant:
        print(f"No archives found since {since_date}", file=sys.stderr)
        return

    with tempfile.TemporaryDirectory() as tmpdir:
        for arch in relevant:
            print(f"Processing {arch['filename']}...")

            archive_path = Path(tmpdir) / arch['filename']

            if not download_archive(session, arch['url'], archive_path):
                continue

            for record in process_archive(archive_path):
                yield record

            archive_path.unlink()
            time.sleep(REQUEST_DELAY)


def save_samples(records: List[Dict]) -> None:
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

    print(f"Saved {len(records)} samples to {SAMPLE_DIR}")


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


def show_status() -> None:
    """Show current checkpoint and status information."""
    print("=== FR/CASS (Court of Cassation) Status ===\n")

    # Show checkpoint
    if CHECKPOINT_FILE.exists():
        checkpoint_data = load_checkpoint()
        print("Checkpoint:")
        print(f"  Archive: {checkpoint_data.get('archive_name', 'N/A')}")
        print(f"  Records processed: {checkpoint_data.get('total_processed', 0)}")
        print(f"  Last updated: {checkpoint_data.get('last_updated', 'N/A')}")
    else:
        print("Checkpoint: None (no checkpoint file)")

    # Show status file
    print()
    if STATUS_FILE.exists():
        try:
            with open(STATUS_FILE) as f:
                status = yaml.safe_load(f) or {}
            print("Status:")
            print(f"  Last run: {status.get('last_run', 'N/A')}")
            print(f"  Last bootstrap: {status.get('last_bootstrap', 'N/A')}")
            if status.get('run_history'):
                last = status['run_history'][-1]
                print(f"  Last fetch: {last.get('records_fetched', 0)} records")
        except Exception:
            print("Status: Error reading status file")
    else:
        print("Status: No status file")

    # Show sample count
    print()
    if SAMPLE_DIR.exists():
        samples = list(SAMPLE_DIR.glob("record_*.json"))
        print(f"Sample records: {len(samples)}")
    else:
        print("Sample records: None")


def main():
    parser = argparse.ArgumentParser(description="FR/legifrance case law data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Bootstrap command
    bootstrap_parser = subparsers.add_parser("bootstrap", help="Initial data fetch")
    bootstrap_parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    bootstrap_parser.add_argument("--full", action="store_true", help="Full fetch from global dump")
    bootstrap_parser.add_argument("--count", type=int, default=15, help="Number of samples")
    bootstrap_parser.add_argument("--no-checkpoint", action="store_true", help="Disable checkpoint (start fresh)")

    # Updates command
    updates_parser = subparsers.add_parser("updates", help="Fetch updates")
    updates_parser.add_argument("--since", required=True, help="Date to fetch from (YYYY-MM-DD)")

    # Status command
    subparsers.add_parser("status", help="Show checkpoint and status info")

    # Clear checkpoint command
    subparsers.add_parser("clear-checkpoint", help="Clear checkpoint file")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "status":
        show_status()
        return

    if args.command == "clear-checkpoint":
        clear_checkpoint()
        return

    session = get_session()

    if args.command == "bootstrap":
        if args.sample:
            print(f"Fetching {args.count} sample case law records...")
            records = fetch_sample(session, args.count)
            if records:
                save_samples(records)
                update_status(len(records), 0, len(records))

                # Print summary
                text_lengths = [len(r.get('text', '')) for r in records]
                avg_len = sum(text_lengths) / len(text_lengths) if text_lengths else 0
                print(f"\nSummary:")
                print(f"  Records: {len(records)}")
                print(f"  Avg text length: {avg_len:.0f} chars")
                print(f"  Min text length: {min(text_lengths)} chars")
                print(f"  Max text length: {max(text_lengths)} chars")
            else:
                print("No records fetched!", file=sys.stderr)
                update_status(0, 1)
                sys.exit(1)

        elif args.full:
            print("Starting full fetch from global CASS dump...")
            print("This downloads ~248MB and processes 500K+ decisions.")
            print()
            use_checkpoint = not args.no_checkpoint

            # Ensure data directory exists
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            records_file = DATA_DIR / "records.jsonl"

            count = 0
            new_records = 0

            # Load existing IDs to avoid duplicates
            existing_ids = set()
            if records_file.exists():
                with open(records_file, "r", encoding="utf-8") as f:
                    for line in f:
                        try:
                            record = json.loads(line.strip())
                            if record.get("_id"):
                                existing_ids.add(record["_id"])
                        except json.JSONDecodeError:
                            pass
                print(f"Found {len(existing_ids)} existing records in {records_file}")

            # Write records to JSONL file
            with open(records_file, "a", encoding="utf-8") as f:
                for record in fetch_all(session, use_checkpoint=use_checkpoint):
                    count += 1

                    # Skip if already exists
                    if record.get("_id") in existing_ids:
                        continue

                    # Write to JSONL
                    line = json.dumps(record, ensure_ascii=False, default=str)
                    f.write(line + "\n")
                    existing_ids.add(record["_id"])
                    new_records += 1

                    # Progress logging
                    if new_records % 5000 == 0:
                        print(f"  Written {new_records} new records to JSONL...")
                        f.flush()

            print(f"\nTotal records processed: {count}")
            print(f"New records written to JSONL: {new_records}")
            print(f"Output file: {records_file}")
            update_status(count, 0)
        else:
            # Default to sample
            print("No action specified. Use --sample or --full")
            print("Example: python bootstrap.py bootstrap --sample")
            sys.exit(1)

    elif args.command == "updates":
        since = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        print(f"Fetching updates since {since.date()}...")
        count = 0
        for record in fetch_updates(session, since):
            count += 1
        print(f"Fetched {count} updated records")
        update_status(count, 0)


if __name__ == "__main__":
    main()
