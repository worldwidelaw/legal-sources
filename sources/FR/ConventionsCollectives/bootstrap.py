#!/usr/bin/env python3
"""
FR/ConventionsCollectives -- French Collective Bargaining Agreements (Base Kali)

Fetches French collective agreements from DILA bulk data archives.
Uses global dump for full bootstrap, then daily incremental updates.

Usage:
    python bootstrap.py bootstrap --sample   # Fetch sample records
    python bootstrap.py bootstrap --full     # Full fetch with checkpointing
    python bootstrap.py updates --since YYYY-MM-DD  # Incremental updates
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
from typing import Any, Generator, Optional
from xml.etree import ElementTree as ET
import html

import requests
import yaml

# Configuration
SOURCE_ID = "FR/ConventionsCollectives"
BASE_URL = "https://echanges.dila.gouv.fr/OPENDATA/KALI/"
USER_AGENT = "LegalDataHunter/1.0 (Open Data Research)"
REQUEST_DELAY = 1.0  # seconds between requests
CHECKPOINT_INTERVAL = 500  # Save checkpoint every N records

# Paths
SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "data"
SAMPLE_DIR = SCRIPT_DIR / "sample"
STATUS_FILE = SCRIPT_DIR / "status.yaml"
CHECKPOINT_FILE = SCRIPT_DIR / "checkpoint.json"
GLOBAL_DUMP_PATH = DATA_DIR / "global_dump.tar.gz"


def get_session() -> requests.Session:
    """Create a requests session with appropriate headers."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "*/*",
    })
    return session


def list_available_archives(session: requests.Session) -> list[dict]:
    """List available KALI archives from the DILA server."""
    response = session.get(BASE_URL, timeout=30)
    response.raise_for_status()

    archives = []
    # Parse the directory listing HTML
    for match in re.finditer(r'<a href="(KALI_(\d{8})-\d+\.tar\.gz)">', response.text):
        filename = match.group(1)
        date_str = match.group(2)
        try:
            date = datetime.strptime(date_str, "%Y%m%d").date()
            archives.append({
                "filename": filename,
                "date": date,
                "url": BASE_URL + filename
            })
        except ValueError:
            continue

    # Sort by date descending (most recent first)
    archives.sort(key=lambda x: x["date"], reverse=True)
    return archives


def find_global_dump(session: requests.Session) -> Optional[dict]:
    """Find the latest global dump file on the DILA server."""
    response = session.get(BASE_URL, timeout=30)
    response.raise_for_status()

    # Look for global dump files: Freemium_kali_global_YYYYMMDD-HHMMSS.tar.gz
    matches = []
    for match in re.finditer(
        r'<a href="(Freemium_kali_global_(\d{8})-(\d+)\.tar\.gz)">',
        response.text
    ):
        filename = match.group(1)
        date_str = match.group(2)
        try:
            date = datetime.strptime(date_str, "%Y%m%d").date()
            matches.append({
                "filename": filename,
                "date": date,
                "url": BASE_URL + filename,
            })
        except ValueError:
            continue

    if not matches:
        return None

    # Return the most recent global dump
    matches.sort(key=lambda x: x["date"], reverse=True)
    return matches[0]


def load_checkpoint() -> dict:
    """Load checkpoint data from file."""
    if not CHECKPOINT_FILE.exists():
        return {}
    try:
        with open(CHECKPOINT_FILE) as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


def save_checkpoint(data: dict) -> None:
    """Save checkpoint data to file."""
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(data, f, indent=2)


def download_archive(session: requests.Session, url: str, dest_path: Path) -> bool:
    """Download a tar.gz archive with retry."""
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(3):
        try:
            response = session.get(url, timeout=120, stream=True)
            response.raise_for_status()

            with open(dest_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            return True
        except Exception as e:
            if attempt < 2:
                print(f"Retry {attempt + 1}/3 for {url}: {e}")
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                print(f"Error downloading {url}: {e}", file=sys.stderr)
                return False
    return False


def download_global_dump(session: requests.Session, url: str, dest_path: Path) -> bool:
    """Download the global dump with progress indicator and resume support."""
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    # Check if we have a partial download
    existing_size = dest_path.stat().st_size if dest_path.exists() else 0

    headers = {}
    if existing_size > 0:
        headers["Range"] = f"bytes={existing_size}-"
        print(f"Resuming download from {existing_size / (1024**2):.1f} MB...")

    try:
        response = session.get(url, timeout=600, stream=True, headers=headers)

        # Handle 416 Range Not Satisfiable (file already complete)
        if response.status_code == 416:
            print("Download already complete.")
            return True

        response.raise_for_status()

        # Get total size from Content-Range or Content-Length
        total_size = None
        if 'Content-Range' in response.headers:
            match = re.search(r'/(\d+)', response.headers['Content-Range'])
            if match:
                total_size = int(match.group(1))
        elif 'Content-Length' in response.headers:
            total_size = existing_size + int(response.headers['Content-Length'])

        mode = 'ab' if existing_size > 0 and response.status_code == 206 else 'wb'
        downloaded = existing_size if mode == 'ab' else 0

        with open(dest_path, mode) as f:
            for chunk in response.iter_content(chunk_size=65536):
                f.write(chunk)
                downloaded += len(chunk)
                if total_size:
                    pct = downloaded / total_size * 100
                    print(f"\rDownloading: {downloaded/(1024**2):.1f}/{total_size/(1024**2):.1f} MB ({pct:.1f}%)", end="", flush=True)
                else:
                    print(f"\rDownloading: {downloaded/(1024**2):.1f} MB", end="", flush=True)

        print()  # newline after progress
        return True

    except Exception as e:
        print(f"\nError downloading global dump: {e}", file=sys.stderr)
        return False


def extract_text_from_html(html_content: str) -> str:
    """Extract clean text from HTML content."""
    if not html_content:
        return ""

    # Decode HTML entities
    text = html.unescape(html_content)

    # Remove HTML tags
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<p[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
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


def parse_kalicont(xml_content: bytes) -> Optional[dict]:
    """Parse a KALICONT XML file (convention collective container with metadata)."""
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        return None

    # Extract metadata
    meta = root.find(".//META")
    if meta is None:
        return None

    # Get document ID
    doc_id = get_element_text(meta.find(".//ID"))
    if not doc_id:
        return None

    # Get IDCC (Identifiant Convention Collective)
    idcc = get_element_text(meta.find(".//NUM"))

    # Get core metadata
    nature = get_element_text(meta.find(".//NATURE"), "CONVENTION")
    cid = get_element_text(meta.find(".//CID"))
    nor = get_element_text(meta.find(".//NOR"))

    # Get title
    title_full = get_element_text(meta.find(".//TITREFULL"))
    title = get_element_text(meta.find(".//TITRE"))
    final_title = title_full or title or f"Convention collective {idcc}" if idcc else "Convention collective"

    # Get dates
    date_signature = get_element_text(meta.find(".//DATE_SIGNATURE"))
    date_publi = get_element_text(meta.find(".//DATE_PUBLI"))
    date_debut = get_element_text(meta.find(".//DATE_DEBUT"))
    date_fin = get_element_text(meta.find(".//DATE_FIN"))

    # Get status
    etat = get_element_text(meta.find(".//ETAT"), "EN_VIGUEUR")

    # Get ministry and brochure number
    ministere = get_element_text(meta.find(".//MINISTERE"))
    num_brochure = get_element_text(meta.find(".//NUM_BROCHURE"))

    # Extract text content from container sections
    text_parts = []

    # VISAS section (legal basis)
    visas = root.find(".//VISAS/CONTENU")
    if visas is not None:
        visas_text = extract_text_from_html(get_all_text(visas))
        if visas_text:
            text_parts.append(f"VISAS:\n{visas_text}")

    # SIGNATAIRES section
    signataires = root.find(".//SIGNATAIRES/CONTENU")
    if signataires is not None:
        sig_text = extract_text_from_html(get_all_text(signataires))
        if sig_text:
            text_parts.append(f"SIGNATAIRES:\n{sig_text}")

    # NOTA section
    nota = root.find(".//NOTA/CONTENU")
    if nota is not None:
        nota_text = extract_text_from_html(get_all_text(nota))
        if nota_text:
            text_parts.append(f"NOTA:\n{nota_text}")

    return {
        "id": doc_id,
        "cid": cid,
        "idcc": idcc,
        "nature": nature,
        "nor": nor,
        "title": final_title,
        "date_signature": date_signature,
        "date_publi": date_publi,
        "date_debut": date_debut,
        "date_fin": date_fin,
        "etat": etat,
        "ministere": ministere,
        "num_brochure": num_brochure,
        "container_text": "\n\n".join(text_parts),
    }


def parse_kaliarti(xml_content: bytes) -> Optional[dict]:
    """Parse a KALIARTI XML file (article content)."""
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError:
        return None

    # Get article ID and number
    doc_id = get_element_text(root.find(".//ID"))
    num = get_element_text(root.find(".//META_ARTICLE/NUM"))
    titre = get_element_text(root.find(".//META_ARTICLE/TITRE"))
    etat = get_element_text(root.find(".//META_ARTICLE/ETAT"))

    # Get article text
    bloc_textuel = root.find(".//BLOC_TEXTUEL/CONTENU")
    if bloc_textuel is not None:
        text = extract_text_from_html(get_all_text(bloc_textuel))
    else:
        text = ""

    # Get parent text info for grouping (KALITEXT cid)
    contexte_texte = root.find(".//CONTEXTE/TEXTE")
    parent_text_cid = contexte_texte.get("cid") if contexte_texte is not None else None

    # Get parent convention info (KALICONT cid)
    contexte_conteneur = root.find(".//CONTEXTE/CONTENEUR")
    parent_cont_cid = contexte_conteneur.get("cid") if contexte_conteneur is not None else None

    return {
        "id": doc_id,
        "num": num,
        "titre": titre,
        "etat": etat,
        "text": text,
        "parent_text_cid": parent_text_cid,
        "parent_cont_cid": parent_cont_cid,
    }


def process_archive(archive_path: Path) -> Generator[dict, None, None]:
    """Process a KALI tar.gz archive and yield normalized records."""

    # Dictionaries to collect data
    conventions = {}    # KALICONT documents by ID
    articles = {}       # KALIARTI articles by parent KALICONT cid

    try:
        with tarfile.open(archive_path, 'r:gz') as tar:
            for member in tar.getmembers():
                if not member.isfile():
                    continue

                # Only process XML files
                if not member.name.endswith('.xml'):
                    continue

                # Skip structure files (we only need article content)
                if '/struct/' in member.name:
                    continue

                try:
                    f = tar.extractfile(member)
                    if f is None:
                        continue
                    content = f.read()
                except Exception:
                    continue

                filename = os.path.basename(member.name)

                # Parse KALICONT files (convention containers)
                if filename.startswith('KALICONT'):
                    data = parse_kalicont(content)
                    if data:
                        conventions[data['id']] = data

                # Parse KALIARTI files (articles)
                elif filename.startswith('KALIARTI'):
                    data = parse_kaliarti(content)
                    if data and data.get('text'):
                        # Group by parent convention (KALICONT)
                        parent_cid = data.get('parent_cont_cid')
                        if parent_cid:
                            if parent_cid not in articles:
                                articles[parent_cid] = []
                            articles[parent_cid].append(data)
    except tarfile.TarError as e:
        print(f"Error reading archive {archive_path}: {e}", file=sys.stderr)
        return

    # Combine conventions with their articles
    for conv_id, conv_data in conventions.items():
        # Look up articles by convention ID
        conv_articles = articles.get(conv_id, [])

        # Gather article texts
        article_texts = []
        if conv_articles:
            # Sort articles by number
            def article_sort_key(a):
                num = a.get('num') or ''
                # Extract digits from article number
                digits = re.sub(r'\D', '', num)
                # Return tuple for sorting: (numeric_part, full_string)
                return (int(digits) if digits else 999, num)

            sorted_articles = sorted(conv_articles, key=article_sort_key)
            for art in sorted_articles:
                if art['text']:
                    # Build article header with number and title
                    header_parts = []
                    if art['num']:
                        header_parts.append(f"Article {art['num']}")
                    else:
                        header_parts.append("Article")
                    if art.get('titre'):
                        header_parts.append(f"- {art['titre']}")
                    header = " ".join(header_parts)
                    article_texts.append(f"{header}:\n{art['text']}")

        # Combine all text
        all_text_parts = []
        if conv_data.get('container_text'):
            all_text_parts.append(conv_data['container_text'])
        if article_texts:
            all_text_parts.append("\n\n".join(article_texts))

        full_text = "\n\n".join(all_text_parts)

        # Skip if no substantial text
        if len(full_text) < 100:
            continue

        # Normalize to standard schema
        record = normalize(conv_data, full_text)
        yield record


def normalize(raw: dict, full_text: str) -> dict:
    """Transform raw KALI data into normalized schema."""

    doc_id = raw.get('id', '')
    cid = raw.get('cid', '')
    idcc = raw.get('idcc', '')

    # Determine best date
    date = raw.get('date_signature') or raw.get('date_publi') or raw.get('date_debut')

    # Build Légifrance URL
    if cid:
        url = f"https://www.legifrance.gouv.fr/conv_coll/id/{cid}"
    elif idcc:
        url = f"https://www.legifrance.gouv.fr/conv_coll/idcc/{idcc}"
    else:
        url = "https://www.legifrance.gouv.fr/conv_coll/"

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "doctrine",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": raw.get('title', ''),
        "text": full_text,
        "date": date,
        "url": url,
        "idcc": idcc,
        "cid": cid,
        "nature": raw.get('nature', ''),
        "nor": raw.get('nor', ''),
        "num_brochure": raw.get('num_brochure', ''),
        "etat": raw.get('etat', ''),
        "date_signature": raw.get('date_signature'),
        "date_debut": raw.get('date_debut'),
        "date_fin": raw.get('date_fin'),
        "ministere": raw.get('ministere', ''),
    }


def fetch_sample(session: requests.Session, count: int = 15) -> list[dict]:
    """Fetch a sample of records by downloading a recent daily update."""

    print("Listing available archives...")
    archives = list_available_archives(session)

    if not archives:
        print("No archives found!", file=sys.stderr)
        return []

    # Pick a recent archive
    target_archive = archives[0] if archives else None

    if not target_archive:
        print("No suitable archive found!", file=sys.stderr)
        return []

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


def fetch_all(session: requests.Session, use_global: bool = True) -> Generator[dict, None, None]:
    """
    Fetch all available records.

    If use_global=True (default), downloads and processes the global dump first,
    then applies daily increments for records created after the global dump date.

    Checkpointing is implemented to survive restarts.
    Deduplication: tracks seen _id values to avoid yielding duplicates.
    """
    checkpoint = load_checkpoint()
    global_dump_date = checkpoint.get("global_dump_date")
    global_dump_complete = checkpoint.get("global_dump_complete", False)
    total_processed = checkpoint.get("total_processed", 0)

    # Track seen IDs to avoid duplicates
    seen_ids: set[str] = set()
    duplicates_skipped = 0

    if use_global and not global_dump_complete:
        # Step 1: Find and download the global dump
        print("Looking for global dump...")
        global_dump = find_global_dump(session)

        if global_dump:
            print(f"Found global dump: {global_dump['filename']} ({global_dump['date']})")
            global_dump_date = global_dump['date'].isoformat()

            # Download if not already present
            if not GLOBAL_DUMP_PATH.exists() or GLOBAL_DUMP_PATH.stat().st_size < 50 * 1024 * 1024:
                print("Downloading global dump (~173 MB)...")
                if not download_global_dump(session, global_dump['url'], GLOBAL_DUMP_PATH):
                    print("Failed to download global dump, falling back to incremental archives", file=sys.stderr)
                    use_global = False

            if use_global and GLOBAL_DUMP_PATH.exists():
                print("Processing global dump...")
                for record in process_archive(GLOBAL_DUMP_PATH):
                    record_id = record.get("_id")
                    if record_id in seen_ids:
                        duplicates_skipped += 1
                        continue
                    seen_ids.add(record_id)
                    total_processed += 1
                    yield record

                    # Save checkpoint periodically
                    if total_processed % CHECKPOINT_INTERVAL == 0:
                        save_checkpoint({
                            "global_dump_date": global_dump_date,
                            "global_dump_complete": False,
                            "total_processed": total_processed,
                        })

                # Mark global dump as complete
                global_dump_complete = True
                save_checkpoint({
                    "global_dump_date": global_dump_date,
                    "global_dump_complete": True,
                    "total_processed": total_processed,
                })
                print(f"Global dump complete: {total_processed} records ({duplicates_skipped} duplicates skipped)")
        else:
            print("No global dump found, using incremental archives only")

    # Step 2: Apply daily increments since the global dump date
    archives = list_available_archives(session)

    if not archives:
        print("No daily archives found!", file=sys.stderr)
        return

    # Filter to archives after the global dump date
    if global_dump_date:
        cutoff = datetime.strptime(global_dump_date, "%Y-%m-%d").date()
        archives = [a for a in archives if a['date'] > cutoff]
        print(f"Processing {len(archives)} daily increments since {global_dump_date}...")
    else:
        print(f"Processing {len(archives)} daily archives...")

    with tempfile.TemporaryDirectory() as tmpdir:
        for arch in archives:
            print(f"Processing {arch['filename']}...")

            archive_path = Path(tmpdir) / arch['filename']

            if not download_archive(session, arch['url'], archive_path):
                continue

            for record in process_archive(archive_path):
                record_id = record.get("_id")
                if record_id in seen_ids:
                    duplicates_skipped += 1
                    continue
                seen_ids.add(record_id)
                total_processed += 1
                yield record

            # Clean up to save space
            archive_path.unlink()
            time.sleep(REQUEST_DELAY)

    print(f"Total: {total_processed} unique records ({duplicates_skipped} duplicates skipped)")

    # Save final checkpoint
    save_checkpoint({
        "global_dump_date": global_dump_date,
        "global_dump_complete": global_dump_complete,
        "total_processed": total_processed,
        "last_full_fetch": datetime.now(timezone.utc).isoformat(),
    })


def fetch_updates(session: requests.Session, since: datetime) -> Generator[dict, None, None]:
    """Fetch updates since a given date. Deduplicates by _id across archives."""

    archives = list_available_archives(session)

    # Filter to archives since the given date
    since_date = since.date()
    relevant = [a for a in archives if a['date'] >= since_date]

    if not relevant:
        print(f"No archives found since {since_date}", file=sys.stderr)
        return

    # Track seen IDs to avoid duplicates
    seen_ids: set[str] = set()

    with tempfile.TemporaryDirectory() as tmpdir:
        for arch in relevant:
            print(f"Processing {arch['filename']}...")

            archive_path = Path(tmpdir) / arch['filename']

            if not download_archive(session, arch['url'], archive_path):
                continue

            for record in process_archive(archive_path):
                record_id = record.get("_id")
                if record_id in seen_ids:
                    continue
                seen_ids.add(record_id)
                yield record

            archive_path.unlink()
            time.sleep(REQUEST_DELAY)


def save_samples(records: list[dict]) -> None:
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


def main():
    parser = argparse.ArgumentParser(description="FR/ConventionsCollectives data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Bootstrap command
    bootstrap_parser = subparsers.add_parser("bootstrap", help="Initial data fetch")
    bootstrap_parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    bootstrap_parser.add_argument("--full", action="store_true", help="Full fetch with global dump")
    bootstrap_parser.add_argument("--incremental-only", action="store_true",
                                  help="Skip global dump, use daily increments only")
    bootstrap_parser.add_argument("--count", type=int, default=15, help="Number of samples")
    bootstrap_parser.add_argument("--reset", action="store_true", help="Clear checkpoint and start fresh")

    # Updates command
    updates_parser = subparsers.add_parser("updates", help="Fetch updates")
    updates_parser.add_argument("--since", required=True, help="Date to fetch from (YYYY-MM-DD)")

    # Status command
    status_parser = subparsers.add_parser("status", help="Show checkpoint status")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    session = get_session()

    if args.command == "status":
        checkpoint = load_checkpoint()
        if checkpoint:
            print("Checkpoint status:")
            print(f"  Global dump date: {checkpoint.get('global_dump_date', 'N/A')}")
            print(f"  Global dump complete: {checkpoint.get('global_dump_complete', False)}")
            print(f"  Total processed: {checkpoint.get('total_processed', 0)}")
            print(f"  Last full fetch: {checkpoint.get('last_full_fetch', 'N/A')}")
            if GLOBAL_DUMP_PATH.exists():
                print(f"  Global dump file: {GLOBAL_DUMP_PATH.stat().st_size / (1024**2):.2f} MB")
        else:
            print("No checkpoint found. Run 'bootstrap --full' to start.")
        return

    if args.command == "bootstrap":
        if hasattr(args, 'reset') and args.reset:
            if CHECKPOINT_FILE.exists():
                CHECKPOINT_FILE.unlink()
                print("Checkpoint cleared.")
            if GLOBAL_DUMP_PATH.exists():
                GLOBAL_DUMP_PATH.unlink()
                print("Global dump file deleted.")

        if args.sample:
            print(f"Fetching {args.count} sample records...")
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
            use_global = not getattr(args, 'incremental_only', False)
            print(f"Starting full fetch (global dump: {'yes' if use_global else 'no'})...")
            count = 0
            for record in fetch_all(session, use_global=use_global):
                count += 1
                if count % 1000 == 0:
                    print(f"  {count} records...")
            print(f"Fetched {count} records")
            update_status(count, 0)

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
