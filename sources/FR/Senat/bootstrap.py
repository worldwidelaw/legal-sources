#!/usr/bin/env python3
"""
FR/Senat -- French Senate Legislative Documents

Fetches legislative documents (bills, proposals, resolutions) from the French Senate
in Akoma Ntoso XML format with full text.

Data sources:
- Index of deposited texts: senat.fr/akomantoso/depots.xml
- Index of adopted texts: senat.fr/akomantoso/adoptions.xml
- Individual documents: senat.fr/akomantoso/{id}.akn.xml

Usage:
    python bootstrap.py bootstrap --sample   # Fetch sample records
    python bootstrap.py bootstrap --full     # Full fetch
    python bootstrap.py updates --since YYYY-MM-DD  # Incremental updates
"""

import argparse
import json
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional

import requests
import yaml

# Configuration
SOURCE_ID = "FR/Senat"
BASE_URL = "https://www.senat.fr/akomantoso"
INDEXES = [
    f"{BASE_URL}/depots.xml",      # Deposited texts
    f"{BASE_URL}/adoptions.xml",   # Adopted texts
]
USER_AGENT = "WorldWideLaw/1.0 (Open Data Research)"
REQUEST_DELAY = 0.5  # seconds between requests

# Akoma Ntoso namespace
AKN_NS = {
    'akn': 'http://docs.oasis-open.org/legaldocml/ns/akn/3.0',
    'data': 'http://data.parlement.fr/v1'
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
        "Accept": "application/xml,*/*",
        "Accept-Language": "fr,en;q=0.5",
    })
    return session


def get_text_recursive(elem: Optional[ET.Element]) -> str:
    """Recursively extract all text from an element and its children."""
    if elem is None:
        return ""

    parts = []

    # Add element's text if any
    if elem.text:
        parts.append(elem.text.strip())

    # Process children
    for child in elem:
        child_text = get_text_recursive(child)
        if child_text:
            parts.append(child_text)
        # Add tail text
        if child.tail:
            parts.append(child.tail.strip())

    return ' '.join(parts)


def parse_akn_document(xml_content: str) -> Optional[dict]:
    """Parse an Akoma Ntoso XML document and extract all fields."""
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        print(f"  XML parse error: {e}", file=sys.stderr)
        return None

    # Find the main document element (bill, act, resolution, etc.)
    doc_elem = None
    for doc_type in ['bill', 'act', 'resolution', 'amendmentList']:
        doc_elem = root.find(f'akn:{doc_type}', AKN_NS)
        if doc_elem is not None:
            break

    if doc_elem is None:
        # Try without namespace
        for doc_type in ['bill', 'act', 'resolution', 'amendmentList']:
            doc_elem = root.find(f'.//{doc_type}')
            if doc_elem is not None:
                break

    if doc_elem is None:
        # Last resort: just get the first child
        doc_elem = root[0] if len(root) > 0 else root

    # Extract metadata
    meta = doc_elem.find('akn:meta', AKN_NS) or doc_elem.find('.//meta')

    # FRBR identifiers
    frbr_work = meta.find('.//akn:FRBRWork', AKN_NS) if meta else None
    frbr_expr = meta.find('.//akn:FRBRExpression', AKN_NS) if meta else None

    if frbr_work is None and meta is not None:
        frbr_work = meta.find('.//FRBRWork')
    if frbr_expr is None and meta is not None:
        frbr_expr = meta.find('.//FRBRExpression')

    def get_frbr_value(parent: Optional[ET.Element], tag: str) -> str:
        if parent is None:
            return ""
        elem = parent.find(f'akn:{tag}', AKN_NS) or parent.find(f'.//{tag}')
        if elem is not None:
            return elem.get('value', '') or elem.text or ''
        return ""

    def get_frbr_date(parent: Optional[ET.Element], name: str) -> str:
        if parent is None:
            return ""
        for elem in parent.findall(f'akn:FRBRdate', AKN_NS) or parent.findall('.//FRBRdate'):
            if elem.get('name', '').lstrip('#') == name:
                return elem.get('date', '')
        return ""

    # Get identifiers
    frbr_this = get_frbr_value(frbr_work, 'FRBRthis')
    frbr_uri = get_frbr_value(frbr_work, 'FRBRuri')

    # Get aliases (short title, signet, url)
    aliases = {}
    if frbr_work is not None:
        for alias in frbr_work.findall('akn:FRBRalias', AKN_NS) or frbr_work.findall('.//FRBRalias'):
            name = alias.get('name', '')
            value = alias.get('value', '')
            if name and value:
                aliases[name] = value

    # Get dates
    date_presentation = get_frbr_date(frbr_work, 'presentation')
    date_depot = get_frbr_date(frbr_expr, 'depot')
    date_adoption = get_frbr_date(frbr_expr, 'adoption')

    # Get authors
    authors = []
    if frbr_work is not None:
        for author in frbr_work.findall('akn:FRBRauthor', AKN_NS) or frbr_work.findall('.//FRBRauthor'):
            href = author.get('href', '').lstrip('#')
            if href:
                authors.append(href)

    # Get workflow steps
    workflow_steps = []
    workflow = meta.find('.//akn:workflow', AKN_NS) if meta else None
    if workflow is None and meta is not None:
        workflow = meta.find('.//workflow')
    if workflow is not None:
        for step in workflow.findall('akn:step', AKN_NS) or workflow.findall('.//step'):
            step_info = {
                'date': step.get('date', ''),
                'outcome': step.get('outcome', ''),
                'by': step.get('by', '').lstrip('#'),
            }
            workflow_steps.append(step_info)

    # Get preamble/title
    preamble = doc_elem.find('akn:preamble', AKN_NS) or doc_elem.find('.//preamble')
    doc_title_elem = preamble.find('akn:docTitle', AKN_NS) if preamble else None
    if doc_title_elem is None and preamble is not None:
        doc_title_elem = preamble.find('.//docTitle')
    doc_title = get_text_recursive(doc_title_elem) if doc_title_elem is not None else ""

    # Extract full text from body
    body = doc_elem.find('akn:body', AKN_NS) or doc_elem.find('.//body')
    full_text_parts = []

    if body is not None:
        # Extract text from all articles and their content
        for article in body.iter():
            tag = article.tag.split('}')[-1] if '}' in article.tag else article.tag

            if tag == 'article':
                # Get article number
                num_elem = article.find('akn:num', AKN_NS) or article.find('.//num')
                if num_elem is not None and num_elem.text:
                    full_text_parts.append(f"\n{num_elem.text.strip()}\n")

            elif tag in ('p', 'content'):
                text = get_text_recursive(article)
                if text:
                    full_text_parts.append(text)

            elif tag == 'heading':
                text = get_text_recursive(article)
                if text:
                    full_text_parts.append(f"\n{text}\n")

    full_text = '\n'.join(full_text_parts)

    # Clean up text
    full_text = re.sub(r'\n{3,}', '\n\n', full_text)
    full_text = re.sub(r'[ \t]+', ' ', full_text)
    full_text = full_text.strip()

    # Determine document type from name or URI
    doc_name = doc_elem.get('name', '')
    doc_type = 'legislation'
    if 'pjl' in frbr_uri.lower() or doc_name == 'pjl':
        doc_type_label = 'Projet de loi'
    elif 'ppl' in frbr_uri.lower() or doc_name == 'ppl':
        doc_type_label = 'Proposition de loi'
    elif 'ppr' in frbr_uri.lower() or doc_name == 'ppr':
        doc_type_label = 'Proposition de résolution'
    else:
        doc_type_label = 'Texte législatif'

    return {
        'frbr_this': frbr_this,
        'frbr_uri': frbr_uri,
        'title': doc_title or aliases.get('intitule-court', ''),
        'short_title': aliases.get('intitule-court', ''),
        'signet': aliases.get('signet-dossier-legislatif-senat', ''),
        'url_senat': aliases.get('url-senat', ''),
        'date_presentation': date_presentation,
        'date_depot': date_depot,
        'date_adoption': date_adoption,
        'authors': authors,
        'workflow': workflow_steps,
        'doc_type': doc_type,
        'doc_type_label': doc_type_label,
        'full_text': full_text,
    }


def normalize(raw: dict, source_url: str) -> dict:
    """Transform raw document data into normalized schema."""

    # Create unique ID from FRBR identifiers or signet
    frbr_uri = raw.get('frbr_uri', '')
    signet = raw.get('signet', '')

    if signet:
        doc_id = signet
    elif frbr_uri:
        doc_id = frbr_uri.replace('/', '_').strip('_')
    else:
        doc_id = source_url.split('/')[-1].replace('.akn.xml', '')

    # Get best date
    date = raw.get('date_depot') or raw.get('date_presentation') or raw.get('date_adoption')

    # Get URL
    url = raw.get('url_senat', '')
    if not url:
        url = f"https://www.senat.fr/dossier-legislatif/{signet}.html" if signet else source_url

    # Build title
    title = raw.get('title', '')
    if not title:
        title = raw.get('short_title', '') or f"{raw.get('doc_type_label', 'Document')} {signet}"

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": raw.get('full_text', ''),
        "date": date,
        "url": url,
        "frbr_uri": frbr_uri,
        "signet": signet,
        "doc_type_label": raw.get('doc_type_label', ''),
        "authors": raw.get('authors', []),
        "workflow": raw.get('workflow', []),
        "date_depot": raw.get('date_depot'),
        "date_adoption": raw.get('date_adoption'),
    }


def get_document_list(session: requests.Session, index_url: str) -> list[dict]:
    """Parse an index XML file and return list of document URLs with dates."""
    try:
        response = session.get(index_url, timeout=30)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching index {index_url}: {e}", file=sys.stderr)
        return []

    try:
        root = ET.fromstring(response.content)
    except ET.ParseError as e:
        print(f"Error parsing index XML: {e}", file=sys.stderr)
        return []

    documents = []
    for text_elem in root.findall('.//text'):
        url_elem = text_elem.find('url')
        date_elem = text_elem.find('lastModifiedDateTime')

        if url_elem is not None and url_elem.text:
            doc = {
                'url': url_elem.text.strip(),
                'lastModified': date_elem.text.strip() if date_elem is not None and date_elem.text else None
            }
            documents.append(doc)

    return documents


def fetch_document(session: requests.Session, url: str) -> Optional[dict]:
    """Fetch and parse a single Akoma Ntoso document."""
    try:
        response = session.get(url, timeout=30)
        if response.status_code == 404:
            return None
        response.raise_for_status()

        raw = parse_akn_document(response.text)
        if raw and raw.get('full_text'):
            return normalize(raw, url)
        return None

    except requests.RequestException as e:
        print(f"  Error fetching {url}: {e}", file=sys.stderr)
        return None


def fetch_sample(session: requests.Session, count: int = 15) -> list[dict]:
    """Fetch a sample of records with full text."""

    records = []
    seen_urls = set()

    print("Fetching document indexes...")

    for index_url in INDEXES:
        print(f"  Reading {index_url.split('/')[-1]}...")
        docs = get_document_list(session, index_url)
        print(f"    Found {len(docs)} documents")

        for doc in docs:
            if len(records) >= count:
                break

            url = doc['url']
            if url in seen_urls:
                continue
            seen_urls.add(url)

            print(f"  [{len(records)+1}/{count}] Fetching {url.split('/')[-1]}...")

            time.sleep(REQUEST_DELAY)

            record = fetch_document(session, url)

            if record and len(record.get('text', '')) > 200:
                records.append(record)
                print(f"    -> {len(record['text'])} chars")
            else:
                print(f"    -> Skipped (no text)")

        if len(records) >= count:
            break

    return records


def fetch_all(session: requests.Session) -> Generator[dict, None, None]:
    """Fetch all available records with full text."""

    seen_urls = set()

    for index_url in INDEXES:
        print(f"Processing {index_url.split('/')[-1]}...")
        docs = get_document_list(session, index_url)

        for doc in docs:
            url = doc['url']
            if url in seen_urls:
                continue
            seen_urls.add(url)

            time.sleep(REQUEST_DELAY)

            record = fetch_document(session, url)
            if record and record.get('text'):
                yield record


def fetch_updates(session: requests.Session, since: datetime) -> Generator[dict, None, None]:
    """Fetch updates since a given date."""

    since_str = since.strftime("%Y-%m-%dT%H:%M:%S")
    seen_urls = set()

    for index_url in INDEXES:
        print(f"Processing {index_url.split('/')[-1]}...")
        docs = get_document_list(session, index_url)

        for doc in docs:
            # Filter by lastModified date
            last_mod = doc.get('lastModified', '')
            if last_mod and last_mod < since_str:
                continue

            url = doc['url']
            if url in seen_urls:
                continue
            seen_urls.add(url)

            time.sleep(REQUEST_DELAY)

            record = fetch_document(session, url)
            if record and record.get('text'):
                yield record


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
    parser = argparse.ArgumentParser(description="FR/Senat data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Bootstrap command
    bootstrap_parser = subparsers.add_parser("bootstrap", help="Initial data fetch")
    bootstrap_parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    bootstrap_parser.add_argument("--full", action="store_true", help="Full fetch")
    bootstrap_parser.add_argument("--count", type=int, default=15, help="Number of samples")

    # Updates command
    updates_parser = subparsers.add_parser("updates", help="Fetch updates")
    updates_parser.add_argument("--since", required=True, help="Date to fetch from (YYYY-MM-DD)")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    session = get_session()

    if args.command == "bootstrap":
        if args.sample:
            print(f"Fetching {args.count} sample records from French Senate...")
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
            print("Starting full fetch...")
            count = 0
            for record in fetch_all(session):
                count += 1
                if count % 100 == 0:
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
