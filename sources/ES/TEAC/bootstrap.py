#!/usr/bin/env python3
"""
ES/TEAC - Spanish Tax Administrative Tribunal (TEAC)
Tribunal Económico-Administrativo Central

Fetches case law from DYCTEA (Doctrina y Criterios de los Tribunales
Económico-Administrativos), the official database of Spanish tax
administrative tribunal decisions.

Data from 1998 to present, updated monthly.
"""

import argparse
import json
import logging
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional
from html import unescape

import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SOURCE_ID = "ES/TEAC"
BASE_URL = "https://serviciostelematicosext.hacienda.gob.es/TEAC/DYCTEA"
SEARCH_URL = f"{BASE_URL}/criterios.aspx"
CRITERION_URL = f"{BASE_URL}/criterio.aspx"
RESOLUTION_URL = f"{BASE_URL}/textoresolucion.aspx"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
}

REQUEST_DELAY = 1.5


def clean_text(text: str) -> str:
    """Clean HTML and normalize whitespace."""
    if not text:
        return ""
    # Decode HTML entities
    text = unescape(text)
    # Remove extra whitespace
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def parse_spanish_date(date_str: str) -> Optional[str]:
    """Parse Spanish date formats to ISO 8601."""
    if not date_str:
        return None
    date_str = date_str.strip()
    # Try formats: dd/mm/yyyy
    for fmt in ["%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d"]:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def fetch_search_page(page: int = 1, session: requests.Session = None) -> tuple[list[dict], int]:
    """
    Fetch a page of search results from DYCTEA.

    Returns:
        Tuple of (list of criterion dicts, total count)
    """
    session = session or requests.Session()

    params = {
        "s": "1",
        "rs": "", "rn": "", "ra": "",  # Resolution number parts
        "fd": "", "fh": "",  # Date range (from/to)
        "u": "",  # Unit
        "n": "", "p": "",  # Norm and provision
        "c1": "", "c2": "", "c3": "",  # Concepts
        "tc": "", "tr": "", "tp": "", "tf": "",  # Type filters
        "c": "",  # Free text
        "pg": str(page),
    }

    response = session.get(SEARCH_URL, params=params, headers=HEADERS, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    # Extract total count from pagination text
    total_count = 0
    pagination_text = soup.find(string=re.compile(r"Se han encontrado \d+"))
    if pagination_text:
        match = re.search(r"Se han encontrado (\d+)", pagination_text)
        if match:
            total_count = int(match.group(1).replace(".", ""))

    # Also try "de X criterios" format
    if total_count == 0:
        pagination_div = soup.find(class_="paginacion")
        if pagination_div:
            text = pagination_div.get_text()
            match = re.search(r"de\s+([\d.]+)\s+criterios", text)
            if match:
                total_count = int(match.group(1).replace(".", ""))

    # Parse search results
    criteria = []
    # Results are typically in a table or list
    for link in soup.find_all("a", href=re.compile(r"criterio\.aspx\?id=")):
        href = link.get("href", "")
        id_match = re.search(r"id=([^&]+)", href)
        if not id_match:
            continue

        criterion_id = id_match.group(1)

        # Get the text description (usually the link text or sibling text)
        title = clean_text(link.get_text())

        # Try to find associated date (usually in parent row/div)
        parent = link.find_parent(["tr", "div", "li"])
        date_str = None
        if parent:
            date_text = parent.get_text()
            date_match = re.search(r"(\d{2}/\d{2}/\d{4})", date_text)
            if date_match:
                date_str = date_match.group(1)

        # Parse resolution number from id
        # Format: XX/NNNNN/YYYY/UU/I/J
        parts = criterion_id.split("/")
        resolution_number = criterion_id

        criteria.append({
            "id": criterion_id,
            "resolution_number": resolution_number,
            "title": title,
            "date_str": date_str,
            "url": f"{CRITERION_URL}?id={criterion_id}",
        })

    return criteria, total_count


def fetch_criterion_detail(criterion_id: str, session: requests.Session = None) -> dict:
    """Fetch the criterion page with summary info."""
    session = session or requests.Session()

    url = f"{CRITERION_URL}?id={criterion_id}"
    response = session.get(url, headers=HEADERS, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    data = {
        "criterion_id": criterion_id,
        "criterion_url": url,
    }

    # Extract criterion text (summary)
    criterion_div = soup.find(id="criterio") or soup.find(class_="criterio")
    if criterion_div:
        data["criterion_summary"] = clean_text(criterion_div.get_text())

    # Extract concepts/tags
    concepts = []
    concept_links = soup.find_all("a", href=re.compile(r"concepto"))
    for link in concept_links:
        concept = clean_text(link.get_text())
        if concept:
            concepts.append(concept)
    data["concepts"] = concepts

    # Extract legal references (normas)
    norms = []
    norm_section = soup.find(string=re.compile(r"Norma"))
    if norm_section:
        parent = norm_section.find_parent(["div", "td", "tr"])
        if parent:
            for link in parent.find_all("a"):
                norm = clean_text(link.get_text())
                if norm:
                    norms.append(norm)
    data["legal_references"] = norms

    # Extract date
    date_elem = soup.find(string=re.compile(r"\d{2}/\d{2}/\d{4}"))
    if date_elem:
        date_match = re.search(r"(\d{2}/\d{2}/\d{4})", str(date_elem))
        if date_match:
            data["date_str"] = date_match.group(1)

    # Extract tribunal unit
    unit_elem = soup.find(string=re.compile(r"TEAC|Tribunal"))
    if unit_elem:
        data["tribunal"] = clean_text(str(unit_elem))

    return data


def fetch_resolution_text(criterion_id: str, session: requests.Session = None) -> str:
    """
    Fetch the full resolution text from textoresolucion.aspx.

    This is the crucial function that gets the FULL TEXT of the decision.
    """
    session = session or requests.Session()

    url = f"{RESOLUTION_URL}?id={criterion_id}"
    response = session.get(url, headers=HEADERS, timeout=60)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    # Remove script and style elements
    for tag in soup(["script", "style", "nav", "header", "footer"]):
        tag.decompose()

    # Try to find the main content area
    content_div = (
        soup.find(id="contenido") or
        soup.find(id="content") or
        soup.find(class_="contenido") or
        soup.find(class_="resolucion") or
        soup.find("article") or
        soup.find("main")
    )

    if content_div:
        text = clean_text(content_div.get_text(separator=" "))
    else:
        # Fallback: get body text
        body = soup.find("body")
        if body:
            text = clean_text(body.get_text(separator=" "))
        else:
            text = clean_text(soup.get_text(separator=" "))

    # Clean up the text more aggressively
    # Remove repetitive navigation/boilerplate patterns
    text = re.sub(r'(Inicio|Menú|Buscar|Accesibilidad)\s*', '', text)
    text = re.sub(r'\s+', ' ', text)

    return text.strip()


def normalize(raw: dict) -> dict:
    """
    Normalize raw TEAC data into standard schema.

    CRITICAL: Must include 'text' field with FULL resolution text.
    """
    criterion_id = raw.get("id", "")
    resolution_number = raw.get("resolution_number", criterion_id)

    # Parse the resolution number for metadata
    # Format: XX/NNNNN/YYYY/UU/I/J
    parts = resolution_number.split("/")
    year = parts[2] if len(parts) > 2 else None

    # Generate unique ID
    doc_id = f"ES:TEAC:{resolution_number.replace('/', '_')}"

    # Parse date
    date_str = raw.get("date_str") or raw.get("date")
    date_iso = parse_spanish_date(date_str) if date_str else None

    # Get title
    title = raw.get("title", "")
    if not title:
        title = f"Resolución TEAC {resolution_number}"

    # Get full text - THIS IS MANDATORY
    text = raw.get("full_text", "")
    if not text:
        logger.warning(f"No full text for {criterion_id}")

    # Build the normalized record
    normalized = {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date_iso,
        "url": f"{RESOLUTION_URL}?id={criterion_id}",
        "resolution_number": resolution_number,
        "criterion_id": criterion_id,
        "year": year,
        "tribunal": "TEAC",
        "language": "es",
        "country": "ES",
    }

    # Add optional fields if present
    if raw.get("criterion_summary"):
        normalized["summary"] = raw["criterion_summary"]
    if raw.get("concepts"):
        normalized["concepts"] = raw["concepts"]
    if raw.get("legal_references"):
        normalized["legal_references"] = raw["legal_references"]

    return normalized


def fetch_all(max_pages: int = None, delay: float = REQUEST_DELAY) -> Iterator[dict]:
    """
    Fetch all TEAC resolutions.

    Yields normalized records with FULL TEXT.
    """
    session = requests.Session()
    page = 1
    total_fetched = 0
    total_count = None

    while True:
        logger.info(f"Fetching search page {page}...")

        try:
            criteria, count = fetch_search_page(page, session)
        except requests.RequestException as e:
            logger.error(f"Failed to fetch page {page}: {e}")
            break

        if total_count is None:
            total_count = count
            logger.info(f"Total criteria available: {total_count}")

        if not criteria:
            logger.info(f"No more criteria on page {page}")
            break

        for criterion in criteria:
            criterion_id = criterion["id"]

            try:
                time.sleep(delay)

                # Fetch criterion details
                details = fetch_criterion_detail(criterion_id, session)
                criterion.update(details)

                time.sleep(delay)

                # Fetch full resolution text - CRITICAL
                full_text = fetch_resolution_text(criterion_id, session)
                criterion["full_text"] = full_text

                # Normalize and yield
                normalized = normalize(criterion)

                # Only yield if we have actual text content
                if normalized.get("text"):
                    yield normalized
                    total_fetched += 1

                    if total_fetched % 50 == 0:
                        logger.info(f"Fetched {total_fetched} resolutions...")
                else:
                    logger.warning(f"Skipping {criterion_id} - no text content")

            except requests.RequestException as e:
                logger.error(f"Failed to fetch {criterion_id}: {e}")
                continue

        page += 1

        if max_pages and page > max_pages:
            logger.info(f"Reached max pages limit ({max_pages})")
            break

        time.sleep(delay)

    logger.info(f"Total resolutions fetched: {total_fetched}")


def fetch_updates(since: str) -> Iterator[dict]:
    """
    Fetch resolutions updated since a given date.

    TEAC search supports date filtering via fd (from date) parameter.
    """
    session = requests.Session()

    # Parse the since date
    since_dt = datetime.fromisoformat(since.replace("Z", "+00:00"))
    since_str = since_dt.strftime("%d/%m/%Y")

    page = 1

    while True:
        logger.info(f"Fetching updates page {page} since {since_str}...")

        params = {
            "s": "1",
            "rs": "", "rn": "", "ra": "",
            "fd": since_str, "fh": "",  # From date filter
            "u": "",
            "n": "", "p": "",
            "c1": "", "c2": "", "c3": "",
            "tc": "", "tr": "", "tp": "", "tf": "",
            "c": "",
            "pg": str(page),
        }

        try:
            response = session.get(SEARCH_URL, params=params, headers=HEADERS, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            criteria = []
            for link in soup.find_all("a", href=re.compile(r"criterio\.aspx\?id=")):
                href = link.get("href", "")
                id_match = re.search(r"id=([^&]+)", href)
                if id_match:
                    criteria.append({
                        "id": id_match.group(1),
                        "title": clean_text(link.get_text()),
                    })

            if not criteria:
                break

            for criterion in criteria:
                criterion_id = criterion["id"]

                try:
                    time.sleep(REQUEST_DELAY)
                    details = fetch_criterion_detail(criterion_id, session)
                    criterion.update(details)

                    time.sleep(REQUEST_DELAY)
                    full_text = fetch_resolution_text(criterion_id, session)
                    criterion["full_text"] = full_text

                    normalized = normalize(criterion)
                    if normalized.get("text"):
                        yield normalized

                except requests.RequestException as e:
                    logger.error(f"Failed to fetch {criterion_id}: {e}")

            page += 1
            time.sleep(REQUEST_DELAY)

        except requests.RequestException as e:
            logger.error(f"Failed to fetch updates page {page}: {e}")
            break


def bootstrap_sample(count: int = 15) -> list[dict]:
    """
    Fetch a sample of resolutions for testing.

    Returns normalized records with FULL TEXT.
    """
    session = requests.Session()
    samples = []

    logger.info(f"Fetching {count} sample resolutions...")

    # Fetch first page of results
    criteria, total = fetch_search_page(1, session)
    logger.info(f"Total available: {total}")

    for criterion in criteria[:count]:
        criterion_id = criterion["id"]
        logger.info(f"Fetching {criterion_id}...")

        try:
            time.sleep(REQUEST_DELAY)

            # Get criterion details
            details = fetch_criterion_detail(criterion_id, session)
            criterion.update(details)

            time.sleep(REQUEST_DELAY)

            # Get FULL TEXT
            full_text = fetch_resolution_text(criterion_id, session)
            criterion["full_text"] = full_text

            # Normalize
            normalized = normalize(criterion)

            if normalized.get("text"):
                samples.append(normalized)
                text_len = len(normalized.get("text", ""))
                logger.info(f"  OK: {text_len} chars of text")
            else:
                logger.warning(f"  No text content for {criterion_id}")

        except Exception as e:
            logger.error(f"  Failed: {e}")

    return samples


def main():
    parser = argparse.ArgumentParser(description="ES/TEAC Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "updates", "test"])
    parser.add_argument("--sample", action="store_true", help="Only fetch sample data")
    parser.add_argument("--count", type=int, default=15, help="Number of samples")
    parser.add_argument("--since", help="ISO date for updates (e.g., 2024-01-01)")
    parser.add_argument("--max-pages", type=int, help="Max pages to fetch")
    args = parser.parse_args()

    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command == "test":
        # Quick connectivity test
        session = requests.Session()
        criteria, total = fetch_search_page(1, session)
        print(f"Connected! Found {total} total criteria")
        if criteria:
            print(f"First criterion: {criteria[0]['id']}")

            # Test full text fetch
            time.sleep(REQUEST_DELAY)
            text = fetch_resolution_text(criteria[0]["id"], session)
            print(f"Full text length: {len(text)} chars")
            print(f"First 500 chars: {text[:500]}...")
        return

    if args.command == "bootstrap":
        if args.sample:
            # Fetch sample data
            samples = bootstrap_sample(args.count)

            if not samples:
                logger.error("No samples fetched!")
                return

            # Save samples
            for i, record in enumerate(samples):
                filepath = sample_dir / f"sample_{i+1:03d}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            # Summary
            print(f"\n{'='*60}")
            print(f"ES/TEAC Sample Bootstrap Complete")
            print(f"{'='*60}")
            print(f"Records: {len(samples)}")

            text_lengths = [len(r.get("text", "")) for r in samples]
            avg_text = sum(text_lengths) / len(text_lengths) if text_lengths else 0
            print(f"Avg text length: {avg_text:,.0f} chars")

            # Validate
            all_have_text = all(r.get("text") for r in samples)
            print(f"All have text: {'YES' if all_have_text else 'NO - PROBLEM!'}")

            print(f"\nSamples saved to: {sample_dir}")

        else:
            # Full bootstrap
            output_file = script_dir / "records.jsonl"
            count = 0

            with open(output_file, "w", encoding="utf-8") as f:
                for record in fetch_all(max_pages=args.max_pages):
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    count += 1

            print(f"Bootstrap complete: {count} records saved to {output_file}")

    elif args.command == "updates":
        if not args.since:
            print("Error: --since required for updates command")
            return

        count = 0
        output_file = script_dir / "updates.jsonl"

        with open(output_file, "w", encoding="utf-8") as f:
            for record in fetch_updates(args.since):
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
                count += 1

        print(f"Updates complete: {count} records saved to {output_file}")


if __name__ == "__main__":
    main()
