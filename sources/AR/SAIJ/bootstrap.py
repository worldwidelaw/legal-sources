#!/usr/bin/env python3
"""
AR/SAIJ -- Sistema Argentino de Información Jurídica Data Fetcher

Fetches Argentine case law from the SAIJ public JSON API.

Data source: https://www.saij.gob.ar
License: Open public access (government data)

Strategy:
  - Search API returns paginated results with document abstracts
  - Each abstract contains nested JSON with case metadata
  - Full text is available inline for summaries (sumario)
  - Full judgments (fallo) may reference PDF files

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py bootstrap            # Full bootstrap (caution: 800K+ records)
  python bootstrap.py test-api             # Quick API connectivity test
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import quote

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

# Setup
SOURCE_ID = "AR/SAIJ"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AR.SAIJ")

# API Configuration
BASE_URL = "https://www.saij.gob.ar"
SEARCH_URL = f"{BASE_URL}/busqueda"
DOCUMENT_URL = f"{BASE_URL}/view-document"

# Default search facets for jurisprudence
DEFAULT_FACETS = "Total|Fecha[20,1]|Estado de Vigencia[5,1]|Tema[5,1]|Organismo[5,1]|Autor[5,1]|Jurisdicción|Tribunal[5,1]|Publicación[5,1]|Colección temática[5,1]|Tipo de Documento/Jurisprudencia"


def clean_text(text: str) -> str:
    """Clean HTML tags and normalize whitespace."""
    if not text:
        return ""
    # Remove [[p]] style tags
    text = re.sub(r'\[\[/?p\]\]', '', text)
    # Remove reference tags [[r uuid:...]]...[[/r uuid:...]] but keep content
    text = re.sub(r'\[\[r uuid:[^\]]+\]\]', '', text)
    text = re.sub(r'\[\[/r uuid:[^\]]+\]\]', '', text)
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def parse_document_abstract(abstract_json: str) -> Optional[dict]:
    """Parse the nested documentAbstract JSON string."""
    try:
        data = json.loads(abstract_json)
        doc = data.get("document", {})
        metadata = doc.get("metadata", {})
        content = doc.get("content", {})
        return {
            "metadata": metadata,
            "content": content
        }
    except (json.JSONDecodeError, KeyError) as e:
        logger.warning(f"Failed to parse document abstract: {e}")
        return None


def search_documents(
    offset: int = 0,
    page_size: int = 20,
    doc_type: str = "Jurisprudencia",
    facets: str = DEFAULT_FACETS,
    timeout: int = 30,
) -> dict:
    """
    Search SAIJ for documents.

    Args:
        offset: Starting result index
        page_size: Number of results per page (max ~100)
        doc_type: Document type filter (Jurisprudencia, Sumario, Fallo)
        facets: Facet filter string
        timeout: Request timeout in seconds

    Returns:
        API response dict with searchResults
    """
    params = {
        "o": offset,
        "p": page_size,
        "f": facets,
        "s": "fecha-rango|DESC",
        "v": "colapsada",
    }

    headers = {
        "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
        "Accept": "application/json",
    }

    try:
        response = requests.get(SEARCH_URL, params=params, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Search API error: {e}")
        raise


def get_document_detail(uuid: str, timeout: int = 30) -> Optional[dict]:
    """
    Fetch full document details by UUID.

    Args:
        uuid: Document UUID
        timeout: Request timeout

    Returns:
        Document content dict or None if failed
    """
    params = {"guid": uuid}
    headers = {
        "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
        "Accept": "application/json",
    }

    try:
        response = requests.get(DOCUMENT_URL, params=params, headers=headers, timeout=timeout)
        response.raise_for_status()
        data = response.json()

        # Parse nested JSON in "data" field
        if "data" in data and isinstance(data["data"], str):
            return json.loads(data["data"])
        return data
    except (requests.RequestException, json.JSONDecodeError) as e:
        logger.warning(f"Failed to fetch document {uuid}: {e}")
        return None


def normalize(raw: dict) -> dict:
    """
    Transform raw SAIJ document to standard schema.

    Args:
        raw: Raw document from API (parsed documentAbstract)

    Returns:
        Normalized record with standard fields
    """
    metadata = raw.get("metadata", {})
    content = raw.get("content", {})

    uuid = metadata.get("uuid", "")
    doc_type = metadata.get("document-content-type", "unknown")
    friendly_url = metadata.get("friendly-url", {})

    # Build URL
    subdomain = friendly_url.get("subdomain", "documento")
    description = friendly_url.get("description", uuid)
    url = f"{BASE_URL}/{subdomain}/{description}"

    # Extract text content
    text = ""
    if "texto" in content:
        text = clean_text(content["texto"])
    elif "texto-doc" in content:
        # Full judgment - text is in a separate file
        text = f"[Full text in PDF: {content['texto-doc'].get('file-name', 'N/A')}]"

    # Get title/caratula
    title = content.get("titulo", "")
    if not title:
        title = content.get("caratula", "")
        if content.get("actor") and content.get("demandado"):
            title = f"{content['actor']} c/ {content['demandado']}"
        elif content.get("actor"):
            title = content["actor"]
        elif content.get("sobre"):
            title = content["sobre"]

    # Get date
    date = content.get("fecha")
    if date:
        try:
            # Dates are in YYYY-MM-DD format
            datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            date = None

    # Jurisdiction
    jurisdiction = content.get("jurisdiccion", {})
    if isinstance(jurisdiction, dict):
        jurisdiction_str = jurisdiction.get("descripcion", "")
    else:
        jurisdiction_str = str(jurisdiction)

    # Descriptors
    descriptors = content.get("descriptores", {})
    if isinstance(descriptors, dict):
        descriptor_list = descriptors.get("descriptor", [])
        if isinstance(descriptor_list, list):
            descriptors_text = ", ".join(
                d.get("elegido", {}).get("termino", "")
                for d in descriptor_list
                if isinstance(d, dict)
            )
        else:
            descriptors_text = ""
    else:
        descriptors_text = ""

    return {
        "_id": uuid,
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "uuid": uuid,
        "doc_type": doc_type,
        "title": title or f"Case {uuid[:20]}",
        "text": text,
        "date": date,
        "url": url,
        "tribunal": content.get("tribunal", ""),
        "jurisdiction": jurisdiction_str,
        "materia": content.get("materia", ""),
        "actor": content.get("actor", ""),
        "demandado": content.get("demandado", ""),
        "sobre": content.get("sobre", ""),
        "magistrados": content.get("magistrados", ""),
        "descriptores": descriptors_text,
        "numero_sumario": content.get("numero-sumario", ""),
        "numero_fallo": content.get("numero-fallo", ""),
        "sumario": clean_text(content.get("sumario", "")),
        "id_infojus": content.get("id-infojus", ""),
    }


def fetch_sample(count: int = 15) -> list:
    """
    Fetch sample documents for validation.

    Fetches a mix of summaries and judgments to ensure variety.
    """
    records = []

    logger.info(f"Fetching {count} sample records from SAIJ...")

    # Fetch recent summaries (have inline text)
    facets_sumario = "Total|Tipo de Documento/Jurisprudencia/Sumario"
    try:
        result = search_documents(offset=0, page_size=count, facets=facets_sumario)
        doc_list = result.get("searchResults", {}).get("documentResultList", [])

        for doc in doc_list[:count]:
            abstract_str = doc.get("documentAbstract", "{}")
            parsed = parse_document_abstract(abstract_str)
            if parsed:
                # Get full details for better text
                uuid = parsed.get("metadata", {}).get("uuid")
                if uuid:
                    time.sleep(0.5)  # Rate limit
                    detail = get_document_detail(uuid)
                    if detail:
                        doc_data = detail.get("document", {})
                        parsed = {
                            "metadata": doc_data.get("metadata", {}),
                            "content": doc_data.get("content", {})
                        }

                normalized = normalize(parsed)
                if normalized.get("text") and len(normalized["text"]) > 50:
                    records.append(normalized)
                    logger.info(f"  [{len(records)}/{count}] {normalized['title'][:60]}...")

                if len(records) >= count:
                    break

    except Exception as e:
        logger.error(f"Error fetching samples: {e}")

    return records


def test_api():
    """Test API connectivity and response structure."""
    logger.info("Testing SAIJ API connectivity...")

    try:
        # Test search
        result = search_documents(offset=0, page_size=2)
        total = result.get("searchResults", {}).get("totalSearchResults", 0)
        logger.info(f"Search API OK - Total results: {total:,}")

        # Test document fetch
        docs = result.get("searchResults", {}).get("documentResultList", [])
        if docs:
            abstract = parse_document_abstract(docs[0].get("documentAbstract", "{}"))
            if abstract:
                uuid = abstract.get("metadata", {}).get("uuid")
                if uuid:
                    detail = get_document_detail(uuid)
                    if detail:
                        logger.info(f"Document API OK - Fetched {uuid[:30]}...")
                        return True

        logger.warning("API test partially successful - could not fetch document details")
        return True

    except Exception as e:
        logger.error(f"API test failed: {e}")
        return False


def fetch_all(max_records: int = None, output_file: str = None) -> Generator[dict, None, None]:
    """
    Fetch all case law documents from SAIJ.

    Args:
        max_records: Maximum number of records to fetch (None = all)
        output_file: Optional path to write JSONL output

    Yields:
        Normalized document records
    """
    # We focus on Sumarios which have inline text content
    # Fallos often only have PDFs which require additional processing
    facets = "Total|Tipo de Documento/Jurisprudencia/Sumario"

    # First, get total count
    result = search_documents(offset=0, page_size=1, facets=facets)
    total = result.get("searchResults", {}).get("totalSearchResults", 0)
    logger.info(f"Total available Sumario records: {total:,}")

    if max_records:
        total = min(total, max_records)
        logger.info(f"Limiting to {total:,} records")

    page_size = 50  # API seems to handle 50 well
    fetched = 0
    errors = 0

    output_fh = None
    if output_file:
        output_fh = open(output_file, "w", encoding="utf-8")

    try:
        for offset in range(0, total, page_size):
            try:
                result = search_documents(offset=offset, page_size=page_size, facets=facets)
                doc_list = result.get("searchResults", {}).get("documentResultList", [])

                if not doc_list:
                    logger.warning(f"No documents at offset {offset}")
                    break

                for doc in doc_list:
                    try:
                        abstract_str = doc.get("documentAbstract", "{}")
                        parsed = parse_document_abstract(abstract_str)

                        if not parsed:
                            errors += 1
                            continue

                        # Get full details for richer text content
                        uuid = parsed.get("metadata", {}).get("uuid")
                        if uuid:
                            time.sleep(0.3)  # Rate limit
                            detail = get_document_detail(uuid)
                            if detail:
                                doc_data = detail.get("document", {})
                                parsed = {
                                    "metadata": doc_data.get("metadata", parsed.get("metadata", {})),
                                    "content": doc_data.get("content", parsed.get("content", {}))
                                }

                        normalized = normalize(parsed)

                        # Skip records without meaningful text
                        if not normalized.get("text") or len(normalized["text"]) < 50:
                            errors += 1
                            continue

                        fetched += 1

                        if output_fh:
                            output_fh.write(json.dumps(normalized, ensure_ascii=False) + "\n")

                        yield normalized

                        if fetched % 100 == 0:
                            logger.info(f"Progress: {fetched:,}/{total:,} records ({fetched*100//total}%)")

                        if max_records and fetched >= max_records:
                            break

                    except Exception as e:
                        logger.warning(f"Error processing document: {e}")
                        errors += 1

                if max_records and fetched >= max_records:
                    break

                time.sleep(1)  # Rate limit between pages

            except Exception as e:
                logger.error(f"Error fetching page at offset {offset}: {e}")
                time.sleep(5)  # Back off on error

    finally:
        if output_fh:
            output_fh.close()

    logger.info(f"Fetch complete: {fetched:,} records, {errors:,} errors")


def bootstrap_full(output_dir: Path = None):
    """
    Full bootstrap: fetch all records and save to JSONL.
    """
    if output_dir is None:
        output_dir = SOURCE_DIR

    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "records.jsonl"

    logger.info(f"Starting full bootstrap to {output_file}")

    count = 0
    for record in fetch_all(output_file=str(output_file)):
        count += 1

    logger.info(f"Bootstrap complete: {count:,} records saved to {output_file}")
    return count > 0


def bootstrap_sample():
    """Fetch and save sample records for validation."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    records = fetch_sample(count=15)

    if not records:
        logger.error("No records fetched!")
        return False

    # Save individual files
    for i, record in enumerate(records, 1):
        filename = f"sample_{i:02d}_{record['uuid'][:20]}.json"
        filepath = SAMPLE_DIR / filename
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    # Print summary
    logger.info(f"\nSaved {len(records)} sample records to {SAMPLE_DIR}")

    # Validate
    text_lengths = [len(r.get("text", "")) for r in records]
    avg_text = sum(text_lengths) / len(text_lengths) if text_lengths else 0

    logger.info(f"Validation:")
    logger.info(f"  - Records with text: {sum(1 for t in text_lengths if t > 0)}/{len(records)}")
    logger.info(f"  - Avg text length: {avg_text:.0f} chars")
    logger.info(f"  - Min text length: {min(text_lengths) if text_lengths else 0} chars")
    logger.info(f"  - Max text length: {max(text_lengths) if text_lengths else 0} chars")

    return len(records) >= 10 and avg_text > 100


def main():
    parser = argparse.ArgumentParser(description="AR/SAIJ Case Law Fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "test-api"],
        help="Command to run"
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Fetch sample records only (for bootstrap)"
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    if args.command == "test-api":
        success = test_api()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        if args.sample:
            success = bootstrap_sample()
            sys.exit(0 if success else 1)
        else:
            success = bootstrap_full()
            sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
