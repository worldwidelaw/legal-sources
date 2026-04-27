#!/usr/bin/env python3
"""
CH/Fedlex - Swiss Federal Legislation
Fetches legislation from Fedlex SPARQL endpoint with full text.
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Generator, Optional
from html import unescape

import requests
from bs4 import BeautifulSoup

# Configuration
SPARQL_ENDPOINT = "https://fedlex.data.admin.ch/sparqlendpoint"
RATE_LIMIT_DELAY = 0.5  # seconds between requests

# JOLux ontology prefixes
PREFIXES = """
PREFIX jolux: <http://data.legilux.public.lu/resource/ontology/jolux#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX dcterms: <http://purl.org/dc/terms/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
"""


def sparql_query(query: str, timeout: int = 60, max_retries: int = 4) -> dict:
    """Execute SPARQL query with retry and exponential backoff."""
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": "LegalDataHunter/1.0 (research project)"
    }

    last_exc = None
    for attempt in range(max_retries + 1):
        try:
            response = requests.post(
                SPARQL_ENDPOINT,
                data={"query": PREFIXES + query},
                headers=headers,
                timeout=timeout,
            )
            response.raise_for_status()
            return response.json()
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as e:
            last_exc = e
            if attempt < max_retries:
                wait = min(2 ** attempt * 2, 60)  # 2s, 4s, 8s, 16s (capped 60s)
                status = getattr(getattr(e, "response", None), "status_code", "?")
                print(f"  SPARQL retry {attempt+1}/{max_retries} after {status} (wait {wait}s)", file=sys.stderr)
                time.sleep(wait)

    raise last_exc  # type: ignore[misc]


def get_recent_acts(limit: int = 100, offset: int = 0) -> list:
    """Get recent legislation acts with metadata."""
    query = f"""
    SELECT DISTINCT ?act ?dateDoc ?processType ?genre ?typeDoc
    WHERE {{
      ?act a jolux:Act ;
           jolux:dateDocument ?dateDoc .
      OPTIONAL {{ ?act jolux:processType ?processType }}
      OPTIONAL {{ ?act jolux:legalResourceGenre ?genre }}
      OPTIONAL {{ ?act jolux:typeDocument ?typeDoc }}
      FILTER(?dateDoc >= "2020-01-01"^^xsd:date)
    }}
    ORDER BY DESC(?dateDoc)
    LIMIT {limit}
    OFFSET {offset}
    """

    result = sparql_query(query)
    return result.get("results", {}).get("bindings", [])


def get_act_expressions(act_uri: str) -> list:
    """Get language expressions for an act."""
    query = f"""
    SELECT ?expr ?title ?lang
    WHERE {{
      <{act_uri}> jolux:isRealizedBy ?expr .
      ?expr jolux:title ?title .
      OPTIONAL {{ ?expr jolux:language ?lang }}
    }}
    """

    result = sparql_query(query)
    return result.get("results", {}).get("bindings", [])


def get_expression_files(expr_uri: str) -> list:
    """Get file manifestations for an expression."""
    query = f"""
    SELECT ?manif ?format ?fileUrl
    WHERE {{
      <{expr_uri}> jolux:isEmbodiedBy ?manif .
      ?manif jolux:format ?format .
      OPTIONAL {{ ?manif jolux:isExemplifiedBy ?fileUrl }}
    }}
    """

    result = sparql_query(query)
    return result.get("results", {}).get("bindings", [])


def extract_text_from_html(html_content: str) -> str:
    """Extract clean text from HTML content."""
    soup = BeautifulSoup(html_content, "html.parser")

    # Remove script and style elements
    for element in soup(["script", "style", "meta", "link"]):
        element.decompose()

    # Get text content
    text = soup.get_text(separator="\n", strip=True)

    # Clean up excessive whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r' {2,}', ' ', text)

    return text.strip()


def fetch_html_content(url: str) -> Optional[str]:
    """Fetch HTML content from URL."""
    try:
        headers = {
            "User-Agent": "LegalDataHunter/1.0 (research project)",
            "Accept": "text/html,application/xhtml+xml"
        }
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        return response.text
    except Exception as e:
        print(f"Error fetching {url}: {e}", file=sys.stderr)
        return None


def fetch_document(act_uri: str) -> Optional[dict]:
    """Fetch a single document with full text."""
    time.sleep(RATE_LIMIT_DELAY)

    # Get expressions (language versions)
    expressions = get_act_expressions(act_uri)
    if not expressions:
        return None

    # Prefer German, then French, then Italian
    lang_priority = ["DEU", "FRA", "ITA", "ROH"]  # Romansh last
    best_expr = None
    best_title = None
    best_lang = None

    for expr in expressions:
        lang_uri = expr.get("lang", {}).get("value", "")
        lang_code = lang_uri.split("/")[-1] if lang_uri else "UNK"

        if best_expr is None:
            best_expr = expr["expr"]["value"]
            best_title = expr["title"]["value"]
            best_lang = lang_code
        elif lang_code in lang_priority:
            current_idx = lang_priority.index(best_lang) if best_lang in lang_priority else 99
            new_idx = lang_priority.index(lang_code)
            if new_idx < current_idx:
                best_expr = expr["expr"]["value"]
                best_title = expr["title"]["value"]
                best_lang = lang_code

    if not best_expr:
        return None

    time.sleep(RATE_LIMIT_DELAY)

    # Get file manifestations
    files = get_expression_files(best_expr)

    # Find HTML file (preferred) or XML
    html_url = None
    xml_url = None

    for f in files:
        format_uri = f.get("format", {}).get("value", "")
        file_url = f.get("fileUrl", {}).get("value", "")

        if "HTML" in format_uri and file_url and "-an" not in file_url:
            html_url = file_url
        elif "XML" in format_uri and file_url and "-an" not in file_url:
            xml_url = file_url

    # Fetch full text
    content_url = html_url or xml_url
    if not content_url:
        return None

    time.sleep(RATE_LIMIT_DELAY)

    raw_content = fetch_html_content(content_url)
    if not raw_content:
        return None

    text = extract_text_from_html(raw_content)
    if not text or len(text) < 50:
        return None

    return {
        "eli_uri": act_uri,
        "expression_uri": best_expr,
        "title": best_title,
        "language": best_lang,
        "file_url": content_url,
        "text": text
    }


def fetch_all() -> Generator[dict, None, None]:
    """Yield all Swiss legislation documents with full text."""
    offset = 0
    batch_size = 100

    while True:
        print(f"Fetching batch at offset {offset}...", file=sys.stderr)
        acts = get_recent_acts(limit=batch_size, offset=offset)

        if not acts:
            break

        for act in acts:
            act_uri = act["act"]["value"]
            date_doc = act.get("dateDoc", {}).get("value", "")
            process_type = act.get("processType", {}).get("value", "").split("/")[-1]
            genre = act.get("genre", {}).get("value", "").split("/")[-1]
            type_doc = act.get("typeDoc", {}).get("value", "").split("/")[-1]

            doc = fetch_document(act_uri)
            if doc:
                doc["date_document"] = date_doc
                doc["process_type"] = process_type
                doc["genre"] = genre
                doc["type_document"] = type_doc
                yield doc

        offset += batch_size


def fetch_updates(since: datetime) -> Generator[dict, None, None]:
    """Yield documents modified since a given date."""
    since_str = since.strftime("%Y-%m-%d")

    query = f"""
    SELECT DISTINCT ?act ?dateDoc
    WHERE {{
      ?act a jolux:Act ;
           jolux:dateDocument ?dateDoc .
      ?act dcterms:modified ?modified .
      FILTER(?modified >= "{since_str}"^^xsd:date)
    }}
    ORDER BY DESC(?dateDoc)
    LIMIT 1000
    """

    result = sparql_query(query)
    acts = result.get("results", {}).get("bindings", [])

    for act in acts:
        act_uri = act["act"]["value"]
        date_doc = act.get("dateDoc", {}).get("value", "")

        doc = fetch_document(act_uri)
        if doc:
            doc["date_document"] = date_doc
            yield doc


def normalize(raw: dict) -> dict:
    """Transform raw data into standard schema."""
    eli_uri = raw.get("eli_uri", "")
    eli_id = eli_uri.replace("https://fedlex.data.admin.ch/eli/", "")

    return {
        "_id": eli_id,
        "_source": "CH/Fedlex",
        "_type": "legislation",
        "_fetched_at": datetime.utcnow().isoformat() + "Z",
        "eli_uri": eli_uri,
        "expression_uri": raw.get("expression_uri", ""),
        "title": raw.get("title", ""),
        "text": raw.get("text", ""),
        "language": raw.get("language", ""),
        "date": raw.get("date_document", ""),
        "process_type": raw.get("process_type", ""),
        "genre": raw.get("genre", ""),
        "type_document": raw.get("type_document", ""),
        "url": f"https://www.fedlex.admin.ch/eli/{eli_id}",
        "file_url": raw.get("file_url", "")
    }


def bootstrap_sample(sample_dir: Path, sample_count: int = 12):
    """Fetch sample documents for testing."""
    print(f"Fetching {sample_count} sample documents...", file=sys.stderr)

    sample_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    total_chars = 0

    for raw_doc in fetch_all():
        if count >= sample_count:
            break

        normalized = normalize(raw_doc)

        # Validate full text
        text = normalized.get("text", "")
        if not text or len(text) < 100:
            print(f"Skipping {normalized['_id']}: insufficient text ({len(text)} chars)", file=sys.stderr)
            continue

        # Save to sample directory
        safe_id = normalized["_id"].replace("/", "_")
        sample_file = sample_dir / f"{safe_id}.json"

        with open(sample_file, "w", encoding="utf-8") as f:
            json.dump(normalized, f, ensure_ascii=False, indent=2)

        print(f"Saved: {normalized['_id']} ({len(text)} chars)", file=sys.stderr)
        total_chars += len(text)
        count += 1

    print(f"\nBootstrap complete: {count} documents, avg {total_chars // max(count, 1)} chars/doc", file=sys.stderr)
    return count


def main():
    parser = argparse.ArgumentParser(description="CH/Fedlex Swiss Legislation Fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Bootstrap command
    bootstrap_parser = subparsers.add_parser("bootstrap", help="Fetch sample documents")
    bootstrap_parser.add_argument("--sample", action="store_true", help="Fetch sample data")
    bootstrap_parser.add_argument("--count", type=int, default=12, help="Number of samples")

    # Update command
    update_parser = subparsers.add_parser("update", help="Fetch recent updates")
    update_parser.add_argument("--days", type=int, default=7, help="Days to look back")
    update_parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()

    script_dir = Path(__file__).parent
    sample_dir = script_dir / "sample"

    if args.command == "bootstrap":
        if args.sample:
            bootstrap_sample(sample_dir, args.count)
        else:
            print("Use --sample flag to fetch sample data", file=sys.stderr)
    elif args.command == "update":
        since = datetime.utcnow() - timedelta(days=args.days)
        for doc in fetch_updates(since):
            print(json.dumps(normalize(doc), ensure_ascii=False))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
