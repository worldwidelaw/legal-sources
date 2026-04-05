#!/usr/bin/env python3
"""
CO/CENDOJ-Jurisprudencia -- Colombia Supreme Court (GraphQL API)

Fetches Colombian Supreme Court decisions from the CENDOJ unified
jurisprudence portal via GraphQL API. Covers Civil, Penal, and Laboral
chambers with 600K+ decisions.

Strategy:
  - GraphQL search across chambers (wildcard or keyword)
  - Full text via getContentSearch query (returns HTML)
  - Clean HTML to plain text

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "CO/CENDOJ-Jurisprudencia"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CO.CENDOJ-Jurisprudencia")

API_URL = "https://consultaprovidenciasbk.cortesuprema.gov.co/api"

CHAMBERS = ["Civil", "Penal", "Laboral"]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

SEARCH_QUERY = """
query {
  getSearchResult(searchQuery: {
    query: "%s",
    typeOfQuery: "%s",
    start: %d,
    isExact: false,
    magistrate: "",
    year: "%s",
    autoSentencia: "",
    order: "",
    roomTutelas: "",
    addedQueries: []
  }) {
    numOfResults
    searchResults {
      id
      title
      doctor
      fechaCreacion
      ano
      autoSentencia
      typeOfDocument
      onlinePath
    }
  }
}
"""

CONTENT_QUERY = """
query {
  getContentSearch(previewDocument: {
    id: "%s",
    room: "%s",
    text: "%s"
  }) {
    id
    title
    contentText
    onlinePath
    typeOfDocument
    aplicationName
  }
}
"""


def clean_html(html_text: str) -> str:
    """Remove HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_doc_id(title: str) -> str:
    """Extract document ID from title (e.g., 'AC055-2024 [2023-04895-00].docx' -> 'AC055-2024')."""
    match = re.match(r'^([A-Z]+\d+[-/]\d+)', title or "")
    if match:
        return match.group(1)
    # Fallback: strip extension
    return re.sub(r'\.(docx?|pdf)$', '', title or "unknown", flags=re.IGNORECASE).strip()


def graphql_request(query: str, session: requests.Session) -> dict:
    """Execute a GraphQL query."""
    resp = session.post(API_URL, json={"query": query}, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if "errors" in data:
        raise ValueError(f"GraphQL errors: {data['errors']}")
    return data.get("data", {})


def search_chamber(chamber: str, query: str, start: int, year: str,
                   session: requests.Session) -> dict:
    """Search a specific chamber."""
    # Escape quotes in query
    safe_query = query.replace('"', '\\"')
    q = SEARCH_QUERY % (safe_query, chamber, start, year)
    return graphql_request(q, session)


def get_content(doc_id: str, room: str, session: requests.Session,
                search_term: str = "derecho") -> dict:
    """Fetch full text for a document."""
    safe_id = doc_id.replace('"', '\\"')
    safe_term = search_term.replace('"', '\\"')
    q = CONTENT_QUERY % (safe_id, room, safe_term)
    return graphql_request(q, session)


def normalize(raw: dict, chamber: str) -> dict:
    """Normalize a raw document to standard schema."""
    doc_id = extract_doc_id(raw.get("title", ""))
    date_str = raw.get("fechaCreacion", "")
    date = None
    if date_str:
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            date = dt.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            date = None

    text = clean_html(raw.get("contentText", ""))
    magistrate = raw.get("doctor", "") or ""
    # Clean magistrate prefix
    magistrate = re.sub(r'^(Dr\.|Dra\.)\s*', '', magistrate).strip()

    year = raw.get("ano", "")
    auto_sentencia = raw.get("autoSentencia", "") or ""

    return {
        "_id": f"CO-CSJ-{chamber}-{doc_id}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": raw.get("title", "").replace(".docx", "").replace(".pdf", "").strip(),
        "text": text,
        "date": date,
        "url": f"https://consultaprovidencias.cortesuprema.gov.co/",
        "chamber": chamber,
        "magistrate": magistrate,
        "year": year,
        "document_type": auto_sentencia,
    }


def fetch_all(session: requests.Session, sample: bool = False,
              sample_count: int = 15) -> Generator[dict, None, None]:
    """Fetch all decisions across chambers."""
    total_yielded = 0
    per_chamber = (sample_count // len(CHAMBERS)) + 1 if sample else None

    for chamber in CHAMBERS:
        logger.info(f"Fetching {chamber} chamber...")
        chamber_count = 0
        start = 0
        page_size = 10  # API returns 10 per page

        # Use common legal terms to search; "derecho" matches most documents
        search_terms = ["derecho"] if sample else ["*"]

        for term in search_terms:
            while True:
                try:
                    data = search_chamber(chamber, term, start, "", session)
                    result = data.get("getSearchResult", {})
                    total = result.get("numOfResults", 0)
                    results = result.get("searchResults", [])

                    if not results:
                        break

                    # Deduplicate: prefer .docx over .pdf for same document
                    seen_docs = {}
                    for r in results:
                        base_title = re.sub(r'\.(docx?|pdf)$', '', r.get("title", ""), flags=re.IGNORECASE)
                        ext = r.get("title", "").rsplit(".", 1)[-1].lower() if "." in r.get("title", "") else ""
                        if base_title not in seen_docs or ext == "docx":
                            seen_docs[base_title] = r

                    for r in seen_docs.values():
                        # Fetch full text
                        time.sleep(1)
                        try:
                            content_data = get_content(r["id"], chamber, session, search_term=term)
                            content = content_data.get("getContentSearch", {})
                            r["contentText"] = content.get("contentText", "")
                        except Exception as e:
                            logger.warning(f"Failed to get content for {r.get('title')}: {e}")
                            r["contentText"] = ""

                        record = normalize(r, chamber)
                        if record["text"] and len(record["text"]) > 100:
                            yield record
                            total_yielded += 1
                            chamber_count += 1
                            logger.info(f"  [{total_yielded}] {record['title']} ({len(record['text'])} chars)")

                        if sample and chamber_count >= per_chamber:
                            break

                    if sample and chamber_count >= per_chamber:
                        break

                    start += page_size
                    if start >= total:
                        break

                    time.sleep(1.5)

                except Exception as e:
                    logger.error(f"Error fetching {chamber} page {start}: {e}")
                    break

            if sample and chamber_count >= per_chamber:
                break

        logger.info(f"  {chamber}: {chamber_count} documents fetched")
        if sample and total_yielded >= sample_count:
            break

    logger.info(f"Total: {total_yielded} documents fetched")


def test_api():
    """Test the GraphQL API connectivity and response."""
    session = requests.Session()
    print("Testing Colombia Supreme Court GraphQL API...")
    print(f"URL: {API_URL}\n")

    for chamber in CHAMBERS:
        try:
            data = search_chamber(chamber, "*", 0, "", session)
            result = data.get("getSearchResult", {})
            total = result.get("numOfResults", 0)
            results = result.get("searchResults", [])
            print(f"  {chamber}: {total:,} total results, {len(results)} returned")
            if results:
                r = results[0]
                print(f"    First: {r.get('title')} ({r.get('fechaCreacion', 'no date')})")
        except Exception as e:
            print(f"  {chamber}: ERROR - {e}")
        time.sleep(1)

    # Test content fetch
    print("\nTesting full text retrieval...")
    try:
        data = search_chamber("Civil", "*", 0, "", session)
        results = data.get("getSearchResult", {}).get("searchResults", [])
        if results:
            doc = results[0]
            content_data = get_content(doc["id"], "Civil", session)
            content = content_data.get("getContentSearch", {})
            text = clean_html(content.get("contentText", ""))
            print(f"  Document: {doc.get('title')}")
            print(f"  Full text length: {len(text)} chars")
            print(f"  Preview: {text[:300]}...")
    except Exception as e:
        print(f"  Content fetch ERROR: {e}")

    print("\nAPI test complete.")


def bootstrap(sample: bool = False, sample_count: int = 15):
    """Run the bootstrap process."""
    session = requests.Session()

    if sample:
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        logger.info(f"Sample mode: fetching {sample_count} records")

    count = 0
    for record in fetch_all(session, sample=sample, sample_count=sample_count):
        if sample:
            path = SAMPLE_DIR / f"record_{count:04d}.json"
            with open(path, "w", encoding="utf-8") as f:
                json.dump(record, f, indent=2, ensure_ascii=False)

        count += 1
        if sample and count >= sample_count:
            break

    logger.info(f"Bootstrap complete: {count} records")

    if sample:
        # Validate
        errors = []
        for i in range(count):
            path = SAMPLE_DIR / f"record_{i:04d}.json"
            with open(path, "r", encoding="utf-8") as f:
                rec = json.load(f)
            if not rec.get("text"):
                errors.append(f"record_{i:04d}: missing text")
            elif len(rec["text"]) < 100:
                errors.append(f"record_{i:04d}: text too short ({len(rec['text'])} chars)")
            if re.search(r'<[a-z]+[^>]*>', rec.get("text", "")):
                errors.append(f"record_{i:04d}: raw HTML in text")

        if errors:
            logger.warning(f"Validation issues:\n" + "\n".join(errors))
        else:
            logger.info(f"All {count} records validated OK")

    return count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CO/CENDOJ-Jurisprudencia bootstrap")
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("test-api", help="Test API connectivity")

    bp = sub.add_parser("bootstrap", help="Run bootstrap")
    bp.add_argument("--sample", action="store_true", help="Fetch sample only")
    bp.add_argument("--count", type=int, default=15, help="Sample count")

    args = parser.parse_args()

    if args.command == "test-api":
        test_api()
    elif args.command == "bootstrap":
        bootstrap(sample=args.sample, sample_count=args.count)
    else:
        parser.print_help()
