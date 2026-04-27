#!/usr/bin/env python3
"""
CO/CorteSuprema -- Colombian Supreme Court Jurisprudence Fetcher

Fetches decisions from Colombia's Corte Suprema de Justicia via GraphQL API
at consultaprovidenciasbk.cortesuprema.gov.co. Full text extracted from
downloaded .docx files (ZIP/XML).

Rooms: Civil, Laboral, Penal, Tutelas (~961K total documents).

Usage:
  python bootstrap.py test-api
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap
"""

import argparse
import io
import json
import logging
import re
import sys
import time
import zipfile
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator, Optional
from urllib.parse import quote

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown


try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

SOURCE_ID = "CO/CorteSuprema"
SOURCE_DIR = Path(__file__).parent
SAMPLE_DIR = SOURCE_DIR / "sample"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CO.CorteSuprema")

API_URL = "https://consultaprovidenciasbk.cortesuprema.gov.co/api"
DOWNLOAD_URL = "https://consultaprovidenciasbk.cortesuprema.gov.co/downloadFile"
FILTERS_URL = "https://consultaprovidenciasbk.cortesuprema.gov.co/filters"

ROOMS = ["Civil", "Laboral", "Penal", "Tutelas"]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Content-Type": "application/json",
}

PAGE_SIZE = 10
REQUEST_DELAY = 1.5


def graphql_query(query: str, retries: int = 3) -> dict:
    """Execute a GraphQL query with retry logic."""
    for attempt in range(retries):
        try:
            resp = requests.post(
                API_URL,
                json={"query": query},
                headers=HEADERS,
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            if "errors" in data:
                logger.warning("GraphQL errors: %s", data["errors"])
                return {}
            return data.get("data", {})
        except requests.RequestException as e:
            logger.warning("Request failed (attempt %d/%d): %s", attempt + 1, retries, e)
            if attempt < retries - 1:
                time.sleep(2 ** (attempt + 1))
    return {}


def search_documents(room: str, year: str = "", start: int = 0,
                     auto_sentencia: str = "") -> tuple:
    """Search documents in a room. Returns (total_count, results_list)."""
    query = """
    {
      getSearchResult(searchQuery: {
        query: "*",
        typeOfQuery: "%s",
        start: %d,
        isExact: false,
        magistrate: "",
        year: "%s",
        autoSentencia: "%s",
        order: "reciente",
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
        }
      }
    }
    """ % (room, start, year, auto_sentencia)

    data = graphql_query(query)
    result = data.get("getSearchResult", {})
    return result.get("numOfResults", 0), result.get("searchResults", [])


def get_content_html(doc_id: str, room: str) -> str:
    """Fetch document content via GraphQL getContentSearch."""
    escaped_id = doc_id.replace('"', '\\"')
    query = """
    {
      getContentSearch(previewDocument: {
        id: "%s",
        room: "%s"
      }) {
        contentText
        title
        id
      }
    }
    """ % (escaped_id, room)

    data = graphql_query(query)
    result = data.get("getContentSearch", {})
    return result.get("contentText", "")


def download_docx_text(doc_path: str) -> str:
    """Download a .docx file and extract text from word/document.xml."""
    try:
        resp = requests.post(
            DOWNLOAD_URL,
            json={"path": doc_path},
            headers=HEADERS,
            timeout=60,
        )
        resp.raise_for_status()

        if len(resp.content) < 100:
            return ""

        # Check if it's actually a ZIP (docx)
        if resp.content[:2] != b'PK':
            # Might be HTML error page
            return ""

        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            if 'word/document.xml' not in zf.namelist():
                return ""
            with zf.open('word/document.xml') as f:
                xml_content = f.read().decode('utf-8', errors='replace')

        # Extract text from XML
        text = re.sub(r'<[^>]+>', '', xml_content)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    except (requests.RequestException, zipfile.BadZipFile, KeyError) as e:
        logger.warning("Failed to download/extract docx %s: %s", doc_path, e)
        return ""


def download_pdf_text(doc_path: str) -> str:
    """Download a PDF and extract text. Falls back gracefully if no PDF library."""
    try:
        resp = requests.post(
            DOWNLOAD_URL,
            json={"path": doc_path},
            headers=HEADERS,
            timeout=60,
        )
        resp.raise_for_status()

        if len(resp.content) < 100 or resp.content[:5] != b'%PDF-':
            return ""

        # Try pdfplumber first, then PyPDF2
        logger.warning("No PDF library available, skipping PDF: %s", doc_path)
        return ""

    except requests.RequestException as e:
        logger.warning("Failed to download PDF %s: %s", doc_path, e)
        return ""


def is_junk_file(doc_id: str) -> bool:
    """Check if a document path is a junk/temp file that should be skipped."""
    basename = Path(doc_id).name
    if basename.startswith("~$") or basename.startswith("~BROMIUM"):
        return True
    if basename in ("Thumbs.db", "desktop.ini", ".DS_Store"):
        return True
    if basename.startswith("prueba"):
        return True
    if not basename.lower().endswith(('.docx', '.pdf', '.doc')):
        return True
    return False


def fetch_full_text(doc_id: str, room: str) -> str:
    """Fetch full text of a document. Tries docx download first, then PDF, then GraphQL."""
    ext = Path(doc_id).suffix.lower()

    # Build list of paths to try: prefer .docx, then .doc (might be mislabeled docx), then .pdf
    attempts = []
    if ext == '.docx':
        attempts.append(('docx', doc_id))
        attempts.append(('pdf', re.sub(r'\.docx$', '.pdf', doc_id)))
    elif ext == '.pdf':
        attempts.append(('docx', re.sub(r'\.pdf$', '.docx', doc_id)))
        attempts.append(('pdf', doc_id))
    elif ext == '.doc':
        # .doc files may actually be docx inside — try docx extraction first
        docx_path = re.sub(r'\.doc$', '.docx', doc_id)
        attempts.append(('docx', docx_path))
        attempts.append(('docx', doc_id))  # try raw .doc as if it were docx
        attempts.append(('pdf', re.sub(r'\.doc$', '.pdf', doc_id)))
    else:
        attempts.append(('docx', doc_id))

    for method, path in attempts:
        if method == 'docx':
            text = download_docx_text(path)
        else:
            text = download_pdf_text(path)
        if text and len(text) > 100:
            return text

    # Fall back to GraphQL content (often just headers though)
    html_content = get_content_html(doc_id, room)
    if html_content:
        text = clean_html(html_content)
        if len(text) > 100:
            return text

    return ""


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


def make_document_id(doc_id: str) -> str:
    """Create a clean document ID from the file path."""
    # Extract the filename without extension
    filename = Path(doc_id).stem
    # Remove temp file prefixes (~$)
    filename = re.sub(r'^~\$', '', filename)
    return filename


def make_url(doc_id: str) -> str:
    """Construct a URL for viewing the document."""
    # The frontend URL pattern: /visualizador/:id/:sala/:text
    return "https://consultaprovidencias.cortesuprema.gov.co/"


def normalize(raw: dict, room: str) -> dict:
    """Transform raw API result into standard schema."""
    doc_id = raw.get("id", "")
    title = raw.get("title", "")
    # Clean temp file names
    title = re.sub(r'^~\$', '', title)
    # Remove file extension from title
    title = re.sub(r'\.(docx?|pdf)$', '', title)

    date_str = raw.get("fechaCreacion")
    if date_str:
        try:
            dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
            date_str = dt.strftime("%Y-%m-%d")
        except (ValueError, AttributeError):
            date_str = None

    year = raw.get("ano")
    if not date_str and year:
        date_str = f"{year}-01-01"

    magistrate = raw.get("doctor", "")
    auto_sentencia = raw.get("autoSentencia", "")

    clean_id = make_document_id(doc_id)

    return {
        "_id": f"CO/CorteSuprema/{room}/{clean_id}",
        "_source": SOURCE_ID,
        "_type": "case_law",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": "",  # filled in later
        "date": date_str,
        "url": f"https://consultaprovidencias.cortesuprema.gov.co/",
        "sala": room,
        "magistrate": magistrate,
        "year": year,
        "auto_sentencia": auto_sentencia,
        "original_path": doc_id,
    }


def fetch_all(rooms: list = None, years: list = None,
              max_per_room: int = 0) -> Generator[dict, None, None]:
    """Yield all documents with full text."""
    if rooms is None:
        rooms = ROOMS

    for room in rooms:
        logger.info("Processing room: %s", room)
        year_list = years if years else [""]

        for year in year_list:
            total, results = search_documents(room, year=year, start=0)
            logger.info("Room %s, year %s: %d results", room, year or "all", total)

            if max_per_room and total > max_per_room:
                total = max_per_room

            start = 0
            yielded = 0
            while start < total:
                if start > 0:
                    _, results = search_documents(room, year=year, start=start)

                if not results:
                    break

                for raw in results:
                    doc_id = raw.get("id", "")
                    if is_junk_file(doc_id):
                        continue

                    record = normalize(raw, room)

                    # Fetch full text
                    time.sleep(REQUEST_DELAY)
                    text = fetch_full_text(doc_id, room)
                    if not text:
                        logger.warning("No text for: %s", doc_id)
                        continue

                    record["text"] = text
                    yield record
                    yielded += 1

                    if max_per_room and yielded >= max_per_room:
                        break

                if max_per_room and yielded >= max_per_room:
                    break

                start += PAGE_SIZE
                time.sleep(REQUEST_DELAY)


def fetch_updates(since: str) -> Generator[dict, None, None]:
    """Fetch documents modified since a date."""
    # The API doesn't support date filtering directly,
    # so we search recent years and filter by date
    try:
        since_dt = datetime.fromisoformat(since)
    except ValueError:
        since_dt = datetime.now(timezone.utc).replace(year=datetime.now().year - 1)

    current_year = datetime.now().year
    years = [str(y) for y in range(since_dt.year, current_year + 1)]

    for record in fetch_all(years=years):
        if record.get("date") and record["date"] >= since:
            yield record


def save_sample(records: list) -> None:
    """Save sample records to the sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    for i, record in enumerate(records):
        filepath = SAMPLE_DIR / f"sample_{i:03d}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        logger.info("Saved sample %d: %s (text: %d chars)", i, record["title"], len(record.get("text", "")))


def test_api():
    """Test API connectivity and basic queries."""
    print("Testing filters endpoint...")
    try:
        resp = requests.get(FILTERS_URL, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        filters = resp.json()
        print(f"  Filters OK: {len(filters)} entries")
        for item in filters:
            for key in item:
                if key != '_id':
                    years = item[key].get('years', {}).get('data', [])
                    print(f"  Room: {key}, years: {years[0] if years else '?'}-{years[-1] if years else '?'}")
    except Exception as e:
        print(f"  FAILED: {e}")
        return

    print("\nTesting search (Civil, 2024, sentencias)...")
    total, results = search_documents("Civil", year="2024", auto_sentencia="SENTENCIA")
    print(f"  Results: {total}")
    if results:
        first = results[0]
        print(f"  First: {first.get('title')}")
        print(f"  Date: {first.get('fechaCreacion')}")
        print(f"  Doctor: {first.get('doctor')}")

        # Test full text
        doc_id = first.get("id", "")
        if doc_id and "~$" not in doc_id:
            print(f"\nTesting full text extraction for: {first.get('title')}...")
            text = fetch_full_text(doc_id, "Civil")
            print(f"  Text length: {len(text)} chars")
            if text:
                print(f"  Preview: {text[:300]}...")

    print("\nTesting download endpoint...")
    if results:
        for r in results[:5]:
            doc_id = r.get("id", "")
            if doc_id.endswith(".docx") and "~$" not in doc_id:
                print(f"  Downloading: {r.get('title')}")
                text = download_docx_text(doc_id)
                print(f"  Text length: {len(text)} chars")
                if text:
                    print(f"  Preview: {text[:200]}...")
                break


def bootstrap(sample: bool = False):
    """Run the bootstrap process."""
    if sample:
        logger.info("Running in SAMPLE mode - fetching ~15 documents across rooms")
        records = []
        # Civil has best file availability; also try other rooms
        sample_plans = [
            ("Civil", "2024", 6),
            ("Civil", "2020", 3),
            ("Laboral", "2020", 3),
            ("Penal", "2020", 3),
        ]
        for room, year, target in sample_plans:
            logger.info("Sampling room: %s year: %s (target: %d)", room, year, target)
            count = 0
            for record in fetch_all(rooms=[room], years=[year], max_per_room=target * 5):
                if len(record.get("text", "")) > 200:
                    records.append(record)
                    count += 1
                if count >= target:
                    break
            time.sleep(REQUEST_DELAY)

        if records:
            save_sample(records)
            # Print summary
            with_text = [r for r in records if r.get("text") and len(r["text"]) > 100]
            print(f"\nSample complete: {len(records)} records, {len(with_text)} with full text")
            for r in records:
                text_len = len(r.get("text", ""))
                print(f"  [{r['sala']}] {r['title']} - {text_len} chars")
        else:
            print("ERROR: No records fetched!")
            sys.exit(1)
    else:
        logger.info("Running FULL bootstrap")
        count = 0
        for record in fetch_all():
            count += 1
            if count % 100 == 0:
                logger.info("Processed %d documents", count)
        logger.info("Bootstrap complete: %d documents", count)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CO/CorteSuprema data fetcher")
    parser.add_argument("command", choices=["test-api", "bootstrap"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Only fetch sample data")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    if args.command == "test-api":
        test_api()
    elif args.command == "bootstrap":
        bootstrap(sample=args.sample)
