#!/usr/bin/env python3
"""
Swedish Parliament (Riksdag) Parliamentary Documents Fetcher

Official open data from the Riksdag (Swedish Parliament)
https://data.riksdagen.se

This fetcher retrieves parliamentary documents with full text:
- Propositioner (prop): Government bills
- Motioner (mot): Member motions
- Betänkanden (bet): Committee reports
- Riksdagsskrivelser (rskr): Parliamentary decisions

Data structure:
- List API: /dokumentlista/?doktyp={type}&utformat=json
- Full JSON: /dokument/{dok_id}.json (includes HTML full text)

No authentication required. Data is public domain.
"""

import json
import logging
import re
import subprocess
import sys
import time
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Constants
BASE_URL = "https://data.riksdagen.se"
LIST_URL = f"{BASE_URL}/dokumentlista/"
DOC_URL = f"{BASE_URL}/dokument"

# Document types to fetch
DOC_TYPES = {
    "prop": "Government Bill (Proposition)",
    "mot": "Motion",
    "bet": "Committee Report (Betänkande)",
    "rskr": "Parliamentary Decision (Riksdagsskrivelse)",
}


class HTMLTextExtractor(HTMLParser):
    """Extract plain text from HTML"""

    def __init__(self):
        super().__init__()
        self.text_parts = []
        self.skip_tags = {"script", "style", "head", "meta", "link"}
        self.current_skip = False

    def handle_starttag(self, tag, attrs):
        if tag.lower() in self.skip_tags:
            self.current_skip = True

    def handle_endtag(self, tag):
        if tag.lower() in self.skip_tags:
            self.current_skip = False
        elif tag.lower() in {"p", "div", "br", "h1", "h2", "h3", "h4", "h5", "h6", "li"}:
            self.text_parts.append("\n")

    def handle_data(self, data):
        if not self.current_skip:
            self.text_parts.append(data)

    def get_text(self):
        return "".join(self.text_parts)


def extract_text_from_html(html: str) -> str:
    """Extract plain text from HTML content"""
    if not html:
        return ""
    parser = HTMLTextExtractor()
    try:
        parser.feed(html)
        text = parser.get_text()
    except Exception:
        # Fallback: simple regex cleanup
        text = re.sub(r"<[^>]+>", " ", html)

    # Clean up whitespace
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r" {2,}", " ", text)
    text = re.sub(r"^\s+", "", text, flags=re.MULTILINE)
    return text.strip()


class RiksdagenFetcher:
    """Fetcher for Swedish Parliament documents"""

    def __init__(self):
        self.session = requests.Session()

        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Accept": "application/json, text/plain, */*",
            }
        )

    def _fetch_document_list(
        self, doktyp: str, page: int = 1, from_date: str = None
    ) -> Dict:
        """Fetch a page of documents using curl for reliability"""
        params = f"doktyp={doktyp}&utformat=json&p={page}&sort=datum&sortorder=desc"
        if from_date:
            params += f"&from={from_date}"

        url = f"{LIST_URL}?{params}"

        for attempt in range(5):
            try:
                result = subprocess.run(
                    ["curl", "-s", "--max-time", "60", "-L", url],
                    capture_output=True,
                    text=True,
                    timeout=70,
                )
                if result.returncode == 0 and result.stdout:
                    return json.loads(result.stdout)
                else:
                    logger.warning(f"Curl list failed on attempt {attempt+1}")
                    time.sleep(5 * (attempt + 1))
            except subprocess.TimeoutExpired:
                logger.warning(f"List timeout on attempt {attempt+1}")
                time.sleep(5 * (attempt + 1))
            except json.JSONDecodeError as e:
                logger.warning(f"JSON decode error on attempt {attempt+1}: {e}")
                time.sleep(5 * (attempt + 1))
            except Exception as e:
                logger.warning(f"Connection error on attempt {attempt+1}: {e}")
                if attempt < 4:
                    time.sleep(5 * (attempt + 1))
                else:
                    raise
        raise Exception("Failed to fetch document list after 5 attempts")

    def _fetch_document_json(self, dok_id: str) -> Optional[Dict]:
        """Fetch full JSON with HTML content"""
        url = f"{DOC_URL}/{dok_id}.json"

        for attempt in range(3):
            try:
                result = subprocess.run(
                    ["curl", "-s", "--max-time", "120", "-L", url],
                    capture_output=True,
                    text=True,
                    timeout=130,
                )
                if result.returncode == 0 and result.stdout:
                    return json.loads(result.stdout)
                else:
                    logger.warning(f"Curl JSON failed for {dok_id}, attempt {attempt+1}")
                    time.sleep(5 * (attempt + 1))
            except subprocess.TimeoutExpired:
                logger.warning(f"JSON timeout for {dok_id}, attempt {attempt+1}")
                time.sleep(5 * (attempt + 1))
            except json.JSONDecodeError as e:
                logger.warning(f"JSON decode error for {dok_id}: {e}")
                time.sleep(3)
            except Exception as e:
                logger.warning(f"Error fetching {dok_id}: {e}")
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
        return None

    def fetch_all(
        self, limit: int = None, doc_types: list = None
    ) -> Iterator[Dict[str, Any]]:
        """
        Fetch Swedish Parliament documents with full text.

        Args:
            limit: Maximum total documents to fetch (None for all)
            doc_types: List of document types to fetch (default: all)

        Yields:
            Raw document dictionaries with full text
        """
        if doc_types is None:
            doc_types = list(DOC_TYPES.keys())

        count = 0
        per_type_limit = (limit // len(doc_types) + 1) if limit else None

        for doktyp in doc_types:
            logger.info(f"Fetching {DOC_TYPES.get(doktyp, doktyp)} documents...")
            page = 1
            type_count = 0

            while True:
                if per_type_limit and type_count >= per_type_limit:
                    break
                if limit and count >= limit:
                    return

                logger.info(f"  [{doktyp}] Fetching page {page}...")
                try:
                    data = self._fetch_document_list(doktyp=doktyp, page=page)
                except Exception as e:
                    logger.error(f"Failed to fetch {doktyp} page {page}: {e}")
                    break

                doc_list = data.get("dokumentlista", {})
                documents = doc_list.get("dokument", [])

                if not documents:
                    break

                for doc in documents:
                    if per_type_limit and type_count >= per_type_limit:
                        break
                    if limit and count >= limit:
                        return

                    dok_id = doc.get("dok_id", "")
                    if not dok_id:
                        continue

                    title = doc.get("titel", "")[:60]
                    logger.info(f"  [{count+1}] Fetching: {title}...")

                    # Fetch full JSON with HTML content
                    full_doc = self._fetch_document_json(dok_id)

                    if full_doc:
                        doc_data = full_doc.get("dokumentstatus", {}).get("dokument", {})
                        html_content = doc_data.get("html", "")

                        if html_content and len(html_content) > 200:
                            doc["full_text"] = extract_text_from_html(html_content)
                            doc["html_length"] = len(html_content)
                            doc["full_doc"] = doc_data  # Store full metadata
                            yield doc
                            count += 1
                            type_count += 1
                        else:
                            logger.warning(f"  Skipping {dok_id}: no HTML content")
                    else:
                        logger.warning(f"  Skipping {dok_id}: fetch failed")

                    time.sleep(2)  # Rate limiting

                # Check for next page
                next_page = doc_list.get("@nasta_sida")
                if not next_page:
                    break
                page += 1

        logger.info(f"Fetched {count} documents with full text")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch documents modified since a given date"""
        from_date = since.strftime("%Y-%m-%d")
        return self.fetch_all(limit=None, doc_types=list(DOC_TYPES.keys()))

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize document to standard schema"""
        dok_id = raw_doc.get("dok_id", "")
        full_doc = raw_doc.get("full_doc", {})

        # Build URL
        url = f"https://www.riksdagen.se/sv/dokument-och-lagar/dokument/{raw_doc.get('doktyp', '')}/{dok_id}/"

        # Parse date
        date_str = raw_doc.get("datum", "")

        # Get document type info
        doktyp = raw_doc.get("doktyp", "")
        doc_type_name = DOC_TYPES.get(doktyp, doktyp)

        # Get rm (parliamentary session) and beteckning (document number)
        rm = raw_doc.get("rm", "")
        beteckning = raw_doc.get("beteckning", "")

        return {
            "_id": dok_id,
            "_source": "SE/RiksdagenDB",
            "_type": "legislation",
            "_fetched_at": datetime.now().isoformat(),
            "title": raw_doc.get("titel", ""),
            "subtitle": raw_doc.get("undertitel", ""),
            "text": raw_doc.get("full_text", ""),
            "date": date_str if date_str else None,
            "published": raw_doc.get("publicerad", ""),
            "url": url,
            "language": "sv",
            "document_type": doktyp,
            "document_type_name": doc_type_name,
            "session": rm,
            "document_number": beteckning,
            "organ": raw_doc.get("organ", ""),
            "summary": raw_doc.get("summary", ""),
        }


def main():
    """Main entry point for testing and bootstrap"""

    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap":
        fetcher = RiksdagenFetcher()
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        logger.info("Starting bootstrap...")

        sample_count = 0
        target_count = 12 if "--sample" in sys.argv else 100

        # Fetch 3 of each document type for sample
        doc_types = ["prop", "mot", "bet", "rskr"]
        per_type = max(target_count // len(doc_types), 3)

        for raw_doc in fetcher.fetch_all(limit=target_count + 10, doc_types=doc_types):
            if sample_count >= target_count:
                break

            normalized = fetcher.normalize(raw_doc)
            text_len = len(normalized.get("text", ""))

            if text_len < 200:
                logger.warning(f"Skipping {normalized['_id']}: text too short ({text_len})")
                continue

            # Save to sample directory
            doc_id = normalized["_id"].replace("/", "_").replace(":", "-")
            filename = f"{doc_id}.json"
            filepath = sample_dir / filename

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            logger.info(
                f"Saved [{sample_count+1}/{target_count}]: {normalized['document_type']} {normalized['document_number']} - {normalized['title'][:40]} ({text_len} chars)"
            )
            sample_count += 1

        logger.info(f"Bootstrap complete. Saved {sample_count} documents to {sample_dir}")

        # Print summary
        files = list(sample_dir.glob("*.json"))
        total_chars = 0
        type_counts = {}
        for f in files:
            with open(f, "r", encoding="utf-8") as fp:
                data = json.load(fp)
                total_chars += len(data.get("text", ""))
                dtype = data.get("document_type", "unknown")
                type_counts[dtype] = type_counts.get(dtype, 0) + 1

        print(f"\n=== SUMMARY ===")
        print(f"Sample files: {len(files)}")
        print(f"Total text chars: {total_chars:,}")
        print(f"Average chars/doc: {total_chars // max(len(files), 1):,}")
        print(f"Document types: {type_counts}")

    else:
        # Test mode
        fetcher = RiksdagenFetcher()
        print("Testing Swedish Parliament fetcher...")

        count = 0
        for raw_doc in fetcher.fetch_all(limit=3, doc_types=["prop"]):
            normalized = fetcher.normalize(raw_doc)
            print(f"\n--- Document {count + 1} ---")
            print(f"ID: {normalized['_id']}")
            print(f"Type: {normalized['document_type_name']}")
            print(f"Session: {normalized['session']}")
            print(f"Number: {normalized['document_number']}")
            print(f"Title: {normalized['title'][:80]}")
            print(f"Date: {normalized['date']}")
            print(f"Text length: {len(normalized.get('text', ''))}")
            print(f"Text preview: {normalized.get('text', '')[:300]}...")
            count += 1


if __name__ == "__main__":
    main()
