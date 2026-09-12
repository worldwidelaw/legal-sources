#!/usr/bin/env python3
"""
INTL/INTERPOL-Legal -- INTERPOL Constitution and Legal Framework

Fetches INTERPOL's core governance documents (constitution, general regulations,
rules of procedure, etc.) from the official Legal Documents page.

~11 English-language PDF documents covering INTERPOL's legal framework.

Data access:
  - Legal documents page: /Who-we-are/Legal-framework/Legal-documents
  - Direct PDF downloads from interpol.int/content/download/
  - Text extracted from PDFs using pdfminer

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Same (only ~11 documents)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import io
import json
import logging
import re
import time
from html import unescape
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import unquote

import requests

try:
    from pdfminer.high_level import extract_text
    HAS_PDFMINER = True
except ImportError:
    HAS_PDFMINER = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.INTERPOL-Legal")

SOURCE_ID = "INTL/INTERPOL-Legal"
BASE_URL = "https://www.interpol.int"
LEGAL_DOCS_URL = BASE_URL + "/Who-we-are/Legal-framework/Legal-documents"
DELAY = 2.0

# Map of document IDs to metadata (title, reference number, year)
# Derived from the filenames and document content
DOCUMENT_METADATA = {
    "590": {"title": "Constitution of the ICPO-INTERPOL", "ref": "I/CONS/GA/1956", "year": "2024"},
    "591": {"title": "General Regulations of the ICPO-INTERPOL", "ref": "I/GR/GA/1956", "year": "2025"},
    "592": {"title": "Financial Regulations of the ICPO-INTERPOL", "ref": "III/FREG/GA/1967", "year": "2024"},
    "5693": {"title": "Rules of Procedure of the General Assembly", "ref": "I/RPGA/GA/1956", "year": "2025"},
    "5694": {"title": "Rules on the Processing of Data", "ref": "III/RPD/GA/2011", "year": "2025"},
    "5695": {"title": "Statute of the Commission for the Control of INTERPOL's Files", "ref": "CCF/Statute", "year": "2025"},
    "12626": {"title": "Repository of Practice on Articles 2 and 3 of the Constitution", "ref": "RoP/Art2-3", "year": "2024"},
    "16992": {"title": "Rules of Procedure of the Executive Committee", "ref": "I/RPEC", "year": "2021"},
    "20741": {"title": "Rules for the Organisation of Sessions of the General Assembly", "ref": "I/ROSGA", "year": "2024"},
    "20742": {"title": "Terms of Reference for Regional Conferences", "ref": "I/TOR/RC", "year": "2024"},
    "22446": {"title": "Implementing Rules on Settlement of Disputes", "ref": "III/IRSD", "year": "2024"},
}


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    return session


def extract_pdf_text(content: bytes) -> str:
    """Extract text from PDF bytes using pdfminer."""
    if not HAS_PDFMINER:
        logger.error("pdfminer not available — cannot extract PDF text")
        return ""
    try:
        text = extract_text(io.BytesIO(content))
        # Clean up: normalize whitespace, remove excessive blank lines
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = text.strip()
        return text
    except Exception as e:
        logger.error("PDF extraction error: %s", e)
        return ""


class InterpolLegalScraper:
    SOURCE_ID = SOURCE_ID

    def __init__(self):
        self.session = get_session()

    def _get(self, url: str, stream: bool = False) -> Optional[requests.Response]:
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=60, stream=stream)
                if resp.status_code == 200:
                    return resp
                if resp.status_code == 429:
                    wait = 15 * (attempt + 1)
                    logger.warning("Rate limited, waiting %ds...", wait)
                    time.sleep(wait)
                    continue
                logger.warning("HTTP %d for %s", resp.status_code, url)
                return None
            except requests.RequestException as e:
                logger.warning("Request error (attempt %d): %s", attempt + 1, e)
                time.sleep(5)
        return None

    def discover_documents(self) -> List[Dict[str, str]]:
        """Scrape the legal documents page to find all English PDF links."""
        resp = self._get(LEGAL_DOCS_URL)
        if not resp:
            logger.error("Failed to fetch legal documents page")
            return []

        html = resp.text
        # Find all PDF links from interpol.int/content/download/
        all_pdfs = re.findall(
            r'href="(https://www\.interpol\.int/content/download/[^"]*\.pdf[^"]*)"',
            html, re.IGNORECASE
        )

        documents = []
        seen = set()
        for pdf_url in all_pdfs:
            pdf_url = unescape(pdf_url)
            # Skip non-English versions
            if "/fr/" in pdf_url or "/es/" in pdf_url or "/ar/" in pdf_url or "/en/" in pdf_url:
                continue
            base = pdf_url.split("?")[0]
            if base in seen:
                continue
            seen.add(base)

            # Extract download ID from URL
            match = re.search(r"/download/(\d+)/", pdf_url)
            doc_id = match.group(1) if match else None

            # Extract filename for fallback title
            fname_match = re.search(r"/file/(.+)$", base)
            filename = unquote(fname_match.group(1)) if fname_match else ""

            documents.append({
                "url": pdf_url,
                "doc_id": doc_id,
                "filename": filename,
            })

        logger.info("Discovered %d English PDF documents", len(documents))
        return documents

    def normalize(self, doc_info: Dict[str, str], text: str) -> Dict[str, Any]:
        """Normalize an INTERPOL legal document into standard schema."""
        doc_id = doc_info.get("doc_id", "")
        meta = DOCUMENT_METADATA.get(doc_id, {})

        title = meta.get("title", "")
        if not title:
            # Derive title from filename
            fname = doc_info.get("filename", "")
            title = re.sub(r"^\d+\s*[A-Z]?\s*", "", fname)
            title = re.sub(r"\.pdf$", "", title, flags=re.IGNORECASE)
            title = re.sub(r"[_]", " ", title).strip()
            if not title:
                title = f"INTERPOL Legal Document {doc_id}"

        ref = meta.get("ref", "")
        year = meta.get("year", "")
        date = f"{year}-01-01" if year else None

        return {
            "_id": f"INTERPOL-{doc_id}" if doc_id else f"INTERPOL-{hash(doc_info['url']) % 100000}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": doc_info["url"],
            "document_number": ref or None,
            "filename": doc_info.get("filename"),
            "organization": "INTERPOL",
        }

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Yield all INTERPOL legal documents with full text."""
        if not HAS_PDFMINER:
            logger.error("pdfminer is required for PDF text extraction. Install: pip install pdfminer.six")
            return

        documents = self.discover_documents()
        if not documents:
            logger.error("No documents discovered")
            return

        count = 0
        for doc in documents:
            time.sleep(DELAY)
            logger.info("Downloading: %s", doc.get("filename", doc["url"]))

            resp = self._get(doc["url"])
            if not resp:
                logger.warning("Failed to download %s", doc["url"])
                continue

            text = extract_pdf_text(resp.content)
            if not text or len(text) < 100:
                logger.warning("Insufficient text extracted from %s (%d chars)",
                              doc.get("filename", ""), len(text))
                continue

            record = self.normalize(doc, text)
            count += 1
            logger.info("[%d] %s: %d chars", count, record["title"][:60], len(text))
            yield record

        logger.info("Total records yielded: %d", count)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Same as fetch_all — only ~11 documents, always re-fetch all."""
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        logger.info("Testing INTERPOL legal documents page...")
        resp = self._get(LEGAL_DOCS_URL)
        if not resp:
            logger.error("Cannot reach legal documents page")
            return False
        logger.info("Page OK (%d bytes)", len(resp.content))

        docs = self.discover_documents()
        if not docs:
            logger.error("No documents found on page")
            return False
        logger.info("Found %d documents", len(docs))

        # Test one PDF download
        test_doc = docs[0]
        logger.info("Testing PDF download: %s", test_doc.get("filename", ""))
        resp = self._get(test_doc["url"])
        if not resp:
            logger.error("Cannot download PDF")
            return False

        text = extract_pdf_text(resp.content)
        if text and len(text) > 100:
            logger.info("PDF extraction OK: %d chars", len(text))
            return True
        else:
            logger.error("PDF extraction failed or too short")
            return False


def main():
    import argparse
    parser = argparse.ArgumentParser(description="INTL/INTERPOL-Legal data fetcher")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Fetch sample records (same as full for this small source)")
    parser.add_argument("--since", type=str, default=None,
                        help="ISO date for incremental updates (YYYY-MM-DD)")
    parser.add_argument("--output", type=str, default=None,
                        help="Output JSONL file path")
    args = parser.parse_args()

    scraper = InterpolLegalScraper()

    if args.command == "test":
        ok = scraper.test()
        sys.exit(0 if ok else 1)

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command == "bootstrap":
        records = scraper.fetch_all(sample=args.sample)
    elif args.command == "update":
        records = scraper.fetch_updates(args.since or "2024-01-01")
    else:
        parser.print_help()
        sys.exit(1)

    output_path = args.output
    if output_path:
        outfile = open(output_path, "w")
    else:
        outfile = None

    count = 0
    for record in records:
        count += 1
        safe_id = re.sub(r"[^a-zA-Z0-9_-]", "_", record["_id"])
        sample_path = sample_dir / f"{safe_id}.json"
        with open(sample_path, "w", encoding="utf-8") as f:
            json.dump(record, f, indent=2, ensure_ascii=False)

        if outfile:
            outfile.write(json.dumps(record, ensure_ascii=False) + "\n")

    if outfile:
        outfile.close()

    logger.info("Done. %d records processed.", count)
    if count == 0:
        logger.error("No records fetched")
        sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
