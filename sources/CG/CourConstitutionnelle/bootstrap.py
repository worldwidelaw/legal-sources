#!/usr/bin/env python3
"""
CG/CourConstitutionnelle -- Congo-Brazzaville Constitutional Court Decisions

Fetches decisions from the Cour Constitutionnelle de la République du Congo
via their Next.js JSON API and extracts full text from PDF attachments.

Strategy:
  - JSON API at /api/documents?type=DECISION_COUR_CONSTITUTIONNELLE&page=N&limit=20
  - Also fetches AVIS (opinions) — same API, different type
  - Each document has a fileUrl pointing to a PDF
  - Download PDF, extract text with PyPDF2
  - ~54 documents total (41 decisions + 2 avis + others)

Usage:
  python bootstrap.py bootstrap          # Fetch all documents
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import io
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CG.CourConstitutionnelle")

BASE_URL = "https://cour-constitutionnelle.cg"
API_URL = f"{BASE_URL}/api/documents"

# Document types to fetch (case_law relevant)
DOC_TYPES = [
    "DECISION_COUR_CONSTITUTIONNELLE",
    "DECISION_CONSEIL_CONSTITUTIONNEL",
    "DECISION_COUR_SUPREME",
    "AVIS",
]

PAGE_SIZE = 20
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "LegalDataHunter/1.0 (open-data research; +https://github.com/worldwidelaw/legal-sources)",
    "Accept": "application/json",
})


def _extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using PyPDF2."""
    try:
        import PyPDF2
        reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
        parts = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                parts.append(text)
        return "\n\n".join(parts).strip()
    except Exception as e:
        logger.warning("PyPDF2 extraction failed: %s", e)
        return ""


def _clean_text(text: str) -> str:
    """Clean up extracted PDF text."""
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    lines = []
    for line in text.split("\n"):
        lines.append(line.strip())
    return "\n".join(lines).strip()


class CourConstitutionnelleScraper(BaseScraper):

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        yield from self._fetch_documents(sample=False)

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        yield from self._fetch_documents(sample=False)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        doc_id = raw.get("numero") or raw.get("id", "")
        doc_id = doc_id.strip()

        date_str = raw.get("dateDecision") or raw.get("createdAt")
        date_iso = None
        if date_str:
            try:
                dt = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                date_iso = dt.strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                date_iso = None

        title = (raw.get("title") or "").strip()
        description = (raw.get("description") or "").strip()
        if description and description != title:
            full_title = f"{title} — {description}" if title else description
        else:
            full_title = title

        doc_type = raw.get("type", "")
        type_label = {
            "DECISION_COUR_CONSTITUTIONNELLE": "Décision de la Cour constitutionnelle",
            "DECISION_CONSEIL_CONSTITUTIONNEL": "Décision du Conseil constitutionnel",
            "DECISION_COUR_SUPREME": "Décision de la Cour suprême",
            "AVIS": "Avis",
        }.get(doc_type, doc_type)

        file_url = raw.get("fileUrl", "")
        if file_url and not file_url.startswith("http"):
            file_url = BASE_URL + file_url

        return {
            "_id": doc_id or raw.get("id", ""),
            "_source": "CG/CourConstitutionnelle",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": full_title,
            "text": raw.get("text", ""),
            "date": date_iso,
            "url": file_url or f"{BASE_URL}/decisions/cour-constitutionnelle",
            "decision_number": doc_id,
            "document_type": type_label,
            "keywords": raw.get("keywords", ""),
            "year": raw.get("year"),
        }

    def _fetch_type(self, doc_type: str, limit: int) -> Generator[Dict[str, Any], None, None]:
        """Fetch all documents of a given type."""
        page = 1
        count = 0

        while count < limit:
            url = f"{API_URL}?type={doc_type}&page={page}&limit={PAGE_SIZE}"
            logger.info("Fetching %s page %d ...", doc_type, page)

            try:
                self.rate_limiter.wait()
                resp = SESSION.get(url, timeout=30)
                resp.raise_for_status()
                data = resp.json()
            except Exception as e:
                logger.error("API request failed: %s", e)
                break

            documents = data.get("documents", [])
            if not documents:
                break

            pagination = data.get("pagination", {})
            total = pagination.get("total", 0)
            logger.info("  Page %d: %d docs (total: %d)", page, len(documents), total)

            for doc in documents:
                if count >= limit:
                    break

                file_url = doc.get("fileUrl", "")
                if file_url and not file_url.startswith("http"):
                    file_url = BASE_URL + file_url

                # Skip oversized PDFs (likely scanned, >5MB)
                file_size = doc.get("fileSize", 0)
                if file_size > 5 * 1024 * 1024:
                    logger.warning("Skipping oversized PDF (%d KB): %s",
                                   file_size // 1024, doc.get("numero"))
                    continue

                text = ""
                if file_url:
                    text = self._download_and_extract(file_url)

                if not text:
                    logger.warning("No text extracted for %s", doc.get("numero") or doc.get("id"))
                    continue

                doc["text"] = text
                normalized = self.normalize(doc)
                yield normalized
                count += 1
                logger.info("  [%d] %s — %d chars", count, normalized["decision_number"], len(text))

            if page * PAGE_SIZE >= total:
                break
            page += 1

    def _download_and_extract(self, url: str) -> str:
        """Download a PDF and extract its text."""
        try:
            self.rate_limiter.wait()
            resp = SESSION.get(url, timeout=60)
            resp.raise_for_status()

            if len(resp.content) < 500:
                logger.warning("PDF too small (%d bytes): %s", len(resp.content), url)
                return ""

            text = _extract_pdf_text(resp.content)
            return _clean_text(text)
        except Exception as e:
            logger.error("PDF download/extraction failed for %s: %s", url, e)
            return ""

    def _fetch_documents(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all documents across all relevant types."""
        count = 0
        sample_limit = 15 if sample else 999999

        for doc_type in DOC_TYPES:
            if count >= sample_limit:
                break
            for record in self._fetch_type(doc_type, sample_limit - count):
                yield record
                count += 1


def main():
    import argparse

    parser = argparse.ArgumentParser(description="CG/CourConstitutionnelle bootstrapper")
    parser.add_argument("command",
                        choices=["bootstrap", "bootstrap-fast", "test", "update"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Only fetch ~15 sample records")
    args = parser.parse_args()

    source_dir = Path(__file__).parent
    scraper = CourConstitutionnelleScraper(str(source_dir))

    if args.command == "test":
        logger.info("Testing API connectivity...")
        try:
            resp = SESSION.get(f"{API_URL}?page=1&limit=1", timeout=15)
            resp.raise_for_status()
            data = resp.json()
            total = data.get("pagination", {}).get("total", 0)
            logger.info("API OK — %d total documents", total)
        except Exception as e:
            logger.error("API test failed: %s", e)
            sys.exit(1)
        return

    # Only `bootstrap --sample` writes the 15-record validation set to sample/.
    # bootstrap-fast (VPS fleet entrypoint) and a plain full `bootstrap` sweep
    # the whole corpus and STREAM to data/records.jsonl via the storage manager.
    # fetch_all() here yields ALREADY-NORMALIZED records, so we write them
    # directly (routing through BaseScraper.bootstrap would double-normalize and
    # drop the _id/date). Previously the full run wrote into sample/ and
    # bootstrap-fast wasn't recognized at all → the fleet ingested only ~15.
    sample_mode = args.sample and args.command == "bootstrap"

    if sample_mode:
        sample_dir = source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)
        records = []
        for record in scraper._fetch_documents(sample=True):
            records.append(record)
            fname = re.sub(r'[^\w\-.]', '_', record["_id"])[:80] + ".json"
            with open(sample_dir / fname, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
        logger.info("Done. %d records saved to %s", len(records), sample_dir)
        text_lens = [len(r.get("text", "")) for r in records]
        if text_lens:
            logger.info(
                "Text stats: min=%d, max=%d, avg=%d chars",
                min(text_lens), max(text_lens), sum(text_lens) // len(text_lens),
            )
    else:
        written = 0
        for record in scraper.fetch_all():
            if not record.get("_id"):
                continue
            scraper.storage.write(scraper._dedup_key(record), record)
            written += 1
        scraper.storage.flush()
        logger.info("Full bootstrap complete: %d records → data/records.jsonl", written)


if __name__ == "__main__":
    main()
