#!/usr/bin/env python3
"""
AI/FSCLegislation -- Anguilla Financial Services Commission Legislation

Fetches legislation PDFs from fsc.org.ai and extracts full text using pdfplumber.
The legislation page lists ~64 documents with links to docviewer pages, each
embedding a PDF from the documents directory.

Source: https://fsc.org.ai/legislation.php
Rate limit: 0.5 req/sec (be polite to small gov server)

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import io
import html as html_mod
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List
from urllib.parse import quote, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AI.FSCLegislation")

BASE_URL = "https://fsc.org.ai"
LEGISLATION_URL = f"{BASE_URL}/legislation.php"


class FSCLegislationScraper(BaseScraper):
    """
    Scraper for AI/FSCLegislation -- Anguilla FSC Legislation.
    Country: AI
    URL: https://fsc.org.ai/legislation.php

    Data types: legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            timeout=60,
        )

    def _get_document_list(self) -> List[dict]:
        """Scrape the legislation page for all document links and titles."""
        self.rate_limiter.wait()
        resp = self.client.get(LEGISLATION_URL, timeout=30)
        content = resp.text

        # Extract all docviewer links with their titles
        links = re.findall(
            r'<a[^>]*href="(docviewer\.php\?doc=[^"]+)"[^>]*>(.*?)</a>',
            content, re.DOTALL
        )

        # Also try to determine category by finding section headers
        docs = []
        current_category = ""

        # Parse the page to extract categories and links
        # Categories are in <h4> or <strong> tags before groups of links
        sections = re.split(r'<h4[^>]*>(.*?)</h4>', content, flags=re.DOTALL)

        for i, section in enumerate(sections):
            if i > 0 and i % 2 == 1:
                current_category = re.sub(r'<[^>]+>', '', section).strip()
                continue

            section_links = re.findall(
                r'<a[^>]*href="(docviewer\.php\?doc=[^"]+)"[^>]*>(.*?)</a>',
                section, re.DOTALL
            )
            for href, text in section_links:
                clean_text = re.sub(r'<[^>]+>', '', text).strip()
                clean_text = html_mod.unescape(clean_text)
                if clean_text:
                    doc_id = href.split('doc=')[1] if 'doc=' in href else href
                    docs.append({
                        "doc_id": doc_id,
                        "title": clean_text,
                        "category": current_category,
                        "viewer_url": f"{BASE_URL}/{href}",
                    })

        # Deduplicate by doc_id
        seen = set()
        unique_docs = []
        for d in docs:
            if d["doc_id"] not in seen:
                seen.add(d["doc_id"])
                unique_docs.append(d)

        return unique_docs

    def _get_pdf_url(self, viewer_url: str) -> Optional[str]:
        """Extract the actual PDF URL from a docviewer page."""
        self.rate_limiter.wait()
        try:
            resp = self.client.get(viewer_url, timeout=30)
            content = resp.text
            # Find the PDF src in an embed or iframe
            match = re.search(
                r'src="(documents/[^"#]+\.pdf)',
                content, re.IGNORECASE
            )
            if match:
                pdf_path = match.group(1)
                return f"{BASE_URL}/{pdf_path}"
        except Exception as e:
            logger.warning(f"Failed to get PDF URL from {viewer_url}: {e}")
        return None

    def _extract_text_from_pdf(self, pdf_url: str) -> str:
        """Download a PDF and extract text using pdfplumber."""
        try:
            import pdfplumber
        except ImportError:
            logger.error("pdfplumber not installed")
            return ""

        self.rate_limiter.wait()
        try:
            resp = self.client.get(pdf_url, timeout=60)
            pdf_bytes = resp.content

            text_parts = []
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass

            return "\n\n".join(text_parts)
        except Exception as e:
            logger.warning(f"Failed to extract text from {pdf_url}: {e}")
            return ""

    def _parse_date_from_title(self, title: str) -> Optional[str]:
        """Try to extract a year from the document title."""
        match = re.search(r'\b(19\d{2}|20\d{2})\b', title)
        if match:
            return f"{match.group(1)}-01-01"
        return None

    def normalize(self, raw: dict) -> dict:
        """Transform a raw record into the standard schema."""
        doc_id = raw.get("doc_id", "unknown")
        return {
            "_id": f"AI-FSCLeg-{doc_id}",
            "_source": "AI/FSCLegislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("pdf_url", raw.get("viewer_url", LEGISLATION_URL)),
            "category": raw.get("category", ""),
        }

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Not supported."""
        return
        yield

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all legislation documents from FSC."""
        logger.info("Fetching document list...")
        docs = self._get_document_list()
        logger.info(f"Found {len(docs)} documents")

        for i, doc in enumerate(docs):
            logger.info(f"[{i+1}/{len(docs)}] {doc['title']}")

            pdf_url = self._get_pdf_url(doc["viewer_url"])
            if not pdf_url:
                logger.warning(f"  No PDF URL found, skipping")
                continue

            text = self._extract_text_from_pdf(pdf_url)
            if len(text.strip()) < 50:
                logger.warning(f"  Insufficient text ({len(text)} chars), skipping")
                continue

            doc["text"] = text
            doc["pdf_url"] = pdf_url
            doc["date"] = self._parse_date_from_title(doc["title"])

            record = self.normalize(doc)
            yield record
            logger.info(f"  OK: {len(text)} chars")

        logger.info("Completed")

    def fetch_sample(self, n: int = 15) -> List[dict]:
        """Fetch a sample of documents."""
        logger.info(f"Fetching {n} sample records...")
        docs = self._get_document_list()
        logger.info(f"Found {len(docs)} documents, sampling first {n}")

        samples = []
        for doc in docs:
            if len(samples) >= n:
                break

            pdf_url = self._get_pdf_url(doc["viewer_url"])
            if not pdf_url:
                continue

            text = self._extract_text_from_pdf(pdf_url)
            if len(text.strip()) < 50:
                continue

            doc["text"] = text
            doc["pdf_url"] = pdf_url
            doc["date"] = self._parse_date_from_title(doc["title"])

            record = self.normalize(doc)
            samples.append(record)
            logger.info(f"  [{len(samples)}/{n}] {doc['title']}: {len(text)} chars")

        logger.info(f"Collected {len(samples)} samples")
        return samples

    def test_api(self):
        """Test connectivity and document listing."""
        logger.info("Testing FSC legislation access...")
        docs = self._get_document_list()
        logger.info(f"Documents: {len(docs)}")
        for d in docs[:5]:
            logger.info(f"  - {d['title']} ({d['category']})")

        if docs:
            pdf_url = self._get_pdf_url(docs[0]["viewer_url"])
            logger.info(f"  First PDF URL: {pdf_url}")
            if pdf_url:
                text = self._extract_text_from_pdf(pdf_url)
                logger.info(f"  Extracted text: {len(text)} chars")

    @staticmethod
    def cli():
        import argparse

        parser = argparse.ArgumentParser(description="AI/FSCLegislation bootstrap")
        parser.add_argument("command", choices=["bootstrap", "test-api"])
        parser.add_argument("--sample", action="store_true")
        parser.add_argument("--full", action="store_true")
        args = parser.parse_args()

        scraper = FSCLegislationScraper()

        if args.command == "test-api":
            scraper.test_api()
            return

        if args.command == "bootstrap":
            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)

            if args.sample:
                records = scraper.fetch_sample(15)
            else:
                records = list(scraper.fetch_all())

            for i, record in enumerate(records):
                out_path = sample_dir / f"{i:04d}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)

            logger.info(f"Wrote {len(records)} records to {sample_dir}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    FSCLegislationScraper.cli()
