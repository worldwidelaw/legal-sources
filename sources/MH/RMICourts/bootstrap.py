#!/usr/bin/env python3
"""
MH/RMICourts -- Republic of Marshall Islands Judiciary

Fetches court decisions from the official RMI Judiciary website
(rmicourts.org). Covers three courts:
  - Supreme Court (recent opinions/orders)
  - High Court (selected decisions)
  - Traditional Rights Court (decisions)

All decisions are individual PDFs. The scraper:
  1. Visits each court's decisions page
  2. Extracts all PDF links and associated metadata
  3. Downloads and extracts text from each PDF

Usage:
  python bootstrap.py bootstrap          # Fetch all documents
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import io
import sys
import time
import logging
import re
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MH.RMICourts")

BASE_URL = "https://rmicourts.org"

COURT_PAGES = [
    {
        "court": "Supreme Court",
        "url": f"{BASE_URL}/supreme-court-decisions/recent-supreme-court-decisions/",
    },
    {
        "court": "High Court",
        "url": f"{BASE_URL}/selected-high-court-decisions/",
    },
    {
        "court": "Traditional Rights Court",
        "url": f"{BASE_URL}/traditional-rights-court-decisions/",
    },
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


class RMICourtsScraper(BaseScraper):
    """Scraper for MH/RMICourts -- Marshall Islands Judiciary."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _request(self, url: str, timeout: int = 30) -> Optional[requests.Response]:
        """HTTP GET with retry and rate limiting."""
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code in (404, 410):
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning("Attempt %d failed for %s: %s", attempt + 1, url[:80], e)
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
        return None

    def _extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download PDF and extract text."""
        resp = self._request(pdf_url, timeout=60)
        if resp is None:
            return None

        # Try common/pdf_extract first
        try:
            from common.pdf_extract import extract_pdf_markdown
            md = extract_pdf_markdown(
                source="MH/RMICourts",
                source_id=hashlib.md5(pdf_url.encode()).hexdigest()[:16],
                pdf_bytes=resp.content,
                table="case_law",
            )
            if md and len(md) >= 50:
                return md
        except Exception:
            pass

        # Fallback: pdfplumber
        try:
            import pdfplumber
            with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
                parts = []
                for page in pdf.pages:
                    t = page.extract_text()
                    if t:
                        parts.append(t.strip())
                text = "\n\n".join(parts)
                if len(text) >= 50:
                    return text
        except Exception as e:
            logger.warning("pdfplumber failed for %s: %s", pdf_url[:60], e)

        # Fallback: pypdf
        try:
            import pypdf
            reader = pypdf.PdfReader(io.BytesIO(resp.content))
            parts = []
            for page in reader.pages:
                t = page.extract_text()
                if t:
                    parts.append(t.strip())
            text = "\n\n".join(parts)
            if len(text) >= 50:
                return text
        except Exception as e:
            logger.warning("pypdf failed for %s: %s", pdf_url[:60], e)

        return None

    def _parse_date_from_filename(self, filename: str) -> Optional[str]:
        """Try to extract a date from the PDF filename."""
        # Pattern: YYMMDD at start (e.g., 260331-..., 240604-...)
        m = re.match(r"(\d{2})(\d{2})(\d{2})", filename)
        if m:
            yy, mm, dd = int(m.group(1)), int(m.group(2)), int(m.group(3))
            # Two-digit year: 00-50 -> 2000s, 51-99 -> 1900s
            year = 2000 + yy if yy <= 50 else 1900 + yy
            if 1 <= mm <= 12 and 1 <= dd <= 31:
                try:
                    return f"{year}-{mm:02d}-{dd:02d}"
                except ValueError:
                    pass

        # Pattern: YY.MM.DD (e.g., 23.02.15...)
        m = re.match(r"(\d{2})\.(\d{2})\.(\d{2})", filename)
        if m:
            yy, mm, dd = int(m.group(1)), int(m.group(2)), int(m.group(3))
            year = 2000 + yy if yy <= 50 else 1900 + yy
            if 1 <= mm <= 12 and 1 <= dd <= 31:
                try:
                    return f"{year}-{mm:02d}-{dd:02d}"
                except ValueError:
                    pass

        return None

    def _discover_decisions(self, max_docs: Optional[int] = None) -> List[Dict[str, str]]:
        """Discover all court decision PDFs from the three court pages."""
        documents = []
        seen_urls = set()

        for court_info in COURT_PAGES:
            court_name = court_info["court"]
            page_url = court_info["url"]

            logger.info("Fetching %s decisions from %s", court_name, page_url)
            resp = self._request(page_url)
            if resp is None:
                logger.warning("Failed to fetch %s page", court_name)
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            # Find all PDF links in the main content area
            content = soup.find("div", class_="entry-content") or soup.find("article") or soup
            for a_tag in content.find_all("a", href=True):
                href = a_tag["href"]
                if not href.lower().endswith(".pdf"):
                    continue

                pdf_url = urljoin(page_url, href)

                # Skip non-decision PDFs (forms, rules, admin docs)
                filename = pdf_url.split("/")[-1].lower()
                # Normalize underscores to hyphens for consistent matching
                fn_normalized = filename.replace("_", "-")
                skip_keywords = [
                    "annual-financial", "periodic-financial", "personnel-guide",
                    "operating-procedures", "admission", "application-form",
                    "professional-conduct", "vmv-stmts", "court-policies",
                    "impvmnt-plan", "time-standards", "coop-plan",
                    "cash-bail", "court-costs", "continuance-policy",
                    "kabua-land", "tobin-land", "majurovillageinm",
                    "pifs-land", "kajin-jikin", "legal-glossary",
                    "ttr-and-milr", "rules-of-admission", "evidence-act",
                    "bail-schedule", "rmi-judiciary-personnel",
                    "rmicjc", "code-of-conduct", "legal-aid-fund",
                ]
                if any(kw in fn_normalized for kw in skip_keywords):
                    continue

                # Skip court rules PDFs
                rules_keywords = [
                    "scrp", "mircp", "mircrp", "trc-rules", "change-of-name",
                    "remote-proceedings", "juvenile-rules", "appellate-rules",
                    "rules-of-procedure", "rules-of-", "const.pdf",
                    "2025-const", "constitution",
                ]
                if any(kw in fn_normalized for kw in rules_keywords):
                    continue

                if pdf_url in seen_urls:
                    continue
                seen_urls.add(pdf_url)

                # Extract title from link text
                title = a_tag.get_text(strip=True)
                if not title or len(title) < 5:
                    title = filename.replace(".pdf", "").replace("-", " ").replace("_", " ")

                # Try to extract date from filename
                date = self._parse_date_from_filename(filename)

                # Generate a stable ID from the PDF URL
                doc_id = hashlib.md5(pdf_url.encode()).hexdigest()[:12]

                documents.append({
                    "id": doc_id,
                    "title": title,
                    "court": court_name,
                    "pdf_url": pdf_url,
                    "date": date,
                    "url": pdf_url,
                })

                if max_docs and len(documents) >= max_docs:
                    return documents

            logger.info("%s: found %d decision PDFs so far", court_name, len(documents))

        logger.info("Total decisions discovered: %d", len(documents))
        return documents

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all court decision records with full text."""
        documents = self._discover_decisions()
        total = len(documents)

        for i, doc in enumerate(documents, 1):
            logger.info("[%d/%d] Processing: %s", i, total, doc["title"][:80])

            text = self._extract_pdf_text(doc["pdf_url"])
            if not text:
                logger.warning("No text extracted for: %s", doc["title"][:80])
                continue

            yield self.normalize({**doc, "text": text})

    def fetch_updates(self, since: Optional[str] = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch updates — for this source, same as fetch_all."""
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw record into the standard schema."""
        return {
            "_id": f"MH-RMICourts-{raw['id']}",
            "_source": "MH/RMICourts",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "court": raw.get("court"),
            "url": raw.get("url", raw.get("pdf_url", "")),
        }


def main():
    scraper = RMICourtsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        logger.info("Testing connectivity to rmicourts.org...")
        resp = scraper._request(f"{BASE_URL}/")
        if resp:
            logger.info("OK — status %d, %d bytes", resp.status_code, len(resp.content))
        else:
            logger.error("FAILED — could not reach rmicourts.org")
            sys.exit(1)
        return

    if command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        max_docs = 15 if sample_mode else None

        if sample_mode:
            documents = scraper._discover_decisions(max_docs=max_docs)
        else:
            documents = scraper._discover_decisions()

        sample_dir = scraper.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        import json
        count = 0
        total = len(documents)

        for i, doc in enumerate(documents, 1):
            logger.info("[%d/%d] Processing: %s", i, total, doc["title"][:80])

            text = scraper._extract_pdf_text(doc["pdf_url"])
            if not text:
                logger.warning("No text extracted for: %s", doc["title"][:80])
                continue

            record = scraper.normalize({**doc, "text": text})
            count += 1

            if sample_mode or count <= 20:
                out_path = sample_dir / f"{record['_id']}.json"
                with open(out_path, "w") as f:
                    json.dump(record, f, indent=2, ensure_ascii=False)
                logger.info("Saved sample: %s (%d chars text)", record["_id"], len(text))

            if sample_mode and count >= 15:
                break

        logger.info("Bootstrap complete: %d records with full text out of %d PDFs", count, total)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
