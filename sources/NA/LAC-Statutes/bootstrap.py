#!/usr/bin/env python3
"""
NA/LAC-Statutes -- Namibia Legal Assistance Centre Annotated Statutes

Fetches ~500 annotated statutes with full text from the Legal Assistance
Centre (LAC) website. Text extracted from Word (.docx) documents.

Strategy:
  - Scrape the statutes listing page (HTML table)
  - Download each statute as .docx (falls back to PDF)
  - Extract full text using python-docx (or pdfplumber for PDF)
  - 2-second delay between downloads

Usage:
  python bootstrap.py bootstrap          # Fetch all statutes
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import subprocess
import tempfile
import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import unquote, urljoin

from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NA.LAC-Statutes")

BASE_URL = "https://www.lac.org.na"
LISTING_URL = f"{BASE_URL}/index.php/laws/statutes/"


class LACStatutesScraper(BaseScraper):
    """Scraper for NA/LAC-Statutes -- Namibian annotated statutes."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _request(self, url: str, timeout: int = 60, binary: bool = False) -> Optional[Any]:
        """HTTP GET via curl (more reliable with this server's SSL config)."""
        for attempt in range(3):
            try:
                time.sleep(2)
                result = subprocess.run(
                    ["curl", "-sL", "-k", "--max-time", str(timeout),
                     "-H", "User-Agent: Legal-Data-Hunter/1.0",
                     "-o", "-", "-w", "\n%{http_code}", url],
                    capture_output=True, timeout=timeout + 10,
                )
                output = result.stdout
                # Last line is the HTTP status code
                parts = output.rsplit(b"\n", 1)
                if len(parts) == 2:
                    body, status_line = parts
                    status_code = int(status_line.strip())
                else:
                    body = output
                    status_code = 200

                if status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                if status_code == 404:
                    logger.warning(f"404: {url}")
                    return None
                if status_code >= 400:
                    logger.warning(f"HTTP {status_code}: {url}")
                    if attempt < 2:
                        time.sleep(5)
                    continue

                return body if binary else body.decode("utf-8", errors="replace")
            except (subprocess.TimeoutExpired, Exception) as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(5)
        return None

    def _parse_listing(self, html: str) -> List[Dict[str, str]]:
        """Parse the statutes listing table."""
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table")
        if not table:
            logger.error("No table found on listing page")
            return []

        entries = []
        rows = table.find_all("tr")
        for row in rows[1:]:  # skip header
            cells = row.find_all("td")
            if len(cells) < 2:
                continue

            # Cell 0: Law name (may have a link or just text)
            law_cell = cells[0]
            # Get text parts separately to avoid concatenation (e.g., "Act 5Regulations")
            title_parts = []
            for s in law_cell.stripped_strings:
                title_parts.append(s)
            title = " ".join(title_parts).strip()
            # Clean up common concatenation issues
            title = re.sub(r"(\d)(Regulations|Proclamation|Act|Ordinance)", r"\1 \2", title)

            # Cell 1: Document links (docx + pdf)
            link_cell = cells[1]
            links = link_cell.find_all("a", href=True)
            docx_url = None
            pdf_url = None
            for a in links:
                href = a["href"]
                if href.lower().endswith(".docx"):
                    docx_url = href if href.startswith("http") else urljoin(BASE_URL, href)
                elif href.lower().endswith(".pdf"):
                    pdf_url = href if href.startswith("http") else urljoin(BASE_URL, href)

            if not docx_url and not pdf_url:
                continue

            # Cell 2: Keyword
            keyword = cells[2].get_text(strip=True) if len(cells) > 2 else ""
            # Cell 3: Additional keyword
            keyword2 = cells[3].get_text(strip=True) if len(cells) > 3 else ""
            # Cell 4: Comments
            comments = cells[4].get_text(strip=True) if len(cells) > 4 else ""

            # If title is empty, try to extract from URL
            if not title and (docx_url or pdf_url):
                url_for_name = docx_url or pdf_url
                filename = unquote(url_for_name.split("/")[-1])
                title = re.sub(r"\.(docx|pdf)$", "", filename)

            if not title:
                continue

            entries.append({
                "title": title,
                "docx_url": docx_url,
                "pdf_url": pdf_url,
                "keyword": keyword,
                "keyword2": keyword2,
                "comments": comments,
            })

        return entries

    def _extract_text_docx(self, content: bytes) -> Optional[str]:
        """Extract text from a Word document."""
        try:
            import docx
            import io
            doc = docx.Document(io.BytesIO(content))
            paragraphs = [p.text for p in doc.paragraphs if p.text.strip()]
            text = "\n".join(paragraphs)
            return text if len(text) > 50 else None
        except Exception as e:
            logger.warning(f"docx extraction failed: {e}")
            return None

    def _extract_text_pdf(self, content: bytes) -> Optional[str]:
        """Extract text from a PDF document."""
        try:
            import pdfplumber
            import io
            text_parts = []
            with pdfplumber.open(io.BytesIO(content)) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass
            text = "\n".join(text_parts)
            return text if len(text) > 50 else None
        except Exception as e:
            logger.warning(f"PDF extraction failed: {e}")
            return None

    def _extract_year(self, title: str) -> Optional[str]:
        """Extract year from statute title like 'Act 8 of 2022'."""
        match = re.search(r"of\s*(\d{4})", title)
        if match:
            return match.group(1)
        match = re.search(r"(1[89]\d{2}|20\d{2})", title)
        if match:
            return match.group(1)
        return None

    def _make_id(self, title: str) -> str:
        """Create a stable ID from the statute title."""
        clean = re.sub(r"[^\w\s-]", "", title)
        clean = re.sub(r"\s+", "-", clean.strip())
        return clean[:120]

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all statutes with full text."""
        yield from self._fetch(sample=False)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch updates — re-fetches all since no update mechanism."""
        yield from self.fetch_all()

    def _fetch(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Core fetch logic."""
        logger.info("Fetching statutes listing page...")
        html = self._request(LISTING_URL)
        if not html:
            logger.error("Failed to fetch listing page")
            return

        entries = self._parse_listing(html)
        logger.info(f"Found {len(entries)} statutes")

        if sample:
            # Pick a diverse sample: first 5, middle 5, last 5
            n = len(entries)
            indices = list(range(min(5, n)))
            if n > 10:
                mid = n // 2
                indices += list(range(mid, min(mid + 5, n)))
            if n > 15:
                indices += list(range(max(n - 5, 0), n))
            indices = sorted(set(indices))
            entries = [entries[i] for i in indices if i < len(entries)]
            logger.info(f"Sample mode: fetching {len(entries)} statutes")

        fetched = 0
        for i, entry in enumerate(entries):
            title = entry["title"]
            logger.info(f"[{i+1}/{len(entries)}] {title}")

            # Try docx first, then PDF
            text = None
            if entry["docx_url"]:
                content = self._request(entry["docx_url"], binary=True)
                if content:
                    text = self._extract_text_docx(content)

            if not text and entry["pdf_url"]:
                content = self._request(entry["pdf_url"], binary=True)
                if content:
                    text = self._extract_text_pdf(content)

            if not text:
                logger.warning(f"No text extracted for: {title}")
                continue

            year = self._extract_year(title)
            record = self.normalize({
                "title": title,
                "text": text,
                "year": year,
                "url": entry["pdf_url"] or entry["docx_url"],
                "docx_url": entry["docx_url"],
                "keyword": entry["keyword"],
                "keyword2": entry["keyword2"],
                "comments": entry["comments"],
            })

            fetched += 1
            yield record

        logger.info(f"Fetched {fetched}/{len(entries)} statutes with full text")

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a statute record."""
        title = raw["title"]
        doc_id = self._make_id(title)
        year = raw.get("year")
        date_str = f"{year}-01-01" if year else None

        return {
            "_id": doc_id,
            "_source": "NA/LAC-Statutes",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw["text"],
            "date": date_str,
            "url": raw["url"],
            "docx_url": raw.get("docx_url"),
            "keyword": raw.get("keyword", ""),
            "keyword2": raw.get("keyword2", ""),
            "comments": raw.get("comments", ""),
        }

    def test_connection(self) -> bool:
        """Test connectivity to LAC website."""
        html = self._request(LISTING_URL)
        if html and "annoSTAT" in html:
            logger.info("Connection successful — found statute links")
            return True
        logger.error("Connection test failed")
        return False


def main():
    scraper = LACStatutesScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        success = scraper.test_connection()
        sys.exit(0 if success else 1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        if sample_mode:
            for record in scraper._fetch(sample=True):
                count += 1
                fname = sample_dir / f"{count:04d}.json"
                with open(fname, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                logger.info(f"  Saved {fname.name} — {record['title'][:60]} ({len(record['text'])} chars)")
        else:
            for record in scraper.fetch_all():
                count += 1
                if count <= 15:
                    fname = sample_dir / f"{count:04d}.json"
                    with open(fname, "w", encoding="utf-8") as f:
                        json.dump(record, f, ensure_ascii=False, indent=2)

        logger.info(f"Bootstrap complete: {count} records")
        print(json.dumps({"_source": "NA/LAC-Statutes", "records": count}))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
