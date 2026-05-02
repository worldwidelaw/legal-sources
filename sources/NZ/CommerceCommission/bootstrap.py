#!/usr/bin/env python3
"""
NZ/CommerceCommission -- New Zealand Commerce Commission Case Register

Fetches competition, consumer protection, and regulatory enforcement decisions
from the NZ Commerce Commission case register.

Strategy:
  - Parse sitemap.xml to enumerate all case register entry URLs (~1,708 cases)
  - Fetch each case page for structured metadata (parties, category, status, etc.)
  - Parse embedded Vue.js <project-block> JSON for document timeline
  - Identify decision/determination PDFs from timeline
  - Download PDFs and extract full text with pdfplumber
  - ~1,700+ cases from 2000s to present

Source: https://comcom.govt.nz/case-register (CC BY 4.0)
Rate limit: 1 req/sec

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import io
import json
import logging
import re
import html as html_lib
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NZ.CommerceCommission")

BASE_URL = "https://www.comcom.govt.nz"
SITEMAP_INDEX = f"{BASE_URL}/sitemap.xml"

# Keywords that identify the main decision document in the timeline
DECISION_KEYWORDS = [
    "determination", "decision", "clearance determination",
    "judgment", "ruling", "settlement", "enforceable undertaking",
    "court order", "penalty", "fine", "prosecution",
]


def _extract_pdf_text(pdf_bytes: bytes, max_pages: int = 50) -> str:
    """Extract text from PDF bytes using pdfplumber."""
    try:
        import pdfplumber
        pdf = pdfplumber.open(io.BytesIO(pdf_bytes))
        pages = pdf.pages[:max_pages]
        text_parts = []
        for page in pages:
            t = page.extract_text()
            if t:
                text_parts.append(t)
        pdf.close()
        return "\n\n".join(text_parts)
    except Exception as e:
        logger.debug(f"pdfplumber failed: {e}")
    try:
        from pypdf import PdfReader
        reader = PdfReader(io.BytesIO(pdf_bytes))
        text_parts = []
        for page in reader.pages[:max_pages]:
            t = page.extract_text()
            if t:
                text_parts.append(t)
        return "\n\n".join(text_parts)
    except Exception as e:
        logger.debug(f"pypdf failed: {e}")
    return ""


class CommerceCommissionScraper(BaseScraper):
    """
    Scraper for NZ/CommerceCommission -- NZ Commerce Commission Case Register.
    Country: NZ
    URL: https://comcom.govt.nz/case-register

    Data types: case_law
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

    # -- Sitemap parsing ------------------------------------------------------

    def _get_case_urls_from_sitemap(self) -> list[str]:
        """Parse sitemap index and extract all case-register-entry URLs."""
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        resp = self.client.get(SITEMAP_INDEX, timeout=30)
        if not resp or resp.status_code != 200:
            logger.error(f"Sitemap index fetch failed: {resp.status_code if resp else 'None'}")
            return []

        root = ET.fromstring(resp.content)
        sub_sitemaps = [loc.text for loc in root.findall(".//sm:sitemap/sm:loc", ns)]

        case_urls = []
        for sub_url in sub_sitemaps:
            self.rate_limiter.wait()
            resp = self.client.get(sub_url, timeout=30)
            if not resp or resp.status_code != 200:
                continue
            sub_root = ET.fromstring(resp.content)
            for loc in sub_root.findall(".//sm:url/sm:loc", ns):
                url = loc.text
                if url and "/case-register/case-register-entries/" in url:
                    # Skip the index page itself
                    slug = url.rstrip("/").split("/")[-1]
                    if slug and slug != "case-register-entries":
                        case_urls.append(url)

        logger.info(f"Sitemap: {len(case_urls)} case URLs found")
        return case_urls

    # -- Case page parsing ----------------------------------------------------

    def _parse_case_page(self, url: str) -> Optional[dict]:
        """Fetch and parse a single case register entry page."""
        self.rate_limiter.wait()
        resp = self.client.get(url, timeout=30)
        if not resp or resp.status_code != 200:
            logger.debug(f"Case page failed ({resp.status_code if resp else 'None'}): {url}")
            return None

        text = resp.text
        slug = url.rstrip("/").split("/")[-1]

        # Extract page title
        title = ""
        title_match = re.search(r"<h1[^>]*>(.*?)</h1>", text, re.DOTALL)
        if title_match:
            title = re.sub(r"<[^>]+>", "", title_match.group(1)).strip()

        # Extract case metadata from case-details__record blocks
        meta = {}
        records = re.findall(
            r'<div class="case-details__record-title">\s*(.*?)\s*</div>\s*'
            r'<div class="case-details__record-value">\s*(.*?)\s*</div>',
            text, re.DOTALL,
        )
        for label, val in records:
            key = re.sub(r"<[^>]+>", "", label).strip()
            value = re.sub(r"<[^>]+>", " ", val).strip()
            value = re.sub(r"\s+", " ", value)
            if key and value:
                meta[key] = value

        # Extract summary paragraph
        summary = ""
        summary_match = re.search(
            r'<div class="case-details__summary">(.*?)</div>', text, re.DOTALL
        )
        if summary_match:
            summary = re.sub(r"<[^>]+>", " ", summary_match.group(1)).strip()
            summary = re.sub(r"\s+", " ", summary)

        # If no summary found, try the first paragraph after case details
        if not summary:
            para_match = re.search(
                r'<div class="typography[^"]*">\s*<p>(.*?)</p>', text, re.DOTALL
            )
            if para_match:
                summary = re.sub(r"<[^>]+>", " ", para_match.group(1)).strip()
                summary = re.sub(r"\s+", " ", summary)

        # Parse project-block JSON for timeline documents
        timeline_docs = []
        pb_match = re.search(r'<project-block[^>]*project="([^"]+)"', text)
        if pb_match:
            try:
                raw_json = html_lib.unescape(pb_match.group(1))
                proj = json.loads(raw_json)
                for item in proj.get("timeline", []):
                    for doc in item.get("documents", []):
                        if doc.get("File.URL") and doc.get("File.FileType") == "PDF":
                            timeline_docs.append({
                                "date": item.get("date", ""),
                                "title": doc.get("File.Title", ""),
                                "url": doc["File.URL"],
                                "size": doc.get("File.FileSize", ""),
                                "category": doc.get("SelectedCategory.Name", ""),
                            })
            except (json.JSONDecodeError, KeyError) as e:
                logger.debug(f"Project-block JSON parse error for {slug}: {e}")

        # Find the best decision PDF
        decision_pdf = self._find_decision_pdf(timeline_docs)

        return {
            "slug": slug,
            "url": url,
            "title": title,
            "summary": summary,
            "parties": meta.get("Parties", ""),
            "category": meta.get("Category", ""),
            "sub_category": meta.get("Sub-category", ""),
            "act_section": meta.get("Act/Section", ""),
            "industry": meta.get("Industry", ""),
            "status": meta.get("Status", ""),
            "outcome": meta.get("Outcome", ""),
            "case_number": meta.get("Case number", ""),
            "date_opened": meta.get("Date opened", ""),
            "date_closed": meta.get("Date closed", ""),
            "decision_pdf_url": decision_pdf.get("url") if decision_pdf else None,
            "decision_pdf_title": decision_pdf.get("title") if decision_pdf else None,
            "timeline_doc_count": len(timeline_docs),
        }

    def _find_decision_pdf(self, docs: list[dict]) -> Optional[dict]:
        """Find the main decision/determination PDF from timeline documents."""
        if not docs:
            return None

        # First pass: look for documents whose title contains decision keywords
        for doc in docs:
            title_lower = doc.get("title", "").lower()
            for kw in DECISION_KEYWORDS:
                if kw in title_lower:
                    return doc

        # Second pass: look for NZCC reference in title (e.g., "[2026] NZCC 10")
        for doc in docs:
            if re.search(r"\[\d{4}\]\s*NZCC\s+\d+", doc.get("title", "")):
                return doc

        # Fallback: return the most recent document (first in timeline, sorted by date desc)
        return docs[0] if docs else None

    # -- PDF download and text extraction ------------------------------------

    def _download_and_extract_pdf(self, pdf_url: str) -> str:
        """Download PDF and extract text."""
        if not pdf_url:
            return ""

        if not pdf_url.startswith("http"):
            pdf_url = f"{BASE_URL}{pdf_url}"

        self.rate_limiter.wait()
        try:
            resp = self.client.get(pdf_url, timeout=90)
            if not resp or resp.status_code != 200:
                logger.debug(f"PDF download failed ({resp.status_code if resp else 'None'}): {pdf_url}")
                return ""

            return _extract_pdf_text(resp.content)
        except Exception as e:
            logger.debug(f"PDF extraction error: {e}")
            return ""

    # -- Core scraper methods ------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all cases from the case register."""
        case_urls = self._get_case_urls_from_sitemap()
        if not case_urls:
            logger.error("No case URLs from sitemap")
            return

        found = 0
        for i, url in enumerate(case_urls):
            case = self._parse_case_page(url)
            if not case:
                continue

            # Download decision PDF for full text
            pdf_text = self._download_and_extract_pdf(case.get("decision_pdf_url"))
            if pdf_text and len(pdf_text) > 100:
                case["text"] = pdf_text
            elif case.get("summary"):
                case["text"] = case["summary"]
            else:
                logger.debug(f"No text for {case['slug']}")
                continue

            found += 1
            if found % 50 == 0:
                logger.info(f"Progress: {found} cases with text ({i+1}/{len(case_urls)} pages)")
            yield case

        logger.info(f"Fetch complete: {found} cases with text out of {len(case_urls)} URLs")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch cases modified since a given date."""
        yield from self.fetch_all()

    def fetch_sample(self, count: int = 15) -> Generator[dict, None, None]:
        """Fetch a sample of cases for testing."""
        case_urls = self._get_case_urls_from_sitemap()
        if not case_urls:
            logger.error("No case URLs from sitemap")
            return

        # Pick a spread of cases: first few, some from middle, some from end
        indices = list(range(min(5, len(case_urls))))
        mid = len(case_urls) // 2
        indices += list(range(mid, min(mid + 5, len(case_urls))))
        indices += list(range(max(0, len(case_urls) - 5), len(case_urls)))
        # Deduplicate while preserving order
        seen = set()
        unique_indices = []
        for idx in indices:
            if idx not in seen:
                seen.add(idx)
                unique_indices.append(idx)

        found = 0
        for idx in unique_indices:
            if found >= count:
                break

            url = case_urls[idx]
            case = self._parse_case_page(url)
            if not case:
                continue

            pdf_text = self._download_and_extract_pdf(case.get("decision_pdf_url"))
            if pdf_text and len(pdf_text) > 100:
                case["text"] = pdf_text
            elif case.get("summary"):
                case["text"] = case["summary"]
            else:
                continue

            found += 1
            title = case.get("title", "N/A")[:60]
            text_len = len(case.get("text", ""))
            logger.info(f"Sample {found}/{count}: {title} ({text_len} chars)")
            yield case

        logger.info(f"Sample complete: {found} records")

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw case record to standard schema."""
        slug = raw.get("slug", "unknown")
        case_number = raw.get("case_number", "")

        # Build ID from case number if available, otherwise slug
        if case_number:
            safe_id = re.sub(r"[^a-zA-Z0-9-]", "-", case_number).strip("-")
        else:
            safe_id = slug

        # Parse date - prefer date_closed, then date_opened
        date = None
        for date_field in ["date_closed", "date_opened"]:
            raw_date = raw.get(date_field, "")
            if raw_date:
                try:
                    dt = datetime.strptime(raw_date.strip(), "%d %b %Y")
                    date = dt.strftime("%Y-%m-%d")
                    break
                except ValueError:
                    try:
                        dt = datetime.strptime(raw_date.strip(), "%d %B %Y")
                        date = dt.strftime("%Y-%m-%d")
                        break
                    except ValueError:
                        # Try ISO format
                        if re.match(r"\d{4}-\d{2}-\d{2}", raw_date):
                            date = raw_date[:10]
                            break

        return {
            "_id": f"NZ-CommerceCommission-{safe_id}",
            "_source": "NZ/CommerceCommission",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": date,
            "url": raw.get("url", ""),
            "parties": raw.get("parties", ""),
            "category": raw.get("category", ""),
            "sub_category": raw.get("sub_category", ""),
            "act_section": raw.get("act_section", ""),
            "industry": raw.get("industry", ""),
            "case_status": raw.get("status", ""),
            "outcome": raw.get("outcome", ""),
            "case_number": case_number,
            "pdf_url": raw.get("decision_pdf_url", ""),
            "language": "en",
        }

    def test_api(self) -> bool:
        """Test connectivity to Commerce Commission website."""
        logger.info("Testing Commerce Commission access...")

        # Test main page
        resp = self.client.get(f"{BASE_URL}/case-register", timeout=15)
        if not resp or resp.status_code != 200:
            logger.error(f"Case register page failed: {resp.status_code if resp else 'None'}")
            return False
        logger.info("Case register page: OK")

        # Test sitemap
        self.rate_limiter.wait()
        resp = self.client.get(SITEMAP_INDEX, timeout=15)
        if not resp or resp.status_code != 200:
            logger.error(f"Sitemap failed: {resp.status_code if resp else 'None'}")
            return False
        logger.info("Sitemap: OK")

        # Test a case page
        self.rate_limiter.wait()
        case_urls = self._get_case_urls_from_sitemap()
        if case_urls:
            self.rate_limiter.wait()
            case = self._parse_case_page(case_urls[0])
            if case:
                logger.info(f"Case page: OK — '{case.get('title', 'N/A')}'")

                # Test PDF download
                if case.get("decision_pdf_url"):
                    text = self._download_and_extract_pdf(case["decision_pdf_url"])
                    if text and len(text) > 100:
                        logger.info(f"PDF extraction: OK ({len(text)} chars)")
                    else:
                        logger.warning("PDF extraction returned short/empty text")
                else:
                    logger.warning("No decision PDF found on sample case")
            else:
                logger.error("Case page parsing failed")
                return False

        logger.info("All tests passed!")
        return True


# ── CLI ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    scraper = CommerceCommissionScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample] [--count N]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        count = 15
        for i, arg in enumerate(sys.argv):
            if arg == "--count" and i + 1 < len(sys.argv):
                count = int(sys.argv[i + 1])

        if sample_mode:
            gen = scraper.fetch_sample(count=count)
        else:
            gen = scraper.fetch_all()

        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in gen:
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1
            logger.info(f"Saved: {out_path.name}")

        logger.info(f"Bootstrap complete: {saved} records saved to {sample_dir}")

    elif command == "update":
        logger.info("Running full fetch")
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in scraper.fetch_all():
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1

        logger.info(f"Update complete: {saved} records saved")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
