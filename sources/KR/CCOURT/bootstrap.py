#!/usr/bin/env python3
"""
KR/CCOURT -- Constitutional Court of Korea (English Decisions)

Fetches English translations of Korean Constitutional Court decisions.
Scrapes the English case search interface, downloads PDF full texts,
and extracts text using PyPDF2.

Source: https://english.ccourt.go.kr/
Total: ~694 decisions (7 pages at 100/page)

Usage:
  python bootstrap.py bootstrap            # Full pull (~694 decisions)
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py update               # Same as bootstrap
  python bootstrap.py test-api             # Quick connectivity test
"""

import io
import re
import sys
import json
import logging
import time
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
logger = logging.getLogger("legal-data-hunter.KR.CCOURT")

SEARCH_URL = "https://english.ccourt.go.kr/site/eng/decisions/casesearch/caseSearch.do"
PDF_BASE = "https://isearch.ccourt.go.kr/download.do?filePath="
PAGE_SIZE = 100


class CCourtScraper(BaseScraper):
    """Scraper for KR/CCOURT -- Constitutional Court of Korea (English)."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url="",
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            timeout=120,
        )

    def _search_page(self, page: int) -> list[dict]:
        """Fetch one page of case search results. Returns list of case dicts."""
        data = {
            "pageIndex": str(page),
            "pg": str(page),
            "outmax": str(PAGE_SIZE),
            "szIsTab": "1",
            "base64": "n",
            "filterYn": "N",
        }
        self.rate_limiter.wait()
        resp = self.client.session.post(SEARCH_URL, data=data, timeout=120)
        resp.raise_for_status()
        html = resp.text

        cases = []
        li_blocks = re.findall(r"<li>\s*<dl>(.*?)</dl>\s*</li>", html, re.DOTALL)

        for block in li_blocks:
            case = self._parse_case_block(block)
            if case and case.get("pdf_path"):
                cases.append(case)

        return cases

    def _parse_case_block(self, block: str) -> Optional[dict]:
        """Parse a single <li><dl>...</dl></li> case block."""
        case = {}

        # Extract PDF path and Korean case number from fn_PopView call
        pdf_match = re.search(r"'(/st_xml[^']+\.pdf)'", block)
        if pdf_match:
            case["pdf_path"] = pdf_match.group(1)

        kr_case = re.search(r"'(\d{4}헌[가-힣]+\d+)'", block)
        if kr_case:
            case["korean_case_number"] = kr_case.group(1)

        seq_match = re.search(r"'영문판례',\s*'(\d+)'", block)
        if seq_match:
            case["seq"] = seq_match.group(1)

        # Extract English case number from button text
        en_case = re.search(r">(\d{4}Hun-[A-Za-z]+\d+(?:\s*\([^)]+\))?)<", block)
        if en_case:
            case["case_number_en"] = en_case.group(1).strip()

        # Extract case name from button text (second <dt> button)
        buttons = re.findall(r"<dt><button[^>]*>([^<]+)</button></dt>", block)
        if len(buttons) >= 1:
            name = buttons[-1].strip()
            if name and not re.match(r"^\d{4}Hun-", name):
                case["case_name"] = name

        # Extract final decision
        final = re.search(r"<dt>Final decision</dt>\s*<dd>([^<]+)</dd>", block)
        if final:
            case["final_decision"] = final.group(1).strip()

        # Extract decision date
        date_match = re.search(r"<dt>Decision date</dt>\s*<dd>([^<]+)</dd>", block)
        if date_match:
            case["decision_date_raw"] = date_match.group(1).strip()

        # Extract text summary
        text_dd = re.search(r'<dd class="text"><p>(.*?)</p>', block, re.DOTALL)
        if text_dd:
            summary = re.sub(r"<[^>]+>", "", text_dd.group(1)).strip()
            summary = re.sub(r"\s+", " ", summary)
            case["summary"] = summary

        # Extract case name from summary if not found elsewhere
        if "case_name" not in case and case.get("summary"):
            name_match = re.match(r"(Case on .+?)\s*\.\.+", case["summary"])
            if name_match:
                case["case_name"] = name_match.group(1).strip()

        # Derive English case number from summary if not in button
        if "case_number_en" not in case and case.get("summary"):
            en_from_summary = re.search(
                r"\[(\d{4}Hun-[A-Za-z]+\d+[^]]*)\]", case["summary"]
            )
            if en_from_summary:
                case["case_number_en"] = en_from_summary.group(1).strip()

        return case if case.get("pdf_path") else None

    def _parse_date(self, raw_date: str) -> Optional[str]:
        """Parse date like 'Dec 18, 2025' to ISO format."""
        for fmt in ("%b %d, %Y", "%B %d, %Y", "%Y-%m-%d", "%Y. %m. %d"):
            try:
                dt = datetime.strptime(raw_date.strip(" ."), fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
        return None

    def _extract_pdf_text(self, pdf_path: str) -> Optional[str]:
        """Download PDF and extract text using PyPDF2."""
        url = PDF_BASE + pdf_path
        self.rate_limiter.wait()
        try:
            resp = self.client.session.get(url, timeout=120)
            resp.raise_for_status()
            if len(resp.content) < 100:
                logger.warning(f"  PDF too small ({len(resp.content)} bytes): {pdf_path}")
                return None
        except Exception as e:
            logger.warning(f"  Failed to download PDF {pdf_path}: {e}")
            return None

        try:
            import PyPDF2

            reader = PyPDF2.PdfReader(io.BytesIO(resp.content))
            text_parts = []
            for page in reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text_parts.append(page_text)
            text = "\n".join(text_parts)
            # Clean up
            text = re.sub(r"\n{3,}", "\n\n", text)
            text = re.sub(r"[ \t]+", " ", text)
            return text.strip() if text.strip() else None
        except Exception as e:
            logger.warning(f"  Failed to extract PDF text: {e}")
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all decisions with full text from PDFs."""
        page = 1
        total_yielded = 0

        while True:
            logger.info(f"Fetching page {page}...")
            cases = self._search_page(page)

            if not cases:
                logger.info(f"No more results at page {page}. Done.")
                break

            for case in cases:
                case_id = case.get("case_number_en") or case.get("korean_case_number", "unknown")
                logger.info(f"  Processing {case_id}...")

                text = self._extract_pdf_text(case["pdf_path"])
                if not text:
                    logger.warning(f"  Skipping {case_id}: no text extracted from PDF")
                    continue

                case["text"] = text
                yield case
                total_yielded += 1

            logger.info(f"Page {page}: {len(cases)} cases, {total_yielded} total yielded")
            page += 1

        logger.info(f"Fetch complete: {total_yielded} decisions with full text")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch recent decisions. Yields all since there's no date filter."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw case record to standard schema."""
        case_number = raw.get("case_number_en") or raw.get("korean_case_number", "")
        korean_case = raw.get("korean_case_number", "")

        date_iso = None
        if raw.get("decision_date_raw"):
            date_iso = self._parse_date(raw["decision_date_raw"])

        # Build URL for the case
        seq = raw.get("seq", "")
        case_url = f"https://english.ccourt.go.kr/site/eng/decisions/casesearch/caseSearch.do"

        return {
            "_id": f"KR-CCOURT-{korean_case or case_number}",
            "_source": "KR/CCOURT",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("case_name", case_number),
            "text": raw.get("text", ""),
            "date": date_iso,
            "url": case_url,
            "case_number": case_number,
            "korean_case_number": korean_case,
            "final_decision": raw.get("final_decision", ""),
            "summary": raw.get("summary", ""),
            "language": "en",
        }

    # -- CLI entry points ------------------------------------------------

    def cmd_bootstrap(self, sample: bool = False):
        """Run the bootstrap process."""
        sample_dir = self.source_dir / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        limit = 15 if sample else 999999

        for raw in self.fetch_all():
            record = self.normalize(raw)

            if not record.get("text"):
                continue

            fname = re.sub(r"[^\w\-]", "_", record["_id"]) + ".json"
            out = sample_dir / fname
            out.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
            count += 1
            logger.info(f"  Saved {fname} ({len(record['text'])} chars)")

            if count >= limit:
                break

        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")

    def cmd_test_api(self):
        """Quick connectivity test."""
        logger.info("Testing English case search endpoint...")
        cases = self._search_page(1)
        logger.info(f"  Page 1: {len(cases)} cases found")

        if cases:
            first = cases[0]
            case_id = first.get("case_number_en") or first.get("korean_case_number")
            logger.info(f"  First case: {case_id}")
            logger.info(f"  Testing PDF download...")
            text = self._extract_pdf_text(first["pdf_path"])
            if text:
                logger.info(f"  PDF text extracted: {len(text)} chars")
                logger.info(f"  First 200 chars: {text[:200]}")
            else:
                logger.error("  Failed to extract PDF text!")


def main():
    scraper = CCourtScraper()

    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "bootstrap":
        sample = "--sample" in sys.argv
        scraper.cmd_bootstrap(sample=sample)
    elif cmd == "update":
        scraper.cmd_bootstrap(sample=False)
    elif cmd == "test-api":
        scraper.cmd_test_api()
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
