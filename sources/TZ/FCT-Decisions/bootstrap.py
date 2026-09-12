#!/usr/bin/env python3
"""
TZ/FCT-Decisions — Fair Competition Tribunal Case Digest (2007-2020)

Fetches and parses the official FCT Case Digest PDF, which contains
substantive summaries of ~107 tribunal decisions including facts, issues,
holdings, and orders. Individual decision PDFs on fct.or.tz are scanned
images without extractable text; the digest is the only machine-readable
source of these decisions.

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py test-api            # Quick connectivity test
"""

import io
import re
import sys
import json
import logging
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TZ.FCT-Decisions")

SOURCE_ID = "TZ/FCT-Decisions"
DIGEST_URL = "https://www.fct.or.tz/uploads/documents/sw-1722335018-FCT%20Digest%20-%202007%20-%202020.pdf"
SOURCE_URL = "https://www.fct.or.tz/publications/decided-cases-digest"


def _extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF using pdfplumber."""
    import pdfplumber
    parts = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                parts.append(t)
            try:
                page.flush_cache(); page.get_textmap.cache_clear()
            except Exception:
                pass
    return "\n".join(parts)


def _parse_appeal_number(header_block: str) -> Optional[str]:
    """Extract appeal/application number from a case header block."""
    m = re.search(
        r'((?:APPEAL|APPLICATION|CONSOLIDATED\s+APPEALS?)\s*N[Oo]\.?\s*'
        r'[\d,\s&/]+(?:of|OF)\s*\d{4})',
        header_block, re.IGNORECASE
    )
    if m:
        return re.sub(r'\s+', ' ', m.group(1)).strip()
    # Try simpler pattern
    m = re.search(r'(APPEAL\s*N[Oo]\.?\s*\d+/\d{4})', header_block, re.IGNORECASE)
    if m:
        return re.sub(r'\s+', ' ', m.group(1)).strip()
    return None


def _extract_year(appeal_str: Optional[str], text: str) -> Optional[str]:
    """Extract year from appeal number or text."""
    if appeal_str:
        m = re.search(r'(\d{4})', appeal_str)
        if m:
            return m.group(1)
    # Try to find a date in the first few lines
    m = re.search(r'filed on\s+[\d\.]+\.(\d{4})', text)
    if m:
        return m.group(1)
    return None


def _extract_date(text: str) -> Optional[str]:
    """Try to extract decision date from case text."""
    # Look for patterns like "delivered on DD.MM.YYYY" or "dated DD/MM/YYYY"
    for pat in [
        r'(?:delivered|decided|dated|judgment)\s+(?:on\s+)?(\d{1,2})[./](\d{1,2})[./](\d{4})',
        r'(\d{1,2})[./](\d{1,2})[./](\d{4})\)',  # date in parens like "19.9.2008)"
    ]:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            day, month, year = m.group(1), m.group(2), m.group(3)
            try:
                return f"{year}-{int(month):02d}-{int(day):02d}"
            except ValueError:
                pass
    return None


def _clean_text(text: str) -> str:
    """Clean up extracted text."""
    # Remove excessive whitespace but keep paragraph breaks
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def parse_digest(full_text: str) -> list[dict]:
    """Parse the digest text into individual cases."""
    lines = full_text.split('\n')

    # Find case start lines: "N. PARTY_NAME V. OTHER_PARTY ..."
    # where N is 1-3 digits at the start of a line
    case_starts = []
    for i, line in enumerate(lines):
        stripped = line.strip()
        m = re.match(r'^(\d{1,3})\.\s+([A-Z][A-Z\s])', stripped)
        if m:
            num = int(m.group(1))
            # Get the header block (next 3 lines) to check for APPEAL/APPLICATION
            header_block = '\n'.join(l.strip() for l in lines[i:i+4])
            # Must contain APPEAL, APPLICATION, or be clearly a case header
            if re.search(r'(APPEAL|APPLICATION|CONSOLIDATED)', header_block, re.IGNORECASE):
                case_starts.append((i, num, header_block))

    cases = []
    for idx, (line_no, case_num, header_block) in enumerate(case_starts):
        # Text runs from this case start to the next
        start_line = line_no
        if idx + 1 < len(case_starts):
            end_line = case_starts[idx + 1][0]
        else:
            end_line = len(lines)

        raw_text = '\n'.join(lines[start_line:end_line])

        # Extract appeal number from header block
        appeal_number = _parse_appeal_number(header_block)

        # Extract parties from first line(s)
        first_line = lines[start_line].strip()
        parties_match = re.match(r'^\d{1,3}\.\s+(.+)', first_line)
        parties = parties_match.group(1).strip() if parties_match else ""

        # If parties line doesn't have the appeal, add next line
        if appeal_number and appeal_number not in parties:
            if start_line + 1 < end_line:
                next_line = lines[start_line + 1].strip()
                if re.match(r'[–\-]', next_line) or 'APPEAL' in next_line.upper():
                    parties = parties + ' ' + next_line

        # Clean up parties for title
        title = re.sub(r'\s+', ' ', parties).strip()
        # Remove trailing " –" or similar
        title = re.sub(r'\s*[–-]\s*$', '', title)

        # Create a full title with appeal number
        if appeal_number and appeal_number not in title:
            title = f"{title} – {appeal_number}"

        year = _extract_year(appeal_number, raw_text)
        date = _extract_date(raw_text)
        if not date and year:
            date = f"{year}-01-01"  # Fallback to year only

        # Generate stable ID
        id_str = f"FCT-{case_num:03d}-{appeal_number or title}"
        _id = hashlib.md5(id_str.encode()).hexdigest()[:16]

        cleaned_text = _clean_text(raw_text)

        cases.append({
            '_id': f"fct-digest-{case_num:03d}",
            '_source': SOURCE_ID,
            '_type': 'case_law',
            '_fetched_at': datetime.now(timezone.utc).isoformat(),
            'title': title,
            'appeal_number': appeal_number,
            'case_number': case_num,
            'text': cleaned_text,
            'date': date,
            'year': int(year) if year else None,
            'url': SOURCE_URL,
            'tribunal': 'Fair Competition Tribunal',
            'jurisdiction': 'TZ',
            'digest_source': 'FCT Case Digest 2007-2020',
        })

    return cases


class FCTDecisionsScraper(BaseScraper):
    SOURCE_ID = SOURCE_ID

    def __init__(self):
        super().__init__(source_dir=str(Path(__file__).parent))
        self.http = HttpClient(
            base_url="https://www.fct.or.tz",
        )

    def test_api(self):
        """Quick connectivity check."""
        resp = self.http.get(DIGEST_URL, timeout=30)
        logger.info(f"Digest PDF: {resp.status_code}, {len(resp.content)} bytes")
        return resp.status_code == 200

    def fetch_all(self, sample: bool = False) -> Generator[dict, None, None]:
        """Download and parse the Case Digest PDF."""
        logger.info("Downloading FCT Case Digest PDF...")
        resp = self.http.get(DIGEST_URL, timeout=120)
        resp.raise_for_status()
        logger.info(f"Downloaded {len(resp.content)} bytes")

        logger.info("Extracting text from PDF...")
        full_text = _extract_pdf_text(resp.content)
        logger.info(f"Extracted {len(full_text)} chars of text")

        logger.info("Parsing individual cases...")
        cases = parse_digest(full_text)
        logger.info(f"Parsed {len(cases)} cases")

        if sample:
            cases = cases[:15]

        for case in cases:
            yield case

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """No incremental updates — digest is a static document."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Records are already normalized during parsing."""
        return raw


# --------------- CLI ---------------
if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = FCTDecisionsScraper()
    args = sys.argv[1:]

    if not args or args[0] == "test-api":
        ok = scraper.test_api()
        print("API OK" if ok else "API FAILED")
        sys.exit(0 if ok else 1)

    if args[0] == "bootstrap":
        sample = "--sample" in args
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        total_text_len = 0
        for record in scraper.fetch_all(sample=sample):
            normalized = scraper.normalize(record)
            text_len = len(normalized.get("text", ""))
            total_text_len += text_len

            if sample or count < 15:
                out_path = sample_dir / f"{normalized['_id']}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(normalized, f, ensure_ascii=False, indent=2)

            count += 1
            if count % 20 == 0:
                logger.info(f"Processed {count} records...")

        logger.info(
            f"Done: {count} records, avg text length {total_text_len // max(count, 1)} chars"
        )
        print(f"Records: {count}")
        print(f"Avg text: {total_text_len // max(count, 1)} chars")
        if sample:
            print(f"Samples saved to: {sample_dir}")
