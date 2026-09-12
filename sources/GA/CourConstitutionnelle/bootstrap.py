#!/usr/bin/env python3
"""
GA/CourConstitutionnelle -- Cour Constitutionnelle du Gabon

Fetches constitutional court decisions from the Journal Officiel de la
République Gabonaise at journal-officiel.ga.

Strategy:
  - Documents are at sequential URLs: /{id}-x/ (IDs ~2000–22000)
  - Filter for CC decisions: title/content contains "Cour Constitutionnelle"
    or decision number matches N°XXX/CC pattern
  - Extract full text from HTML between </nav> and <footer>
  - Skip subscription-only pages (< 200 chars useful text)

Data:
  - ~100-300 constitutional court decisions
  - Language: French
  - Coverage: 2010–present
  - Rate limit: 1 request/second

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test                 # Quick connectivity test
"""

import sys
import json
import logging
import re
import html as html_mod
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GA.CourConstitutionnelle")

BASE_URL = "https://journal-officiel.ga"

# Known CC decision IDs (discovered via search — used for --sample mode)
KNOWN_CC_IDS = [
    21553, 21428, 20745, 20019, 19428, 18063, 17753, 17115,
    12304, 12207, 11078, 8094, 8093, 6344, 4745, 3181,
]

# Full scan range
ID_START = 22000
ID_END = 2000

# French months
MONTHS_FR = {
    "janvier": "01", "février": "02", "fevrier": "02",
    "mars": "03", "avril": "04", "mai": "05", "juin": "06",
    "juillet": "07", "août": "08", "aout": "08",
    "septembre": "09", "octobre": "10", "novembre": "11",
    "décembre": "12", "decembre": "12",
}


class GabonCourConstitutionnelleScraper(BaseScraper):
    """
    Scraper for GA/CourConstitutionnelle — Constitutional Court of Gabon.
    Country: GA
    URL: https://journal-officiel.ga

    Data types: case_law
    Auth: none (Open public access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "fr,en;q=0.5",
            },
            timeout=60,
        )

    def _clean_html(self, text: str) -> str:
        """Strip HTML tags and clean up whitespace."""
        text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<br\s*/?\s*>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<p[^>]*>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', ' ', text)
        text = html_mod.unescape(text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n[ \t]+', '\n', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _is_cc_decision(self, html_content: str, text_content: str) -> bool:
        """Determine if the page is a Constitutional Court decision."""
        # Check URL slug in the HTML for "cc" pattern
        cc_patterns = [
            r'COUR CONSTITUTIONNELLE',
            r'Cour [Cc]onstitutionnelle',
            r'N°\s*\d+/CC[T]?\b',
            r'Décision\s+N°\s*\d+/CC',
            r'LA COUR CONSTITUTIONNELLE',
        ]
        combined = html_content[:5000] + "\n" + text_content[:3000]
        for pattern in cc_patterns:
            if re.search(pattern, combined, re.IGNORECASE):
                return True
        return False

    def _extract_page_content(self, html_content: str) -> Dict[str, Any]:
        """Extract structured content from a document page."""
        result = {
            "title": "",
            "text": "",
            "date": "",
            "decision_number": "",
            "journal_number": "",
            "subject": "",
        }

        # Extract content between </nav> and <footer>
        body_match = re.search(r'</nav>(.*?)(?:<footer|$)', html_content, re.DOTALL)
        if not body_match:
            return result

        main_html = body_match.group(1)
        main_text = self._clean_html(main_html)

        # Skip subscription-only pages
        if len(main_text) < 200:
            return result
        if "Abonnez-vous" in main_text and len(main_text) < 400:
            return result

        lines = [l.strip() for l in main_text.split('\n') if l.strip()]

        content_start = 0
        for i, line in enumerate(lines):
            if 'JOURNAL OFFICIEL' in line.upper() and ('N°' in line or 'DU' in line.upper()):
                content_start = i
                break

        if content_start == 0:
            for i, line in enumerate(lines):
                if any(kw in line.lower() for kw in ['décision', 'cour constitutionnelle', 'au nom du peuple']):
                    content_start = i
                    break

        if content_start == 0:
            return result

        # Extract journal number from header
        jo_line = lines[content_start] if content_start < len(lines) else ""
        jo_match = re.search(r'JOURNAL\s+OFFICIEL\s+N°\s*(.+)', jo_line, re.IGNORECASE)
        if jo_match:
            result["journal_number"] = jo_match.group(1).strip()

        # Date from header
        date_line_idx = content_start + 1
        if date_line_idx < len(lines):
            date_line = lines[date_line_idx]
            date_match = re.search(r'DU\s+(\d{1,2})\s+(\w+)\s+(\d{4})', date_line, re.IGNORECASE)
            if date_match:
                day = date_match.group(1).zfill(2)
                month = MONTHS_FR.get(date_match.group(2).lower(), "")
                year = date_match.group(3)
                if month:
                    result["date"] = f"{year}-{month}-{day}"
                content_start = date_line_idx + 1
            else:
                content_start += 1

        content_lines = lines[content_start:]

        # Remove trailing subscription text
        clean_content = []
        for line in content_lines:
            if "Abonnez-vous" in line or "ABONNEZ VOUS" in line:
                break
            if "Inscrivez-vous et recevez" in line:
                break
            clean_content.append(line)

        if not clean_content:
            return result

        # Title: first line(s) containing "Décision" or "N°"
        title_parts = []
        for line in clean_content[:5]:
            if re.search(r'(décision|N°|/CC|cour constitutionnelle|portant|relative)', line, re.IGNORECASE):
                title_parts.append(line)
            elif title_parts:
                break
        result["title"] = ' '.join(title_parts) if title_parts else clean_content[0]

        # Full text
        result["text"] = '\n'.join(clean_content)

        # Extract decision number (e.g., "015/CC", "043/CC", "002/CCT")
        num_match = re.search(r'N°\s*([\d]+/CC[T]?)', result["text"][:500], re.IGNORECASE)
        if num_match:
            result["decision_number"] = num_match.group(1).strip()

        # Extract decision date from text if not from header
        if not result["date"]:
            date_match = re.search(
                r'du\s+(\d{1,2})\s+(\w+)\s+(\d{4})',
                result["text"][:1000],
                re.IGNORECASE
            )
            if date_match:
                day = date_match.group(1).zfill(2)
                month = MONTHS_FR.get(date_match.group(2).lower(), "")
                year = date_match.group(3)
                if month:
                    result["date"] = f"{year}-{month}-{day}"

        # Extract subject from "relative à" or "portant" clauses
        subj_match = re.search(
            r'(?:relative? [àa]|portant)\s+(.{10,150}?)(?:\.|;|\n)',
            result["title"] + " " + result["text"][:500],
            re.IGNORECASE,
        )
        if subj_match:
            result["subject"] = subj_match.group(1).strip()

        return result

    def _fetch_cc_decision(self, doc_id: int) -> Optional[Dict[str, Any]]:
        """Fetch a single page and return it only if it's a CC decision."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(f"/{doc_id}-x/")
            if resp.status_code != 200:
                return None
            if len(resp.text) < 2000:
                return None

            content = self._extract_page_content(resp.text)
            if not content["text"] or len(content["text"]) < 100:
                return None

            # Check if this is actually a CC decision
            if not self._is_cc_decision(resp.text, content["text"]):
                return None

            return {
                "doc_id": str(doc_id),
                "url": f"{BASE_URL}/{doc_id}-x/",
                **content,
            }
        except Exception as e:
            logger.debug(f"Failed to fetch ID {doc_id}: {e}")
            return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw record into standard schema."""
        return {
            "_id": f"GA-CC-{raw['doc_id']}",
            "_source": "GA/CourConstitutionnelle",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("url", ""),
            "decision_number": raw.get("decision_number", ""),
            "journal_number": raw.get("journal_number", ""),
            "subject": raw.get("subject", ""),
            "language": "fr",
            "court": "Cour Constitutionnelle du Gabon",
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all CC decisions by scanning ID range."""
        logger.info(f"Scanning IDs {ID_START} down to {ID_END} for CC decisions...")
        found = 0
        empty_streak = 0

        for doc_id in range(ID_START, ID_END, -1):
            record = self._fetch_cc_decision(doc_id)
            if record:
                found += 1
                empty_streak = 0
                logger.info(f"[{found}] CC decision at ID {doc_id}: {record.get('decision_number', 'N/A')}")
                yield record
            else:
                empty_streak += 1
                # Don't give up — CC decisions are sparse among 20k docs
                if empty_streak > 500 and found > 10:
                    logger.info(f"500 consecutive non-CC IDs after finding {found} decisions, continuing...")
                    empty_streak = 0

        logger.info(f"Scan complete. Found {found} CC decisions.")

    def fetch_sample(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch sample using known CC decision IDs."""
        logger.info(f"Fetching {len(KNOWN_CC_IDS)} known CC decision IDs...")
        found = 0

        for doc_id in KNOWN_CC_IDS:
            record = self._fetch_cc_decision(doc_id)
            if record:
                found += 1
                logger.info(f"[{found}] ID {doc_id}: {record.get('decision_number', 'N/A')} - {record.get('title', '')[:60]}")
                yield record
            else:
                logger.warning(f"ID {doc_id}: not a CC decision or failed to fetch")

            if found >= 15:
                break

        logger.info(f"Sample complete. Found {found} CC decisions.")

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent CC decisions (scan newest IDs)."""
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        logger.info(f"Checking recent IDs for new CC decisions since {since}...")
        found = 0
        for doc_id in range(ID_START, ID_START - 2000, -1):
            record = self._fetch_cc_decision(doc_id)
            if record:
                found += 1
                yield record
                if found >= 50:
                    break
        logger.info(f"Update complete. Found {found} new CC decisions.")


def main():
    scraper = GabonCourConstitutionnelleScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test":
        logger.info("Testing connectivity to journal-officiel.ga...")
        try:
            scraper.rate_limiter.wait()
            resp = scraper.client.get(f"/{KNOWN_CC_IDS[0]}-x/")
            if resp.status_code == 200 and len(resp.text) > 2000:
                logger.info(f"SUCCESS: Got {len(resp.text)} bytes from ID {KNOWN_CC_IDS[0]}")
            else:
                logger.error(f"FAILED: status={resp.status_code}, len={len(resp.text)}")
                sys.exit(1)
        except Exception as e:
            logger.error(f"FAILED: {e}")
            sys.exit(1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        if sample_mode:
            gen = scraper.fetch_sample()
        else:
            gen = scraper.fetch_all()

        count = 0
        for raw in gen:
            record = scraper.normalize(raw)
            count += 1
            out_file = sample_dir / f"{record['_id']}.json"
            with open(out_file, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)

            if sample_mode and count >= 15:
                break

        logger.info(f"Done. Saved {count} records to {sample_dir}/")

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
