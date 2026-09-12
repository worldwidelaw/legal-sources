#!/usr/bin/env python3
"""
NU/GovLegislation -- Niue Government Official Legislation

Fetches consolidated laws, constitution, supplements, and regulations
from the Government of Niue website (gov.nu).

Strategy:
  - Scrapes https://www.gov.nu/information/ for PDF links
  - Downloads consolidated volume PDFs (all have OCR text layers)
  - Extracts full text via pdfplumber
  - Splits consolidated volumes into individual acts/regulations
  - Individual act PDFs from the page are scanned images (no OCR) → skipped

Data:
  - 4-volume consolidated laws (as at 31 December 2019)
  - Constitution and associated documents
  - Legislation supplements (Acts 334-349+)
  - Yields ~170 individual acts/regulations after splitting

License: Open Government Data (Niue)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch ~10 records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import time
import logging
import hashlib
import requests
import pdfplumber
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NU.GovLegislation")

BASE_URL = "https://www.gov.nu"
INFO_PAGE = f"{BASE_URL}/information/"

# Volumes that should be split into individual acts/regulations
SPLITTABLE_VOLUMES = {
    "niue_laws_vol1",
    "niue_laws_vol2",
    "niue_laws_vol3",
    "niue_laws_vol4",
    "niue_legislation_supplement__part1",
    "niue_legislation_supplement__part2",
    "niue_legislation_supplement__part3",
}

# Pattern for act/regulation headers in all-caps
ACT_HEADER_RE = re.compile(
    r"^([A-Z][A-Z\s\(\)&,'\-]+(?:ACT|REGULATIONS?|CODE)\s+(?:REPRINT\s+)?\d{4})\s*$",
    re.MULTILINE,
)


class NiueGovLegislationScraper(BaseScraper):
    """Scraper for NU/GovLegislation — Niue Government Official Legislation."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
        })

    def _wait(self):
        time.sleep(1.5)

    def _discover_pdfs(self) -> List[Dict[str, str]]:
        """Scrape the information page for PDF links."""
        self._wait()
        resp = self.session.get(INFO_PAGE, timeout=30)
        resp.raise_for_status()

        pdf_links = re.findall(r'href="(https?://[^"]*\.pdf[^"]*)"', resp.text)

        results = []
        seen = set()
        for url in pdf_links:
            clean_url = url.split("?")[0]
            if clean_url in seen:
                continue
            seen.add(clean_url)

            filename = clean_url.rsplit("/", 1)[-1]
            # Skip non-legislation files
            if "public-seal" in filename.lower() or "guidelines" in filename.lower():
                continue

            results.append({"url": url, "filename": filename})

        logger.info(f"Discovered {len(results)} PDF documents")
        return results

    def _is_splittable(self, filename: str) -> bool:
        """Check if a PDF is a consolidated volume that should be split."""
        base = filename.replace(".pdf", "").replace("-1", "").lower().replace("-", "_")
        return any(vol in base for vol in SPLITTABLE_VOLUMES)

    def _extract_pdf_text(self, content: bytes) -> str:
        """Extract text from PDF bytes using pdfplumber."""
        pages_text = []
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages_text.append(text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
        full_text = "\n\n".join(pages_text)
        full_text = re.sub(r"\n{3,}", "\n\n", full_text)
        full_text = re.sub(r" {2,}", " ", full_text)
        return full_text.strip()

    def _split_volume(self, full_text: str, volume_name: str) -> List[Dict]:
        """Split a consolidated volume into individual acts/regulations."""
        matches = list(ACT_HEADER_RE.finditer(full_text))
        if not matches:
            logger.info(f"No act headers found in {volume_name}, keeping as single record")
            return [{"title": volume_name, "text": full_text, "act_number": None, "year": None}]

        acts = []
        for i, match in enumerate(matches):
            start = match.start()
            end = matches[i + 1].start() if i + 1 < len(matches) else len(full_text)
            act_text = full_text[start:end].strip()

            # Skip very short fragments (< 200 chars likely page artifacts)
            if len(act_text) < 200:
                continue

            title = match.group(1).strip().title()

            # Extract act number from line after header (e.g. "348/2019 – 6 August 2019")
            act_num_match = re.search(r"(\d+/\d{4})\s*[–\-]", act_text[:300])
            act_number = act_num_match.group(1) if act_num_match else None

            # Extract year from header
            year_match = re.search(r"(\d{4})", match.group(1))
            year = year_match.group(1) if year_match else None

            acts.append({
                "title": title,
                "text": act_text,
                "act_number": act_number,
                "year": year,
                "volume": volume_name,
            })

        logger.info(f"Split {volume_name} into {len(acts)} individual acts")
        return acts

    def _download_pdf(self, url: str) -> Optional[bytes]:
        """Download a PDF with retry logic."""
        for attempt in range(3):
            try:
                self._wait()
                resp = self.session.get(url, timeout=120)
                if resp.status_code == 200:
                    return resp.content
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt+1}/3 failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(5)
        return None

    def _title_from_filename(self, filename: str) -> str:
        """Derive a readable title from the PDF filename."""
        title_map = {
            "niue_constitution": "Constitution of Niue and Associated Documents",
            "niue_legislation_tables": "Niue Legislation Tables",
        }
        base = filename.replace(".pdf", "").replace("-1", "")
        for key, title in title_map.items():
            if key in base.lower().replace("-", "_"):
                return title

        clean = base.replace("_", " ").replace("-", " ")
        clean = re.sub(r"^(act|reg)\s+\d+\s+", "", clean, flags=re.IGNORECASE)
        clean = re.sub(r"^(reg)\s+\d{4}\s+\d+\s+", "", clean, flags=re.IGNORECASE)
        return clean.strip().title()

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all legislation documents from gov.nu, split into individual acts."""
        pdfs = self._discover_pdfs()

        for pdf_info in pdfs:
            url = pdf_info["url"]
            filename = pdf_info["filename"]
            logger.info(f"Downloading: {filename}")

            content = self._download_pdf(url)
            if content is None:
                logger.error(f"Failed to download: {url}")
                continue

            text = self._extract_pdf_text(content)
            if len(text) < 50:
                logger.warning(f"Scanned PDF (no extractable text): {filename}")
                continue

            logger.info(f"Extracted {len(text)} chars from {filename}")

            if self._is_splittable(filename):
                # Split consolidated volume into individual acts
                volume_name = filename.replace(".pdf", "").replace("-1", "")
                acts = self._split_volume(text, volume_name)
                for act in acts:
                    year = act.get("year")
                    yield {
                        "url": url,
                        "filename": filename,
                        "title": act["title"],
                        "date": year,
                        "text": act["text"],
                        "act_number": act.get("act_number"),
                        "volume": act.get("volume"),
                    }
            else:
                # Single-document PDF (constitution, tables, etc.)
                year_match = re.search(r"(20\d{2}|19\d{2})", filename)
                yield {
                    "url": url,
                    "filename": filename,
                    "title": self._title_from_filename(filename),
                    "date": year_match.group(1) if year_match else None,
                    "text": text,
                    "act_number": None,
                    "volume": None,
                }

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Niue legislation is updated infrequently; re-run full bootstrap."""
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw data into standardized schema."""
        title = raw.get("title", "")
        act_num = raw.get("act_number", "")
        filename = raw.get("filename", "")

        # Create a stable ID from title + act_number (or filename for non-split records)
        id_source = f"{title}|{act_num}" if act_num else f"{title}|{filename}"
        doc_id = hashlib.sha256(id_source.encode()).hexdigest()[:16]

        return {
            "_id": f"NU-GOV-{doc_id}",
            "_source": "NU/GovLegislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "act_number": act_num,
            "volume": raw.get("volume"),
            "jurisdiction": "NU",
        }

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            resp = self.session.get(INFO_PAGE, timeout=15)
            ok = resp.status_code == 200 and ".pdf" in resp.text
            logger.info(f"Test {'passed' if ok else 'FAILED'}: HTTP {resp.status_code}")
            return ok
        except Exception as e:
            logger.error(f"Test failed: {e}")
            return False


# ── CLI ──────────────────────────────────────────────────────────────
def main():
    import argparse
    parser = argparse.ArgumentParser(description="NU/GovLegislation bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true",
                        help="Sample mode: fetch ~10 records for validation")
    parser.add_argument("--full", action="store_true",
                        help="Full mode: fetch all records")
    args = parser.parse_args()

    scraper = NiueGovLegislationScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        sample_mode = args.sample and not args.full
        stats = scraper.bootstrap(sample_mode=sample_mode)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")
    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
