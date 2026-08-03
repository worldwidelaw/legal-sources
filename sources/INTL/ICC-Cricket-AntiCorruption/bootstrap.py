#!/usr/bin/env python3
"""
INTL/ICC-Cricket-AntiCorruption -- ICC (cricket) Anti-Corruption Decisions

Fetches the published full-text decisions issued under the International Cricket
Council's Anti-Corruption Code for Participants. When the ICC's Anti-Corruption
Tribunal (or a sole adjudicator / appeal panel) determines a charge brought by
the ICC Integrity Unit against a player, official or other participant, the ICC
publishes the full, redacted decision (the substantive findings on liability,
the sanction, the period of ineligibility, and the full legal reasoning) as a
PDF on its Anti-Corruption "ACU publications" page.

NOTE: "ICC" here is the *International Cricket Council* (the cricket world
governing body), NOT the International Criminal Court (INTL/ICCCaseLaw /
INTL/ICC-TrialChamber) nor ICC arbitration (INTL/JusMundi-ICC).

Strategy:
  - The ACU publications page
    (icc-cricket.com/about/integrity/anti-corruption/acu-publications) is a
    single server-rendered HTML page that links every named-matter decision PDF
    (~38 documents, reverse-chronological). Each <a> wraps the decision title and
    its date, e.g. "Decision of the ICC in the matter of Mr Irfan Ahmed -
    20 April 2016".
  - For each link we read the title + date from the anchor text, download the
    PDF, and extract its full text. Most PDFs are hosted on
    images.icc-cricket.com (which extracts cleanly); a small minority on the
    legacy resources.pulse.icc-cricket.com host may be unavailable and are
    skipped.

The site is openly published (no login, no WAF), reachable from any IP.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py test               # Print discovered decision entries
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.ICC-Cricket-AntiCorruption")

BASE_URL = "https://www.icc-cricket.com"
LISTING_URL = "https://www.icc-cricket.com/about/integrity/anti-corruption/acu-publications"
MAX_PDF_BYTES = 50 * 1024 * 1024

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}
# Date inside the anchor text, e.g. "20 April 2016" or "(18 October 2024)".
DATE_RE = re.compile(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})")

# Anchor text that names a generic code/rules document rather than a case
# decision (these are not case_law). We only keep decisions in named matters.
NON_DECISION_RE = re.compile(
    r"anti-?corruption code|procedural rules|code of conduct|"
    r"educational|fact[\s-]?sheet|annual report|terms of reference",
    re.IGNORECASE,
)


class ICCCricketAntiCorruptionScraper(BaseScraper):
    """Scraper for ICC (cricket) Anti-Corruption full-text decisions."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 "
                          "Safari/537.36 LegalDataHunter/1.0",
            "Accept": "text/html,application/xhtml+xml,application/pdf",
            "Accept-Language": "en",
        })

    def _slugify(self, text: str) -> str:
        return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")[:90]

    def _parse_date(self, text: str) -> Optional[str]:
        m = DATE_RE.search(text)
        if not m:
            return None
        day, mon, year = m.group(1), m.group(2).lower(), m.group(3)
        if mon not in MONTHS:
            return None
        try:
            return datetime(int(year), MONTHS[mon], int(day)).date().isoformat()
        except ValueError:
            return None

    def _collect_entries(self) -> list[dict]:
        """Parse the ACU publications page into per-decision PDF entries."""
        try:
            time.sleep(1.0)
            resp = self.session.get(LISTING_URL, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch listing page: {e}")
            return []

        soup = BeautifulSoup(resp.content, "html.parser")
        entries = []
        seen = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if ".pdf" not in href.lower():
                continue
            pdf_url = urljoin(BASE_URL, href).replace("http://", "https://")
            if pdf_url in seen:
                continue

            title = re.sub(r"\s+", " ", a.get_text(" ", strip=True)).strip()
            if not title:
                continue
            # Skip generic code / rules / educational documents (not decisions).
            if NON_DECISION_RE.search(title):
                continue
            # Keep only entries that look like an actual case decision.
            if not re.search(r"\b(decision|award|matter of|ruling|determination)\b",
                             title, re.IGNORECASE):
                continue

            seen.add(pdf_url)
            entries.append({
                "pdf_url": pdf_url,
                "title": title,
                "date": self._parse_date(title),
            })
        logger.info(f"Discovered {len(entries)} ICC anti-corruption decision PDFs")
        return entries

    def _download_pdf(self, url: str) -> Optional[bytes]:
        try:
            time.sleep(1.0)
            resp = self.session.get(url, timeout=90)
            resp.raise_for_status()
            if len(resp.content) > MAX_PDF_BYTES:
                logger.warning(f"  PDF too large ({len(resp.content)} bytes), skipping")
                return None
            if len(resp.content) < 500:
                return None
            return resp.content
        except Exception as e:
            logger.warning(f"  PDF download failed: {e}")
            return None

    def _extract_pdf_text(self, pdf_bytes: bytes, source_id: str) -> Optional[str]:
        text = extract_pdf_markdown(
            source="INTL/ICC-Cricket-AntiCorruption",
            source_id=source_id,
            pdf_bytes=pdf_bytes,
            table="case_law",
        )
        if text and len(text.strip()) >= 100:
            return text
        import io
        try:
            import pdfplumber
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                pages = [p.extract_text() or "" for p in pdf.pages]
                text = "\n\n".join(p for p in pages if p)
                if text and len(text.strip()) >= 100:
                    return text
        except Exception:
            pass
        try:
            from pypdf import PdfReader
            reader = PdfReader(io.BytesIO(pdf_bytes))
            pages = [p.extract_text() or "" for p in reader.pages]
            text = "\n\n".join(p for p in pages if p)
            if text and len(text.strip()) >= 100:
                return text
        except Exception:
            pass
        return None

    def fetch_all(self) -> Generator[dict, None, None]:
        entries = self._collect_entries()
        for i, entry in enumerate(entries):
            try:
                logger.info(f"[{i+1}/{len(entries)}] {entry['title'][:70]}")
                pdf_bytes = self._download_pdf(entry["pdf_url"])
                if not pdf_bytes:
                    continue
                sid = self._slugify(entry["title"]) or "decision"
                text = self._extract_pdf_text(pdf_bytes, sid)
                if not text or len(text.strip()) < 100:
                    logger.warning(f"  Insufficient text for {entry['title'][:50]}, skipping")
                    continue
                entry["_extracted_text"] = text
                yield entry
            except Exception as e:
                logger.error(f"  Error processing {entry['title'][:50]}: {e}")
                continue

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        since_iso = since.date().isoformat()
        for rec in self.fetch_all():
            if not rec.get("date") or rec["date"] >= since_iso:
                yield rec

    def normalize(self, raw: dict) -> dict:
        # Title (including its date) is unique per decision; date alone collides
        # for "Decision on merits" / "Decision on sanction" pairs in one matter.
        slug = self._slugify(raw.get("title", "")) or "decision"
        return {
            "_id": f"icc-cricket-acu-{slug}",
            "_source": "INTL/ICC-Cricket-AntiCorruption",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("_extracted_text", ""),
            "date": raw.get("date"),
            "url": raw.get("pdf_url", ""),
            "pdf_url": raw.get("pdf_url", ""),
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = ICCCricketAntiCorruptionScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        entries = scraper._collect_entries()
        for e in entries[:40]:
            print(f"  {e['date'] or '----------'}  {e['title'][:70]}")
        print(f"\nTotal: {len(entries)} entries")
        sys.exit(0)

    if command in ("bootstrap", "bootstrap-fast"):
        result = scraper.bootstrap(sample_mode=sample, sample_size=12)
        print(json.dumps(result, indent=2, default=str))
    elif command == "update":
        result = scraper.update()
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
