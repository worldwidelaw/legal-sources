#!/usr/bin/env python3
"""
Legal Data Hunter - UK Legal Ombudsman (LeO) Public Interest Decisions Scraper

Fetches the published "public interest" decisions of the Legal Ombudsman (LeO),
the statutory ombudsman scheme (Office for Legal Complaints, established under the
Legal Services Act 2007) that resolves complaints about legal service providers
in England & Wales. Under its policy statement on publishing decisions, LeO
publishes selected final ombudsman decisions in the public interest — full,
reasoned determinations naming the legal service provider = case_law.

Source: https://www.legalombudsman.org.uk/information-centre/public-interest-decisions/
  - A single listing page holds one card per published decision, each linking to
    the full decision PDF under /media/{id}/{slug}-pid.pdf (born-digital, text
    layer present — extracted via common.pdf_extract, no OCR).
  - The decision date is stamped in the PDF body ("Final Decision / Date DD
    Month YYYY"); the named firm is derivable from the file slug and card.

Note: LeO's separate "ombudsman decision data" (data-centre) is a firm-level
aggregate CSV of complaint *outcomes* (metadata only, no decision text) and is
deliberately NOT used here — this source captures the full-text decisions only.

Coverage: ~25-30 published public-interest decisions (a curated set, growing).

License: no Open Government Licence statement; content published under LeO's
"policy statement on publishing our decisions". Treated as custom terms,
commercial use flagged.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records for validation
  python bootstrap.py bootstrap-fast     # Alias for full pull (fleet runner)
"""

import re
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common import pdf_extract

import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("UK/LegalOmbudsman")

MIN_TEXT_CHARS = 200

_MONTHS = {
    "january": "01", "february": "02", "march": "03", "april": "04",
    "may": "05", "june": "06", "july": "07", "august": "08",
    "september": "09", "october": "10", "november": "11", "december": "12",
}


class UKLegalOmbudsmanScraper(BaseScraper):
    """Scraper for the Legal Ombudsman (LeO) public interest decisions."""

    BASE_URL = "https://www.legalombudsman.org.uk"
    LISTING_URL = BASE_URL + "/information-centre/public-interest-decisions/"
    # Public-interest decision PDFs live under /media/ and carry a "pid" marker.
    PDF_RE = re.compile(r'href="(/media/[^"]+?pid[^"]*\.pdf)"', re.I)

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0; legal research)",
            "Accept": "text/html,application/xhtml+xml",
        })

    # ------------------------------------------------------------------- fetch
    def _get(self, url: str) -> Optional[str]:
        self.rate_limiter.wait()
        try:
            resp = self.session.get(url, timeout=45)
            if resp.status_code == 200:
                return resp.text
            logger.warning(f"{resp.status_code} for {url}")
            return None
        except Exception as e:
            logger.warning(f"Request failed for {url}: {e}")
            return None

    # --------------------------------------------------------------- discovery
    def _discover(self, html: str) -> list:
        """Extract per-decision entries (pdf url, firm, insight summary) from the listing."""
        soup = BeautifulSoup(html, "html.parser")
        entries = []
        seen = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].split("?")[0].split("#")[0]
            if not re.search(r"/media/[^/]+/[^/]*pid[^/]*\.pdf$", href, re.I):
                continue
            if href in seen:
                continue
            seen.add(href)

            slug = href.rstrip("/").split("/")[-1]
            firm = self._firm_from_slug(slug)

            # The insight/summary sits in the same card as the "Read the full
            # decision" link — grab the nearest enclosing paragraph text.
            insight = ""
            block = a.find_parent(["p", "div", "li", "article"])
            if block:
                insight = re.sub(r"\s+", " ", block.get_text(" ", strip=True))
                insight = re.sub(r"\s*Read the (full )?decision\s*$", "", insight, flags=re.I).strip()

            entries.append({
                "url": urljoin(self.BASE_URL, href),
                "slug": slug,
                "firm": firm,
                "insight": insight,
            })
        return entries

    @staticmethod
    def _firm_from_slug(slug: str) -> str:
        name = re.sub(r"\.pdf$", "", slug, flags=re.I)
        name = re.sub(r"-(pid|final)\b", "", name, flags=re.I)
        name = name.strip("-")
        small = {"and", "of", "the", "co"}
        parts = []
        for p in name.split("-"):
            if not p:
                continue
            parts.append(p.upper() if p.lower() == "fw" else
                         (p if p.lower() in small else p.capitalize()))
        return " ".join(parts)

    @staticmethod
    def _date_from_text(text: str) -> Optional[str]:
        """Extract the decision date, stamped near the top of the PDF.

        Formats seen: 'Date 10 July 2025', 'Date: 18 June 2025', or a bare
        'DD Month YYYY' line at the very top of the document.
        """
        head = text[:800]
        m = re.search(r"\bDate[:\s]\s*(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", head)
        if not m:
            # Fallback: first 'DD Month YYYY' in the document head is the
            # decision date (the body's later dates are event references).
            m = re.search(r"\b(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", head)
        if not m:
            return None
        day, month, year = m.group(1), m.group(2).lower(), m.group(3)
        mm = _MONTHS.get(month)
        if not mm:
            return None
        return f"{year}-{mm}-{int(day):02d}"

    # ---------------------------------------------------------------- iteration
    def fetch_all(self) -> Generator[dict, None, None]:
        html = self._get(self.LISTING_URL)
        if not html:
            raise RuntimeError(
                "Legal Ombudsman public-interest-decisions listing unreachable — "
                "possible IP block (fail loud rather than emit an empty corpus)"
            )
        entries = self._discover(html)
        if not entries:
            raise RuntimeError(
                "No public-interest decision PDFs found on the listing — layout "
                "may have changed (fail loud rather than emit an empty corpus)"
            )
        logger.info(f"Found {len(entries)} public-interest decisions")

        count = 0
        skipped = 0
        for e in entries:
            try:
                text = (pdf_extract.extract_pdf_markdown(
                    "UK/LegalOmbudsman", e["slug"], pdf_url=e["url"],
                ) or "").strip()
            except Exception as exc:
                logger.warning(f"PDF extraction failed for {e['url']}: {exc}")
                text = ""
            if len(text) < MIN_TEXT_CHARS:
                skipped += 1
                logger.warning(f"Insufficient text, skipping: {e['url']} ({len(text)} chars)")
                continue
            count += 1
            raw = dict(e)
            raw["text"] = text
            raw["date"] = self._date_from_text(text)
            yield raw
        logger.info(f"Total: {count} decisions ({skipped} skipped)")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Small curated corpus with no per-item update signal — re-sweep all.

        The loader upserts on the primary key, so re-yielding is idempotent.
        """
        yield from self.fetch_all()

    # ---------------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        text = (raw.get("text", "") or "").strip()
        if not text:
            return None

        slug = re.sub(r"\.pdf$", "", raw.get("slug", ""), flags=re.I)
        firm = raw.get("firm", "") or slug
        title = f"{firm} — Legal Ombudsman public interest decision"

        return {
            "_id": f"UK/LegalOmbudsman/{slug}",
            "_source": "UK/LegalOmbudsman",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "case_id": slug,
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "firm": firm or None,
            "summary": raw.get("insight", "") or None,
            "url": raw.get("url", ""),
        }


# ── CLI entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    scraper = UKLegalOmbudsmanScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py bootstrap [--sample]")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd in ("bootstrap", "bootstrap-fast"):
        sample = "--sample" in sys.argv
        result = scraper.bootstrap(sample_mode=sample, sample_size=12)
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
