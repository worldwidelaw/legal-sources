#!/usr/bin/env python3
"""
INTL/ITIA -- International Tennis Integrity Agency (ITIA) Sanctions & Decisions

Fetches the published sanctions and disciplinary decisions of the International
Tennis Integrity Agency (ITIA), the independent body responsible for safeguarding
the integrity of professional tennis worldwide. The ITIA administers the Tennis
Anti-Corruption Program (TACP) and the Tennis Anti-Doping Programme (TADP). When
an independent Anti-Corruption Hearing Officer (AHO) or Independent Tribunal
reaches a decision, or a player accepts an agreed sanction, the ITIA publishes a
detailed, full-text announcement (and, for tribunal cases, the full redacted
decision PDF).

Strategy:
  - The "Sanctions" listing (itia.tennis/news/sanctions/) is a single
    server-rendered HTML page that links to every individual decision article at
    /news/sanctions/{slug}/ (~388 articles, reverse-chronological).
  - For each article we fetch the detail page and extract:
      * the headline (<h1 class="title">)
      * the full body text (<div class="article__inner">), which carries the
        substantive findings: the breach(es), the sanction, the period of
        ineligibility, fines, and the reasoning summary.
      * the published date (<p class="small"><i>Published DD Month YYYY HH:MM</i></p>)
      * any linked full tribunal decision PDF (/media/.../*.pdf), which we
        download and append in full when present.

The site is an Umbraco CMS, openly published (no login, no WAF, reachable from
any IP).

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py test               # Print parsed listing entries
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

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
logger = logging.getLogger("legal-data-hunter.INTL.ITIA")

BASE_URL = "https://www.itia.tennis"
LISTING_URL = "https://www.itia.tennis/news/sanctions/"
MAX_PDF_BYTES = 50 * 1024 * 1024

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}
PUBLISHED_RE = re.compile(
    r"Published\s+(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", re.IGNORECASE
)


class ITIAScraper(BaseScraper):
    """Scraper for ITIA published sanctions and decisions."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/worldwidelaw/legal-sources)",
            "Accept": "text/html,application/xhtml+xml,application/pdf",
            "Accept-Language": "en",
        })

    def _slug_from_url(self, url: str) -> str:
        slug = url.rstrip("/").rsplit("/", 1)[-1]
        slug = re.sub(r"[^a-z0-9]+", "-", slug.lower()).strip("-")
        return slug or "decision"

    def _get_entries(self) -> list[dict]:
        """Parse the sanctions listing into a list of article URLs."""
        resp = self.session.get(LISTING_URL, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")

        entries = []
        seen = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if "/news/sanctions/" not in href:
                continue
            # Skip the listing page itself and any non-article anchors.
            path = href.split("?")[0].split("#")[0]
            if path.rstrip("/").endswith("/news/sanctions"):
                continue
            slug = self._slug_from_url(path)
            if not slug or slug == "sanctions":
                continue
            full = path if path.startswith("http") else BASE_URL + path
            full = full.replace("http://", "https://")
            if full in seen:
                continue
            seen.add(full)
            entries.append({"url": full, "id_slug": slug})

        logger.info(f"Parsed {len(entries)} sanction article links from listing page")
        return entries

    def _download_pdf(self, url: str) -> Optional[bytes]:
        try:
            time.sleep(1.0)
            resp = self.session.get(url, timeout=60)
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
            source="INTL/ITIA",
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

    def _fetch_detail(self, entry: dict) -> Optional[dict]:
        """Fetch one article detail page and extract title, date, body, PDF."""
        try:
            time.sleep(1.0)
            resp = self.session.get(entry["url"], timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"  Detail fetch failed for {entry['id_slug']}: {e}")
            return None

        soup = BeautifulSoup(resp.content, "html.parser")

        # Headline
        h1 = soup.find("h1", class_="title")
        title = h1.get_text(" ", strip=True) if h1 else None
        if not title:
            t = soup.find("title")
            title = t.get_text(strip=True).replace("ITIA - ", "").strip() if t else entry["id_slug"]

        # Body
        body_div = soup.find("div", class_="article__inner")
        body_text = ""
        if body_div:
            # Drop the "Published ..." meta line which sits inside the article block.
            for small in body_div.find_all("p", class_="small"):
                small.extract()
            body_text = body_div.get_text("\n", strip=True)
        body_text = re.sub(r"\n{3,}", "\n\n", body_text).strip()

        # Published date
        date_iso = None
        page_text = soup.get_text(" ", strip=True)
        m = PUBLISHED_RE.search(page_text)
        if m:
            day, mon, year = m.group(1), m.group(2).lower(), m.group(3)
            if mon in MONTHS:
                try:
                    date_iso = datetime(int(year), MONTHS[mon], int(day)).date().isoformat()
                except ValueError:
                    date_iso = None

        # Optional full decision PDF(s)
        pdf_url = None
        if body_div:
            for a in body_div.find_all("a", href=True):
                href = a["href"].strip()
                if href.lower().endswith(".pdf"):
                    pdf_url = href if href.startswith("http") else BASE_URL + href
                    pdf_url = pdf_url.replace("http://", "https://")
                    break

        text = body_text
        if pdf_url:
            pdf_bytes = self._download_pdf(pdf_url)
            if pdf_bytes:
                pdf_text = self._extract_pdf_text(pdf_bytes, entry["id_slug"])
                if pdf_text:
                    text = (body_text + "\n\n---\n\nFULL DECISION:\n\n" + pdf_text).strip()

        if not text or len(text.strip()) < 100:
            logger.warning(f"  Insufficient text for {entry['id_slug']}, skipping")
            return None

        return {
            "id_slug": entry["id_slug"],
            "url": entry["url"],
            "title": title,
            "date": date_iso,
            "pdf_url": pdf_url or "",
            "_extracted_text": text,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        entries = self._get_entries()
        logger.info(f"Total entries to process: {len(entries)}")
        for i, entry in enumerate(entries):
            try:
                logger.info(f"[{i+1}/{len(entries)}] {entry['id_slug'][:60]}")
                rec = self._fetch_detail(entry)
                if rec:
                    yield rec
            except Exception as e:
                logger.error(f"  Error processing {entry['id_slug']}: {e}")
                continue

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        since_iso = since.date().isoformat()
        for rec in self.fetch_all():
            if not rec.get("date") or rec["date"] >= since_iso:
                yield rec

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"itia-{raw.get('id_slug', 'decision')}",
            "_source": "INTL/ITIA",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("_extracted_text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", LISTING_URL),
            "pdf_url": raw.get("pdf_url", ""),
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = ITIAScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        entries = scraper._get_entries()
        for e in entries[:30]:
            print(f"  {e['id_slug'][:60]}")
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
