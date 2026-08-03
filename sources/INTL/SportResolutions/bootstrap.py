#!/usr/bin/env python3
"""
INTL/SportResolutions -- Sport Resolutions (UK) Published Decisions

Fetches the full text of published sports-arbitration and anti-doping tribunal
decisions hosted by Sport Resolutions, the independent London-based dispute
resolution body that provides the secretariat for the UK National Anti-Doping
Panel (NADP) and hears/administers cases for numerous sports governing bodies.

The "Published & Time-Limited Decisions" database (sportresolutions.com/decisions)
collects reasoned decisions across many sports — anti-doping (UK Anti-Doping /
NADP), World Athletics Disciplinary & Appeals Tribunal, International Tennis
Integrity Agency (ITIA), football (FA / EFL / CFRU financial regulation), rugby,
snooker (WPBSA), sailing, golf, cricket, skiing (FIS), etc. Each published case
links to a born-digital, full-text decision PDF hosted openly on the site.

Strategy:
  - The listing is a server-rendered HTML page paginated by an offset path
    segment: /decisions, /decisions/P6, /decisions/P12, ... (6 items per page).
    We walk the offsets until no new decision slugs appear.
  - Each listing item exposes the detail URL (h3 anchor), a
    "<date> | <sport> | <type>" line, and a one-line description.
  - Each detail page links the full-text decision PDF(s) under
    /assets/documents/*.pdf. We download and extract full text via
    common/pdf_extract (pdfplumber/pypdf fallback).

The decisions are openly published (no login, no WAF) and reachable from any IP.
Note: Sport Resolutions removes anti-doping decisions once a ban is served, so the
live corpus is a rolling set (~60 decisions at time of writing).

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
logger = logging.getLogger("legal-data-hunter.INTL.SportResolutions")

BASE_URL = "https://www.sportresolutions.com"
LISTING_URL = f"{BASE_URL}/decisions"
PAGE_SIZE = 6
MAX_OFFSET = 600  # safety ceiling on pagination walk
MAX_PDF_BYTES = 50 * 1024 * 1024

# "May 11, 2026 | Athletics | Arbitration"
META_RE = re.compile(r"^(.*?)\|(.*?)(?:\|(.*))?$")


class SportResolutionsScraper(BaseScraper):
    """Scraper for Sport Resolutions published decisions."""

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

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Parse 'May 11, 2026' to ISO 'YYYY-MM-DD'."""
        date_str = (date_str or "").strip()
        for fmt in ("%B %d, %Y", "%b %d, %Y", "%d %B %Y"):
            try:
                return datetime.strptime(date_str, fmt).date().isoformat()
            except ValueError:
                continue
        # Fall back to a bare year if present.
        m = re.search(r"\b(19|20)\d{2}\b", date_str)
        if m:
            return f"{m.group(0)}-01-01"
        return None

    def _slug_from_url(self, url: str) -> str:
        slug = url.rstrip("/").rsplit("/", 1)[-1]
        slug = re.sub(r"[^a-z0-9]+", "-", slug.lower()).strip("-")
        return slug or "decision"

    def _get_entries(self) -> list[dict]:
        """Walk the paginated listing and parse each decision panel."""
        entries = []
        seen_slugs = set()
        offset = 0
        while offset <= MAX_OFFSET:
            path = "/decisions" if offset == 0 else f"/decisions/P{offset}"
            try:
                resp = self.session.get(BASE_URL + path, timeout=30)
                resp.raise_for_status()
            except Exception as e:
                logger.error(f"Listing fetch failed at offset {offset}: {e}")
                break

            soup = BeautifulSoup(resp.content, "html.parser")
            panels = soup.select("div.decision-panel")
            page_new = 0
            for panel in panels:
                h3 = panel.find("h3")
                a = h3.find("a", href=True) if h3 else None
                if not a:
                    continue
                detail_url = a["href"].strip().replace("http://", "https://")
                if not detail_url.startswith("https://"):
                    detail_url = BASE_URL + "/" + detail_url.lstrip("/")
                slug = self._slug_from_url(detail_url)
                if slug in seen_slugs:
                    continue

                title = a.get_text(" ", strip=True)

                date_iso = sport = dtype = None
                meta_p = panel.find("p", class_="decision-date")
                if meta_p:
                    meta = meta_p.get_text(" ", strip=True)
                    m = META_RE.match(meta)
                    if m:
                        date_iso = self._parse_date(m.group(1))
                        sport = (m.group(2) or "").strip() or None
                        dtype = (m.group(3) or "").strip() or None

                # Description: first <p> that is not the meta line.
                description = ""
                for p in panel.find_all("p"):
                    if p is meta_p:
                        continue
                    txt = p.get_text(" ", strip=True)
                    if txt:
                        description = txt
                        break

                seen_slugs.add(slug)
                page_new += 1
                entries.append({
                    "id_slug": slug,
                    "title": title,
                    "date": date_iso,
                    "sport": sport,
                    "decision_type": dtype,
                    "description": description,
                    "detail_url": detail_url,
                })

            logger.info(f"offset {offset}: {len(panels)} panels, {page_new} new")
            if page_new == 0:
                break
            offset += PAGE_SIZE
            time.sleep(1.0)

        logger.info(f"Parsed {len(entries)} decision entries across listing")
        return entries

    def _get_pdf_urls(self, detail_url: str) -> list[str]:
        """Return the full-text decision PDF URL(s) from a detail page."""
        try:
            time.sleep(1.0)
            resp = self.session.get(detail_url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"  Detail fetch failed: {e}")
            return []
        soup = BeautifulSoup(resp.content, "html.parser")
        urls = []
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if ".pdf" not in href.lower():
                continue
            if "/assets/documents/" not in href.lower():
                continue
            full = href.replace("http://", "https://")
            if not full.startswith("https://"):
                full = BASE_URL + "/" + full.lstrip("/")
            if full not in urls:
                urls.append(full)
        return urls

    def _download_pdf(self, url: str) -> Optional[bytes]:
        try:
            time.sleep(1.5)
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            if len(resp.content) > MAX_PDF_BYTES:
                logger.warning(f"  PDF too large ({len(resp.content)} bytes), skipping")
                return None
            if len(resp.content) < 500:
                logger.warning(f"  PDF too small ({len(resp.content)} bytes), likely error")
                return None
            return resp.content
        except Exception as e:
            logger.error(f"  PDF download failed: {e}")
            return None

    def _extract_text(self, pdf_bytes: bytes, source_id: str) -> Optional[str]:
        text = extract_pdf_markdown(
            source="INTL/SportResolutions",
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
        entries = self._get_entries()
        logger.info(f"Total entries to process: {len(entries)}")
        for i, entry in enumerate(entries):
            try:
                logger.info(
                    f"[{i+1}/{len(entries)}] {entry['id_slug'][:50]} - "
                    f"{entry['title'][:50]}..."
                )
                pdf_urls = self._get_pdf_urls(entry["detail_url"])
                if not pdf_urls:
                    logger.warning(f"  No PDF found for {entry['id_slug']}, skipping")
                    continue

                texts = []
                for pdf_url in pdf_urls:
                    pdf_bytes = self._download_pdf(pdf_url)
                    if not pdf_bytes:
                        continue
                    t = self._extract_text(pdf_bytes, entry["id_slug"])
                    if t:
                        texts.append(t)
                if not texts:
                    logger.warning(f"  Insufficient text for {entry['id_slug']}, skipping")
                    continue

                entry["_extracted_text"] = "\n\n".join(texts)
                entry["pdf_url"] = pdf_urls[0]
                yield entry
            except Exception as e:
                logger.error(f"  Error processing {entry['id_slug']}: {e}")
                continue

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        since_iso = since.date().isoformat()
        for entry in self.fetch_all():
            if not entry.get("date") or entry["date"] >= since_iso:
                yield entry

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": f"sr-{raw.get('id_slug', 'decision')}",
            "_source": "INTL/SportResolutions",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("_extracted_text", ""),
            "date": raw.get("date"),
            "url": raw.get("detail_url", LISTING_URL),
            "sport": raw.get("sport", ""),
            "decision_type": raw.get("decision_type", ""),
            "description": raw.get("description", ""),
            "pdf_url": raw.get("pdf_url", ""),
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = SportResolutionsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        entries = scraper._get_entries()
        for e in entries:
            print(f"  {e['date']}  {e['id_slug'][:45]:45}  {(e['sport'] or '')[:20]}")
        print(f"\nTotal: {len(entries)} entries")
        sys.exit(0)

    if command in ("bootstrap", "bootstrap-fast"):
        result = scraper.bootstrap(sample_mode=sample, sample_size=10)
        print(json.dumps(result, indent=2, default=str))
    elif command == "update":
        result = scraper.update()
        print(json.dumps(result, indent=2, default=str))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
