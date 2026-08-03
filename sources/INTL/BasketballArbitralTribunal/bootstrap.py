#!/usr/bin/env python3
"""
INTL/BasketballArbitralTribunal -- FIBA Basketball Arbitral Tribunal (BAT) Awards

Fetches arbitral awards from the FIBA Basketball Arbitral Tribunal (BAT), the
specialised sports-arbitration body that resolves financial and contractual
disputes in international basketball (players/coaches/agents vs. clubs).

Strategy:
  - The awards listing at about.fiba.basketball/.../bat-awards is a Next.js page
    that server-renders the most-recent batch of awards as cards. Each card
    carries the case number, publication date, party names, a summary, and a
    direct <a download> link to the born-digital award PDF hosted on the open
    Cloudinary host assets.fiba.basketball.
  - We parse those cards, download each award PDF, and extract full text via
    common/pdf_extract.

Limitation:
  - The server-rendered page exposes the most-recent ~10 awards. The full
    historical corpus (case numbers reach BAT 2037/23; BAT has operated since
    2007, est. ~1,000-1,500 awards) is paginated client-side through the
    Contentful gateway at digital-api.fiba.basketball/hapi/getcustomgateway.
    Extending fetch_all to that gateway is a follow-up; the static page already
    yields full-text awards for the recent set.

Usage:
  python bootstrap.py bootstrap          # Full pull (recent awards)
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
logger = logging.getLogger("legal-data-hunter.INTL.BasketballArbitralTribunal")

LISTING_URL = "https://about.fiba.basketball/en/services/basketball-arbitral-tribunal/bat-awards"
MAX_PDF_BYTES = 50 * 1024 * 1024

CASE_RE = re.compile(r"^\d{1,4}/\d{2,4}$")
DATE_RE = re.compile(r"^[A-Z][a-z]+ \d{1,2}, \d{4}$")


class BasketballArbitralTribunalScraper(BaseScraper):
    """Scraper for FIBA Basketball Arbitral Tribunal awards."""

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
        """Parse 'May 19, 2026' to ISO '2026-05-19'."""
        try:
            return datetime.strptime(date_str.strip(), "%B %d, %Y").date().isoformat()
        except (ValueError, AttributeError):
            return None

    def _leaf_texts(self, node) -> list[str]:
        """Return non-empty text of leaf <div>s (no nested <div>) under node, in order."""
        out = []
        for d in node.find_all("div"):
            if d.find("div"):
                continue
            t = d.get_text(strip=True)
            if t:
                out.append(t)
        return out

    def _get_entries(self) -> list[dict]:
        """Parse the awards listing page into structured entries."""
        resp = self.session.get(LISTING_URL, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")

        anchors = [
            a for a in soup.find_all("a", href=True)
            if a["href"].lower().endswith(".pdf") and a.has_attr("download")
        ]

        entries = []
        seen = set()
        for a in anchors:
            pdf_url = a["href"]
            # Normalize to https and strip Cloudinary transform segment.
            pdf_url = pdf_url.replace("http://", "https://")
            pdf_url = re.sub(r"/image/upload/[^/]*f_auto[^/]*/", "/image/upload/", pdf_url)
            if pdf_url in seen:
                continue

            # Walk up to the card node whose leaf divs include a case number.
            case = date_iso = title = summary = None
            node = a
            for _ in range(6):
                node = node.parent
                if node is None:
                    break
                texts = self._leaf_texts(node)
                cnums = [t for t in texts if CASE_RE.match(t)]
                if not cnums:
                    continue
                case = cnums[0]
                ci = texts.index(case)
                rest = texts[ci:]  # [case, date?, title?, summary?]
                dates = [t for t in rest if DATE_RE.match(t)]
                date_iso = self._parse_date(dates[0]) if dates else None
                meta = [t for t in rest if t != case and not DATE_RE.match(t)]
                if meta:
                    title = meta[0]
                if len(meta) > 1:
                    summary = meta[1]
                break

            if not case:
                logger.warning(f"  Could not resolve metadata for {pdf_url}")
                continue

            seen.add(pdf_url)
            entries.append({
                "case_number": case,
                "date": date_iso,
                "title": title or f"BAT Award {case}",
                "summary": summary or "",
                "pdf_url": pdf_url,
            })

        logger.info(f"Parsed {len(entries)} award entries from listing page")
        return entries

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
            source="INTL/BasketballArbitralTribunal",
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
                    f"[{i+1}/{len(entries)}] BAT {entry['case_number']} - "
                    f"{entry['title'][:50]}..."
                )
                pdf_bytes = self._download_pdf(entry["pdf_url"])
                if not pdf_bytes:
                    continue
                text = self._extract_text(pdf_bytes, entry["case_number"])
                if not text:
                    logger.warning(f"  Insufficient text for {entry['case_number']}, skipping")
                    continue
                entry["_extracted_text"] = text
                yield entry
            except Exception as e:
                logger.error(f"  Error processing {entry['case_number']}: {e}")
                continue

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """The listing already surfaces the most-recent awards; yield those."""
        since_iso = since.date().isoformat()
        for entry in self.fetch_all():
            if not entry.get("date") or entry["date"] >= since_iso:
                yield entry

    def normalize(self, raw: dict) -> dict:
        case_num = raw.get("case_number", "unknown")
        uid_slug = re.sub(r"[^a-z0-9]+", "-", case_num.lower()).strip("-")
        return {
            "_id": f"fiba-bat-{uid_slug}",
            "_source": "INTL/BasketballArbitralTribunal",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("_extracted_text", ""),
            "date": raw.get("date"),
            "url": LISTING_URL,
            "case_number": case_num,
            "summary": raw.get("summary", ""),
            "pdf_url": raw.get("pdf_url", ""),
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = BasketballArbitralTribunalScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        entries = scraper._get_entries()
        for e in entries:
            print(f"  BAT {e['case_number']}  {e['date']}  {e['title'][:60]}")
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
