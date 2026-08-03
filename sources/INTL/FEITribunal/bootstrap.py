#!/usr/bin/env python3
"""
INTL/FEITribunal -- Fédération Équestre Internationale (FEI) Tribunal Decisions

Fetches the published decisions of the FEI Tribunal, the independent judicial
body of the Fédération Équestre Internationale (the world governing body for
equestrian sport) established under Article 38 of the FEI Statutes. The Tribunal
decides Equine Anti-Doping & Controlled Medication (EADCM) cases, Human
Anti-Doping cases, and "other" disciplinary matters (horse abuse, conflicts of
interest, eligibility, etc.).

Strategy:
  - Two category index pages (Equine Anti-Doping Decisions, Other Decisions) each
    link a set of year / year-range sub-pages (2006 -> present).
  - Each year sub-page links the born-digital, full-text decision PDFs hosted
    openly under inside.fei.org/system/files/{YYYY.MM.DD}_..._{Cnn-nnnn}_...pdf.
  - We crawl both categories -> all year sub-pages -> all decision PDFs, download
    each PDF and extract full text via common/pdf_extract (pdfplumber/pypdf).
  - Case metadata (decision date, FEI Tribunal reference number) is recovered
    from the PDF filename, which encodes the date and the C-reference.

inside.fei.org is openly published (no login, no WAF, HTTP 200 from any IP).

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py test               # Print parsed PDF entries
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from urllib.parse import unquote
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
logger = logging.getLogger("legal-data-hunter.INTL.FEITribunal")

BASE_URL = "https://inside.fei.org"
CATEGORY_PAGES = {
    "equine-anti-doping": "https://inside.fei.org/fei/your-role/athletes/fei-tribunal/equine-anti-doping-decisions",
    "other": "https://inside.fei.org/fei/your-role/athletes/fei-tribunal/other-decisions",
}
MAX_PDF_BYTES = 50 * 1024 * 1024

DATE_RE = re.compile(r"(20\d{2})[.\-_](\d{1,2})[.\-_](\d{1,2})")
CREF_RE = re.compile(r"\bC\d{2}-\d{3,4}\b", re.IGNORECASE)


class FEITribunalScraper(BaseScraper):
    """Scraper for FEI Tribunal decisions."""

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

    def _get_year_pages(self, category: str, index_url: str) -> list[str]:
        """Return the year / year-range sub-page URLs for one category."""
        try:
            resp = self.session.get(index_url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"  Index fetch failed for {category}: {e}")
            return []
        soup = BeautifulSoup(resp.content, "html.parser")
        base_path = index_url.replace(BASE_URL, "")
        pages = []
        seen = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].split("?")[0].split("#")[0]
            path = href.replace(BASE_URL, "")
            # Year sub-pages are the index path plus one trailing segment.
            if path.startswith(base_path + "/") and path.rstrip("/") != base_path:
                tail = path[len(base_path) + 1:].strip("/")
                if tail and "/" not in tail and re.search(r"\d{4}", tail):
                    full = BASE_URL + path if not href.startswith("http") else href
                    if full not in seen:
                        seen.add(full)
                        pages.append(full)
        return pages

    def _get_pdf_links(self, page_url: str) -> list[str]:
        try:
            resp = self.session.get(page_url, timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"  Year-page fetch failed {page_url}: {e}")
            return []
        soup = BeautifulSoup(resp.content, "html.parser")
        links = []
        seen = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if not href.lower().split("?")[0].endswith(".pdf"):
                continue
            if "/system/files/" not in href and "/sites/default/files/" not in href:
                continue
            full = href if href.startswith("http") else BASE_URL + href
            full = full.replace("http://", "https://")
            if full not in seen:
                seen.add(full)
                links.append(full)
        return links

    def _get_entries(self) -> list[dict]:
        """Crawl both categories -> year pages -> decision PDF entries."""
        entries = []
        seen_pdf = set()
        for category, index_url in CATEGORY_PAGES.items():
            year_pages = self._get_year_pages(category, index_url)
            logger.info(f"{category}: {len(year_pages)} year sub-pages")
            for yp in year_pages:
                time.sleep(0.5)
                for pdf_url in self._get_pdf_links(yp):
                    if pdf_url in seen_pdf:
                        continue
                    seen_pdf.add(pdf_url)
                    fname = unquote(pdf_url.rsplit("/", 1)[-1])
                    stem = re.sub(r"\.pdf$", "", fname, flags=re.IGNORECASE)

                    date_iso = None
                    dm = DATE_RE.search(stem)
                    if dm:
                        try:
                            date_iso = datetime(
                                int(dm.group(1)), int(dm.group(2)), int(dm.group(3))
                            ).date().isoformat()
                        except ValueError:
                            date_iso = None

                    cref = None
                    cm = CREF_RE.search(stem)
                    if cm:
                        cref = cm.group(0).upper()

                    # Human-readable title: drop the leading date token, tidy.
                    title_src = DATE_RE.sub("", stem, count=1).strip(" _-")
                    title_src = re.sub(r"[_]+", " ", title_src).strip()
                    title = f"FEI Tribunal Decision — {title_src}" if title_src else f"FEI Tribunal Decision {cref or ''}".strip()

                    slug = re.sub(r"[^a-z0-9]+", "-", stem.lower()).strip("-")[:120] or "decision"
                    entries.append({
                        "id_slug": slug,
                        "category": category,
                        "date": date_iso,
                        "case_ref": cref or "",
                        "title": title,
                        "pdf_url": pdf_url,
                        "source_page": yp,
                    })
        logger.info(f"Parsed {len(entries)} decision PDF entries across all categories")
        return entries

    def _download_pdf(self, url: str) -> Optional[bytes]:
        try:
            time.sleep(1.2)
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

    def _extract_text(self, pdf_bytes: bytes, source_id: str) -> Optional[str]:
        text = extract_pdf_markdown(
            source="INTL/FEITribunal",
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
                logger.info(f"[{i+1}/{len(entries)}] {entry['case_ref'] or entry['id_slug'][:40]}")
                pdf_bytes = self._download_pdf(entry["pdf_url"])
                if not pdf_bytes:
                    continue
                text = self._extract_text(pdf_bytes, entry["id_slug"])
                if not text:
                    logger.warning(f"  Insufficient text for {entry['id_slug']}, skipping")
                    continue
                entry["_extracted_text"] = text
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
            "_id": f"fei-{raw.get('id_slug', 'decision')}",
            "_source": "INTL/FEITribunal",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("_extracted_text", ""),
            "date": raw.get("date"),
            "url": raw.get("pdf_url", ""),
            "case_ref": raw.get("case_ref", ""),
            "category": raw.get("category", ""),
            "source_page": raw.get("source_page", ""),
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = FEITribunalScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        entries = scraper._get_entries()
        for e in entries[:30]:
            print(f"  {e['date']}  {e['case_ref'][:12]:12}  {e['category'][:18]:18}  {e['title'][:45]}")
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
