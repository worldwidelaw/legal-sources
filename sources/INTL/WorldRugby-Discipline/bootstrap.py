#!/usr/bin/env python3
"""
INTL/WorldRugby-Discipline -- World Rugby Disciplinary & Judicial Decisions

Fetches the published full-text decisions of World Rugby's independent
disciplinary process: Judicial Committee / Judicial Officer decisions,
Disciplinary Committee decisions, Foul Play Review Committee (FPRC) decisions,
anti-doping decisions, appeal decisions, and the CAS awards rendered on appeal.
World Rugby publishes the full, redacted written decision (with the panel's
findings, reasoning and sanction) as a PDF for each case it adjudicates,
including Rugby World Cup and international-window matters.

Strategy:
  - The disciplinary decisions hub
        world.rugby/organisation/governance/discipline/decisions
    is a single server-rendered HTML page (no login, no WAF, reachable from any
    IP) that links every published decision PDF directly. The PDFs are hosted on
    pulse-static-files.s3.amazonaws.com and resources.world.rugby under a
    /document/YYYY/MM/DD/<uuid>/<descriptive-filename>.pdf path.
  - The decision date is taken from the /document/YYYY/MM/DD/ path segment (the
    most reliable signal); the descriptive filename supplies the title (player /
    union names and decision type). For each PDF we download and extract the full
    text.

The site openly publishes these decisions; build from a normal IP.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py test               # Print discovered PDF entries
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import unquote, urljoin

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
logger = logging.getLogger("legal-data-hunter.INTL.WorldRugby-Discipline")

BASE_URL = "https://www.world.rugby"
DECISIONS_URL = "https://www.world.rugby/organisation/governance/discipline/decisions?lang=en"

MAX_PDF_BYTES = 50 * 1024 * 1024

# Date embedded in the document storage path, e.g. /document/2021/07/14/<uuid>/...
PATH_DATE_RE = re.compile(r"/document/(\d{4})/(\d{2})/(\d{2})/")
# Some filenames also carry a YYMMDD prefix, e.g. "210714-July-Int-ls-...".
FN_DATE_RE = re.compile(r"^(\d{2})(\d{2})(\d{2})[-_]")


class WorldRugbyDisciplineScraper(BaseScraper):
    """Scraper for World Rugby published full-text disciplinary decisions."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/124.0 Safari/537.36 LegalDataHunter/1.0",
            "Accept": "text/html,application/xhtml+xml,application/pdf",
            "Accept-Language": "en",
        })

    def _get(self, url: str, timeout: int = 30) -> Optional[requests.Response]:
        try:
            time.sleep(1.0)
            resp = self.session.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp
        except Exception as e:
            logger.warning(f"  GET failed for {url}: {e}")
            return None

    def _date_from_url(self, url: str, filename: str) -> Optional[str]:
        m = PATH_DATE_RE.search(url)
        if m:
            try:
                return datetime(int(m.group(1)), int(m.group(2)),
                                int(m.group(3))).date().isoformat()
            except ValueError:
                pass
        m = FN_DATE_RE.match(filename)
        if m:
            yy = int(m.group(1))
            year = 2000 + yy if yy < 80 else 1900 + yy
            try:
                return datetime(year, int(m.group(2)), int(m.group(3))).date().isoformat()
            except ValueError:
                pass
        return None

    def _title_from_filename(self, stem: str) -> str:
        # Strip a leading YYMMDD- date prefix, turn separators into spaces,
        # collapse whitespace. Filenames reliably name the decision type + party.
        title = FN_DATE_RE.sub("", stem)
        title = re.sub(r"[._]+", " ", title)
        title = re.sub(r"[-]+", " ", title)
        title = re.sub(r"\s+", " ", title).strip(" -–")
        return title or stem

    def _collect_entries(self) -> list[dict]:
        resp = self._get(DECISIONS_URL)
        if not resp:
            logger.error("Could not fetch the decisions hub page")
            return []
        soup = BeautifulSoup(resp.content, "html.parser")
        entries = []
        seen = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if ".pdf" not in href.lower() or "/document/" not in href.lower():
                continue
            pdf_url = urljoin(BASE_URL, href).replace("http://", "https://")
            if pdf_url in seen:
                continue
            seen.add(pdf_url)

            filename = unquote(pdf_url.rsplit("/", 1)[-1])
            stem = re.sub(r"\.pdf$", "", filename, flags=re.IGNORECASE)
            date_iso = self._date_from_url(pdf_url, filename)
            title = self._title_from_filename(stem)

            link_text = re.sub(r"\s+", " ", a.get_text(" ", strip=True)).strip()
            row = a.find_parent(["tr", "li"])
            row_text = ""
            if row:
                row_text = re.sub(r"\s+", " ", row.get_text(" ", strip=True)).strip()

            entries.append({
                "pdf_url": pdf_url,
                "filename": filename,
                "stem": stem,
                "date": date_iso,
                "title": title,
                "description": (row_text or link_text)[:500],
            })
        # Newest first.
        entries.sort(key=lambda e: e.get("date") or "", reverse=True)
        logger.info(f"Total unique decision PDFs discovered: {len(entries)}")
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
            source="INTL/WorldRugby-Discipline",
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
                logger.info(f"[{i+1}/{len(entries)}] {entry['filename'][:70]}")
                pdf_bytes = self._download_pdf(entry["pdf_url"])
                if not pdf_bytes:
                    continue
                sid = entry["stem"][:60]
                text = self._extract_pdf_text(pdf_bytes, sid)
                if not text or len(text.strip()) < 100:
                    logger.warning(f"  Insufficient text for {entry['filename'][:50]}, skipping")
                    continue
                entry["_extracted_text"] = text
                yield entry
            except Exception as e:
                logger.error(f"  Error processing {entry['filename'][:50]}: {e}")
                continue

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        since_iso = since.date().isoformat()
        for rec in self.fetch_all():
            if not rec.get("date") or rec["date"] >= since_iso:
                yield rec

    def _make_id(self, raw: dict) -> str:
        # The filename stem is unique per decision PDF.
        base = raw.get("stem") or "decision"
        slug = re.sub(r"[^a-z0-9]+", "-", base.lower()).strip("-")[:90]
        return f"worldrugby-discipline-{slug or 'decision'}"

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": self._make_id(raw),
            "_source": "INTL/WorldRugby-Discipline",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("_extracted_text", ""),
            "date": raw.get("date"),
            "url": raw.get("pdf_url", ""),
            "description": raw.get("description", ""),
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = WorldRugbyDisciplineScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        entries = scraper._collect_entries()
        for e in entries[:30]:
            print(f"  {e['date']}  {e['title'][:65]}")
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
