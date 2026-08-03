"""
Legal Data Hunter — TT/e-Gazette

Trinidad & Tobago Official Gazette / Government Printery e-Gazette.

Source site (open Apache directory listing):
  https://printery.gov.tt/e-gazette/

Access strategy (no API; open directory of per-document PDFs):
  1. The e-gazette root lists year directories (2000, 2001, 2023-2026).
  2. Recent years (2023+) are organized into subfolders:
       Acts/            -> enacted Acts of Parliament (legislation)
       Legal Notices/   -> statutory instruments / subsidiary legislation
       Gazette(s)/, Bills/, Hansard/, Debate.../  (not collected here)
     We collect "Acts" and "Legal Notices" only — both are legislation with
     full text. Bills are not yet law; Gazette compilations and Hansard
     debates are out of scope for the legislation namespace.
  3. Each PDF is digital-native; full text is extracted with pdfplumber.

Records whose PDF yields no extractable text (scanned-image documents) are
skipped — this source only contributes full-text records.
"""

import io
import re
import sys
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin, unquote

import requests
import pdfplumber

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter")

BASE = "https://www.printery.gov.tt/e-gazette/"

# Minimum extracted characters for a PDF to count as "full text" (filters
# out scanned-image documents that yield little/no text).
MIN_TEXT_CHARS = 400

# Folder names (case-insensitive substring match) we treat as legislation.
WANTED_FOLDERS = ("act", "legal notice")

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}


class SourceScraper(BaseScraper):
    """Scraper for TT/e-Gazette (legislation, full text via PDF)."""

    def __init__(self):
        super().__init__(Path(__file__).parent)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (compatible; LegalDataHunter/1.0; open legal data research)"
            ),
            "Accept-Language": "en-TT,en;q=0.9",
        })

    # ── HTTP helpers ──────────────────────────────────────────────

    def _get(self, url: str) -> Optional[requests.Response]:
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=90)
                if resp.status_code == 200:
                    return resp
                logger.debug(f"HTTP {resp.status_code} for {url}")
            except requests.RequestException as e:
                logger.debug(f"Request error for {url}: {e}")
            time.sleep(2 * (attempt + 1))
        return None

    @staticmethod
    def _list_links(html: str) -> list:
        """Return href values from an Apache directory listing, excluding
        sort/parent links."""
        out = []
        for href in re.findall(r'href="([^"?][^"]*)"', html):
            if href.startswith("/") or href.startswith("http"):
                continue
            out.append(href)
        return out

    # ── Enumeration ───────────────────────────────────────────────

    def _list_years(self) -> list:
        # The /e-gazette/ index now 302-redirects to the printery homepage,
        # whose year links are ABSOLUTE URLs (e.g. ".../e-gazette/2026/") rather
        # than the relative "2026/" of the old Apache listing. Match both forms
        # so a redesign of the index page can't silently yield 0 years (#1130).
        resp = self._get(BASE)
        if not resp:
            return []
        years = sorted(
            {int(m) for m in re.findall(r'href="(?:[^"]*/e-gazette/)?(\d{4})/"', resp.text)},
            reverse=True,
        )
        return years

    def _list_wanted_folders(self, year: int) -> list:
        """Return (category, folder_url) for Acts / Legal Notices folders of a year."""
        year_url = urljoin(BASE, f"{year}/")
        resp = self._get(year_url)
        if not resp:
            return []
        folders = []
        for href in self._list_links(resp.text):
            if not href.endswith("/"):
                continue
            name = unquote(href).rstrip("/")
            low = name.lower()
            if any(w in low for w in WANTED_FOLDERS):
                folders.append((name, urljoin(year_url, href)))
        return folders

    def _list_pdfs(self, folder_url: str) -> list:
        resp = self._get(folder_url)
        if not resp:
            return []
        pdfs = []
        for href in self._list_links(resp.text):
            if href.lower().endswith(".pdf"):
                pdfs.append(urljoin(folder_url, href))
        return pdfs

    # ── Per-document fetch ────────────────────────────────────────

    def _extract_pdf_text(self, pdf_url: str) -> str:
        resp = self._get(pdf_url)
        if not resp:
            return ""
        data = resp.content
        head = data.find(b"%PDF")
        if head > 0:
            data = data[head:]
        try:
            with pdfplumber.open(io.BytesIO(data)) as pdf:
                parts = [(page.extract_text() or "") for page in pdf.pages]
        except Exception as e:
            logger.debug(f"PDF parse failed for {pdf_url}: {e}")
            return ""
        text = "\n".join(parts)
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
        return text

    # ── Abstract methods ──────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        for year in self._list_years():
            folders = self._list_wanted_folders(year)
            if not folders:
                continue
            for category, folder_url in folders:
                pdfs = self._list_pdfs(folder_url)
                logger.info(f"Year {year} / {category}: {len(pdfs)} PDFs")
                for pdf_url in pdfs:
                    yield {
                        "year": year,
                        "category": category,
                        "pdf_url": pdf_url,
                        "filename": unquote(pdf_url.rsplit("/", 1)[-1]),
                    }
                    time.sleep(0.3)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        # The directory listing exposes no per-file modified filter; restrict the
        # year sweep to the year of `since` onward. Dedup is handled by the base class.
        start_year = since.year
        for year in self._list_years():
            if year < start_year:
                break
            for category, folder_url in self._list_wanted_folders(year):
                for pdf_url in self._list_pdfs(folder_url):
                    yield {
                        "year": year,
                        "category": category,
                        "pdf_url": pdf_url,
                        "filename": unquote(pdf_url.rsplit("/", 1)[-1]),
                    }
                    time.sleep(0.3)

    def normalize(self, raw: dict) -> Optional[dict]:
        text = self._extract_pdf_text(raw["pdf_url"])
        if len(text) < MIN_TEXT_CHARS:
            # Scanned-image document without OCR-able text — skip (no full text).
            return None

        filename = raw["filename"]
        title, doc_number = _title_from_filename(filename, text)
        date = _first_date(text) or f"{raw['year']}-01-01"

        return {
            "_id": f"TT-GAZ-{_slug(filename)}",
            "_source": "TT/e-Gazette",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": raw["pdf_url"],
            "pdf_url": raw["pdf_url"],
            "document_number": doc_number,
            "category": raw["category"],
            "year": raw["year"],
            "language": "en",
            "jurisdiction": "TT",
        }


# ── Helpers ───────────────────────────────────────────────────────

def _slug(filename: str) -> str:
    s = re.sub(r"\.pdf$", "", filename, flags=re.I)
    s = re.sub(r"[^A-Za-z0-9]+", "-", s).strip("-")
    return s


def _title_from_filename(filename: str, text: str) -> tuple:
    """Derive (title, document_number) from the PDF filename, falling back to
    the document text where the filename carries no descriptive title."""
    base = re.sub(r"\.pdf$", "", filename, flags=re.I).replace("_", " ").strip()

    # Acts: "Act No. 3 of 2026 - The Copyright (Amendment) Act, 2026"
    m = re.match(r"(Act\s+No\.?\s*\d+\s+of\s+\d{4})\s*[-–]\s*(.+)", base, re.I)
    if m:
        return m.group(2).strip(), m.group(1).strip()

    # Legal Notice / Gazette with no descriptive title in filename:
    # use the filename as a label and try to pull a subject line from the text.
    doc_number = base
    subject = _subject_from_text(text)
    # Avoid "Act No. 2 of 2026 — Act No. 2 of 2026" style duplication when the
    # text subject just echoes the filename label.
    if subject and subject.lower() not in base.lower() and base.lower() not in subject.lower():
        title = f"{base} — {subject}"
    else:
        title = base
    return title, doc_number


def _subject_from_text(text: str) -> str:
    """Best-effort subject line for a Legal Notice: the first ALL-CAPS or
    'Act'-referencing line after the notice header."""
    for line in text.splitlines():
        line = line.strip()
        if len(line) < 8 or len(line) > 160:
            continue
        low = line.lower()
        if low.startswith(("legal notice", "trinidad and tobago gazette", "vol.", "republic of")):
            continue
        # A subject line typically references an Act/Order/Regulations.
        if re.search(r"\b(Act|Order|Regulations|Rules|Notice|Proclamation)\b", line):
            return re.sub(r"\s+", " ", line)
    return ""


def _first_date(text: str) -> Optional[str]:
    """Find the first 'Nth Month, YYYY' or 'Month DD, YYYY' date and return ISO."""
    # 12th February, 2026  /  1st January 2025
    m = re.search(
        r"(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+),?\s+(\d{4})", text)
    if m:
        d, mon, y = m.group(1), m.group(2).lower(), m.group(3)
        if mon in MONTHS:
            return f"{int(y):04d}-{MONTHS[mon]:02d}-{int(d):02d}"
    # February 12, 2026
    m = re.search(r"([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})", text)
    if m:
        mon, d, y = m.group(1).lower(), m.group(2), m.group(3)
        if mon in MONTHS:
            return f"{int(y):04d}-{MONTHS[mon]:02d}-{int(d):02d}"
    return None


# ── CLI Entry Point ───────────────────────────────────────────────

def main():
    scraper = SourceScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 10
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    # bootstrap-fast is the VPS fleet entrypoint; alias it to the full bootstrap
    # path so it runs the full corpus (streamed to data/records.jsonl by
    # BaseScraper) instead of erroring out and falling back to samples (#1130).
    if command in ("bootstrap", "bootstrap-fast"):
        if sample_mode and command != "bootstrap-fast":
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, {stats['records_updated']} updated, {stats['records_skipped']} skipped")
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

    import json
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
