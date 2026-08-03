#!/usr/bin/env python3
"""
GM/Judiciary -- Judiciary of The Gambia: Law Reports & Judgments

Fetches Gambian superior-court decisions published by the National Council for
Law Reporting on judiciary.gov.gm. The site hosts bound law-report volumes and
the Cadi (Sharia) Appeals Panel report as multi-case PDFs. This scraper
downloads each volume and SPLITS it into individual reported cases, each with
its own full text.

Two PDF layouts are handled:
  - Gambia Law Reports volumes: each reported case begins with a standalone
    court-header line ("COURT OF APPEAL OF THE GAMBIA", "SUPREME COURT OF THE
    GAMBIA", "HIGH COURT OF THE GAMBIA"), preceded by the ALL-CAPS case name.
  - Sharia Law Report: each Cadi Appeals Panel case begins with
    "IN THE HIGH COURT OF THE GAMBIA ... APPEAL NO. AP/N/YYYY ... BETWEEN: ...".

Strategy:
  - Scrape /law-report and /sharia-law-report for /sites/default/files/**/*.pdf
  - Download each PDF (the host has an incomplete TLS chain -> verify=False)
  - Extract per-page text (PyMuPDF, pdfplumber fallback) and split into cases

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap --full
  python bootstrap.py test
"""

import argparse
import io
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Tuple
from urllib.parse import urljoin, unquote

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GM.Judiciary")

BASE_URL = "https://judiciary.gov.gm"

# Section pages that link to the report PDFs.
SECTION_PAGES = [
    "https://judiciary.gov.gm/law-report",
    "https://judiciary.gov.gm/sharia-law-report",
]

COURT_LINES = {
    "IN THE HIGH COURT OF THE GAMBIA",
    "COURT OF APPEAL OF THE GAMBIA",
    "SUPREME COURT OF THE GAMBIA",
    "HIGH COURT OF THE GAMBIA",
}

MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}

APPEAL_NO_RE = re.compile(
    r"APPEAL\s*NO\.?\s*[:.]?\s*([A-Z]{0,4}/?\s*\d+\s*/\s*\d{4})", re.I)
DATE_RE = re.compile(
    r"\b(\d{1,2})(?:st|nd|rd|th)?\s+"
    r"(January|February|March|April|May|June|July|August|September|"
    r"October|November|December)\s+(\d{4})\b", re.I)


def _slug(text: str, maxlen: int = 60) -> str:
    s = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return s[:maxlen] or "case"


def _parse_date(text: str) -> Optional[str]:
    m = DATE_RE.search(text)
    if not m:
        yr = re.search(r"\b(19\d{2}|20\d{2})\b", text)
        return f"{yr.group(1)}-01-01" if yr else None
    day = int(m.group(1))
    month = MONTHS[m.group(2).lower()]
    year = int(m.group(3))
    try:
        return f"{year:04d}-{month:02d}-{day:02d}"
    except Exception:
        return f"{year:04d}-01-01"


def _extract_pages(pdf_bytes: bytes) -> List[str]:
    """Return per-page plain text. PyMuPDF first, pdfplumber fallback."""
    try:
        import fitz  # PyMuPDF
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        pages = [doc[i].get_text() for i in range(doc.page_count)]
        doc.close()
        return pages
    except Exception as e:
        logger.warning("PyMuPDF unavailable/failed (%s); trying pdfplumber", e)
    try:
        import pdfplumber
        pages = []
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for p in pdf.pages:
                pages.append(p.extract_text() or "")
        return pages
    except Exception as e:
        logger.error("pdfplumber also failed: %s", e)
        return []


def _is_court_line(s: str) -> bool:
    return re.sub(r"\s+", " ", s.strip()).upper().rstrip(".") in COURT_LINES


def _looks_name(s: str) -> bool:
    letters = [c for c in s if c.isalpha()]
    if len(letters) < 3:
        return False
    return sum(c.isupper() for c in letters) / len(letters) > 0.7


def _strip_headers(page_text: str) -> List[str]:
    out = []
    for line in page_text.split("\n"):
        s = line.strip()
        if not s:
            continue
        if s.upper().startswith("GAMBIA LAW REPORTS"):
            continue
        if re.fullmatch(r"[ivxlcdm]+", s.lower()):  # roman page number
            continue
        if re.fullmatch(r"\d+", s):                  # arabic page number
            continue
        out.append(s)
    return out


def _clean_name(name: str) -> str:
    name = re.sub(r"\s+", " ", name).strip()
    # Drop a leading headnote abbreviation that occasionally bleeds up.
    name = re.sub(r"^(FLD\.?|HELD\.?)\s*", "", name, flags=re.I)
    return name.strip(" .")


def split_law_report(pages: List[str], body_start: int) -> List[Tuple[str, str]]:
    """Split a Gambia Law Reports volume into (case_name, full_text)."""
    flat: List[str] = []
    for i in range(body_start, len(pages)):
        flat.extend(_strip_headers(pages[i]))

    marks = [j for j, s in enumerate(flat) if _is_court_line(s)]
    starts: List[Tuple[int, str]] = []
    for j in marks:
        k = j - 1
        name_lines: List[str] = []
        while k >= 0 and len(name_lines) < 6 and (
                _looks_name(flat[k]) or flat[k].strip().lower() == "v"):
            name_lines.insert(0, flat[k])
            k -= 1
        name = _clean_name(" ".join(name_lines)) or "(unnamed)"
        starts.append((k + 1, name))

    seen = set()
    starts = [x for x in starts if not (x[0] in seen or seen.add(x[0]))]
    starts.sort()

    cases: List[Tuple[str, str]] = []
    for idx, (ns, name) in enumerate(starts):
        end = starts[idx + 1][0] if idx + 1 < len(starts) else len(flat)
        text = "\n".join(flat[ns:end]).strip()
        if len(text) >= 300:
            cases.append((name, text))
    return cases


def _find_body_start(pages: List[str]) -> Optional[int]:
    """Return the PDF page index where the arabic-numbered case body begins."""
    for i, t in enumerate(pages):
        if re.search(r"VOL\.\s*\d+\s*\n\s*1\s*\n", t):
            return i
    # Fallback: first standalone court line's page.
    for i, t in enumerate(pages):
        if any(_is_court_line(l) for l in t.split("\n")):
            return i
    return None


def split_sharia(pages: List[str]) -> List[Tuple[str, str, str]]:
    """Split the Cadi Appeals report into (appeal_no, party, full_text)."""
    full = "\n".join(pages)
    anchor = re.compile(r"U?IN THE HIGH COURT OF THE GAMBIA", re.I)
    starts = []
    for m in APPEAL_NO_RE.finditer(full):
        prev = None
        for a in anchor.finditer(full[:m.start()]):
            prev = a
        starts.append(prev.start() if prev else m.start())
    starts = sorted(set(starts))

    cases: List[Tuple[str, str, str]] = []
    for k, s in enumerate(starts):
        e = starts[k + 1] if k + 1 < len(starts) else len(full)
        seg = full[s:e].strip()
        if len(seg) < 300:
            continue
        apm = APPEAL_NO_RE.search(seg)
        appeal_no = re.sub(r"\s+", "", apm.group(1)) if apm else f"case-{k+1}"
        pm = re.search(r"BETWEEN:?\s*(.{0,120}?)\s*(?:AND|VS)\b", seg, re.S | re.I)
        party = re.sub(r"[.…\s]+", " ", pm.group(1)).strip() if pm else ""
        cases.append((appeal_no, party, seg))
    return cases


class JudiciaryScraper(BaseScraper):
    """Scraper for GM/Judiciary -- Gambian law reports & judgments."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/605.1.15 (KHTML, like Gecko) Safari/605.1.15",
        })
        # The host serves an incomplete TLS chain ("unable to verify the first
        # certificate"); PDF/HTML endpoints themselves are valid.
        self.session.verify = False

    def _get(self, url: str, timeout: int = 90) -> Optional[requests.Response]:
        for attempt in range(3):
            try:
                time.sleep(1.5)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 429:
                    time.sleep(30)
                    continue
                if resp.status_code >= 500:
                    logger.warning("%s for %s", resp.status_code, url)
                    time.sleep(5)
                    continue
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning("Attempt %d failed for %s: %s", attempt + 1, url, e)
                time.sleep(6)
        return None

    def _discover_pdfs(self) -> List[str]:
        seen, out = set(), []
        for page in SECTION_PAGES:
            resp = self._get(page)
            if resp is None:
                logger.warning("Cannot fetch section page %s", page)
                continue
            for href in re.findall(r'href="([^"]+\.pdf)"', resp.text, re.I):
                full = urljoin(BASE_URL, unescape(href))
                if full not in seen:
                    seen.add(full)
                    out.append(full)
        logger.info("Discovered %d report PDF(s)", len(out))
        return out

    def _cases_from_pdf(self, url: str) -> Generator[Dict[str, Any], None, None]:
        resp = self._get(url)
        if resp is None or not resp.content:
            logger.warning("No content for %s", url)
            return
        pages = _extract_pages(resp.content)
        if not pages:
            logger.warning("No extractable text: %s", url)
            return
        fname = unquote(url.rsplit("/", 1)[-1])
        joined = "\n".join(pages[:5]).upper()
        is_sharia = ("SHARIA" in fname.upper() or "CADI APPEALS" in joined
                     or "SHARIA" in joined)

        if is_sharia:
            for appeal_no, party, text in split_sharia(pages):
                title = f"{party} ({appeal_no})" if party else appeal_no
                yield {
                    "doc_id": f"sharia-{_slug(appeal_no)}",
                    "title": title.strip(),
                    "text": text,
                    "date": _parse_date(text),
                    "url": url,
                    "court": "Cadi Appeals Panel, High Court of The Gambia",
                    "citation": appeal_no,
                }
            return

        body_start = _find_body_start(pages)
        if body_start is None:
            logger.info("No case body detected (index-only volume): %s", fname)
            return
        vol_slug = _slug(fname.replace(".pdf", ""), 40)
        cases = split_law_report(pages, body_start)
        for i, (name, text) in enumerate(cases):
            court_m = re.search(
                r"(COURT OF APPEAL|SUPREME COURT|HIGH COURT) OF THE GAMBIA",
                text)
            yield {
                "doc_id": f"{vol_slug}-{i:03d}-{_slug(name, 40)}",
                "title": name,
                "text": text,
                "date": _parse_date(text[:1500]),
                "url": url,
                "court": (court_m.group(0).title() if court_m
                          else "Superior Courts of The Gambia"),
                "citation": None,
            }

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw.get("doc_id", ""),
            "_source": "GM/Judiciary",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", None),
            "url": raw.get("url", ""),
            "court": raw.get("court", None),
            "citation": raw.get("citation", None),
            "jurisdiction": "GM",
            "language": "en",
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        pdfs = self._discover_pdfs()
        if not pdfs:
            logger.error("No report PDFs discovered")
            return
        count = 0
        for url in pdfs:
            logger.info("Processing %s", url)
            for raw in self._cases_from_pdf(url):
                if not raw.get("text") or len(raw["text"]) < 300:
                    continue
                yield raw
                count += 1
        logger.info("Completed: %d cases fetched", count)

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()

    def test(self) -> bool:
        pdfs = self._discover_pdfs()
        if not pdfs:
            logger.error("Cannot discover PDFs")
            return False
        logger.info("Discovered PDFs: %s", pdfs)
        n = 0
        for url in pdfs:
            for raw in self._cases_from_pdf(url):
                n += 1
                if n == 1:
                    logger.info("First case: %s (%d chars)",
                                raw["title"], len(raw["text"]))
                if n >= 5:
                    break
            if n >= 5:
                break
        logger.info("Extracted %d sample cases", n)
        return n > 0


def main():
    parser = argparse.ArgumentParser(description="GM/Judiciary data fetcher")
    parser.add_argument("command", choices=["bootstrap", "bootstrap-fast",
                                            "update", "test"])
    parser.add_argument("--sample", action="store_true",
                        help="Fetch a small sample (default for bootstrap)")
    parser.add_argument("--full", action="store_true", help="Fetch everything")
    args = parser.parse_args()

    scraper = JudiciaryScraper()
    if args.command == "test":
        ok = scraper.test()
        sys.exit(0 if ok else 1)
    elif args.command == "bootstrap":
        scraper.bootstrap(sample_mode=args.sample, sample_size=15)
    elif args.command == "bootstrap-fast":
        scraper.bootstrap_fast()
    elif args.command == "update":
        scraper.bootstrap(sample_mode=False)


if __name__ == "__main__":
    main()
