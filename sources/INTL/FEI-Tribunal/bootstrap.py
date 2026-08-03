#!/usr/bin/env python3
"""
INTL/FEI-Tribunal -- FEI Tribunal Decisions (international equestrian sport)

Fetches the published full-text decisions of the FEI Tribunal, the independent
judicial body of the Fédération Équestre Internationale (FEI), the world
governing body for equestrian sport (jumping, dressage, eventing, driving,
endurance, vaulting, reining, para-equestrian). The FEI Tribunal rules on
equine and human anti-doping cases, controlled-medication cases, horse-abuse and
other disciplinary matters, and appeals against Ground Jury decisions. The FEI
publishes the full, redacted decision (or consent award) as a PDF for each case,
plus the CAS awards in FEI-related appeals.

Strategy:
  - The FEI Tribunal hub (inside.fei.org/.../fei-tribunal) links to several
    decision categories, each a server-rendered landing page:
      * equine-anti-doping-decisions
      * equine-anti-doping-consent-awards
      * human-anti-doping-decisions
      * other-decisions
      * cas-decisions
  - Each category landing page links to year / year-range sub-pages
    (e.g. .../other-decisions/2022-2025), and each sub-page is a single
    server-rendered HTML table whose rows each carry a decision date, the
    party names, a short description, and a link to the full decision PDF in
    /system/files/.
  - For each PDF we derive the decision date and case reference from the
    filename (which follows a strict "YYYY.MM.DD_<...case ref...>.pdf" pattern),
    download the PDF, and extract its full text.

The site is openly published (no login, no WAF), but inside.fei.org WAF-rejects
some datacenter IPs; build from a normal/residential IP.

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
logger = logging.getLogger("legal-data-hunter.INTL.FEI-Tribunal")

BASE_URL = "https://inside.fei.org"
HUB_URL = "https://inside.fei.org/fei/your-role/athletes/fei-tribunal"

CATEGORIES = [
    "equine-anti-doping-decisions",
    "equine-anti-doping-consent-awards",
    "human-anti-doping-decisions",
    "other-decisions",
    "cas-decisions",
]

MAX_PDF_BYTES = 50 * 1024 * 1024

# Filename pattern, e.g. "2026.03.24_FINAL DECISION A25-0003 DELESTRE v. FEI.pdf"
DATE_FN_RE = re.compile(r"(\d{4})\.(\d{2})\.(\d{2})")
# Case reference, e.g. A25-0003, C24-0029, PR2020-01
CASEREF_RE = re.compile(r"\b([A-Z]{1,3}\d{2,4}-\d{2,4})\b")


class FEITribunalScraper(BaseScraper):
    """Scraper for FEI Tribunal published full-text decisions."""

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

    def _discover_subpages(self, category: str) -> list[str]:
        """Find the year / year-range sub-pages for a decision category."""
        landing = f"{HUB_URL}/{category}"
        resp = self._get(landing)
        if not resp:
            return []
        soup = BeautifulSoup(resp.content, "html.parser")
        prefix = f"/fei/your-role/athletes/fei-tribunal/{category}/"
        subs = []
        seen = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            path = href.replace(BASE_URL, "")
            if prefix in path and path.rstrip("/") != prefix.rstrip("/"):
                full = urljoin(BASE_URL, path).replace("http://", "https://")
                if full not in seen:
                    seen.add(full)
                    subs.append(full)
        logger.info(f"[{category}] discovered {len(subs)} sub-pages")
        return subs

    def _parse_subpage(self, url: str, category: str) -> list[dict]:
        """Parse one year sub-page table into per-PDF entries."""
        resp = self._get(url)
        if not resp:
            return []
        soup = BeautifulSoup(resp.content, "html.parser")
        entries = []
        seen_local = set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if ".pdf" not in href.lower():
                continue
            pdf_url = urljoin(BASE_URL, href).replace("http://", "https://")
            if pdf_url in seen_local:
                continue
            seen_local.add(pdf_url)

            filename = unquote(pdf_url.rsplit("/", 1)[-1])
            stem = re.sub(r"\.pdf$", "", filename, flags=re.IGNORECASE)

            # Decision date from filename (most reliable).
            date_iso = None
            m = DATE_FN_RE.search(stem)
            if m:
                try:
                    date_iso = datetime(int(m.group(1)), int(m.group(2)),
                                        int(m.group(3))).date().isoformat()
                except ValueError:
                    date_iso = None

            # Case reference from filename.
            cm = CASEREF_RE.search(stem)
            case_ref = cm.group(1) if cm else None

            # Row context (date + parties + description) for the title.
            row = a.find_parent(["tr", "li"])
            row_text = ""
            if row:
                row_text = re.sub(r"\s+", " ", row.get_text(" ", strip=True)).strip()
            link_text = re.sub(r"\s+", " ", a.get_text(" ", strip=True)).strip()

            # Build the title from the (unique, descriptive) PDF filename: strip
            # the leading "YYYY.MM.DD" date, turn separators into spaces, and
            # collapse whitespace. The filename reliably names the decision type
            # and parties; row context is fragile (one container can hold
            # several rows) so it is kept only as a supplementary description.
            title = DATE_FN_RE.sub("", stem, count=1)
            title = re.sub(r"[._]+", " ", title)
            title = re.sub(r"\s+", " ", title).strip(" -–")
            title = title or link_text or stem

            entries.append({
                "pdf_url": pdf_url,
                "filename": filename,
                "stem": stem,
                "case_ref": case_ref,
                "date": date_iso,
                "title": title,
                "description": row_text[:500],
                "category": category,
                "source_page": url,
            })
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
            source="INTL/FEI-Tribunal",
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

    def _collect_entries(self) -> list[dict]:
        all_entries = []
        seen = set()
        for category in CATEGORIES:
            for sub in self._discover_subpages(category):
                for e in self._parse_subpage(sub, category):
                    if e["pdf_url"] in seen:
                        continue
                    seen.add(e["pdf_url"])
                    all_entries.append(e)
        logger.info(f"Total unique decision PDFs discovered: {len(all_entries)}")
        return all_entries

    def fetch_all(self) -> Generator[dict, None, None]:
        entries = self._collect_entries()
        for i, entry in enumerate(entries):
            try:
                logger.info(f"[{i+1}/{len(entries)}] {entry['filename'][:70]}")
                pdf_bytes = self._download_pdf(entry["pdf_url"])
                if not pdf_bytes:
                    continue
                sid = entry["case_ref"] or entry["stem"][:60]
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
        # The filename stem is unique per decision PDF (case refs alone collide,
        # since one case can have an operative + a final + a settlement PDF).
        base = raw.get("stem") or raw.get("case_ref") or "decision"
        slug = re.sub(r"[^a-z0-9]+", "-", base.lower()).strip("-")[:90]
        return f"fei-tribunal-{slug or 'decision'}"

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": self._make_id(raw),
            "_source": "INTL/FEI-Tribunal",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("_extracted_text", ""),
            "date": raw.get("date"),
            "url": raw.get("pdf_url", ""),
            "case_ref": raw.get("case_ref") or "",
            "category": raw.get("category", ""),
            "description": raw.get("description", ""),
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
        entries = scraper._collect_entries()
        for e in entries[:30]:
            print(f"  {e['date']}  {e['case_ref'] or '-':12} {e['title'][:60]}")
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
