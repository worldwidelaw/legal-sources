#!/usr/bin/env python3
"""
KG/GKTS-Telecom — Kyrgyzstan telecommunications regulator (decisions & regulations)

The telecom regulator has been renamed several times: GKTS → NAS (National
Communications Agency) → SRNOS (Service for Regulation and Supervision in the
Communications Sector under the Ministry of Digital Development). Its public
site is nas.gov.kg.

Content: normative acts (положения, инструкции, методические рекомендации),
licensing rules, certification rules, radio-frequency-spectrum rules, and
individual regulatory orders (приказы) on numbering-resource allocation.

Strategy:
  1. Crawl the regulator's topic pages under /dp/ for linked PDF documents.
  2. Download each PDF and extract full text with pdfplumber.
  3. Keep records whose extracted text is clean and substantial
     (born-digital regulations extract cleanly; heavily-scanned scans with
     garbled OCR are dropped by a quality filter).

The site serves an expired TLS certificate, so HTTPS verification is disabled.

Usage:
  python bootstrap.py bootstrap --sample   # sample records for validation
  python bootstrap.py bootstrap            # full pull
  python bootstrap.py update               # incremental (re-crawl; no date API)
  python bootstrap.py test-api             # connectivity / link-count check
"""

import io
import re
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime, timezone
from html import unescape
from typing import Generator, Optional
from urllib.parse import urljoin, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KG.GKTS-Telecom")

BASE_URL = "https://nas.gov.kg"
SOURCE_ID = "KG/GKTS-Telecom"

# Topic pages under /dp/ carrying regulatory documents. Born-digital
# regulation pages are listed first so a --sample run reaches its quota with
# the cleanest content before touching the heavily-scanned order archives.
SECTION_PAGES = [
    ("dp/licenzionnyj-kontrol", "License Control"),
    ("dp/licenzirovanie", "Licensing"),
    ("dp/sertifikaciya", "Certification"),
    ("dp/radiochastotnyj-spektr", "Radio Frequency Spectrum"),
    ("dp/gosudarstvennye-uslugi", "State Services Standards"),
    ("dp/resurs-numeracii", "Numbering Resource"),
    ("dp/razreshenie-na-vvoz", "Import Permits"),
    ("dp/formy-otchetnosti", "Reporting Forms"),
    ("dp/prikazy-slujby", "Service Orders"),
    ("dp/pri-videl-tel-nom-res", "Numbering Allocation Orders"),
]

# Skip oversized PDFs (slow server ~0.5 MB/s; large scans rarely add clean text).
MAX_PDF_BYTES = 12_000_000
MAX_PDF_PAGES = 40
MIN_TEXT_CHARS = 450
MIN_CYRILLIC = 200

RU_MONTHS = {
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4, "мая": 5, "июня": 6,
    "июля": 7, "августа": 8, "сентября": 9, "октября": 10, "ноября": 11,
    "декабря": 12,
}


def _clean_text(text: str) -> str:
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _is_clean(text: str) -> bool:
    """Reject garbled OCR. Clean Cyrillic prose has many long word tokens;
    bad scans dissolve into single/double-character fragments."""
    if len(text) < MIN_TEXT_CHARS:
        return False
    cyr = sum(1 for c in text if "Ѐ" <= c <= "ӿ")
    if cyr < MIN_CYRILLIC:
        return False
    tokens = [t for t in text.split() if any(ch.isalpha() for ch in t)]
    if not tokens:
        return False
    long_tokens = [t for t in tokens if len(t) >= 5]
    return (len(long_tokens) / len(tokens)) >= 0.28


def _parse_date(label: str, href: str) -> Optional[str]:
    """Best-effort ISO date from the label or filename."""
    hay = unquote(href) + " " + label
    m = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", hay)
    if m:
        d, mo, y = (int(x) for x in m.groups())
        try:
            return datetime(y, mo, d).date().isoformat()
        except ValueError:
            pass
    m = re.search(r"(\d{1,2})\s+([А-Яа-яёЁ]+)\s+(\d{4})", hay)
    if m:
        d = int(m.group(1)); mo = RU_MONTHS.get(m.group(2).lower()); y = int(m.group(3))
        if mo:
            try:
                return datetime(y, mo, d).date().isoformat()
            except ValueError:
                pass
    m = re.search(r"\b(20\d{2})\b", hay)
    if m:
        return f"{m.group(1)}-01-01"
    return None


class GKTSTelecomScraper(BaseScraper):

    def __init__(self):
        super().__init__(str(Path(__file__).parent))
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/pdf,*/*",
                "Accept-Language": "ru-RU,ru;q=0.9,ky;q=0.8,en;q=0.7",
            },
            timeout=90,
            verify=False,
        )
        self._seen_urls: set[str] = set()

    def _list_pdfs(self, section_path: str) -> list[dict]:
        url = f"{BASE_URL}/{section_path}/"
        try:
            resp = self.http.get(url, timeout=60)
        except Exception as e:
            logger.warning("Section fetch error %s: %s", section_path, e)
            return []
        if resp.status_code != 200:
            logger.warning("Section %s -> HTTP %d", section_path, resp.status_code)
            return []

        out = []
        pattern = r'<a\b[^>]*href="([^"]+\.(?:pdf|PDF))"[^>]*>(.*?)</a>'
        for href, label in re.findall(pattern, resp.text, re.DOTALL | re.IGNORECASE):
            full = urljoin(url, unescape(href))
            if full in self._seen_urls:
                continue
            self._seen_urls.add(full)
            label = unescape(re.sub(r"<[^>]+>", " ", label))
            label = re.sub(r"\b(insert_drive_file|picture_as_pdf|description)\b", "", label)
            label = re.sub(r"\s+", " ", label).strip(" . ")
            if not label or len(label) < 5:
                # fall back to a readable filename
                label = unquote(href.rsplit("/", 1)[-1]).rsplit(".", 1)[0]
                label = re.sub(r"[_]+", " ", label).strip()
            out.append({"url": full, "title": label})
        return out

    def _extract_pdf(self, url: str) -> Optional[str]:
        try:
            resp = self.http.get(url, timeout=90)
        except Exception as e:
            logger.warning("PDF fetch error: %s (%s)", url[:80], e)
            return None
        if resp.status_code != 200:
            return None
        data = resp.content
        if not data or not data[:5].startswith(b"%PDF"):
            return None
        if len(data) > MAX_PDF_BYTES:
            logger.info("Skip oversized PDF (%d bytes): %s", len(data), url[:80])
            return None
        try:
            import pdfplumber
            with pdfplumber.open(io.BytesIO(data)) as pdf:
                pages = pdf.pages[:MAX_PDF_PAGES]
                parts = [(p.extract_text() or "") for p in pages]
        except Exception as e:
            logger.warning("PDF parse error: %s (%s)", url[:80], e)
            return None
        return _clean_text("\n".join(parts))

    def _build(self, item: dict, category: str) -> Optional[dict]:
        text = self._extract_pdf(item["url"])
        if not text or not _is_clean(text):
            return None
        doc_id = "gkts-" + re.sub(r"[^a-z0-9]+", "-",
                                  unquote(item["url"].rsplit("/", 1)[-1]).lower()).strip("-")[:80]
        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": item["title"],
            "text": text,
            "date": _parse_date(item["title"], item["url"]),
            "url": item["url"],
            "category": category,
            "language": "ru",
        }

    # ── BaseScraper interface ───────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        self._seen_urls = set()
        for section_path, category in SECTION_PAGES:
            links = self._list_pdfs(section_path)
            logger.info("%s: %d PDF links", category, len(links))
            time.sleep(1)
            for link in links:
                rec = self._build(link, category)
                if rec:
                    yield rec
                time.sleep(1.5)

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        return raw


# ── CLI ─────────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="KG/GKTS-Telecom scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    scraper = GKTSTelecomScraper()

    if args.command == "test-api":
        total = 0
        for path, cat in SECTION_PAGES:
            n = len(scraper._list_pdfs(path))
            total += n
            logger.info("%s: %d PDF links", cat, n)
            time.sleep(1)
        logger.info("Total PDF links: %d", total)
        return

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    limit = 15 if args.sample else None
    count = 0
    for record in scraper.fetch_all():
        count += 1
        if args.sample or count <= 15:
            with open(sample_dir / f"{count:04d}.json", "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
        logger.info("[%d] %s — %d chars (%s)", count,
                    record["title"][:55], len(record["text"]), record.get("date"))
        if limit and count >= limit:
            break
    logger.info("Done: %d records", count)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
