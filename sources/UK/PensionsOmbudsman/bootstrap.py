#!/usr/bin/env python3
"""
Legal Data Hunter - UK Pensions Ombudsman (TPO) Determinations Scraper

Fetches the published determinations of The Pensions Ombudsman (TPO) — the
statutory tribunal (established under the Pension Schemes Act 1993) that
investigates and decides complaints and disputes about the administration and
management of occupational and personal pension schemes. TPO determinations are
final and binding on the parties (enforceable in the county court) and are
subject to appeal only on a point of law to the High Court = case_law.

Source: https://www.pensions-ombudsman.org.uk/decisions
  - The decisions listing is a paginated Drupal archive (/decisions?page={n},
    12 cards/page, ~565 pages) whose cards carry all metadata:
    title, complainant, respondent, outcome, complaint topic, case reference
    and decision date.
  - Each decision has its own page /decision/{year}/{ref}/{slug} which links to
    the determination FILE under /sites/default/files/decisions/.
  - Modern determinations (≈2010-present) are born-digital PDFs; older ones are
    legacy Word .doc/.docx files. Full text is extracted from the PDF (shared
    common.pdf_extract, no OCR) or the .docx (python-docx). Legacy binary .doc
    files have no text layer extractor available and are skipped (logged), so
    only full-text records are emitted.

Coverage: ~6,700 determinations (1998-present); ~3,000 born-digital PDF
determinations extract cleanly.

License: no explicit Open Government Licence statement; the site asserts
"© The Pensions Ombudsman". Treated as custom terms, commercial use flagged.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records for validation
  python bootstrap.py bootstrap-fast     # Alias for full pull (fleet runner)
"""

import io
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
logger = logging.getLogger("UK/PensionsOmbudsman")

MIN_TEXT_CHARS = 200
# Determination file path fragment on the TPO Drupal site.
FILE_FRAGMENT = "/sites/default/files/decisions/"


class UKPensionsOmbudsmanScraper(BaseScraper):
    """Scraper for The Pensions Ombudsman (TPO) determinations."""

    BASE_URL = "https://www.pensions-ombudsman.org.uk"
    LISTING_URL = BASE_URL + "/decisions?page={page}"

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

    def _get_bytes(self, url: str) -> Optional[bytes]:
        self.rate_limiter.wait()
        try:
            resp = self.session.get(url, timeout=90)
            if resp.status_code == 200:
                return resp.content
            logger.warning(f"{resp.status_code} for {url}")
            return None
        except Exception as e:
            logger.warning(f"Request failed for {url}: {e}")
            return None

    # --------------------------------------------------------------- discovery
    @staticmethod
    def _last_page(html: str) -> int:
        pages = [int(n) for n in re.findall(r"[?&]page=(\d+)", html)]
        return max(pages) if pages else 0

    def _parse_cards(self, html: str) -> list:
        """Extract per-decision metadata from a listing page's cards."""
        soup = BeautifulSoup(html, "html.parser")
        cards = []
        for card in soup.select("div.card-item"):
            a = card.find("h3")
            a = a.find("a", href=True) if a else None
            if not a:
                continue
            href = a["href"].split("?")[0].split("#")[0]
            if "/decision/" not in href:
                continue
            title = a.get_text(" ", strip=True)

            fields = {}
            for div in card.find_all("div"):
                txt = div.get_text(" ", strip=True)
                m = re.match(r"^([A-Za-z ]+?):\s*(.+)$", txt)
                if m:
                    key = m.group(1).strip().lower()
                    if key not in fields:
                        fields[key] = m.group(2).strip()

            date = None
            t = card.find("time")
            if t and t.get("datetime"):
                date = t["datetime"][:10]

            cards.append({
                "url": urljoin(self.BASE_URL, href),
                "title": title,
                "ref": fields.get("ref", ""),
                "complainant": fields.get("complainant", ""),
                "respondent": fields.get("respondent", ""),
                "outcome": fields.get("outcome", ""),
                "topic": fields.get("complaint topic", ""),
                "date": date,
            })
        return cards

    def _file_url(self, decision_html: str) -> Optional[str]:
        """Find the determination file (pdf/doc/docx) URL on a decision page."""
        soup = BeautifulSoup(decision_html, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if FILE_FRAGMENT in href and re.search(r"\.(pdf|docx?|)$", href.split("?")[0], re.I):
                if re.search(r"\.(pdf|docx?)$", href.split("?")[0], re.I):
                    return urljoin(self.BASE_URL, href)
        return None

    # --------------------------------------------------------------- extraction
    def _extract_docx(self, url: str) -> str:
        data = self._get_bytes(url)
        if not data:
            return ""
        try:
            import docx
            doc = docx.Document(io.BytesIO(data))
            return "\n".join(p.text for p in doc.paragraphs if p.text.strip()).strip()
        except Exception as e:
            logger.warning(f"docx extraction failed for {url}: {e}")
            return ""

    def _extract_text(self, ref: str, file_url: str) -> str:
        low = file_url.split("?")[0].lower()
        if low.endswith(".pdf"):
            try:
                t = pdf_extract.extract_pdf_markdown(
                    "UK/PensionsOmbudsman", ref or file_url, pdf_url=file_url,
                )
            except Exception as e:
                logger.warning(f"PDF extraction failed for {file_url}: {e}")
                return ""
            return (t or "").strip()
        if low.endswith(".docx"):
            return self._extract_docx(file_url)
        # Legacy binary .doc — no reliable text extractor available; skip.
        return ""

    # ---------------------------------------------------------------- iteration
    def fetch_all(self) -> Generator[dict, None, None]:
        first = self._get(self.LISTING_URL.format(page=0))
        if not first:
            raise RuntimeError(
                "Pensions Ombudsman listing unreachable — possible IP block "
                "(fail loud rather than emit an empty corpus)"
            )
        last = self._last_page(first)
        logger.info(f"Decisions archive: {last + 1} listing pages")

        count = 0
        skipped_doc = 0
        skipped_other = 0
        for page in range(0, last + 1):
            html = first if page == 0 else self._get(self.LISTING_URL.format(page=page))
            if not html:
                continue
            for card in self._parse_cards(html):
                dhtml = self._get(card["url"])
                if not dhtml:
                    skipped_other += 1
                    continue
                file_url = self._file_url(dhtml)
                if not file_url:
                    skipped_other += 1
                    continue
                text = self._extract_text(card["ref"], file_url)
                if len(text) < MIN_TEXT_CHARS:
                    if file_url.split("?")[0].lower().endswith(".doc"):
                        skipped_doc += 1
                    else:
                        skipped_other += 1
                    continue
                count += 1
                raw = dict(card)
                raw["text"] = text
                raw["file_url"] = file_url
                yield raw
            if page % 20 == 0:
                logger.info(
                    f"  page {page}/{last} — {count} determinations "
                    f"({skipped_doc} legacy .doc, {skipped_other} other skips)"
                )
        logger.info(
            f"Total: {count} determinations "
            f"({skipped_doc} legacy .doc skipped, {skipped_other} other skips)"
        )

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Newest determinations are listed first; stop once past `since`."""
        since_str = since.strftime("%Y-%m-%d")
        first = self._get(self.LISTING_URL.format(page=0))
        if not first:
            return
        last = self._last_page(first)
        for page in range(0, last + 1):
            html = first if page == 0 else self._get(self.LISTING_URL.format(page=page))
            if not html:
                continue
            page_new = 0
            for card in self._parse_cards(html):
                if card.get("date") and card["date"] < since_str:
                    continue
                dhtml = self._get(card["url"])
                if not dhtml:
                    continue
                file_url = self._file_url(dhtml)
                if not file_url:
                    continue
                text = self._extract_text(card["ref"], file_url)
                if len(text) < MIN_TEXT_CHARS:
                    continue
                page_new += 1
                raw = dict(card)
                raw["text"] = text
                raw["file_url"] = file_url
                yield raw
            if page > 0 and page_new == 0:
                break

    # ---------------------------------------------------------------- normalize
    def normalize(self, raw: dict) -> dict:
        text = (raw.get("text", "") or "").strip()
        if not text:
            return None

        ref = (raw.get("ref", "") or "").strip()
        if not ref:
            m = re.search(r"/decision/\d{4}/([^/]+)/", raw.get("url", ""))
            ref = m.group(1).upper() if m else raw.get("url", "").rstrip("/").split("/")[-1]

        return {
            "_id": f"UK/PensionsOmbudsman/{ref}",
            "_source": "UK/PensionsOmbudsman",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "case_id": ref,
            "title": raw.get("title", ""),
            "text": text,
            "date": raw.get("date"),
            "complainant": raw.get("complainant", "") or None,
            "respondent": raw.get("respondent", "") or None,
            "outcome": raw.get("outcome", "") or None,
            "topic": raw.get("topic", "") or None,
            "url": raw.get("url", ""),
            "file_url": raw.get("file_url", ""),
        }


# ── CLI entry point ───────────────────────────────────────────────
if __name__ == "__main__":
    scraper = UKPensionsOmbudsmanScraper()

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
