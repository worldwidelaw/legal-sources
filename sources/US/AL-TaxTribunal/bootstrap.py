#!/usr/bin/env python3
"""
US/AL-TaxTribunal -- Alabama Tax Tribunal (Decisions & Orders)

Fetches the full text of every decision and order published by the Alabama
Tax Tribunal — Alabama's independent quasi-judicial tribunal (operative
2014; successor to the Department of Revenue Administrative Law Division)
that hears appeals of tax matters administered by the Alabama Department of
Revenue and self-administered local taxing authorities. Disputes are
taxpayer v. the Revenue Department (or a county/municipality), so the
corpus is case_law.

Access (no JavaScript, no CAPTCHA, no auth):
  The site is WordPress with a custom "decisions" post type exposed through
  the WP REST API:

    GET /wp-json/wp/v2/decisions?per_page=100&page=N   (newest-first)

  Each item gives the post id, slug, the entered date (post `date`), the
  docket number (`title`), the canonical /decisions/{slug}/ link and the
  decision-type / appeal-type / decision-category taxonomy term IDs (mapped
  to labels via the matching /wp-json/wp/v2/<taxonomy> endpoints).

  The full opinion text lives only in the PDF linked from each decision
  page by a single "Download PDF" button — an Elementor anchor whose class
  contains "elementor-button-link" and whose href is the born-digital
  text-layer PDF under /wp-content/uploads/.

Strategy:
  1. Page through the REST API to discover all decisions.
  2. For each, fetch the decision page and extract the Download-PDF href.
  3. Download the PDF and extract text via the shared, OOM-hardened
     common.pdf_extract helper.
  4. Parse the taxpayer/case name from the PDF body; normalize into the
     standard case_law schema (date = the WP post / "Entered" date).

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~12 samples
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import html as _htmllib
import time
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.AL-TaxTribunal")

BASE_URL = "https://www.taxtribunal.alabama.gov"
REST_DECISIONS = BASE_URL + "/wp-json/wp/v2/decisions"
PER_PAGE = 100
MIN_TEXT_CHARS = 200

TAG_RE = re.compile(r"<[^>]+>")
# The single "Download PDF" button: an Elementor anchor (class contains
# "elementor-button-link") whose href is a .pdf. The sidebar "Related
# Decisions" table uses "jet-table__cell-link" instead, so this is unique.
DOWNLOAD_RE = re.compile(
    r'<a\b[^>]*class="[^"]*elementor-button-link[^"]*"[^>]*href="([^"]+\.pdf[^"]*)"',
    re.I,
)
DOWNLOAD_RE_ALT = re.compile(
    r'<a\b[^>]*href="([^"]+\.pdf[^"]*)"[^>]*class="[^"]*elementor-button-link[^"]*"',
    re.I,
)

# Taxpayer name in the PDF body: "ALABAMA TAX TRIBUNAL\n<NAME>, §\nTaxpayer,"
PARTY_RE = re.compile(
    r"ALABAMA\s+TAX\s+TRIBUNAL\s+(.*?)[,§\s]*\b(?:Taxpayer|Petitioner|Appellant)s?\b",
    re.I | re.S,
)


def clean_text(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _strip_tags(html: str) -> str:
    return re.sub(r"\s+", " ", _htmllib.unescape(TAG_RE.sub(" ", html))).strip()


class ALTaxTribunalScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=90,
        )
        self.delay = 1.0
        self._tax_cache: dict[str, dict[int, str]] = {}

    # ---- fetch helpers -------------------------------------------------

    def _get(self, url: str, retries: int = 4) -> str:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200:
                    return resp.text
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return ""

    def _get_json(self, url: str, retries: int = 4):
        txt = self._get(url, retries=retries)
        if not txt:
            return None
        try:
            return json.loads(txt)
        except (ValueError, TypeError):
            logger.warning(f"Non-JSON response for {url}")
            return None

    def _get_bytes(self, url: str, retries: int = 3) -> bytes | None:
        for attempt in range(retries + 1):
            time.sleep(self.delay)
            try:
                resp = self.http.get(url)
                if resp.status_code == 200 and resp.content:
                    return resp.content
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except Exception as e:
                logger.warning(f"Error fetching PDF {url} (attempt {attempt + 1}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
        return None

    # ---- taxonomy ------------------------------------------------------

    def _taxonomy(self, name: str) -> dict[int, str]:
        """Return {term_id: label} for a custom taxonomy, fetched once."""
        if name in self._tax_cache:
            return self._tax_cache[name]
        mapping: dict[int, str] = {}
        page = 1
        while True:
            url = (f"{BASE_URL}/wp-json/wp/v2/{name}"
                   f"?per_page=100&page={page}&_fields=id,name")
            data = self._get_json(url)
            if not data or not isinstance(data, list):
                break
            for term in data:
                tid = term.get("id")
                if tid is not None:
                    mapping[tid] = _htmllib.unescape(term.get("name") or "").strip()
            if len(data) < 100:
                break
            page += 1
        self._tax_cache[name] = mapping
        return mapping

    def _label(self, taxonomy: str, ids) -> str | None:
        if not ids:
            return None
        mapping = self._taxonomy(taxonomy)
        labels = [mapping.get(i) for i in ids if mapping.get(i)]
        return ", ".join(labels) if labels else None

    # ---- discovery -----------------------------------------------------

    def discover_documents(self, sample: bool = False) -> Generator[dict, None, None]:
        """Yield decision descriptors from the WP REST API (newest-first)."""
        page = 1
        total = 0
        fields = "id,slug,date,link,title,decision-type,appeal-type,decision-category"
        while True:
            url = f"{REST_DECISIONS}?per_page={PER_PAGE}&page={page}&_fields={fields}"
            data = self._get_json(url)
            if not data or not isinstance(data, list):
                break
            for item in data:
                slug = item.get("slug")
                if not slug:
                    continue
                title = ""
                t = item.get("title")
                if isinstance(t, dict):
                    title = _strip_tags(t.get("rendered") or "")
                date = (item.get("date") or "")[:10] or None
                yield {
                    "id": item.get("id"),
                    "slug": slug,
                    "docket": title,
                    "date": date,
                    "link": item.get("link") or f"{BASE_URL}/decisions/{slug}/",
                    "decision_type": self._label("decision-type",
                                                 item.get("decision-type")),
                    "appeal_type": self._label("appeal-type",
                                               item.get("appeal-type")),
                    "category": self._label("decision-category",
                                            item.get("decision-category")),
                }
                total += 1
            if len(data) < PER_PAGE:
                break
            if sample and total >= 40:
                break
            page += 1
        logger.info(f"Discovered {total} decisions via REST API")

    def _pdf_url(self, page_html: str, page_url: str) -> str | None:
        m = DOWNLOAD_RE.search(page_html) or DOWNLOAD_RE_ALT.search(page_html)
        if not m:
            return None
        return urllib.parse.urljoin(page_url, _htmllib.unescape(m.group(1)))

    @staticmethod
    def _party_name(text: str) -> str | None:
        m = PARTY_RE.search(text[:1200])
        if not m:
            return None
        name = re.sub(r"\s+", " ", m.group(1)).strip(" ,.§-")
        # Guard against runaway matches if the layout is unusual.
        if 0 < len(name) <= 160 and "\n" not in name:
            return name
        return None

    def _build_raw(self, doc: dict) -> dict | None:
        page_html = self._get(doc["link"])
        if not page_html:
            logger.warning(f"No decision page for {doc['slug']}")
            return None
        pdf_url = self._pdf_url(page_html, doc["link"])
        if not pdf_url:
            logger.warning(f"No Download-PDF link for {doc['slug']}")
            return None
        pdf_bytes = self._get_bytes(pdf_url)
        if not pdf_bytes:
            return None
        text = extract_pdf_markdown(
            "US/AL-TaxTribunal",
            doc["slug"],
            pdf_bytes=pdf_bytes,
            table="case_law",
            force=True,
        )
        text = clean_text(text or "")
        if len(text) < MIN_TEXT_CHARS:
            logger.warning(f"Insufficient text ({len(text)} chars), likely "
                           f"scanned: {doc['slug']}")
            return None
        doc = dict(doc)
        doc["text"] = text
        doc["url"] = pdf_url
        doc["case_name"] = self._party_name(text)
        return doc

    # ---- public interface ----------------------------------------------

    def test_api(self) -> bool:
        logger.info("Testing Alabama Tax Tribunal REST API...")
        try:
            docs = []
            for d in self.discover_documents(sample=True):
                docs.append(d)
                if len(docs) >= 5:
                    break
            if not docs:
                logger.error("  No decisions discovered")
                return False
            logger.info(f"  Discovered {len(docs)}+ decisions (partial crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) >= MIN_TEXT_CHARS:
                logger.info(f"  Text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('case_name') or raw['docket']}")
            else:
                logger.error("  Text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard case_law schema."""
        docket = (raw.get("docket") or "").strip()
        case_name = (raw.get("case_name") or "").strip()
        dtype = (raw.get("decision_type") or "Decision").strip()
        parts = []
        if case_name:
            parts.append(case_name)
        if docket:
            parts.append(f"(Docket {docket})")
        if dtype:
            parts.append(f"— {dtype}")
        title = " ".join(parts).strip() or f"Alabama Tax Tribunal {docket or raw['slug']}"
        title = title[:300]
        return {
            "_id": f"US/AL-TaxTribunal/{raw['slug']}",
            "_source": "US/AL-TaxTribunal",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "docket_number": docket or None,
            "case_name": case_name or None,
            "decision_type": raw.get("decision_type"),
            "tax_type": raw.get("appeal_type"),
            "court": "Alabama Tax Tribunal",
            "title": title,
            "text": raw["text"],
            "url": raw.get("url") or raw.get("link"),
            "date": raw.get("date") or None,
            "jurisdiction": "US-AL",
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        examined = 0
        for doc in self.discover_documents(sample=sample):
            examined += 1
            raw = self._build_raw(doc)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return
            if sample and examined >= 40:
                return

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield RAW records (framework normalizes via normalize())."""
        yield from self._iter_raw(sample=False)

    def fetch_sample(self) -> Generator[dict, None, None]:
        yield from self._iter_raw(sample=True)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        for raw in self.fetch_all():
            if not since or (raw.get("date") and raw["date"] >= since):
                yield raw


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/AL-TaxTribunal bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = ALTaxTribunalScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
