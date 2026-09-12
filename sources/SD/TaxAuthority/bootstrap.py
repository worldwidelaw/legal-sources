#!/usr/bin/env python3
"""
SD/TaxAuthority -- Sudan Taxation Chamber Tax Laws, Regulations & Guidance

Fetches full text of Sudan's tax legislation and the Taxation Chamber's
official tax guidance from tax.gov.sd.

Strategy (two complementary sources, both open, no auth):
  1. Legislation -- the consolidated English tax Acts published as PDFs on the
     /en/tax-laws/ page (Income Tax Act 1986, Stamp Duty Act 1986, Capital
     Gains Tax Act 1986, ...). Downloaded and extracted via common.pdf_extract.
     The site also hosts scanned (image-only) and broken-font Arabic PDFs that
     yield no usable text -- these are filtered out by a readability check.
  2. Doctrine -- the Taxation Chamber's official, substantive tax guidance
     pages exposed through the WordPress REST API (wp-json/wp/v2/pages). These
     reproduce the operative rules for each tax (procedures, levy, scope,
     exemptions, appeals, sanctions ...) in clean HTML, in English and Arabic.

A record is kept only if its text is genuinely readable -- predominantly Latin
(English) or predominantly Arabic-script. Documents that extract to control
characters / private-use glyphs (scanned or broken-font PDFs) are skipped.

Data:
  - 3-4 primary tax Acts (full text, English) -> _type=legislation
  - ~8-11 official tax-guidance pages (English + Arabic) -> _type=doctrine

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import html as html_module
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any
from urllib.parse import unquote

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SD.TaxAuthority")

BASE_URL = "https://tax.gov.sd"
TAX_LAWS_PAGE = f"{BASE_URL}/en/tax-laws/"
WP_PAGES_API = f"{BASE_URL}/wp-json/wp/v2/pages"

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# PDF filenames that are scanned images or broken-font Arabic (no usable text).
# These patterns are excluded before download to avoid wasted bandwidth; the
# readability filter is the ultimate safety net for anything that slips through.
PDF_SKIP_PATTERNS = re.compile(r"(?:^|/)(?:0?2?law\d|list\d|vat_list)", re.I)

# Correct, human-readable titles for the consolidated Act PDFs. The page itself
# labels most links only "Download" and some labels are mismatched, so we prefer
# a title parsed from the PDF's own cover page and fall back to this map.
TITLE_MAP = {
    "The-Stamp-Duty-1.pdf": "The Stamp Duty Act, 1986",
    "Income-tax.pdf": "The Income Tax Act, 1986",
    "The-Income-Tax-Act-1986.pdf": "The Income Tax Act, 1986",
    "Capital-Gains-.pdf": "The Capital Gains Tax Act, 1986",
    "The-Value-Add-tax.pdf": "The Value Added Tax Act, 2001",
}

# WordPress page slugs whose substring marks navigation / chrome rather than
# substantive tax content -- excluded from the doctrine pull.
NAV_DENY = (
    "news", "about", "secretary", "objective", "challenge", "plan", "project",
    "conference", "workshop", "sitemap", "terms", "privacy", "question",
    "network", "vision", "organiz", "achievement", "e_service", "e-service",
    "publication", "home", "feed", "chart", "key-achiev", "services",
)
# Arabic title terms that mark navigation / landing pages (services, home page,
# contact, sitemap) rather than legal content.
NAV_DENY_AR = ("خدمات", "الرئيس", "اتصل", "خريطة")

# Tax-domain keywords used to confirm a page is real tax guidance.
TAX_KEYWORDS = (
    "tax", "vat", "duty", "income", "levy", "exempt", "customs", "registration",
    "return", "agreement", "assessment", "deduction", "invoice", "profit",
    "stamp", "capital gain", "taxpayer", "chamber",
)
# Arabic equivalents so the Chamber's Arabic-language guidance pages are kept
# too (the site mirrors most pages in both languages). "ضريب" is a substring of
# ضريبة / الضريبة / ضرائب / الضريبية.
TAX_KEYWORDS_AR = ("ضريب", "دمغة", "جمارك", "دخل", "قانون", "أرباح", "إعفاء")

MIN_TEXT_CHARS = 400          # minimum usable body length
MIN_PAGE_CHARS = 2500         # WP guidance pages must be substantial
READABLE_RATIO = 0.45         # share of visible chars that must be Latin or Arabic


class SudanTaxScraper(BaseScraper):
    """Scraper for SD/TaxAuthority -- Sudan tax legislation & guidance."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self._seen_hashes: set[str] = set()

    # ----------------------------------------------------------------- helpers

    @staticmethod
    def _is_readable(text: str) -> bool:
        """True if text is predominantly Latin (English) or Arabic script.

        Scanned PDFs extract to ~nothing; broken-font Arabic PDFs extract to
        control chars and Unicode private-use glyphs. Both fail this check,
        while genuine English and genuine Arabic (UTF-8) text pass.
        """
        visible = [c for c in text if not c.isspace()]
        if len(visible) < MIN_TEXT_CHARS:
            return False
        n = len(visible)
        latin = sum(1 for c in text if c.isascii() and c.isalpha())
        arabic = sum(1 for c in text if "؀" <= c <= "ۿ")
        return (latin / n) >= READABLE_RATIO or (arabic / n) >= READABLE_RATIO

    @staticmethod
    def _content_hash(text: str) -> str:
        norm = re.sub(r"\s+", "", text)[:4000]
        return hashlib.sha1(norm.encode("utf-8", "ignore")).hexdigest()

    @staticmethod
    def _html_to_text(html: str) -> str:
        """Strip tags, drop scripts/styles, decode entities, collapse space."""
        html = re.sub(r"(?is)<(script|style)[^>]*>.*?</\1>", " ", html)
        html = re.sub(r"(?i)<br\s*/?>", "\n", html)
        html = re.sub(r"(?i)</(p|div|li|h[1-6]|tr)>", "\n", html)
        text = re.sub(r"<[^>]+>", " ", html)
        text = html_module.unescape(text)
        text = text.replace("​", "").replace("\xa0", " ")
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
        return text.strip()

    @staticmethod
    def _year_to_date(title: str) -> Optional[str]:
        m = re.search(r"\b(19|20)\d{2}\b", title)
        return f"{m.group(0)}-01-01" if m else None

    @staticmethod
    def _make_id(prefix: str, key: str) -> str:
        slug = re.sub(r"[^a-zA-Z0-9]+", "-", key.replace(".pdf", "")).strip("-").lower()
        return f"sd-tax-{prefix}-{slug}"

    @staticmethod
    def _title_from_pdf(text: str, fallback: str) -> str:
        head = re.sub(r"\s+", " ", text[:2500])
        m = re.search(
            r"(The\s+[A-Z][A-Za-z ]{2,45}?(?:Act|Tax|Duty|Law)[A-Za-z ]{0,18}?,?\s*(?:19|20)\d{2})",
            head,
        )
        if m:
            return m.group(1).strip()
        m = re.search(r"(The\s+[A-Z][A-Za-z ]{2,45}?(?:Act|Tax|Duty|Law))\b", head)
        return m.group(1).strip() if m else fallback

    # ----------------------------------------------------------------- sources

    def _fetch(self, url: str, **kw) -> requests.Response:
        self.rate_limiter.wait()
        resp = self.session.get(url, timeout=60, **kw)
        resp.raise_for_status()
        return resp

    def _parse_pdf_links(self, html: str) -> list[dict]:
        """Extract candidate Act PDF links, fixing the site's URL bugs."""
        seen: set[str] = set()
        results: list[dict] = []
        for url in re.findall(r'href="([^"]*\.pdf)"', html):
            if "localhost" in url:
                url = url.replace("http://localhost/tax/", BASE_URL + "/")
            url = re.sub(r"/(\d{2})(law)", r"/\1/\2", url)  # missing slash
            if url.startswith("/"):
                url = BASE_URL + url
            elif not url.startswith("http"):
                url = BASE_URL + "/" + url

            filename = unquote(url.rsplit("/", 1)[-1])
            if filename in seen:
                continue
            seen.add(filename)

            # Skip scanned / broken-font PDFs up front (readability is backstop).
            if PDF_SKIP_PATTERNS.search(filename) or PDF_SKIP_PATTERNS.search(url):
                continue
            # Skip the Arabic committee report (not legislation).
            if "%D8" in url and filename not in TITLE_MAP:
                continue

            results.append({"filename": filename, "pdf_url": url})
        return results

    def _iter_legislation(self) -> Generator[dict, None, None]:
        logger.info("Fetching tax-laws page: %s", TAX_LAWS_PAGE)
        html = self._fetch(TAX_LAWS_PAGE).text
        docs = self._parse_pdf_links(html)
        logger.info("Found %d candidate Act PDF(s)", len(docs))

        for doc in docs:
            doc_id = self._make_id("law", doc["filename"])
            logger.info("Downloading PDF: %s", doc["filename"])
            text = extract_pdf_markdown(
                source="SD/TaxAuthority",
                source_id=doc_id,
                pdf_url=doc["pdf_url"],
                table="legislation",
                force=True,
            ) or ""

            if not self._is_readable(text):
                logger.warning("Skipping unreadable PDF (%d chars): %s",
                               len(text), doc["filename"])
                continue

            h = self._content_hash(text)
            if h in self._seen_hashes:
                logger.info("Skipping duplicate content: %s", doc["filename"])
                continue
            self._seen_hashes.add(h)

            title = self._title_from_pdf(
                text, TITLE_MAP.get(doc["filename"],
                                    doc["filename"].replace(".pdf", "").replace("-", " ").title()))
            yield {
                "_id": doc_id,
                "_type": "legislation",
                "title": title,
                "text": text,
                "date": self._year_to_date(title),
                "url": doc["pdf_url"],
                "category": "Law",
            }

    def _iter_doctrine(self, since: Optional[datetime] = None) -> Generator[dict, None, None]:
        params = {
            "per_page": "100",
            "_fields": "id,slug,title,link,content,date,modified",
        }
        if since is not None:
            params["modified_after"] = since.strftime("%Y-%m-%dT%H:%M:%S")
        logger.info("Fetching WP guidance pages: %s", WP_PAGES_API)
        pages = self._fetch(WP_PAGES_API, params=params).json()
        logger.info("WP API returned %d page(s)", len(pages))

        for p in pages:
            slug = (p.get("slug") or "").lower()
            if any(bad in slug for bad in NAV_DENY):
                continue
            link = p.get("link", "")
            # Skip the site landing page (bare root URL).
            if link.rstrip("/").split("tax.gov.sd", 1)[-1] in ("", "/"):
                continue
            title = self._html_to_text(p.get("title", {}).get("rendered", "")) or slug
            if any(bad in title for bad in NAV_DENY_AR):
                continue
            text = self._html_to_text(p.get("content", {}).get("rendered", ""))
            if len(text) < MIN_PAGE_CHARS or not self._is_readable(text):
                continue
            low = text.lower()
            en_hits = sum(1 for kw in TAX_KEYWORDS if kw in low)
            ar_hits = sum(1 for kw in TAX_KEYWORDS_AR if kw in text)
            if en_hits < 4 and ar_hits < 2:
                continue
            h = self._content_hash(text)
            if h in self._seen_hashes:
                continue
            self._seen_hashes.add(h)

            date = (p.get("date") or "")[:10] or None
            yield {
                "_id": self._make_id("page", slug or str(p.get("id"))),
                "_type": "doctrine",
                "title": title,
                "text": text,
                "date": date,
                "url": p.get("link", ""),
                "category": "Guidance",
            }

    # ----------------------------------------------------------------- API

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": raw["_id"],
            "_source": "SD/TaxAuthority",
            "_type": raw.get("_type", "legislation"),
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "category": raw.get("category", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        yield from self._iter_legislation()
        yield from self._iter_doctrine()

    def fetch_updates(self, since=None) -> Generator[Dict[str, Any], None, None]:
        # Acts change rarely and carry no reliable per-document date -> re-yield.
        yield from self._iter_legislation()
        # Guidance pages support incremental fetch via WP modified_after.
        yield from self._iter_doctrine(since=since)

    def test_connection(self) -> bool:
        try:
            docs = self._parse_pdf_links(self._fetch(TAX_LAWS_PAGE).text)
            pages = self._fetch(WP_PAGES_API, params={"per_page": "1"}).json()
            logger.info("Connection OK: %d Act PDFs, WP API reachable (%d sample page)",
                        len(docs), len(pages))
            return len(docs) > 0
        except Exception as e:
            logger.error("Connection test failed: %s", e)
            return False


def main():
    import argparse
    parser = argparse.ArgumentParser(description="SD/TaxAuthority Bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = SudanTaxScraper()

    if args.command == "test":
        sys.exit(0 if scraper.test_connection() else 1)
    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=18)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info("Bootstrap complete: %d records — %s", fetched, stats)
        if fetched == 0:
            sys.exit(1)
    elif args.command == "update":
        stats = scraper.update()
        logger.info("Update complete: %s", stats)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
