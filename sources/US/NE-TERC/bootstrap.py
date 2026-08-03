#!/usr/bin/env python3
"""
US/NE-TERC -- Nebraska Tax Equalization and Review Commission (Decisions & Orders)

Fetches the full text of every decision and order of the Nebraska Tax
Equalization and Review Commission (TERC) — Nebraska's independent
quasi-judicial commission that hears and decides appeals of property-tax
valuations and equalization, exemptions, and related ad-valorem tax
controversies (taxpayer / subdivision v. a County Board of Equalization,
plus statewide equalization proceedings). Each decision resolves a
specific appeal, so the corpus is case_law.

TERC publishes its decisions on a server-rendered Drupal site. The
/decisions-search page is a Search-API view whose results are produced by
the Drupal views AJAX endpoint (/views/ajax, view "decisions"). That view
returns, in a single response, the full list of ~1,100 "week ending …"
index pages spanning April 2003 to the present. Each weekly index page
links the born-digital, text-layer decision PDFs issued that week under
/sites/default/files/... . No JavaScript rendering, no CAPTCHA, no auth.

Strategy:
  1. POST the Drupal views AJAX endpoint to obtain every weekly index
     page URL (each carries its own week-ending date).
  2. Fetch each weekly index page and parse the decision-PDF anchors
     (href + anchor text = docket + parties).
  3. Download each PDF and extract its text layer via common.pdf_extract.
  4. Normalize into the standard case_law schema; the decision date is the
     week-ending date of the index page on which the PDF was published.

Usage:
  python bootstrap.py bootstrap            # Full pull (all decisions)
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample decisions
  python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
  python bootstrap.py test-api             # Connectivity test
"""

from __future__ import annotations

import sys
import json
import logging
import re
import subprocess
import time
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common import pdf_extract

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.NE-TERC")

BASE_URL = "https://terc.nebraska.gov"
AJAX_PATH = "/views/ajax"
VIEW_NAME = "decisions"
VIEW_DISPLAY = "page_1"

# Weekly index page links, e.g. /decisions/decisions-week-ending-june-26-2026
WEEK_RE = re.compile(
    r'href="((?:/[^"]*)?decisions-week-ending-[a-z]+-\d{1,2}-\d{4})"', re.I
)
# Decision PDF anchors: <a href="/sites/default/files/....pdf">case name</a>
PDF_ANCHOR_RE = re.compile(
    r'<a\s+[^>]*href="(/sites/default/files/[^"]+?\.pdf)"[^>]*>(.*?)</a>',
    re.I | re.S,
)
WEEK_DATE_RE = re.compile(
    r"week-ending-([a-z]+)-(\d{1,2})-(\d{4})", re.I
)
MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}
# Leading TERC docket token, e.g. "22R 0633", "23C 0039", "08R-232"
DOCKET_RE = re.compile(r"^\s*(\d{2}[A-Z]\s*[-]?\s*\d{2,4})", re.I)


class NETERCScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 1.0
        self._ua = (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120 Safari/537.36"
        )

    def _curl_bytes(self, url: str, post: str | None = None) -> bytes | None:
        for attempt in range(3):
            time.sleep(self.delay)
            try:
                cmd = ["curl", "-s", "-L", "--connect-timeout", "15",
                       "--max-time", "50", "-A", self._ua, "-H", "Accept: */*"]
                if post is not None:
                    cmd += ["-H", "X-Requested-With: XMLHttpRequest",
                            "--data", post]
                cmd.append(url)
                out = subprocess.run(cmd, capture_output=True, timeout=70)
                if out.returncode == 0 and out.stdout:
                    return out.stdout
            except Exception as e:
                logger.warning(f"curl failed for {url} (attempt {attempt + 1}): {e}")
            time.sleep(2 ** attempt)
        return None

    @staticmethod
    def _unescape(s: str | None) -> str:
        if not s:
            return ""
        s = re.sub(r"<[^>]+>", " ", s)
        s = (s.replace("&amp;", "&").replace("&#039;", "'")
              .replace("&#39;", "'").replace("&quot;", '"')
              .replace("&nbsp;", " ").replace("&ndash;", "-")
              .replace("&rsquo;", "'"))
        return re.sub(r"\s+", " ", s).strip()

    @staticmethod
    def _week_date(week_path: str) -> str | None:
        m = WEEK_DATE_RE.search(week_path)
        if not m:
            return None
        mo = MONTHS.get(m.group(1).lower())
        d = int(m.group(2))
        y = int(m.group(3))
        if mo and 1 <= d <= 31 and 2000 <= y <= 2035:
            return f"{y:04d}-{mo:02d}-{d:02d}"
        return None

    @staticmethod
    def _slug(pdf_href: str) -> str:
        name = urllib.parse.unquote(pdf_href.split("/")[-1])
        name = re.sub(r"\.pdf$", "", name, flags=re.I)
        slug = re.sub(r"[^A-Za-z0-9._-]+", "-", name).strip("-")
        return slug[:180] or "decision"

    @staticmethod
    def _docket(case_name: str) -> str | None:
        m = DOCKET_RE.search(case_name)
        if not m:
            return None
        return re.sub(r"\s*-\s*", "-", re.sub(r"\s+", " ", m.group(1))).strip().upper()

    def discover_week_pages(self) -> list[str]:
        """Return all weekly index page paths (newest-first) via views AJAX."""
        post = urllib.parse.urlencode({
            "view_name": VIEW_NAME,
            "view_display_id": VIEW_DISPLAY,
            "page": "0",
        })
        raw = self._curl_bytes(BASE_URL + AJAX_PATH, post=post)
        weeks: list[str] = []
        seen: set[str] = set()
        if raw:
            try:
                data = json.loads(raw.decode("utf-8", "replace"))
                blob = "".join(
                    c.get("data", "") for c in data
                    if isinstance(c, dict) and c.get("command") == "insert"
                )
            except Exception:
                blob = raw.decode("utf-8", "replace")
            for href in WEEK_RE.findall(blob):
                path = href if href.startswith("/") else "/" + href
                if path not in seen:
                    seen.add(path)
                    weeks.append(path)
        if not weeks:
            # Fallback: parse the plain decisions-search page.
            raw = self._curl_bytes(BASE_URL + "/decisions-search")
            if raw:
                for href in WEEK_RE.findall(raw.decode("utf-8", "replace")):
                    path = href if href.startswith("/") else "/" + href
                    if path not in seen:
                        seen.add(path)
                        weeks.append(path)
        logger.info(f"Discovered {len(weeks)} weekly index pages")
        return weeks

    def discover_documents(self, sample: bool = False) -> list[dict]:
        out: list[dict] = []
        seen_pdf: set[str] = set()
        weeks = self.discover_week_pages()
        for week_path in weeks:
            week_date = self._week_date(week_path)
            html = self._curl_bytes(BASE_URL + week_path)
            if not html:
                continue
            html = html.decode("utf-8", "replace")
            for href, anchor in PDF_ANCHOR_RE.findall(html):
                pdf_url = urllib.parse.urljoin(BASE_URL, href)
                if pdf_url in seen_pdf:
                    continue
                seen_pdf.add(pdf_url)
                case_name = self._unescape(anchor)
                if not case_name:
                    continue
                out.append({
                    "case_name": case_name,
                    "docket": self._docket(case_name),
                    "date": week_date,
                    "pdf_url": pdf_url,
                    "slug": self._slug(href),
                })
            if sample and len(out) >= 16:
                break
        out.sort(key=lambda r: r.get("date") or "0000", reverse=True)
        logger.info(f"Discovered {len(out)} TERC decision PDFs")
        return out

    def _build_raw(self, doc: dict) -> dict | None:
        pdf_bytes = self._curl_bytes(doc["pdf_url"])
        if not pdf_bytes or pdf_bytes[:4] != b"%PDF":
            logger.warning(f"PDF download failed or not a PDF: {doc['pdf_url']}")
            return None
        text = pdf_extract.extract_pdf_markdown(
            "US/NE-TERC", doc["slug"], pdf_bytes=pdf_bytes,
            table="case_law", force=True,
        )
        if not text or len(text.strip()) < 150:
            logger.warning(f"No usable text for {doc['pdf_url']} "
                           f"({len(text) if text else 0} chars)")
            return None
        doc = dict(doc)
        doc["text"] = text.strip()
        return doc

    def test_api(self) -> bool:
        logger.info("Testing Nebraska TERC discovery + PDF extraction...")
        try:
            docs = self.discover_documents(sample=True)
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} decisions (sample crawl)")
            raw = self._build_raw(docs[0])
            if raw and raw["text"] and len(raw["text"]) > 150:
                logger.info(f"  PDF text extraction OK ({len(raw['text'])} chars) — "
                            f"{raw.get('case_name')}")
            else:
                logger.error("  PDF text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw record into the standard case_law schema."""
        case_name = (raw.get("case_name") or "").strip()
        title = case_name or "Nebraska TERC — Decision"
        title = title[:300]
        return {
            "_id": f"US/NE-TERC/{raw['slug']}",
            "_source": "US/NE-TERC",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "docket": raw.get("docket"),
            "court": "Nebraska Tax Equalization and Review Commission",
            "case_name": case_name or None,
            "title": title,
            "text": raw["text"],
            "url": raw["pdf_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-NE",
        }

    def _iter_docs(self, sample: bool = False) -> Generator[dict, None, None]:
        """Stream decision-doc metadata week-by-week.

        Unlike discover_documents (which crawls every weekly index page before
        returning), this yields each week's docs as soon as that week is parsed,
        so sample mode can stop after a handful of weeks instead of enumerating
        the full ~1,100-week corpus first.
        """
        seen_pdf: set[str] = set()
        for week_path in self.discover_week_pages():
            week_date = self._week_date(week_path)
            html = self._curl_bytes(BASE_URL + week_path)
            if not html:
                continue
            html = html.decode("utf-8", "replace")
            for href, anchor in PDF_ANCHOR_RE.findall(html):
                pdf_url = urllib.parse.urljoin(BASE_URL, href)
                if pdf_url in seen_pdf:
                    continue
                seen_pdf.add(pdf_url)
                case_name = self._unescape(anchor)
                if not case_name:
                    continue
                yield {
                    "case_name": case_name,
                    "docket": self._docket(case_name),
                    "date": week_date,
                    "pdf_url": pdf_url,
                    "slug": self._slug(href),
                }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        examined = 0
        for doc in self._iter_docs(sample=sample):
            examined += 1
            raw = self._build_raw(doc)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 12:
                    return
            if sample and examined >= 60:
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

    parser = argparse.ArgumentParser(description="US/NE-TERC bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = NETERCScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
