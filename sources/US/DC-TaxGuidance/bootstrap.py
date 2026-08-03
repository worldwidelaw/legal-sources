#!/usr/bin/env python3
"""
US/DC-TaxGuidance -- DC Office of Tax and Revenue (OTR): Tax Notices,
                     Rulings, Private Letter Rulings & Declaratory Orders

Fetches the FULL TEXT of the District of Columbia Office of Tax and Revenue's
published interpretive tax guidance -- OTR Tax Notices, Sales Tax Notices, Tax
Rulings, Private Letter Rulings, Declaratory Orders and combined-reporting
notices. These are the DC tax authority's official interpretive guidance on how
DC tax law applies = doctrine.

These are public District of Columbia government works (public domain,
government-edicts doctrine).

Site: otr.cfo.dc.gov (Drupal). Each guidance category has a landing page under
/page/... whose main content lists the individual guidance documents, either as
a direct born-digital PDF link (under /sites/default/files/.../attachments/) or
as a /node/{id} link. Each /node/{id} page renders an <h1> title and embeds the
document's born-digital PDF under /attachments/. The scraper walks every
category page, collects both direct-PDF and node links, resolves each node to
its embedded PDF, downloads and extracts the full text (born-digital text layer,
no OCR needed). No JavaScript, no CAPTCHA, no auth.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample docs
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
import html as html_lib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.DC-TaxGuidance")

BASE = "https://otr.cfo.dc.gov"

# OTR guidance category landing pages (each lists individual documents).
CATEGORY_PAGES = [
    ("Tax Notice", "/page/otr-tax-notices-and-guidance"),
    ("Sales Tax Notice", "/page/sales-tax-information-and-guidance"),
    ("Tax Ruling", "/page/otr-tax-rulings"),
    ("Private Letter Ruling", "/page/otr-private-letter-rulings"),
    ("Declaratory Order", "/page/otr-declaratory-orders"),
    ("Combined Reporting Notice", "/page/notices-regarding-combined-reporting"),
]

NODE_RE = re.compile(r'href="(/node/\d+)"', re.I)
PDF_RE = re.compile(
    r'href="(https?://otr\.cfo\.dc\.gov/sites/default/files/[^"]*?\.pdf)"', re.I)
ATTACH_RE = re.compile(r'href="([^"]*?/attachments/[^"]*?\.pdf)"', re.I)
H1_RE = re.compile(r"<h1[^>]*>(.*?)</h1>", re.S | re.I)
TAG_RE = re.compile(r"<[^>]+>")

MON = ["January", "February", "March", "April", "May", "June", "July",
       "August", "September", "October", "November", "December"]
DATE_RE = re.compile(r"\b(" + "|".join(MON) + r")\s+(\d{1,2}),?\s+(\d{4})")
NDATE_RE = re.compile(r"\b(\d{1,2})/(\d{1,2})/(\d{4})\b")
YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")


class DCTaxGuidanceScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.delay = 0.6
        self._ua = (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
        )

    # ---- HTTP helpers ----------------------------------------------------

    def _curl_text(self, url: str) -> str | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "90", "-A", self._ua,
                     "-H", "Accept: */*", url],
                    capture_output=True, timeout=120,
                )
                if out.returncode == 0 and out.stdout:
                    return out.stdout.decode("utf-8", "replace")
            except Exception as e:
                logger.warning(f"curl GET failed for {url} (try {attempt+1}): {e}")
            time.sleep(2 ** attempt)
        return None

    def _curl_bytes(self, url: str) -> bytes | None:
        for attempt in range(4):
            time.sleep(self.delay)
            try:
                out = subprocess.run(
                    ["curl", "-s", "-L", "--max-time", "120", "-A", self._ua,
                     "-H", "Accept: */*", url],
                    capture_output=True, timeout=150,
                )
                if out.returncode == 0 and out.stdout and out.stdout[:5] == b"%PDF-":
                    return out.stdout
            except Exception as e:
                logger.warning(f"curl PDF failed for {url} (try {attempt+1}): {e}")
            time.sleep(2 ** attempt)
        return None

    # ---- parsing ---------------------------------------------------------

    @staticmethod
    def _clean(fragment: str) -> str:
        return re.sub(r"\s+", " ",
                      html_lib.unescape(TAG_RE.sub(" ", fragment))).strip()

    @staticmethod
    def _abs(href: str) -> str:
        href = html_lib.unescape(href)
        if href.lower().startswith("http"):
            return href
        return BASE + "/" + href.lstrip("/")

    @staticmethod
    def _slug(pdf_url: str) -> str:
        name = unquote(pdf_url.rstrip("/").rsplit("/", 1)[-1])
        name = re.sub(r"\.pdf$", "", name, flags=re.I)
        slug = re.sub(r"[^A-Za-z0-9._-]+", "-", name).strip("-")
        return (slug or "otr-guidance")[:140]

    @classmethod
    def _date_iso(cls, text: str, title: str) -> str | None:
        for src in (text, title):
            for mo, d, y in DATE_RE.findall(src or ""):
                yi = int(y)
                if 1990 <= yi <= 2035:
                    return f"{yi:04d}-{MON.index(mo)+1:02d}-{int(d):02d}"
            for mm, dd, y in NDATE_RE.findall(src or ""):
                yi, mi, di = int(y), int(mm), int(dd)
                if 1990 <= yi <= 2035 and 1 <= mi <= 12 and 1 <= di <= 31:
                    return f"{yi:04d}-{mi:02d}-{di:02d}"
        ym = YEAR_RE.search(title or "")
        if ym:
            return f"{ym.group(0)}-01-01"
        return None

    def discover_documents(self, sample: bool = False) -> list[dict]:
        seen_pdf: set[str] = set()
        seen_node: set[str] = set()
        out: list[dict] = []
        for category, path in CATEGORY_PAGES:
            html = self._curl_text(BASE + path)
            if not html:
                logger.warning(f"Failed to fetch category page {path}")
                continue
            direct = 0
            for pdf in PDF_RE.findall(html):
                url = self._abs(pdf)
                if url in seen_pdf:
                    continue
                seen_pdf.add(url)
                out.append({"category": category, "pdf_url": url,
                            "node_url": None, "slug": self._slug(url)})
                direct += 1
            nodes = 0
            for node in NODE_RE.findall(html):
                nurl = self._abs(node)
                if nurl in seen_node:
                    continue
                seen_node.add(nurl)
                out.append({"category": category, "pdf_url": None,
                            "node_url": nurl, "slug": None})
                nodes += 1
            logger.info(f"  {path}: {direct} direct PDFs + {nodes} nodes")
            if sample and len(out) >= 24:
                break
        logger.info(f"Discovered {len(out)} candidate documents")
        return out

    def _resolve_node(self, doc: dict) -> dict | None:
        html = self._curl_text(doc["node_url"])
        if not html:
            return None
        h1 = H1_RE.search(html)
        title = self._clean(h1.group(1)) if h1 else None
        m = ATTACH_RE.search(html) or PDF_RE.search(html)
        if not m:
            return None
        doc = dict(doc)
        doc["pdf_url"] = self._abs(m.group(1))
        doc["slug"] = self._slug(doc["pdf_url"])
        doc["title"] = title
        doc["_node_html"] = html
        return doc

    def _build_raw(self, doc: dict) -> dict | None:
        node_html = None
        if doc.get("node_url") and not doc.get("pdf_url"):
            resolved = self._resolve_node(doc)
            if not resolved:
                return None
            doc = resolved
            node_html = doc.pop("_node_html", None)
        pdf_bytes = self._curl_bytes(doc["pdf_url"])
        if not pdf_bytes:
            logger.warning(f"PDF download failed: {doc['pdf_url']}")
            return None
        try:
            text = extract_pdf_markdown(
                "US/DC-TaxGuidance", doc["slug"],
                pdf_bytes=pdf_bytes, table="doctrine", force=True,
            )
        except Exception as e:
            logger.warning(f"Extraction error for {doc['slug']}: {e}")
            return None
        if not text or len(text.strip()) < 80:
            logger.warning(f"No usable text for {doc['pdf_url']} "
                           f"({len(text or '')} chars)")
            return None
        title = doc.get("title") or unquote(doc["slug"]).replace("-", " ").strip()
        doc = dict(doc)
        doc["title"] = title
        doc["text"] = text.strip()
        doc["date"] = self._date_iso(text[:4000], title)
        return doc

    def test_api(self) -> bool:
        logger.info("Testing DC OTR guidance discovery + PDF extraction...")
        try:
            docs = self.discover_documents(sample=True)
            if not docs:
                logger.error("  No documents discovered")
                return False
            logger.info(f"  Discovered {len(docs)} candidates (sample)")
            got = None
            for d in docs:
                got = self._build_raw(d)
                if got:
                    break
            if got and got["text"] and len(got["text"]) > 80:
                logger.info(f"  Text OK ({len(got['text'])} chars) — "
                            f"{got.get('title')} [{got.get('date')}]")
            else:
                logger.error("  Text extraction failed or too short")
                return False
            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict) -> dict:
        title = raw.get("title") or raw["slug"]
        return {
            "_id": f"US/DC-TaxGuidance/{raw['slug']}",
            "_source": "US/DC-TaxGuidance",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "category": raw.get("category") or "Tax Notice",
            "issuer": "District of Columbia Office of Tax and Revenue",
            "title": title[:300],
            "text": raw["text"],
            "url": raw.get("node_url") or raw["pdf_url"],
            "pdf_url": raw["pdf_url"],
            "date": raw.get("date") or None,
            "jurisdiction": "US-DC",
        }

    def _iter_raw(self, sample: bool = False) -> Generator[dict, None, None]:
        emitted = 0
        for doc in self.discover_documents(sample=sample):
            raw = self._build_raw(doc)
            if raw:
                yield raw
                emitted += 1
                if sample and emitted >= 12:
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

    parser = argparse.ArgumentParser(description="US/DC-TaxGuidance bootstrap")
    parser.add_argument(
        "command",
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = DCTaxGuidanceScraper()

    if args.command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    stats = scraper.bootstrap(sample_mode=args.sample, sample_size=12)
    logger.info(f"Bootstrap complete: {json.dumps(stats, default=str)}")


if __name__ == "__main__":
    main()
