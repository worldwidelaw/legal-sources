#!/usr/bin/env python3
"""
AZ/CBA-Regulations — Central Bank of Azerbaijan — Regulations & Decisions

Fetches the English-language legal framework of the Central Bank of the
Republic of Azerbaijan (CBAR) from cbar.az.

Strategy (no public API; open HTML site):
  1. Walk the legal-framework listing pages (/page-{id}/x?language=en) for
     each section (Credit Institutions, Payments, Capital Market, Currency,
     Other) and document kind (regulations, presidential/cabinet decrees,
     codes, methodological documents).
  2. Collect /law-{id}/{slug} detail-page links — these are full-text HTML
     acts. (Pages whose items are external/PDF "Laws" yield no /law- links and
     are skipped; the actual binding regulations/decisions live on /law- pages.)
  3. Fetch each detail page; the act body is in <div class="type_text ...">,
     the clean title in <meta property="og:title">, and state-registration
     metadata (Ministry of Justice registration #, approval / registration
     dates) in the header text / <meta property="og:description">.

All records carry full text (HTML-extracted, tags stripped, entities decoded).
Pages yielding < 300 chars of extractable text are skipped.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records for validation
  python bootstrap.py update             # Incremental (recent acts)
"""

import re
import sys
import html as htmlmod
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any
from html.parser import HTMLParser

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.AZ.cba-regulations")

BASE = "https://www.cbar.az"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,*/*",
    "Accept-Language": "en,az;q=0.9",
    "Referer": "https://www.cbar.az/page-214/laws?language=en",
}

# Legal-framework listing pages → (section label, document kind).
# Pages whose entries are external statutes/PDFs (the "Laws" pages) carry no
# /law- detail links and simply contribute nothing.
LISTING_PAGES = [
    (214, "Credit Institutions", "law"),
    (215, "Credit Institutions", "presidential_decree"),
    (216, "Credit Institutions", "cabinet_decree"),
    (217, "Credit Institutions", "regulation"),
    (220, "Payments", "law"),
    (221, "Payments", "presidential_decree"),
    (222, "Payments", "cabinet_decree"),
    (223, "Payments", "regulation"),
    (225, "Capital Market", "law"),
    (226, "Capital Market", "presidential_decree"),
    (227, "Capital Market", "cabinet_decree"),
    (228, "Capital Market", "regulation"),
    (231, "Other", "code"),
    (232, "Other", "presidential_decree"),
    (233, "Other", "cabinet_decree"),
    (234, "Other", "regulation"),
    (460, "Currency regulation", "law"),
    (461, "Currency regulation", "regulation"),
    (848, "Methodological documents", "methodological"),
]

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}


class _BodyExtractor(HTMLParser):
    """Extract clean text from the first <div class*='type_text'> (balanced)."""

    def __init__(self):
        super().__init__()
        self.capturing = False
        self.depth = 0
        self.skip = 0
        self.parts = []

    def handle_starttag(self, tag, attrs):
        d = dict(attrs)
        cls = d.get("class", "") or ""
        if not self.capturing and tag == "div" and "type_text" in cls:
            self.capturing = True
            self.depth = 1
            return
        if self.capturing:
            if tag == "div":
                self.depth += 1
            if tag in ("script", "style"):
                self.skip += 1
            if tag in ("p", "div", "br", "tr", "li", "h1", "h2", "h3", "h4", "h5", "h6"):
                if self.parts and not self.parts[-1].endswith("\n"):
                    self.parts.append("\n")

    def handle_endtag(self, tag):
        if self.capturing:
            if tag in ("script", "style") and self.skip > 0:
                self.skip -= 1
            if tag == "div":
                self.depth -= 1
                if self.depth == 0:
                    self.capturing = False

    def handle_data(self, data):
        if self.capturing and self.skip == 0:
            t = data.strip()
            if t:
                self.parts.append(t)

    def get_text(self) -> str:
        return " ".join(self.parts)


class CBARScraper(BaseScraper):
    """
    Scraper for AZ/CBA-Regulations — Central Bank of Azerbaijan.
    Country: AZ
    Auth: none (public government data)
    """

    def __init__(self):
        super().__init__(Path(__file__).parent)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    # ── helpers ──────────────────────────────────────────────────

    def _get(self, url: str) -> Optional[str]:
        try:
            self.rate_limiter.wait()
            resp = self.session.get(url, timeout=45)
            if resp.status_code != 200:
                logger.debug(f"HTTP {resp.status_code} for {url}")
                return None
            return resp.content.decode("utf-8", errors="ignore")
        except Exception as e:
            logger.debug(f"Fetch failed for {url}: {e}")
            return None

    @staticmethod
    def _clean(text: str) -> str:
        if not text:
            return ""
        text = htmlmod.unescape(text)
        text = text.replace("\xa0", " ").replace("​", "")
        text = re.sub(r"\r\n", "\n", text)
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r" *\n *", "\n", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    @staticmethod
    def _meta(html: str, prop: str) -> Optional[str]:
        m = re.search(
            rf'<meta property="{re.escape(prop)}" content="(.*?)">', html, re.S
        )
        return htmlmod.unescape(m.group(1)).strip() if m else None

    @staticmethod
    def _find_date(blob: str) -> Optional[str]:
        """First date in `blob`, accepting 'Month DD, YYYY' or 'DD Month YYYY'."""
        if not blob:
            return None
        # 'November 20, 2009'
        m = re.search(r"([A-Za-z]+)\s+(\d{1,2}),\s+(\d{4})", blob)
        if m and m.group(1).lower() in MONTHS:
            return f"{int(m.group(3)):04d}-{MONTHS[m.group(1).lower()]:02d}-{int(m.group(2)):02d}"
        # '14 July 2021'
        m = re.search(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", blob)
        if m and m.group(2).lower() in MONTHS:
            return f"{int(m.group(3)):04d}-{MONTHS[m.group(2).lower()]:02d}-{int(m.group(1)):02d}"
        return None

    @staticmethod
    def _find_doc_number(blob: str) -> Optional[str]:
        """Registration / Decision / Decree number, several header formats."""
        if not blob:
            return None
        for pat in (
            r"Registration\s*[#№]\s*([0-9A-Za-z][0-9A-Za-z\-/]*)",
            r"Decision\s*[#№]\s*([0-9A-Za-z][0-9A-Za-z\-/]*)",
            r"Decree\s*[#№]\s*([0-9A-Za-z][0-9A-Za-z\-/]*)",
            r"Protocol\s*[#№]\s*([0-9A-Za-z][0-9A-Za-z\-/]*)",
        ):
            m = re.search(pat, blob)
            if m:
                return m.group(1)
        return None

    # ── discovery ────────────────────────────────────────────────

    def _discover(self) -> Dict[int, Dict[str, str]]:
        """Return {law_id: {url, slug, category, doc_kind}} across all sections."""
        found: Dict[int, Dict[str, str]] = {}
        for pid, section, kind in LISTING_PAGES:
            html = self._get(f"{BASE}/page-{pid}/x?language=en")
            if not html:
                continue
            links = re.findall(r'href="(/law-(\d+)/([^"#?]*))"', html)
            n = 0
            for path, lid, slug in links:
                lid = int(lid)
                if lid in found:
                    continue
                found[lid] = {
                    "url": f"{BASE}{path}?language=en",
                    "slug": slug,
                    "category": f"{section} / {kind.replace('_', ' ').title()}",
                    "doc_kind": kind,
                }
                n += 1
            logger.info(f"page-{pid} ({section}/{kind}): +{n} new acts")
        logger.info(f"Discovered {len(found)} unique CBAR acts")
        return found

    # ── BaseScraper API ──────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        catalog = self._discover()
        for lid in sorted(catalog.keys(), reverse=True):
            meta = catalog[lid]
            html = self._get(meta["url"])
            if not html:
                continue
            yield {"law_id": lid, "html": html, **meta}

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        since_s = since.date().isoformat() if hasattr(since, "date") else str(since)
        for raw in self.fetch_all():
            rec = self.normalize(raw)
            if rec and (rec.get("date") or "") >= since_s:
                yield raw

    def normalize(self, raw: dict) -> Optional[dict]:
        html = raw.get("html", "")
        if not html:
            return None

        title = self._meta(html, "og:title")
        desc = self._meta(html, "og:description") or ""

        parser = _BodyExtractor()
        parser.feed(html)
        text = self._clean(parser.get_text())

        # Fall back to og:description if the body div was empty/short.
        if len(text) < 300 and desc:
            text = self._clean(desc)
        if len(text) < 300:
            return None  # not enough full text — skip

        if not title:
            title = (text.split("\n", 1)[0])[:200]

        # Metadata lives at the very top of the act (og:description / body head).
        header = (desc + " " + text[:600]) if desc else text[:600]
        reg_no = self._find_doc_number(header)
        date = self._find_date(header)

        return {
            "_id": f"AZ-CBAR-law-{raw['law_id']}",
            "_source": "AZ/CBA-Regulations",
            "_type": "doctrine" if raw.get("doc_kind") == "methodological" else "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title.strip(),
            "text": text,
            "date": date,
            "url": raw["url"],
            "document_number": reg_no,
            "category": raw.get("category"),
            "doc_kind": raw.get("doc_kind"),
            "language": "en",
            "jurisdiction": "AZ",
        }


def main():
    scraper = CBARScraper()
    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 10
    if "--sample-size" in sys.argv:
        sample_size = int(sys.argv[sys.argv.index("--sample-size") + 1])

    if command == "bootstrap":
        if sample_mode:
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
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
