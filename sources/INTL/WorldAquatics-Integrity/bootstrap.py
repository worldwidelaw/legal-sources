#!/usr/bin/env python3
"""
INTL/WorldAquatics-Integrity -- Aquatics Integrity Unit (AQIU) Decisions

Fetches the published disciplinary decisions of the Aquatics Integrity Unit
(AQIU), the independent integrity body of World Aquatics (formerly FINA) that
investigates and adjudicates anti-doping, integrity-code, safeguarding and
competition-manipulation matters across the aquatic disciplines (swimming,
water polo, diving, artistic swimming, open water, high diving).

Two complementary, openly published full-text corpora on aquaticsintegrity.com:

  1. Adjudicatory-Body / Doping-Panel decision PDFs linked from the
     "Suspended Persons" registry (/suspended-persons/). These are born-digital,
     multi-page reasoned decisions (facts, rules, reasoning, sanction) hosted
     openly under /wp-content/uploads/. Extracted via common/pdf_extract.

  2. Sanction-decision news articles (/news/). Each disciplinary outcome is
     published as an official AQIU notice carrying the respondent, the rule(s)
     violated, the sanction and the ineligibility period. The article body text
     is extracted from the server-rendered HTML.

Non-decision news items (statistics reports, workshops, conferences, strategic
plans, governance/appointment notices) are filtered out.

The site is a public WordPress install — no login, no WAF, reachable from any IP.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py test               # Print discovered entries
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

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
logger = logging.getLogger("legal-data-hunter.INTL.WorldAquatics-Integrity")

SOURCE_ID = "INTL/WorldAquatics-Integrity"
BASE = "https://aquaticsintegrity.com"
NEWS_URL = f"{BASE}/news/"
SUSPENDED_URL = f"{BASE}/suspended-persons/"
MAX_PDF_BYTES = 50 * 1024 * 1024

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}
# "6 March 2024" / "6 March , 2024"
DATE_TEXT_RE = re.compile(
    r"\b(\d{1,2})\s+(January|February|March|April|May|June|July|August|"
    r"September|October|November|December)\s*,?\s+(\d{4})\b",
    re.IGNORECASE,
)
# leading YYYY.MM.DD in a PDF filename
DATE_FNAME_RE = re.compile(r"(\d{4})[.\-_](\d{1,2})[.\-_](\d{1,2})")
# /uploads/YYYY/MM/
DATE_URL_RE = re.compile(r"/uploads/(\d{4})/(\d{1,2})/")

# News slugs that are NOT specific disciplinary decisions.
SKIP_SLUG_RE = re.compile(
    r"statistic|workshop|conference|hosts|delivers|strategic-plan|pro-bono|"
    r"establishes|training|anniversary|bird-bird|-event-|appoint|vacancy|"
    r"recruit|tender|welcomes-new|insightful|takes-(a|another)-step|governance|"
    r"programme|prohibited-list|pre-olympic|ensuring-fair-play|strengthening|"
    r"comprehensive-and-rigorous|engages-with",
    re.IGNORECASE,
)

# Fixed navigation / site-section pages (not news posts).
NAV_SLUGS = {
    "news", "the-unit", "about", "contact", "privacy-policy", "anti-doping",
    "safe-sport", "competition-manipulation", "suspended-persons", "report",
    "policies", "governance", "safeguarding", "education", "home",
    "cookie-policy", "who-we-are", "introduction", "disciplinary-violations",
    "ethical-violations", "rules-and-regulations", "make-a-report", "resources",
    "education-awareness", "legal", "doping-control", "results-management",
}


class WorldAquaticsIntegrityScraper(BaseScraper):
    """Scraper for Aquatics Integrity Unit (AQIU) decisions."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/worldwidelaw/legal-sources)",
            "Accept": "text/html,application/xhtml+xml,application/pdf",
            "Accept-Language": "en",
        })

    # ── helpers ──────────────────────────────────────────────────────

    def _slug(self, s: str) -> str:
        s = re.sub(r"\.pdf$", "", s.rsplit("/", 1)[-1], flags=re.IGNORECASE)
        s = re.sub(r"[^a-z0-9]+", "-", s.lower()).strip("-")
        return s or "decision"

    def _date_from_text(self, text: str) -> Optional[str]:
        m = DATE_TEXT_RE.search(text or "")
        if not m:
            return None
        day, mon, year = int(m.group(1)), MONTHS[m.group(2).lower()], int(m.group(3))
        try:
            return datetime(year, mon, day).date().isoformat()
        except ValueError:
            return None

    def _date_from_pdf_url(self, url: str) -> Optional[str]:
        fname = url.rsplit("/", 1)[-1]
        m = DATE_FNAME_RE.search(fname)
        if m:
            y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if 2000 <= y <= 2100 and 1 <= mo <= 12 and 1 <= d <= 31:
                try:
                    return datetime(y, mo, d).date().isoformat()
                except ValueError:
                    pass
        m = DATE_URL_RE.search(url)
        if m:
            y, mo = int(m.group(1)), int(m.group(2))
            if 1 <= mo <= 12:
                return f"{y:04d}-{mo:02d}-01"
        return None

    # ── discovery ────────────────────────────────────────────────────

    def _discover_pdfs(self) -> list[dict]:
        """Adjudicatory-Body / Doping-Panel decision PDFs from the registry."""
        resp = self.session.get(SUSPENDED_URL, timeout=30)
        resp.raise_for_status()
        urls = sorted(set(re.findall(
            r"https://aquaticsintegrity\.com/wp-content/uploads/[^\"'\s<>]+\.pdf",
            resp.text,
        )))
        entries = []
        for url in urls:
            fname = url.rsplit("/", 1)[-1]
            raw = re.sub(r"\.pdf$", "", fname, flags=re.IGNORECASE)
            raw = re.sub(r"^\d{4}[.\-_]\d{1,2}[.\-_]\d{1,2}[.\-_]*", "", raw)
            title = re.sub(r"[._\-]+", " ", raw).strip()
            entries.append({
                "kind": "pdf",
                "id_slug": self._slug(url),
                "title": f"AQIU Decision — {title}" if title else "AQIU Decision",
                "url": url,
                "pdf_url": url,
                "date": self._date_from_pdf_url(url),
            })
        logger.info(f"Discovered {len(entries)} decision PDFs from registry")
        return entries

    def _discover_news(self) -> list[dict]:
        """Sanction-decision news articles from /news/."""
        resp = self.session.get(NEWS_URL, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")
        seen = set()
        entries = []
        for a in soup.find_all("a", href=True):
            href = a["href"].strip().split("#")[0].split("?")[0]
            m = re.fullmatch(r"https://aquaticsintegrity\.com/([a-z0-9][a-z0-9-]+)/", href)
            if not m:
                continue
            slug = m.group(1)
            # drop site-section / nav pages and non-decision news
            if slug in NAV_SLUGS:
                continue
            if SKIP_SLUG_RE.search(slug):
                continue
            if href in seen:
                continue
            seen.add(href)
            entries.append({
                "kind": "news",
                "id_slug": slug,
                "url": href,
            })
        logger.info(f"Discovered {len(entries)} sanction-decision news articles")
        return entries

    # ── fetch ────────────────────────────────────────────────────────

    def _fetch_pdf_text(self, url: str) -> Optional[str]:
        try:
            time.sleep(1.5)
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            data = resp.content
            if len(data) > MAX_PDF_BYTES or len(data) < 500:
                logger.warning(f"  PDF size out of range ({len(data)} bytes), skipping")
                return None
        except Exception as e:
            logger.error(f"  PDF download failed: {e}")
            return None
        text = extract_pdf_markdown(
            source=SOURCE_ID, source_id=self._slug(url),
            pdf_bytes=data, table="case_law",
        )
        if text and len(text.strip()) >= 100:
            return text.strip()
        return None

    def _fetch_news(self, entry: dict) -> Optional[dict]:
        try:
            time.sleep(1.0)
            resp = self.session.get(entry["url"], timeout=30)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"  Article fetch failed: {e}")
            return None
        soup = BeautifulSoup(resp.content, "html.parser")
        title = (soup.title.get_text(strip=True) if soup.title else "")
        title = re.sub(r"\s*[–|-]\s*Aquatics Integrity Unit\s*$", "", title).strip()
        main = soup.find("main") or soup.find("article") or soup.body
        if main is None:
            return None
        for t in main(["script", "style", "nav", "header", "footer", "form"]):
            t.decompose()
        text = main.get_text(" ", strip=True)
        text = re.sub(r"\s+", " ", text)
        # strip leading "News <date>" breadcrumb chrome
        body = re.sub(
            r"^News\s+\d{1,2}\s+[A-Za-z]+\s*,?\s+\d{4}\s+", "", text
        ).strip()
        # strip a duplicated leading copy of the headline
        if title and body.startswith(title):
            body = body[len(title):].strip(" –-:")
        # drop trailing site boilerplate (share / related / footer menus)
        body = re.split(
            r"(?:Share this|Related (?:News|Posts)|Back to news|"
            r"Subscribe|All rights reserved|Privacy Policy)\b", body, maxsplit=1
        )[0].strip()
        if len(body) < 80:
            return None
        entry["title"] = title or entry["id_slug"].replace("-", " ").title()
        entry["text"] = body
        entry["date"] = self._date_from_text(body) or self._date_from_text(text)
        return entry

    # ── BaseScraper contract ─────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        pdfs = self._discover_pdfs()
        news = self._discover_news()
        total = len(pdfs) + len(news)
        logger.info(f"Total entries to process: {total}")

        n = 0
        for entry in pdfs:
            n += 1
            try:
                logger.info(f"[{n}/{total}] PDF {entry['id_slug'][:50]}")
                text = self._fetch_pdf_text(entry["pdf_url"])
                if not text:
                    logger.warning(f"  Insufficient text, skipping {entry['id_slug']}")
                    continue
                entry["text"] = text
                yield entry
            except Exception as e:
                logger.error(f"  Error on {entry['id_slug']}: {e}")
                continue

        for entry in news:
            n += 1
            try:
                logger.info(f"[{n}/{total}] NEWS {entry['id_slug'][:50]}")
                got = self._fetch_news(entry)
                if not got:
                    continue
                yield got
            except Exception as e:
                logger.error(f"  Error on {entry['id_slug']}: {e}")
                continue

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        since_iso = since.date().isoformat()
        for entry in self.fetch_all():
            if not entry.get("date") or entry["date"] >= since_iso:
                yield entry

    def normalize(self, raw: dict) -> dict:
        kind = raw.get("kind", "news")
        return {
            "_id": f"aqiu-{kind}-{raw.get('id_slug', 'decision')}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "pdf_url": raw.get("pdf_url", ""),
            "document_kind": "reasoned_decision" if kind == "pdf" else "sanction_notice",
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = WorldAquaticsIntegrityScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        pdfs = scraper._discover_pdfs()
        news = scraper._discover_news()
        for e in pdfs:
            print(f"  PDF  {e['date']}  {e['id_slug'][:55]}")
        for e in news:
            print(f"  NEWS         {e['id_slug'][:55]}")
        print(f"\nTotal: {len(pdfs)} PDFs + {len(news)} news = {len(pdfs)+len(news)}")
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
