#!/usr/bin/env python3
"""
INTL/AquaticsIntegrityUnit -- Aquatics Integrity Unit (AQIU) Decisions

Fetches the published full-text disciplinary and integrity decisions of the
Aquatics Integrity Unit (AQIU), the independent body that, since 1 January 2023,
handles disciplinary, ethics, competition-manipulation, safe-sport and
anti-doping matters for World Aquatics (formerly FINA) across swimming, water
polo, diving, artistic swimming, open-water swimming and high diving.

Two complementary corpora, both openly published (no login, no WAF) and
reachable from any IP:

  1. AQIU Adjudicatory Body reasoned decisions -- full multi-page reasoned
     rulings (findings on liability, the sanction, the period of ineligibility
     and full legal reasoning) published as PDFs on the AQIU "Suspended Persons"
     page (incl. legacy FINA Doping Panel decisions). Rich full text.

  2. AQIU published case outcomes -- the AQIU's official published account of
     each specific sanction (suspensions, bans for anti-doping rule violations,
     whereabouts failures, reprimands, match misconduct etc.), served as posts
     via the site's WordPress REST API with full rendered body text. Pure news
     items, anti-doping statistics roundups and administrative announcements are
     excluded so only decisions in named matters are kept.

NOTE: "World Aquatics" anti-doping rule violations are heard on the merits by
the CAS Anti-Doping Division; the AQIU publishes the resulting outcome here.

Strategy:
  - Reasoned-decision PDFs are discovered by parsing the Suspended Persons page
    for .pdf hrefs, then downloaded and text-extracted.
  - Case outcomes are pulled from /wp-json/wp/v2/posts (paginated), filtered to
    decision/sanction items, and the rendered HTML body is cleaned to plain text.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py test               # Print discovered decision entries
"""

import sys
import json
import logging
import re
import time
import html as htmllib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin

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
logger = logging.getLogger("legal-data-hunter.INTL.AquaticsIntegrityUnit")

BASE_URL = "https://aquaticsintegrity.com"
SUSPENDED_URL = "https://aquaticsintegrity.com/suspended-persons/"
WP_POSTS_URL = "https://aquaticsintegrity.com/wp-json/wp/v2/posts"
MAX_PDF_BYTES = 50 * 1024 * 1024

# A post is kept only if it announces a decision/sanction in a named matter.
DECISION_RE = re.compile(
    r"suspen|ban(?:ned|s)?\b|ineligib|reprimand|sanction|disqualif|"
    r"whereabouts|anti-?doping rule violation|provisional|misconduct|"
    r"period of ineligibility|accepts? a |handed|adjudicatory|"
    r"competition manipulation|violation of",
    re.IGNORECASE,
)
# Administrative / statistical / general-news items that are NOT case decisions.
NON_DECISION_RE = re.compile(
    r"\bstatistics\b|testing figures|pro bono|vacanc|appoint|webinar|"
    r"establishes|launches|annual report|education|e-learning|newsletter|"
    r"call for|consultation|terms of reference|integrity code\b|"
    r"strategic plan|governance takes|workshop|engages with|"
    r"delivers|engagement|partners\b|symposium|conference|seminar",
    re.IGNORECASE,
)


class AquaticsIntegrityUnitScraper(BaseScraper):
    """Scraper for Aquatics Integrity Unit (AQIU) full-text decisions."""

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 "
                          "Safari/537.36 LegalDataHunter/1.0",
            "Accept": "text/html,application/xhtml+xml,application/json,application/pdf",
            "Accept-Language": "en",
        })

    def _slugify(self, text: str) -> str:
        return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")[:90]

    def _clean_html(self, raw_html: str) -> str:
        soup = BeautifulSoup(raw_html, "html.parser")
        for tag in soup(["script", "style"]):
            tag.decompose()
        text = soup.get_text("\n", strip=True)
        text = htmllib.unescape(text)
        text = re.sub(r" ", " ", text)
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    # ── Corpus 1: reasoned-decision PDFs ────────────────────────────────

    def _collect_pdf_entries(self) -> list[dict]:
        """Parse the Suspended Persons page into reasoned-decision PDF entries."""
        try:
            time.sleep(1.0)
            resp = self.session.get(SUSPENDED_URL, timeout=40)
            resp.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch suspended-persons page: {e}")
            return []

        soup = BeautifulSoup(resp.content, "html.parser")
        entries, seen = [], set()
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            if ".pdf" not in href.lower():
                continue
            pdf_url = urljoin(BASE_URL, href).replace("http://", "https://")
            if pdf_url in seen:
                continue
            seen.add(pdf_url)
            # Title from the file name (anchor text is often blank "Decision").
            fname = pdf_url.rsplit("/", 1)[-1].rsplit(".", 1)[0]
            title = re.sub(r"[-_]+", " ", fname).strip()
            title = re.sub(r"\s+", " ", title)
            entries.append({
                "kind": "pdf",
                "pdf_url": pdf_url,
                "title": title,
                "date": self._date_from_pdf_name(pdf_url),
            })
        logger.info(f"Discovered {len(entries)} AQIU reasoned-decision PDFs")
        return entries

    def _date_from_pdf_name(self, url: str) -> Optional[str]:
        fname = url.rsplit("/", 1)[-1]
        # Leading YYYY.MM.DD in the file name.
        m = re.match(r"(\d{4})\.(\d{2})\.(\d{2})", fname)
        if m:
            try:
                return datetime(int(m.group(1)), int(m.group(2)),
                                int(m.group(3))).date().isoformat()
            except ValueError:
                pass
        # Fall back to the /uploads/YYYY/MM/ path.
        m = re.search(r"/uploads/(\d{4})/(\d{2})/", url)
        if m:
            try:
                return datetime(int(m.group(1)), int(m.group(2)), 1).date().isoformat()
            except ValueError:
                pass
        return None

    def _download_pdf(self, url: str) -> Optional[bytes]:
        try:
            time.sleep(1.0)
            resp = self.session.get(url, timeout=90)
            resp.raise_for_status()
            if len(resp.content) > MAX_PDF_BYTES or len(resp.content) < 500:
                return None
            return resp.content
        except Exception as e:
            logger.warning(f"  PDF download failed: {e}")
            return None

    def _extract_pdf_text(self, pdf_bytes: bytes, source_id: str) -> Optional[str]:
        text = extract_pdf_markdown(
            source="INTL/AquaticsIntegrityUnit",
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

    # ── Corpus 2: WordPress case-outcome posts ──────────────────────────

    def _collect_post_entries(self) -> list[dict]:
        """Pull WP REST posts and keep only decision/sanction items."""
        entries = []
        page = 1
        while page <= 30:
            try:
                time.sleep(1.0)
                resp = self.session.get(
                    WP_POSTS_URL,
                    params={"per_page": 100, "page": page, "_fields":
                            "id,date,link,title,content,slug,modified"},
                    timeout=40,
                )
                if resp.status_code == 400:
                    break  # past the last page
                resp.raise_for_status()
            except Exception as e:
                logger.warning(f"WP posts page {page} failed: {e}")
                break
            batch = resp.json()
            if not isinstance(batch, list) or not batch:
                break
            for p in batch:
                title = htmllib.unescape(
                    re.sub(r"<[^>]+>", "", p.get("title", {}).get("rendered", "")).strip())
                content_html = p.get("content", {}).get("rendered", "")
                text = self._clean_html(content_html)
                blob = f"{title}\n{text}"
                if NON_DECISION_RE.search(title):
                    continue
                if not DECISION_RE.search(blob):
                    continue
                if len(text) < 250:
                    continue
                entries.append({
                    "kind": "post",
                    "post_id": p.get("id"),
                    "slug": p.get("slug") or "",
                    "title": title,
                    "text": text,
                    "url": p.get("link", ""),
                    "date": (p.get("date") or "")[:10] or None,
                })
            total_pages = resp.headers.get("X-WP-TotalPages")
            if total_pages and page >= int(total_pages):
                break
            page += 1
        logger.info(f"Discovered {len(entries)} AQIU decision posts")
        return entries

    # ── Driver ──────────────────────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        # Reasoned PDFs first (richest text).
        pdf_entries = self._collect_pdf_entries()
        for i, entry in enumerate(pdf_entries):
            try:
                logger.info(f"[pdf {i+1}/{len(pdf_entries)}] {entry['title'][:70]}")
                pdf_bytes = self._download_pdf(entry["pdf_url"])
                if not pdf_bytes:
                    continue
                sid = self._slugify(entry["title"]) or "decision"
                text = self._extract_pdf_text(pdf_bytes, sid)
                if not text or len(text.strip()) < 100:
                    logger.warning(f"  Insufficient text, skipping {entry['title'][:50]}")
                    continue
                entry["_extracted_text"] = text
                yield entry
            except Exception as e:
                logger.error(f"  Error on PDF {entry['title'][:50]}: {e}")
                continue

        # Then the published case-outcome posts.
        post_entries = self._collect_post_entries()
        for entry in post_entries:
            yield entry

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        since_iso = since.date().isoformat()
        for rec in self.fetch_all():
            if not rec.get("date") or rec["date"] >= since_iso:
                yield rec

    def normalize(self, raw: dict) -> dict:
        if raw.get("kind") == "pdf":
            slug = self._slugify(raw.get("title", "")) or "decision"
            return {
                "_id": f"aqiu-decision-{slug}",
                "_source": "INTL/AquaticsIntegrityUnit",
                "_type": "case_law",
                "_fetched_at": datetime.now(timezone.utc).isoformat(),
                "title": raw.get("title", ""),
                "text": raw.get("_extracted_text", ""),
                "date": raw.get("date"),
                "url": raw.get("pdf_url", ""),
                "pdf_url": raw.get("pdf_url", ""),
                "document_type": "reasoned_decision",
            }
        slug = raw.get("slug") or self._slugify(raw.get("title", "")) or "decision"
        return {
            "_id": f"aqiu-outcome-{slug}",
            "_source": "INTL/AquaticsIntegrityUnit",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "document_type": "published_outcome",
        }


# ── CLI entry point ──────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = AquaticsIntegrityUnitScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        pdfs = scraper._collect_pdf_entries()
        posts = scraper._collect_post_entries()
        for e in pdfs:
            print(f"  PDF  {e['date'] or '----------'}  {e['title'][:70]}")
        for e in posts[:40]:
            print(f"  POST {e['date'] or '----------'}  {e['title'][:70]}")
        print(f"\nTotal: {len(pdfs)} PDFs + {len(posts)} posts = {len(pdfs)+len(posts)}")
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
