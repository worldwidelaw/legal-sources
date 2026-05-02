#!/usr/bin/env python3
"""
KE/CBK-PrudentialGuidelines — Central Bank of Kenya Regulations

Fetches prudential guidelines, risk management guidelines, AML/CFT
guidance notes, banking regulations, and circulars from the CBK website.

Strategy:
  - Scrape the legislation-and-guidelines page for PDF links
  - Download each PDF
  - Extract full text via pdfminer

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records
"""

import sys
import json
import logging
import os
import re
import subprocess
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KE.CBK-PrudentialGuidelines")

BASE_URL = "https://www.centralbank.go.ke"
LISTING_URL = f"{BASE_URL}/policy-procedures/legislation-and-guidelines/"
UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
REQUEST_DELAY = 2  # seconds between requests


def curl_get(url: str, timeout: int = 60) -> str:
    """GET text via curl subprocess."""
    result = subprocess.run(
        [
            "curl", "-sL",
            "--connect-timeout", "15",
            "--max-time", str(timeout),
            "-H", f"User-Agent: {UA}",
            url,
        ],
        capture_output=True,
        text=True,
        timeout=timeout + 10,
    )
    if result.returncode != 0:
        raise RuntimeError(f"curl GET failed (exit {result.returncode}): {result.stderr.strip()}")
    return result.stdout


def curl_get_bytes(url: str, timeout: int = 120) -> bytes:
    """GET binary content via curl subprocess."""
    result = subprocess.run(
        [
            "curl", "-sL",
            "--connect-timeout", "15",
            "--max-time", str(timeout),
            "-H", f"User-Agent: {UA}",
            url,
        ],
        capture_output=True,
        timeout=timeout + 10,
    )
    if result.returncode != 0:
        raise RuntimeError(f"curl GET failed (exit {result.returncode}): {result.stderr.strip()}")
    return result.stdout


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using pdfminer."""
    import tempfile
    from pdfminer.high_level import extract_text as pdfminer_extract

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(pdf_bytes)
        tmp_path = tmp.name

    try:
        text = pdfminer_extract(tmp_path)
        return text.strip() if text else ""
    except Exception as e:
        logger.warning("pdfminer extraction failed: %s", e)
        return ""
    finally:
        os.unlink(tmp_path)


def slugify(text: str) -> str:
    """Create a URL-safe slug from text."""
    text = text.lower().strip()
    text = re.sub(r'[^\w\s-]', '', text)
    text = re.sub(r'[\s_]+', '-', text)
    text = re.sub(r'-+', '-', text)
    return text[:80].strip('-')


def extract_date_from_url(url: str) -> Optional[str]:
    """Try to extract a date from the URL path (e.g., /uploads/2022/03/...)."""
    m = re.search(r'/uploads/(\d{4})/(\d{2})/', url)
    if m:
        year, month = m.group(1), m.group(2)
        return f"{year}-{month}-01"
    return None


def extract_date_from_title(title: str) -> Optional[str]:
    """Try to extract a year from the title."""
    m = re.search(r'\b(19\d{2}|20\d{2})\b', title)
    if m:
        return f"{m.group(1)}-01-01"
    return None


def categorize_document(title: str, url: str) -> str:
    """Categorize the document based on title/URL keywords."""
    lower = (title + " " + url).lower()
    if any(w in lower for w in ['act', 'constitution']):
        return "legislation"
    if any(w in lower for w in ['regulation', 'rules']):
        return "regulation"
    if any(w in lower for w in ['guidance', 'guide', 'guideline']):
        return "guideline"
    if 'circular' in lower:
        return "circular"
    if any(w in lower for w in ['framework', 'charter', 'code']):
        return "framework"
    if 'schedule' in lower:
        return "schedule"
    if 'draft' in lower:
        return "draft"
    return "doctrine"


class CBKPrudentialGuidelinesScraper(BaseScraper):
    SOURCE_ID = "KE/CBK-PrudentialGuidelines"

    def _parse_pdf_links(self, html: str) -> list:
        """Parse HTML to extract (title, url) pairs for PDF documents."""
        pattern = r'<a[^>]*href=["\']([^"\']*\.pdf)["\'][^>]*>(.*?)</a>'
        matches = re.findall(pattern, html, re.IGNORECASE | re.DOTALL)

        results = []
        seen_urls = set()
        for url, raw_title in matches:
            title = unescape(re.sub(r'<[^>]+>', '', raw_title).strip())
            if not title:
                title = Path(url).stem.replace('-', ' ').replace('_', ' ').title()

            if url.startswith('/'):
                url = f"{BASE_URL}{url}"

            norm_url = url.lower()
            if norm_url in seen_urls:
                continue
            seen_urls.add(norm_url)

            results.append({"title": title, "url": url})

        return results

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        sample_mode = getattr(self, '_sample_mode', False)
        max_docs = 15 if sample_mode else 999

        logger.info("Fetching listing page: %s", LISTING_URL)
        html = curl_get(LISTING_URL)
        docs = self._parse_pdf_links(html)
        logger.info("Found %d PDF documents", len(docs))

        count = 0
        for doc in docs:
            if count >= max_docs:
                break

            title = doc["title"]
            url = doc["url"]
            doc_id = slugify(title) or hashlib.md5(url.encode()).hexdigest()[:16]

            logger.info("[%d/%d] Downloading: %s", count + 1, min(len(docs), max_docs), title)
            time.sleep(REQUEST_DELAY)

            try:
                pdf_bytes = curl_get_bytes(url, timeout=120)
            except Exception as e:
                logger.warning("Failed to download %s: %s", url, e)
                continue

            if len(pdf_bytes) < 100:
                logger.warning("PDF too small (%d bytes), skipping: %s", len(pdf_bytes), url)
                continue

            text = extract_pdf_text(pdf_bytes)
            if len(text) < 50:
                logger.warning("Insufficient text (%d chars) from %s — likely scanned PDF", len(text), title)
                if sample_mode:
                    continue
                # In full mode, still yield with empty text to record metadata

            date = extract_date_from_title(title) or extract_date_from_url(url)
            category = categorize_document(title, url)

            record = {
                "doc_id": doc_id,
                "title": title,
                "text": text,
                "url": url,
                "date": date,
                "category": category,
                "pdf_size_bytes": len(pdf_bytes),
            }

            count += 1
            yield record

        logger.info("Fetched %d documents total", count)

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """CBK has no update API; re-run fetch_all."""
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": f"KE-CBK-{raw['doc_id']}",
            "_source": self.SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw["url"],
            "category": raw.get("category", "doctrine"),
            "language": "en",
        }


# ── CLI ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    SOURCE_DIR = Path(__file__).parent
    scraper = CBKPrudentialGuidelinesScraper(str(SOURCE_DIR))

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap --sample|test]")
        sys.exit(1)

    cmd = sys.argv[1]

    if cmd == "test":
        logger.info("Testing connectivity...")
        html = curl_get(LISTING_URL)
        docs = scraper._parse_pdf_links(html)
        logger.info("OK — Found %d PDF documents on the listing page", len(docs))
        for d in docs[:5]:
            print(f"  - {d['title']}: {d['url']}")

    elif cmd == "bootstrap":
        sample = "--sample" in sys.argv
        if sample:
            scraper._sample_mode = True

        out_dir = SOURCE_DIR / ("sample" if sample else "data")
        out_dir.mkdir(parents=True, exist_ok=True)

        count = 0
        for raw in scraper.fetch_all():
            rec = scraper.normalize(raw)

            if sample and len(rec.get("text", "")) < 50:
                logger.warning("Skipping %s — insufficient text for sample", rec["title"])
                continue

            fname = re.sub(r'[^\w\-]', '_', rec["_id"])[:100] + ".json"
            fpath = out_dir / fname
            with open(fpath, "w", encoding="utf-8") as f:
                json.dump(rec, f, ensure_ascii=False, indent=2)

            text_len = len(rec.get("text", ""))
            logger.info("Saved: %s (%d chars text)", fpath.name, text_len)
            count += 1

        logger.info("Done. Saved %d records to %s/", count, out_dir)

    elif cmd == "update":
        logger.info("Update mode — re-fetching all documents")
        scraper._sample_mode = False
        out_dir = SOURCE_DIR / "data"
        out_dir.mkdir(parents=True, exist_ok=True)

        count = 0
        for raw in scraper.fetch_all():
            rec = scraper.normalize(raw)
            fname = re.sub(r'[^\w\-]', '_', rec["_id"])[:100] + ".json"
            fpath = out_dir / fname
            with open(fpath, "w", encoding="utf-8") as f:
                json.dump(rec, f, ensure_ascii=False, indent=2)
            count += 1

        logger.info("Done. Saved %d records to %s/", count, out_dir)

    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
