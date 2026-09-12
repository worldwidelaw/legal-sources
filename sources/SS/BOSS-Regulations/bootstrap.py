#!/usr/bin/env python3
"""
SS/BOSS-Regulations — Bank of South Sudan Regulations & Circulars

Fetches regulatory documents from boss.gov.ss via the WordPress REST API.
The old static PDF paths (/reg/, /cir/) are broken (404), but the WordPress
media library has all documents accessible via /wp-json/wp/v2/media.

Strategy:
  1. Paginate through the WP REST API for all PDF media items
  2. Filter by title/filename for regulatory content (circulars, guidelines,
     directives, acts, policies, oversight frameworks)
  3. Deduplicate (keep latest version of repeated uploads)
  4. Download PDFs and extract text with pdfplumber (fallback PyMuPDF)

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py test-api            # Quick connectivity test
"""

import io
import re
import sys
import json
import logging
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SS.BOSS-Regulations")

BASE_URL = "https://boss.gov.ss"
SOURCE_ID = "SS/BOSS-Regulations"
WP_MEDIA_API = f"{BASE_URL}/wp-json/wp/v2/media"

# Patterns that indicate regulatory/legal content
REGULATORY_PATTERNS = re.compile(
    r"(circular|regulation|guideline|directive|act[\s_-]|law[\s_-]|"
    r"policy[\s_-]|licensing|oversight|framework|compliance|amendment|"
    r"banking.?act|money.?laundering|aml|kyc|dormant|reserves|"
    r"liquidation|credit.?report|governance|prudential|supervision|"
    r"monetary.?and.?banking.?polic|requirements.?for.?opening|"
    r"fit.?and.?proper|exchange.?rate.?reg|electronic.?money|"
    r"foreign.?exchange.?business|prompt.?corrective|"
    r"letters?.?of.?guarantee|offshore.?banking|hoarding|"
    r"cash.?movement|interbank|insurance.?compan|"
    r"minimum.?reserve|solvabilit|"
    r"functions.?of.?the.?bank|risk.?based.?supervision|"
    r"approved.?monetary|banknotes.?circular|"
    r"term.?deposit.?facility|financing.?agreement|"
    r"communiqu|financial.?sector|exchange.?rate|"
    r"banking.?and.?finance.?institute|statistical.?bulletin)",
    re.IGNORECASE,
)

# Patterns for content to EXCLUDE (non-regulatory operational docs)
EXCLUDE_PATTERNS = re.compile(
    r"(fx.?auction|tdf.?pa|tdf.?statistical|statistical.?summary|"
    r"press.?release|expression.?of.?interest|"
    r"3sf.?project|3sf.?corrected|3sf.?communication|3sf.?progress|"
    r"milestone|public.?announcement|salary.?tariff|"
    r"risk.?management.?department|document_\d{6})",
    re.IGNORECASE,
)


def _extract_text_pdfplumber(pdf_bytes: bytes) -> str:
    import pdfplumber
    parts = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                parts.append(t)
            try:
                page.flush_cache(); page.get_textmap.cache_clear()
            except Exception:
                pass
    return "\n\n".join(parts)


def _extract_text_pymupdf(pdf_bytes: bytes) -> str:
    import fitz
    parts = []
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    for page in doc:
        t = page.get_text()
        if t:
            parts.append(t)
    doc.close()
    return "\n\n".join(parts)


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    text = ""
    try:
        text = _extract_text_pdfplumber(pdf_bytes)
    except Exception as e:
        logger.debug(f"pdfplumber failed: {e}")
    if not text.strip():
        try:
            text = _extract_text_pymupdf(pdf_bytes)
        except Exception as e:
            logger.warning(f"Both PDF extractors failed: {e}")
    return text.strip()


def _clean_title(raw_title: str) -> str:
    """Clean WP HTML entities and trailing version numbers from title."""
    title = raw_title.replace("&#8211;", "—").replace("&#8212;", "—")
    title = re.sub(r"_\d{4}$", "", title)  # Remove trailing _0001 etc.
    title = re.sub(r"-\d+$", "", title)    # Remove trailing -1, -2 etc.
    title = title.replace("_", " ").strip()
    return title


def _make_id(pdf_url: str) -> str:
    return hashlib.sha256(pdf_url.encode()).hexdigest()[:16]


def _is_regulatory(title: str, url: str) -> bool:
    """Check if a PDF is regulatory content (not operational/admin)."""
    combined = f"{title} {url}"
    if EXCLUDE_PATTERNS.search(combined):
        return False
    if REGULATORY_PATTERNS.search(combined):
        return True
    return False


class BOSSRegulationsScraper(BaseScraper):
    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            max_retries=3,
            timeout=60,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/json",
            },
        )

    def test_api(self) -> bool:
        try:
            resp = self.http.get(WP_MEDIA_API, params={"per_page": 1})
            if resp.status_code == 200:
                total = resp.headers.get("X-WP-Total", "0")
                logger.info(f"API test passed — {total} total media items")
                return True
            logger.error(f"API test failed — status {resp.status_code}")
            return False
        except Exception as e:
            logger.error(f"API test failed: {e}")
            return False

    def _collect_regulatory_pdfs(self) -> list[dict]:
        """Paginate through WP media API and collect regulatory PDFs."""
        all_docs = []
        page = 1
        total_pages = 1

        while page <= total_pages:
            logger.info(f"Fetching WP media API page {page}/{total_pages}")
            try:
                resp = self.http.get(
                    WP_MEDIA_API,
                    params={
                        "per_page": 100,
                        "media_type": "application",
                        "page": page,
                    },
                    timeout=30,
                )
                if resp.status_code != 200:
                    logger.warning(f"API page {page} returned {resp.status_code}")
                    break
                if page == 1:
                    total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
                    total = resp.headers.get("X-WP-Total", "?")
                    logger.info(f"Total media items: {total}, pages: {total_pages}")
            except Exception as e:
                logger.warning(f"Failed to fetch API page {page}: {e}")
                break

            items = resp.json()
            for item in items:
                if item.get("mime_type") != "application/pdf":
                    continue
                title = item.get("title", {}).get("rendered", "")
                source_url = item.get("source_url", "")
                if not source_url:
                    continue
                if not _is_regulatory(title, source_url):
                    continue
                all_docs.append({
                    "pdf_url": source_url,
                    "title": _clean_title(title),
                    "date": item.get("date", "")[:10] or None,
                    "wp_id": item.get("id"),
                })

            page += 1
            time.sleep(1)

        # Deduplicate: keep the latest version of each title
        seen_titles = {}
        for doc in all_docs:
            key = re.sub(r"[\s\-_]+", " ", doc["title"].lower()).strip()
            if key not in seen_titles:
                seen_titles[key] = doc
            else:
                # Keep the one with the later date
                existing_date = seen_titles[key].get("date") or ""
                new_date = doc.get("date") or ""
                if new_date > existing_date:
                    seen_titles[key] = doc

        deduped = list(seen_titles.values())
        logger.info(f"Found {len(all_docs)} regulatory PDFs, {len(deduped)} after dedup")
        return deduped

    def _download_and_extract(self, pdf_url: str) -> Optional[str]:
        try:
            resp = self.http.get(
                pdf_url,
                timeout=90,
                headers={
                    "Accept": "application/pdf,*/*",
                    "Referer": "https://boss.gov.ss/",
                },
            )
            resp.raise_for_status()
            if len(resp.content) < 500:
                logger.warning(f"PDF too small ({len(resp.content)} bytes): {pdf_url}")
                return None
            text = extract_text_from_pdf(resp.content)
            if not text or len(text) < 50:
                logger.warning(f"Insufficient text from {pdf_url} ({len(resp.content)} bytes)")
                return None
            return text
        except Exception as e:
            logger.warning(f"Failed to download/extract {pdf_url}: {e}")
            return None

    def _classify_type(self, title: str, url: str) -> str:
        """Classify as legislation or doctrine."""
        combined = f"{title} {url}".lower()
        if re.search(r"\bact\b|amendment", combined):
            return "legislation"
        return "doctrine"

    def normalize(self, raw: dict) -> dict:
        return {
            "_id": _make_id(raw["pdf_url"]),
            "_source": SOURCE_ID,
            "_type": self._classify_type(raw["title"], raw["pdf_url"]),
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["title"],
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw["pdf_url"],
            "language": "en",
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        pdf_docs = self._collect_regulatory_pdfs()
        for i, doc in enumerate(pdf_docs):
            logger.info(f"[{i+1}/{len(pdf_docs)}] Downloading: {doc['title'][:80]}")
            text = self._download_and_extract(doc["pdf_url"])
            if not text:
                continue
            doc["text"] = text
            yield self.normalize(doc)
            time.sleep(1)

    def fetch_updates(self, since: str = None) -> Generator[dict, None, None]:
        yield from self.fetch_all()


def main():
    import argparse
    parser = argparse.ArgumentParser(description="SS/BOSS-Regulations bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test-api"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    scraper = BOSSRegulationsScraper()

    if args.command == "test-api":
        sys.exit(0 if scraper.test_api() else 1)

    if args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        limit = 15 if args.sample else None

        for record in scraper.fetch_all():
            count += 1
            text_len = len(record.get("text", ""))
            logger.info(
                f"  #{count} | {record['title'][:60]} | "
                f"text={text_len} chars | type={record['_type']}"
            )
            if args.sample or count <= 15:
                fname = f"{record['_id']}.json"
                with open(sample_dir / fname, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            if limit and count >= limit:
                logger.info(f"Sample limit reached ({limit})")
                break

        logger.info(f"Done. {count} records fetched.")
        print(json.dumps({"_source": SOURCE_ID, "records": count}))


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
