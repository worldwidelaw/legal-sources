#!/usr/bin/env python3
"""
Czech Energy Regulatory Office (ERÚ) — Binding Decisions

Scrapes eru.gov.cz/pravomocna-rozhodnuti paginated list.
Each decision page has metadata + PDF attachments with full text.
~5,700+ decisions covering energy regulation, license violations, etc.
"""

import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

import requests

try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

try:
    import PyPDF2
    HAS_PYPDF2 = True
except ImportError:
    HAS_PYPDF2 = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://eru.gov.cz"
LIST_URL = f"{BASE_URL}/pravomocna-rozhodnuti"
SOURCE_ID = "CZ/ERU"
RATE_LIMIT = 1.5


def extract_pdf_text(pdf_bytes: bytes) -> str:
    """Extract text from PDF bytes using available library."""
    if HAS_PDFPLUMBER:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages.append(text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
            return "\n\n".join(pages)
    elif HAS_PYPDF2:
        reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
        pages = []
        for page in reader.pages:
            text = page.extract_text()
            if text:
                pages.append(text)
            try:
                page.flush_cache(); page.get_textmap.cache_clear()
            except Exception:
                pass
        return "\n\n".join(pages)
    else:
        raise RuntimeError("No PDF library available (install pdfplumber or PyPDF2)")


class ERUFetcher:
    """Fetcher for Czech Energy Regulatory Office decisions."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (open-data-research)",
            "Accept": "text/html,application/xhtml+xml",
        })
        self._last_request = 0.0

    def _rate_limit(self):
        elapsed = time.time() - self._last_request
        if elapsed < RATE_LIMIT:
            time.sleep(RATE_LIMIT - elapsed)
        self._last_request = time.time()

    def _get(self, url: str, timeout: int = 30) -> requests.Response:
        self._rate_limit()
        resp = self.session.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp

    def _parse_list_page(self, html: str) -> List[Tuple[str, str]]:
        """Extract decision (url, title) pairs from a list page."""
        matches = re.findall(
            r'<h3[^>]*class="[^"]*summary-title[^"]*"[^>]*>'
            r'\s*<a\s+href="(/[^"]+)"\s+hreflang="cs">([^<]+)</a>',
            html,
        )
        return [(url, unescape(title.strip())) for url, title in matches]

    def _parse_detail_page(self, html: str) -> Dict[str, Any]:
        """Extract metadata and PDF links from a decision detail page."""
        meta: Dict[str, Any] = {}

        article_match = re.search(r"<article[^>]*>(.*?)</article>", html, re.DOTALL)
        if not article_match:
            return meta

        article = article_match.group(1)

        # Party name
        party_match = re.search(
            r"Účastník řízení\s*</div>.*?<div[^>]*>([^<]+)</div>",
            article, re.DOTALL,
        )
        if not party_match:
            # Fallback: simpler pattern
            party_match = re.search(
                r"Účastník řízení\s*\n\s*(.+)",
                re.sub(r"<[^>]+>", "\n", article),
            )
        if party_match:
            meta["party_name"] = party_match.group(1).strip()

        # IČO
        ico_match = re.search(r"IČO[^0-9]*(\d{5,8})", article)
        if ico_match:
            meta["ico"] = ico_match.group(1)

        # File reference (spisová značka)
        ref_match = re.search(r"Spisová značka\s*</div>.*?<div[^>]*>([^<]+)</div>", article, re.DOTALL)
        if not ref_match:
            ref_match = re.search(r"(\d{4,6}/\d{4}-ERU)", article)
        if ref_match:
            meta["file_reference"] = ref_match.group(1).strip()

        # Case reference (číslo jednací)
        case_match = re.search(r"Číslo jednací\s*</div>.*?<div[^>]*>([^<]+)</div>", article, re.DOTALL)
        if not case_match:
            case_match = re.search(r"(\d{4,6}-\d+/\d{4}-ERU)", article)
        if case_match:
            meta["case_reference"] = case_match.group(1).strip()

        # Legal effect date — stored in <time datetime="..."> tag
        date_match = re.search(
            r'Nabytí právní moci.*?<time\s+datetime="([^"]+)"',
            article, re.DOTALL,
        )
        if date_match:
            # datetime attr is ISO: 2026-04-09T12:00:00Z → take date part
            dt_str = date_match.group(1).strip()
            meta["legal_effect_date"] = dt_str[:10] if "T" in dt_str else dt_str
        else:
            # Fallback: DD.MM.YYYY in text
            date_match = re.search(
                r"Nabytí právní moci.*?(\d{2}\.\d{2}\.\d{4})",
                re.sub(r"<[^>]+>", " ", article),
            )
            if date_match:
                meta["legal_effect_date"] = date_match.group(1).strip()

        # Summary text from the article (between metadata and downloads)
        clean_text = re.sub(r"<[^>]+>", "\n", article)
        clean_text = unescape(clean_text)
        lines = [l.strip() for l in clean_text.split("\n") if l.strip()]
        # Find summary: lines after "Nabytí právní moci" date and before "Ke stažení"
        summary_lines = []
        capture = False
        for line in lines:
            if re.match(r"\d{2}\.\d{2}\.\d{4}$", line):
                capture = True
                continue
            if "Ke stažení" in line or "Stáhnout" in line:
                break
            if capture and len(line) > 20:
                summary_lines.append(line)
        meta["summary"] = " ".join(summary_lines) if summary_lines else ""

        # PDF links
        pdfs = list(set(re.findall(r'href="(/sites/default/files/[^"]*\.pdf)"', article)))
        meta["pdf_urls"] = [f"{BASE_URL}{p}" for p in pdfs]

        return meta

    def _extract_pdf_text(self, pdf_urls: List[str]) -> str:
        """Download PDFs and extract text."""
        all_text = []
        for url in pdf_urls:
            try:
                resp = self._get(url, timeout=60)
                text = extract_pdf_text(resp.content)
                if text and len(text) > 100:
                    all_text.append(text)
            except Exception as e:
                logger.warning(f"Failed to extract PDF {url}: {e}")
        return "\n\n---\n\n".join(all_text)

    def _parse_date(self, date_str: str) -> str:
        """Convert DD.MM.YYYY to ISO 8601."""
        if not date_str:
            return ""
        match = re.match(r"(\d{2})\.(\d{2})\.(\d{4})", date_str)
        if match:
            return f"{match.group(3)}-{match.group(2)}-{match.group(1)}"
        return date_str

    def fetch_all(self, limit: Optional[int] = None) -> Iterator[Dict[str, Any]]:
        """Yield raw decision data from paginated list + detail pages + PDFs."""
        count = 0
        page = 0

        while True:
            if limit and count >= limit:
                return

            url = f"{LIST_URL}?page={page}"
            logger.info(f"Fetching list page {page}: {url}")

            try:
                resp = self._get(url)
            except Exception as e:
                logger.error(f"Failed to fetch list page {page}: {e}")
                break

            decisions = self._parse_list_page(resp.text)
            if not decisions:
                logger.info(f"No decisions on page {page}, stopping.")
                break

            for path, title in decisions:
                if limit and count >= limit:
                    return

                detail_url = f"{BASE_URL}{path}"
                logger.info(f"  Fetching detail: {title[:60]}...")

                try:
                    detail_resp = self._get(detail_url)
                    meta = self._parse_detail_page(detail_resp.text)
                except Exception as e:
                    logger.warning(f"  Failed to fetch detail {detail_url}: {e}")
                    continue

                # Extract PDF text
                pdf_urls = meta.get("pdf_urls", [])
                if not pdf_urls:
                    logger.warning(f"  No PDFs found for {title[:60]}")
                    continue

                pdf_text = self._extract_pdf_text(pdf_urls)
                if len(pdf_text) < 100:
                    logger.warning(f"  Insufficient PDF text for {title[:60]}")
                    continue

                meta["title"] = title
                meta["detail_url"] = detail_url
                meta["pdf_text"] = pdf_text
                count += 1
                yield meta

                if count % 5 == 0:
                    logger.info(f"  Fetched {count} decisions so far...")

            page += 1

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw decision into standard schema."""
        file_ref = raw.get("file_reference", "")
        case_ref = raw.get("case_reference", "")
        doc_id = file_ref or case_ref or raw.get("title", "unknown")
        # Clean ID for filesystem
        safe_id = re.sub(r"[/\\]", "-", doc_id)

        date = self._parse_date(raw.get("legal_effect_date", ""))

        return {
            "_id": f"ERU-{safe_id}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("pdf_text", ""),
            "date": date,
            "url": raw.get("detail_url", ""),
            "file_reference": file_ref,
            "case_reference": case_ref,
            "party_name": raw.get("party_name", ""),
            "ico": raw.get("ico", ""),
            "legal_effect_date": date,
            "summary": raw.get("summary", ""),
            "pdf_urls": raw.get("pdf_urls", []),
        }


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap":
        if not HAS_PDFPLUMBER and not HAS_PYPDF2:
            logger.error("No PDF library available. Install pdfplumber or PyPDF2.")
            sys.exit(1)

        fetcher = ERUFetcher()
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        logger.info("Starting bootstrap of CZ/ERU...")

        sample_count = 0
        target = 15 if "--sample" in sys.argv else 50

        for raw_doc in fetcher.fetch_all(limit=target * 2):
            if sample_count >= target:
                break

            normalized = fetcher.normalize(raw_doc)
            text_len = len(normalized.get("text", ""))

            if text_len < 500:
                continue

            doc_id = normalized["_id"].replace("/", "_").replace(" ", "_")
            filepath = sample_dir / f"{doc_id}.json"

            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(normalized, f, indent=2, ensure_ascii=False)

            sample_count += 1
            logger.info(
                f"Saved [{sample_count}/{target}]: {normalized.get('file_reference', '')} "
                f"({text_len:,} chars)"
            )

        logger.info(f"Bootstrap complete. {sample_count} decisions saved to {sample_dir}")

        files = list(sample_dir.glob("*.json"))
        total_chars = sum(
            len(json.load(open(f, encoding="utf-8")).get("text", ""))
            for f in files
        )
        logger.info(f"Summary: {len(files)} files, {total_chars:,} total text chars")
        if files:
            logger.info(f"Average: {total_chars // len(files):,} chars/decision")
    else:
        print("Usage: python bootstrap.py bootstrap [--sample]")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
