#!/usr/bin/env python3
"""
INTL/EAEUCourt -- Eurasian Economic Union Court Decisions

Fetches decisions from the Court of the Eurasian Economic Union (EAEU)
at courteurasian.org. Includes both EAEU cases and historical EurAsEC cases.

Strategy:
  - Use sitemap (sitemap-iblock-9.xml) for complete case URL list (~135 cases)
  - Scrape Russian case pages for metadata (type, subject, parties, keywords)
  - Extract case description + legal positions from inline HTML
  - Download decision PDFs and extract text with pdfplumber
  - Each case may have multiple documents; we take the main decision/ruling

Data Coverage:
  - ~113 EAEU cases + ~22 EurAsEC cases = ~135 total
  - Member states: Russia, Kazakhstan, Belarus, Armenia, Kyrgyzstan
  - Languages: Russian (primary)

Usage:
  python bootstrap.py bootstrap          # Fetch all decisions
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import io
import time
import urllib.parse
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple

import requests
import pdfplumber
import xml.etree.ElementTree as ET

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.EAEUCourt")

BASE_URL = "https://courteurasian.org"
SITEMAP_URL = f"{BASE_URL}/sitemap-iblock-9.xml"


class EAEUCourtScraper(BaseScraper):
    """Scraper for INTL/EAEUCourt -- EAEU Court decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.5",
        })

    def _request(self, url: str, timeout: int = 60, binary: bool = False) -> Optional[requests.Response]:
        """HTTP GET with delay and retry."""
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(10)
        return None

    def _get_case_urls_from_sitemap(self) -> List[Tuple[str, str]]:
        """Get all case page URLs and lastmod dates from the sitemap.
        Returns list of (url, lastmod_date) tuples."""
        resp = self._request(SITEMAP_URL)
        if resp is None:
            logger.error("Failed to fetch sitemap")
            return []

        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        root = ET.fromstring(resp.content)
        results = []
        for url_elem in root.findall("sm:url", ns):
            loc = url_elem.findtext("sm:loc", "", ns)
            lastmod = url_elem.findtext("sm:lastmod", "", ns)
            if loc and "/court_cases/" in loc:
                # Normalize to https
                loc = loc.replace("http://", "https://")
                results.append((loc, lastmod[:10] if lastmod else ""))

        logger.info(f"Sitemap returned {len(results)} case URLs")
        return results

    def _extract_case_metadata(self, html: str) -> Dict[str, str]:
        """Extract case metadata from a Russian case page."""
        meta = {
            "case_number": "",
            "case_type": "",
            "subject": "",
            "sphere": "",
            "applicant": "",
            "defendant": "",
            "stage": "",
            "nomenclature": "",
            "keywords": "",
            "summary": "",
        }

        # Title contains case number: "С-1/15: ТОО «Гамма»..."
        title_match = re.search(r'<title>(.*?)</title>', html)
        if title_match:
            title_text = title_match.group(1).strip()
            # Extract case number from title (before colon)
            num_match = re.match(r'([^:]+)', title_text)
            if num_match:
                meta["case_number"] = num_match.group(1).strip()

        # Extract ttl->value pairs: <div class="case-card__ttl">Label:</div> <p>Value</p>
        parts = re.split(r'<div class="case-card__ttl">', html)
        for part in parts[1:]:
            ttl_end = part.find('</div>')
            if ttl_end < 0:
                continue
            ttl = re.sub(r'<[^>]+>', '', part[:ttl_end]).strip().rstrip(':')
            rest = part[ttl_end:]
            val_match = re.search(r'<p>(.*?)</p>', rest, re.DOTALL)
            if val_match:
                val = re.sub(r'<[^>]+>', ' ', val_match.group(1)).strip()
                val = re.sub(r'\s+', ' ', val)
            else:
                val = ""

            ttl_lower = ttl.lower()
            if "тип дела" in ttl_lower:
                meta["case_type"] = val
            elif "предмет спора" in ttl_lower or "предмет" in ttl_lower:
                meta["subject"] = val
            elif "сфера права" in ttl_lower:
                meta["sphere"] = val
            elif "заявитель" in ttl_lower or "истец" in ttl_lower:
                meta["applicant"] = val
            elif "ответчик" in ttl_lower:
                meta["defendant"] = val
            elif "стадия" in ttl_lower:
                meta["stage"] = val
            elif "номенклатур" in ttl_lower:
                meta["nomenclature"] = val
            elif "ключевые слова" in ttl_lower:
                meta["keywords"] = val

        # Extract case summary (show-hide-text sections)
        show_sections = re.findall(
            r'<div class="show-hide-text js-show-hide">(.*?)</div>\s*</div>',
            html, re.DOTALL
        )
        summary_parts = []
        for section in show_sections:
            text = re.sub(r'<[^>]+>', ' ', section).strip()
            text = re.sub(r'\s+', ' ', text)
            if len(text) > 50:
                summary_parts.append(text)
        meta["summary"] = "\n\n".join(summary_parts)

        return meta

    def _extract_pdf_links(self, html: str) -> List[Tuple[str, str, str]]:
        """Extract PDF links with their document types and dates.
        Returns list of (url, doc_name, date)."""
        results = []

        # Find document table rows
        rows = re.findall(
            r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL
        )

        for row in rows:
            pdf_match = re.search(r'href="(/upload/[^"]+\.pdf)"', row)
            if not pdf_match:
                continue

            pdf_url = pdf_match.group(1)

            # Extract document name from the text near the link
            row_text = re.sub(r'<[^>]+>', ' ', row)
            row_text = re.sub(r'\s+', ' ', row_text).strip()

            # Extract date (DD.MM.YYYY format)
            date_match = re.search(r'(\d{2}\.\d{2}\.\d{4})', row_text)
            date_str = date_match.group(1) if date_match else ""

            results.append((pdf_url, row_text, date_str))

        # If no table rows found, just get all PDFs
        if not results:
            pdfs = re.findall(r'href="(/upload/[^"]+\.pdf)"', html)
            for p in pdfs:
                # Decode the URL to get the filename
                name = urllib.parse.unquote(p.split("/")[-1]).replace(".pdf", "")
                results.append((p, name, ""))

        return results

    def _select_main_decision_pdf(
        self, pdfs: List[Tuple[str, str, str]]
    ) -> Optional[Tuple[str, str, str]]:
        """Select the main decision PDF from a list of documents.
        Priority: Решение (Decision) > Постановление (Ruling) > others."""
        if not pdfs:
            return None

        # Look for main decision types (Russian terms)
        priority_terms = [
            "Решение",        # Decision
            "Консультативное заключение",  # Advisory opinion
            "Постановление",  # Ruling/Resolution
        ]

        # Exclude dissenting opinions and procedural docs
        exclude_terms = [
            "Особое мнение",   # Dissenting opinion
            "без движения",    # Left without progress
            "принятие",        # Acceptance to proceedings
            "принятии к производству",
            "приемлемости",    # Admissibility
            "отказе",          # Refusal
        ]

        for term in priority_terms:
            for pdf_url, doc_name, date in pdfs:
                decoded = urllib.parse.unquote(pdf_url)
                if term.lower() in decoded.lower() or term.lower() in doc_name.lower():
                    # Check it's not an excluded type
                    excluded = any(
                        ex.lower() in decoded.lower() or ex.lower() in doc_name.lower()
                        for ex in exclude_terms
                    )
                    if not excluded:
                        return (pdf_url, doc_name, date)

        # If no match found, try any Решение/Постановление (even with qualifiers)
        for pdf_url, doc_name, date in pdfs:
            decoded = urllib.parse.unquote(pdf_url)
            for term in ["Решение", "Постановление"]:
                if term.lower() in decoded.lower() or term.lower() in doc_name.lower():
                    return (pdf_url, doc_name, date)

        # Last resort: return the first PDF
        return pdfs[0]

    def _extract_pdf_text(self, content: bytes) -> str:
        """Extract text from a PDF using pdfplumber."""
        try:
            with pdfplumber.open(io.BytesIO(content)) as pdf:
                parts = []
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        parts.append(text)
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass
                return "\n\n".join(parts).strip()
        except Exception as e:
            logger.warning(f"PDF extraction failed: {e}")
            return ""

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        case_num = raw.get("case_number", "")
        subject = raw.get("subject", "")
        section = raw.get("section", "eaeu")
        court_name = "Court of the Eurasian Economic Union" if section == "eaeu" \
            else "EurAsEC Court"
        title = raw.get("title", "") or f"{court_name} {case_num}"
        if not raw.get("title") and subject:
            title = f"{title}: {subject[:100]}"

        # Combine PDF text with case summary for comprehensive text
        text = raw.get("text", "")
        summary = raw.get("summary", "")
        if summary and summary not in text:
            text = f"{text}\n\n--- Case Summary ---\n{summary}"

        return {
            "_id": f"eaeu-court-{case_num}".replace("/", "-").replace(" ", "-"),
            "_source": "INTL/EAEUCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date", ""),
            "court": court_name,
            "case_number": case_num,
            "case_type": raw.get("case_type", ""),
            "subject": subject,
            "applicant": raw.get("applicant", ""),
            "defendant": raw.get("defendant", ""),
            "sphere": raw.get("sphere", ""),
            "keywords": raw.get("keywords", ""),
            "url": raw.get("url", ""),
        }

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch all EAEU Court decisions using sitemap."""
        count = 0
        case_entries = self._get_case_urls_from_sitemap()
        if not case_entries:
            logger.error("No case URLs from sitemap")
            return

        for case_url, lastmod in case_entries:
            if max_records and count >= max_records:
                return

            # Determine section from URL
            if "/eurazes/" in case_url or "/eurazes/" in case_url:
                section = "eurazes"
            else:
                section = "eaeu"

            logger.info(f"Processing: {case_url}")

            resp = self._request(case_url)
            if resp is None:
                logger.warning(f"Failed to fetch case page: {case_url}")
                continue

            # Extract metadata from Russian page
            meta = self._extract_case_metadata(resp.text)

            # Extract title from page <title>
            title_match = re.search(r'<title>(.*?)</title>', resp.text)
            title = ""
            if title_match:
                title = title_match.group(1).strip()
                # Clean up HTML entities
                title = title.replace("&quot;", '"').replace("&amp;", "&")

            # Extract PDF links and select main decision
            pdfs = self._extract_pdf_links(resp.text)
            if not pdfs:
                logger.warning(f"No PDFs found for {case_url}")
                continue

            selected = self._select_main_decision_pdf(pdfs)
            if not selected:
                logger.warning(f"Could not select decision PDF for {case_url}")
                continue

            pdf_url, doc_name, doc_date = selected

            # Download and extract PDF text
            safe_chars = "/:@!$&'()*+,;=-._~"
            full_pdf_url = f"{BASE_URL}{urllib.parse.quote(pdf_url, safe=safe_chars)}"
            pdf_resp = self._request(full_pdf_url, binary=True)
            if pdf_resp is None:
                logger.warning(f"Failed to download PDF: {pdf_url}")
                continue

            text = self._extract_pdf_text(pdf_resp.content)
            if not text or len(text) < 100:
                logger.warning(f"Insufficient text from PDF ({len(text)} chars): {pdf_url}")
                continue

            # Parse date from doc_date (DD.MM.YYYY) or use lastmod
            date_str = ""
            if doc_date:
                try:
                    dt = datetime.strptime(doc_date, "%d.%m.%Y")
                    date_str = dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass
            if not date_str and lastmod:
                date_str = lastmod

            raw = {
                "case_number": meta["case_number"],
                "title": title,
                "case_type": meta["case_type"],
                "subject": meta["subject"],
                "sphere": meta["sphere"],
                "applicant": meta["applicant"],
                "defendant": meta["defendant"],
                "keywords": meta["keywords"],
                "summary": meta["summary"],
                "text": text,
                "date": date_str,
                "url": case_url,
                "section": section,
            }
            count += 1
            yield raw

        logger.info(f"Completed: {count} decisions fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch all decisions (corpus is small enough)."""
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        entries = self._get_case_urls_from_sitemap()
        if not entries:
            logger.error("Cannot fetch sitemap from courteurasian.org")
            return False
        logger.info(f"Sitemap OK: {len(entries)} case URLs")

        # Test first case page
        test_url = entries[0][0]
        resp = self._request(test_url)
        if resp:
            meta = self._extract_case_metadata(resp.text)
            pdfs = self._extract_pdf_links(resp.text)
            logger.info(
                f"Test case: number={meta['case_number']}, "
                f"type={meta['case_type'][:40]}, "
                f"{len(pdfs)} PDFs"
            )
        return True


def main():
    import argparse

    parser = argparse.ArgumentParser(description="INTL/EAEUCourt data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
    )
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true")
    args = parser.parse_args()

    scraper = EAEUCourtScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        max_records = 15 if args.sample else None

        for record in scraper.fetch_all(max_records=max_records):
            out_path = sample_dir / f"record_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            text_len = len(record.get("text", ""))
            logger.info(
                f"[{count + 1}] {record.get('case_number', '?')} "
                f"({text_len:,} chars)"
            )
            count += 1

        logger.info(f"Bootstrap complete: {count} records saved to sample/")

    elif args.command == "update":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in scraper.fetch_updates():
            out_path = sample_dir / f"update_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        logger.info(f"Update complete: {count} records")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
