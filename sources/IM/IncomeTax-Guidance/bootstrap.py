#!/usr/bin/env python3
"""
IM/IncomeTax-Guidance -- Isle of Man Income Tax Practice Notes & Guidance

Fetches practice notes and guidance notes from the Isle of Man Income Tax Division.
Covers corporate tax (0% standard rate), personal income tax (10%/20%),
national insurance, economic substance, pensions, benefits in kind, etc.

Strategy:
  - Fetch the practice notes and guidance notes index pages
  - Extract PDF links and titles from the HTML
  - Download each PDF and extract full text
  - ~298 documents (236 practice notes + 62 guidance notes)

Usage:
  python bootstrap.py bootstrap          # Full fetch
  python bootstrap.py bootstrap --sample # Fetch 15 samples
  python bootstrap.py test               # Connectivity test
"""

import sys
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IM.IncomeTax-Guidance")

BASE_URL = "https://www.gov.im"
PRACTICE_NOTES_PATH = "/categories/tax-vat-and-your-money/income-tax-and-national-insurance/tax-practitioners-and-technical-information/practice-notes/"
GUIDANCE_NOTES_PATH = "/categories/tax-vat-and-your-money/income-tax-and-national-insurance/tax-practitioners-and-technical-information/guidance-notes/"
DELAY = 2.0


def extract_reference(pdf_url: str) -> str:
    """Extract a reference ID from the PDF filename."""
    filename = pdf_url.rstrip("/").split("/")[-1]
    name = filename.replace(".pdf", "").replace("_compressed", "")
    # Normalize: pn-227-26-budget -> PN-227-26
    # gn1-return-form -> GN-1
    match = re.match(r'(pn|gn)[-_]?(\d+)', name, re.IGNORECASE)
    if match:
        prefix = match.group(1).upper()
        num = match.group(2)
        return f"{prefix}-{num}"
    return name[:50]


class IncomeTaxGuidance:
    SOURCE_ID = "IM/IncomeTax-Guidance"

    def __init__(self):
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                              "AppleWebKit/537.36 (KHTML, like Gecko) "
                              "Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
            },
            respect_robots=False,
        )

    def list_documents(self) -> List[Dict[str, Any]]:
        """Fetch practice notes and guidance notes index pages, extract PDF links."""
        entries = []

        pages = [
            (PRACTICE_NOTES_PATH, "practice_note"),
            (GUIDANCE_NOTES_PATH, "guidance_note"),
        ]

        for page_path, doc_type in pages:
            try:
                resp = self.http.get(page_path)
                time.sleep(1.0)
                if not resp or resp.status_code != 200:
                    logger.warning("Failed to fetch %s (status %s)",
                                   page_path, resp.status_code if resp else "None")
                    continue
            except Exception as e:
                logger.warning("Error fetching %s: %s", page_path, e)
                continue

            html = resp.text
            # Extract PDF links with anchor text
            links = re.findall(
                r'<a[^>]+href="([^"]+\.pdf)"[^>]*>(.*?)</a>',
                html, re.DOTALL | re.IGNORECASE
            )

            for pdf_url, link_text in links:
                clean_title = re.sub(r'<[^>]+>', '', link_text).strip()
                if not clean_title:
                    continue
                full_url = pdf_url if pdf_url.startswith("http") else BASE_URL + pdf_url
                ref = extract_reference(pdf_url)

                entries.append({
                    "title": clean_title,
                    "pdf_url": full_url,
                    "doc_type": doc_type,
                    "reference": ref,
                })

            logger.info("Found %d PDFs on %s page", len(links), doc_type)

        # Deduplicate by URL
        seen = set()
        unique = []
        for e in entries:
            if e["pdf_url"] not in seen:
                seen.add(e["pdf_url"])
                unique.append(e)

        logger.info("Total unique documents: %d", len(unique))
        return unique

    def fetch_pdf_text(self, pdf_url: str, ref: str) -> Optional[str]:
        """Download a PDF and extract text."""
        try:
            resp = self.http.get(pdf_url)
            time.sleep(DELAY)
            if not resp or resp.status_code != 200:
                logger.warning("Failed to download PDF: %s (status %s)",
                               ref, resp.status_code if resp else "None")
                return None
        except Exception as e:
            logger.warning("Error downloading %s: %s", ref, e)
            return None

        text = extract_pdf_markdown(
            source=self.SOURCE_ID,
            source_id=ref,
            pdf_bytes=resp.content,
            table="doctrine",
        )
        return text if text and len(text) >= 50 else None

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        """Fetch all documents with full text."""
        entries = self.list_documents()
        if not entries:
            logger.error("No document entries found")
            return

        sample_limit = 15 if sample else len(entries)
        total_yielded = 0
        failed = 0

        for entry in entries:
            if total_yielded >= sample_limit:
                break

            ref = entry["reference"]
            text = self.fetch_pdf_text(entry["pdf_url"], ref)
            if not text:
                failed += 1
                logger.warning("No text for %s (failed: %d)", ref, failed)
                continue

            record = {
                "_id": f"im-tax-{ref}",
                "_source": self.SOURCE_ID,
                "_type": "doctrine",
                "_fetched_at": datetime.now(timezone.utc).isoformat(),
                "title": entry["title"],
                "text": text,
                "date": None,
                "url": entry["pdf_url"],
                "language": "en",
                "doc_type": entry["doc_type"],
                "reference": ref,
            }

            yield record
            total_yielded += 1

            if total_yielded % 10 == 0:
                logger.info("  Progress: %d/%d documents (failed: %d)",
                            total_yielded, sample_limit, failed)

        logger.info("Fetch complete: %d documents yielded, %d failed",
                    total_yielded, failed)

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            entries = self.list_documents()
            if not entries:
                logger.error("Test failed: no entries found")
                return False
            logger.info("Test: found %d entries", len(entries))

            # Test one PDF download
            entry = entries[0]
            text = self.fetch_pdf_text(entry["pdf_url"], entry["reference"])
            if text and len(text) >= 50:
                logger.info("Test passed: extracted %d chars from %s",
                            len(text), entry["reference"])
                return True
            logger.error("Test failed: could not extract text from %s", entry["reference"])
            return False
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="IM/IncomeTax-Guidance bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = IncomeTaxGuidance()

    if args.command == "test":
        sys.exit(0 if scraper.test() else 1)

    if args.command == "bootstrap":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for record in scraper.fetch_all(sample=args.sample):
            safe_name = re.sub(r'[^\w\-.]', '_', record['_id'])[:100]
            out_file = sample_dir / f"{safe_name}.json"
            out_file.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
            count += 1
            text_len = len(record.get("text", ""))
            logger.info("  [%d] %s | %s | text=%d chars",
                        count, record["doc_type"], record["title"][:60], text_len)

        logger.info("Bootstrap complete: %d records saved to sample/", count)
        sys.exit(0 if count >= 10 else 1)

    if args.command == "update":
        count = sum(1 for _ in scraper.fetch_all(sample=False))
        logger.info("Update complete: %d documents", count)


if __name__ == "__main__":
    main()
