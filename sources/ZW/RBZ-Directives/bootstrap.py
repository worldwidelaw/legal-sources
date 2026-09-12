#!/usr/bin/env python3
"""
ZW/RBZ-Directives — Reserve Bank of Zimbabwe Exchange Control Directives

Fetches exchange control directives, circulars, and orders from the RBZ website.
The HTML listing pages are protected by Radware Bot Manager (CAPTCHA), but
direct PDF document URLs are publicly accessible. This scraper uses a curated
list of known document paths discovered via search engines.

Usage:
  python bootstrap.py bootstrap           # Full initial pull
  python bootstrap.py bootstrap --sample  # Fetch sample records for validation
  python bootstrap.py test-api            # Quick connectivity test
"""

import io
import re
import sys
import json
import time
import logging
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ZW.RBZ-Directives")

SOURCE_ID = "ZW/RBZ-Directives"
BASE_URL = "https://www.rbz.co.zw"

# Curated list of known document paths (discovered via search engines).
# HTML listing pages are CAPTCHA-protected, so we maintain this list manually.
KNOWN_DOCUMENTS = [
    # 2025
    {
        "path": "documents/Regulations_Acts/2025/Guidelines_to_Authorised_Dealers_and_their_Clients_on_Foreign_Exchange_Transactions_8_Aug_2025_FXD4.pdf",
        "title": "Guidelines to Authorised Dealers and Their Clients on Foreign Exchange Transactions (FXD4/2025)",
        "year": 2025,
        "doc_type": "guideline",
    },
    # 2024
    {
        "path": "documents/Regulations_Acts/2024/Exchange_Control_Directives_2024/EXCHANGE_CONTROL_DIRECTIVE_RZ70-2024.pdf",
        "title": "Exchange Control Directive RZ70/2024",
        "year": 2024,
        "doc_type": "directive",
    },
    {
        "path": "documents/Regulations_Acts/2024/Exchange_Control_Directives_2024/Exchange_Control_Directive_RZ56.pdf",
        "title": "Exchange Control Directive RZ56/2024",
        "year": 2024,
        "doc_type": "directive",
    },
    {
        "path": "documents/Regulations_Acts/2024/Exchange_Control_Directives_2024/EXCHANGE_CONTROL_CIRCULAR_NO_3.pdf",
        "title": "Exchange Control Circular No. 3 of 2024",
        "year": 2024,
        "doc_type": "circular",
    },
    # 2023
    {
        "path": "documents/publications/circulars/2023/EXCHANGE_CONTROL_DIRECTIVE_RY002_2023001.pdf",
        "title": "Exchange Control Directive RY002/2023",
        "year": 2023,
        "doc_type": "directive",
    },
    {
        "path": "documents/publications/circulars/2023/Exchange_Control_Circular_number_4_of_2023_to_Authorised_Dealers.pdf",
        "title": "Exchange Control Circular No. 4 of 2023",
        "year": 2023,
        "doc_type": "circular",
    },
    {
        "path": "documents/publications/circulars/2023/Exchange_Control_Circular_No_3_of_2023.pdf",
        "title": "Exchange Control Circular No. 3 of 2023",
        "year": 2023,
        "doc_type": "circular",
    },
    {
        "path": "documents/publications/circulars/2023/Exchange_Control_Circular_1_of_2023_to_ADLAs.pdf",
        "title": "Exchange Control Circular No. 1 of 2023 to ADLAs",
        "year": 2023,
        "doc_type": "circular",
    },
    # 2022
    {
        "path": "documents/publications/circulars/2022/Exchange-Control--Directive-RX03.pdf",
        "title": "Exchange Control Directive RX003/2022",
        "year": 2022,
        "doc_type": "directive",
    },
    {
        "path": "documents/publications/circulars/2022/Exchange-Control-Directive-RX20-on-Gold-Coins.pdf",
        "title": "Exchange Control Directive RX20/2022 — Gold Coins",
        "year": 2022,
        "doc_type": "directive",
    },
    {
        "path": "documents/publications/circulars/2022/Exchange-Control-Circular-number-3-of-2022-to-Authorised-Dealers_220509_201631.pdf",
        "title": "Exchange Control Circular No. 3 of 2022",
        "year": 2022,
        "doc_type": "circular",
    },
    # 2021
    {
        "path": "documents/publications/circulars/2021/Exchange-Control-Circular-Number-5---Treatment-of-Commissions-and-Royalties.pdf",
        "title": "Exchange Control Circular No. 5 of 2021 — Treatment of Commissions and Royalties",
        "year": 2021,
        "doc_type": "circular",
    },
    # 2020
    {
        "path": "documents/publications/circulars/2020/Exchange-Control-Directive-RV-176-Exchange-Control-Staff.pdf",
        "title": "Exchange Control Directive RV176/2020",
        "year": 2020,
        "doc_type": "directive",
    },
    {
        "path": "documents/publications/circulars/2020/EXCHANGE-CONTROL-RV175.pdf",
        "title": "Exchange Control Directive RV175/2020",
        "year": 2020,
        "doc_type": "directive",
    },
    # 2019
    {
        "path": "documents/publications/circulars/Exchange-Control-Directive-RU102-of-2019.pdf",
        "title": "Exchange Control Directive RU102/2019",
        "year": 2019,
        "doc_type": "directive",
    },
    {
        "path": "documents/publications/circulars/Exchange-Control-Circular-No--8-of-2019-24-July-2019-PDF-Signed.pdf",
        "title": "Exchange Control Circular No. 8 of 2019",
        "year": 2019,
        "doc_type": "circular",
    },
    {
        "path": "documents/publications/circulars/Operational-Guidelines-for-Bureaux-de-Change-2019.pdf",
        "title": "Operational Guidelines for Bureaux de Change 2019",
        "year": 2019,
        "doc_type": "guideline",
    },
    # Acts & consolidated guidelines
    {
        "path": "documents/acts/Exchange_Control_Act-_Updated.pdf",
        "title": "Exchange Control Act (Chapter 22:05) — Updated",
        "year": None,
        "doc_type": "act",
    },
    {
        "path": "documents/mps/foreignexchange.pdf",
        "title": "Foreign Exchange Guidelines (Consolidated)",
        "year": None,
        "doc_type": "guideline",
    },
    # Monetary Policy Statements
    {
        "path": "documents/mps/2025/MPS_February_06_2025.pdf",
        "title": "Monetary Policy Statement — February 2025",
        "year": 2025,
        "doc_type": "monetary_policy",
    },
    {
        "path": "documents/mps/2024_Monetary_Policy_Statement.pdf",
        "title": "Monetary Policy Statement 2024 — Back to Basics",
        "year": 2024,
        "doc_type": "monetary_policy",
    },
    {
        "path": "documents/mps/2024/CONSOLIDATED_MPS_Review_-_August_2024.pdf",
        "title": "Mid-Term Monetary Policy Review — August 2024",
        "year": 2024,
        "doc_type": "monetary_policy",
    },
    # Banking regulation
    {
        "path": "documents/acts/ZIMBABWE_Banking_Act_2023_updated.pdf",
        "title": "Banking Act (Chapter 24:20) — 2023 Updated",
        "year": 2023,
        "doc_type": "act",
    },
    {
        "path": "documents/Regulations_Acts/2025/Operational_Guidelines_for_Money_Transfer_Agents_and_Bureaux_De_Change.pdf",
        "title": "Operational Guidelines for Money Transfer Agents and Bureaux De Change 2025",
        "year": 2025,
        "doc_type": "guideline",
    },
    {
        "path": "documents/Regulations_Acts/2025/Cybersecurity_and_Resilience_Guideline_-_August_2025.pdf",
        "title": "Cybersecurity and Resilience Guideline — August 2025",
        "year": 2025,
        "doc_type": "guideline",
    },
    {
        "path": "documents/BLSS/2022/Prudential_Standard_No02-2022_BSD_LCR.pdf",
        "title": "Prudential Standard No. 02-2022/BSD — Liquidity Coverage Ratio",
        "year": 2022,
        "doc_type": "prudential_standard",
    },
    {
        "path": "documents/press/2024/April/PRESS_STATEMENT_6_APRIL_2024.pdf",
        "title": "Press Statement — Introduction of ZiG Currency (6 April 2024)",
        "year": 2024,
        "doc_type": "press_statement",
    },
]


def _extract_pdf_text(content: bytes) -> str:
    """Extract text from PDF bytes using pdfplumber."""
    import pdfplumber

    text_parts = []
    with pdfplumber.open(io.BytesIO(content)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text_parts.append(page_text)
            try:
                page.flush_cache(); page.get_textmap.cache_clear()
            except Exception:
                pass
    return "\n".join(text_parts)


def _parse_date_from_text(text: str, year: Optional[int]) -> Optional[str]:
    """Try to extract a date from the document text."""
    # Common patterns in RBZ directives
    patterns = [
        r"(?:dated?|issued)\s+(?:this\s+)?(\d{1,2})\s*(?:st|nd|rd|th)?\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s*,?\s*(\d{4})",
        r"(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})",
        r"(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2})\s*,?\s+(\d{4})",
    ]
    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            groups = m.groups()
            try:
                if groups[0].isdigit():
                    day, month_name, yr = groups
                else:
                    month_name, day, yr = groups
                dt = datetime.strptime(f"{day} {month_name} {yr}", "%d %B %Y")
                return dt.strftime("%Y-%m-%d")
            except (ValueError, AttributeError):
                continue

    if year:
        return f"{year}-01-01"
    return None


def _make_id(raw: Dict[str, Any]) -> str:
    """Generate a stable document ID from the path."""
    path = raw.get("_raw_path", raw.get("path", ""))
    return hashlib.sha256(path.encode()).hexdigest()[:16]


class RBZDirectivesScraper(BaseScraper):
    SOURCE_ID = SOURCE_ID

    def __init__(self):
        super().__init__(source_dir=str(Path(__file__).parent))
        self.http = HttpClient(
            headers={"User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)"},
            timeout=60,
        )

    def fetch_all(self, sample: bool = False) -> Generator[Dict[str, Any], None, None]:
        docs = KNOWN_DOCUMENTS
        if sample:
            docs = docs[:15]

        for i, doc in enumerate(docs):
            url = f"{BASE_URL}/{doc['path']}"
            logger.info(f"[{i+1}/{len(docs)}] Fetching: {doc['title']}")

            try:
                resp = self.http.get(url)
                if resp.status_code != 200:
                    logger.warning(f"HTTP {resp.status_code} for {url}")
                    continue

                content_type = resp.headers.get("Content-Type", "")
                if "pdf" not in content_type and len(resp.content) < 1000:
                    logger.warning(f"Not a PDF response for {url}")
                    continue

                text = _extract_pdf_text(resp.content)
                if not text or len(text.strip()) < 50:
                    logger.warning(f"No extractable text from {url}")
                    continue

                record = {
                    "_raw_path": doc["path"],
                    "_raw_title": doc["title"],
                    "_raw_year": doc["year"],
                    "_raw_doc_type": doc["doc_type"],
                    "_raw_url": url,
                    "_raw_text": text,
                    "_raw_pdf_size": len(resp.content),
                }
                yield record
                time.sleep(1.5)

            except Exception as e:
                logger.error(f"Error fetching {url}: {e}")
                continue

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        text = raw["_raw_text"]
        title = raw["_raw_title"]

        # Try to get a better title from the first line of the document
        first_lines = text.strip().split("\n")[:3]
        first_line = " ".join(first_lines).strip()
        if len(first_line) > 10 and len(first_line) < 300:
            title = re.sub(r"\s+", " ", first_line)

        date = _parse_date_from_text(text, raw["_raw_year"])

        return {
            "_id": _make_id(raw),
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw["_raw_title"],
            "text": text.strip(),
            "date": date,
            "url": raw["_raw_url"],
            "doc_type": raw["_raw_doc_type"],
            "year": raw["_raw_year"],
            "pdf_size_bytes": raw["_raw_pdf_size"],
        }

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """No incremental update support — re-fetch all."""
        yield from self.fetch_all()


# ── CLI ─────────────────────────────────────────────────────────────────────
def main():
    import argparse

    parser = argparse.ArgumentParser(description="ZW/RBZ-Directives bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Fetch and store records")
    boot.add_argument("--sample", action="store_true", help="Sample mode (15 docs)")

    sub.add_parser("test-api", help="Quick connectivity test")

    args = parser.parse_args()

    if args.command == "test-api":
        scraper = RBZDirectivesScraper()
        url = f"{BASE_URL}/{KNOWN_DOCUMENTS[0]['path']}"
        logger.info(f"Testing: {url}")
        resp = scraper.http.get(url)
        logger.info(f"Status: {resp.status_code}, Size: {len(resp.content)} bytes")
        if resp.status_code == 200:
            text = _extract_pdf_text(resp.content)
            logger.info(f"Extracted text: {len(text)} chars")
            logger.info(f"Preview: {text[:200]}")
        return

    if args.command == "bootstrap":
        scraper = RBZDirectivesScraper()
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        for raw in scraper.fetch_all(sample=args.sample):
            record = scraper.normalize(raw)
            out_path = sample_dir / f"{record['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(
                f"Saved {out_path.name}: {record['title'][:60]}... "
                f"({len(record['text'])} chars)"
            )
            count += 1

        logger.info(f"Done. {count} records saved to {sample_dir}")
        return

    parser.print_help()


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
