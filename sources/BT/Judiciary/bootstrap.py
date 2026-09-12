#!/usr/bin/env python3
"""
BT/Judiciary -- Bhutan Judiciary Acts, Rules & Judgments

Fetches acts (~130), rules (~17), and landmark judgments (~10) from
judiciary.gov.bt. All documents are PDFs; text is extracted via pdfplumber.

Usage:
  python bootstrap.py bootstrap          # Fetch all documents
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import io
import json
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests
import pdfplumber
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.BT.Judiciary")

BASE_URL = "https://www.judiciary.gov.bt"
ACTS_URL = f"{BASE_URL}/posts/first-category/acts"
RULES_URL = f"{BASE_URL}/posts/first-category/rules"
JUDGMENTS_URL = f"{BASE_URL}/archive-content/judgments"


class BTJudiciaryScraper(BaseScraper):
    """Scraper for BT/Judiciary — Bhutan judiciary acts, rules & judgments."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

    def _request(self, url: str, timeout: int = 90) -> Optional[requests.Response]:
        """HTTP GET with delay and retry."""
        for attempt in range(3):
            try:
                time.sleep(1)
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
                    time.sleep(5)
        return None

    def _extract_pdf_text(self, content: bytes) -> str:
        """Extract text from PDF bytes using pdfplumber."""
        try:
            pages_text = []
            with pdfplumber.open(io.BytesIO(content)) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages_text.append(text)
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass
            full_text = "\n\n".join(pages_text)
            full_text = re.sub(r"\n{3,}", "\n\n", full_text)
            full_text = re.sub(r" {2,}", " ", full_text)
            return full_text.strip()
        except Exception as e:
            logger.warning(f"PDF extraction failed: {e}")
            return ""

    def _extract_year(self, title: str) -> str:
        """Extract year from title like 'Some Act of Bhutan 2004'."""
        m = re.search(r"\b(19\d{2}|20\d{2})\b", title)
        return m.group(1) if m else ""

    def _make_full_url(self, href: str) -> str:
        """Convert relative URL to absolute."""
        if href.startswith("http"):
            return href
        return BASE_URL + href

    def _parse_acts_page(self, html: str) -> List[Dict[str, str]]:
        """Parse the acts listing page for PDF links and titles."""
        soup = BeautifulSoup(html, "html.parser")
        acts = []
        seen_urls = set()

        for link in soup.find_all("a", href=True):
            href = link["href"]
            if not href.endswith(".pdf"):
                continue

            full_url = self._make_full_url(href)
            if full_url in seen_urls:
                continue

            # Get title from parent context
            parent = link.find_parent(["div", "li", "p", "td"])
            title = ""
            if parent:
                # Get text of the parent but not the "Click Here to Download" link text
                for child in parent.children:
                    if child == link:
                        continue
                    if hasattr(child, "get_text"):
                        t = child.get_text(strip=True)
                        if t and t != "Click Here to Download":
                            title = t
                            break
                    elif isinstance(child, str) and child.strip():
                        title = child.strip()
                        break

            if not title:
                # Fallback: derive from filename
                fname = href.split("/")[-1]
                title = fname.replace(".pdf", "").replace("%20", " ").replace("_", " ").replace("-", " ")

            # Skip Dzongkha-only documents
            if "Dzo" in title and "Eng" not in title and "English" not in title:
                # Check filename for Dzo-only markers
                fname_lower = href.lower()
                if "dzo" in fname_lower and "eng" not in fname_lower:
                    logger.info(f"Skipping Dzongkha-only: {title[:60]}")
                    continue

            seen_urls.add(full_url)
            acts.append({
                "title": title.strip(),
                "pdf_url": full_url,
                "doc_type": "legislation",
            })

        return acts

    def _parse_rules_page(self, html: str) -> List[Dict[str, str]]:
        """Parse the rules listing page for PDF links and titles."""
        soup = BeautifulSoup(html, "html.parser")
        rules = []
        seen_urls = set()

        for link in soup.find_all("a", href=True):
            href = link["href"]
            if not href.endswith(".pdf"):
                continue

            full_url = self._make_full_url(href)
            if full_url in seen_urls:
                continue

            parent = link.find_parent(["div", "li", "p", "td"])
            title = ""
            if parent:
                for child in parent.children:
                    if child == link:
                        continue
                    if hasattr(child, "get_text"):
                        t = child.get_text(strip=True)
                        if t and t != "Click Here to Download":
                            title = t
                            break
                    elif isinstance(child, str) and child.strip():
                        title = child.strip()
                        break

            if not title:
                fname = href.split("/")[-1]
                title = fname.replace(".pdf", "").replace("%20", " ").replace("_", " ")

            seen_urls.add(full_url)
            rules.append({
                "title": title.strip(),
                "pdf_url": full_url,
                "doc_type": "legislation",
            })

        return rules

    def _parse_judgments_page(self, html: str) -> List[Dict[str, str]]:
        """Parse the judgments listing page for PDF links and titles."""
        soup = BeautifulSoup(html, "html.parser")
        judgments = []
        seen_urls = set()

        # Find blocks with PDF links and titles
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if not href.endswith(".pdf"):
                continue

            full_url = self._make_full_url(href)
            if full_url in seen_urls:
                continue

            parent = link.find_parent(["div", "article", "li", "tr"])
            title = ""
            if parent:
                h = parent.find(["h1", "h2", "h3", "h4", "h5"])
                if h:
                    title = h.get_text(strip=True)
                else:
                    for child in parent.children:
                        if child == link:
                            continue
                        if hasattr(child, "get_text"):
                            t = child.get_text(strip=True)
                            if t and t != "Click Here to Download" and len(t) > 5:
                                title = t
                                break

            if not title:
                fname = href.split("/")[-1]
                title = fname.replace(".pdf", "").replace("%20", " ").replace("-", " ")

            # Skip non-judgment items (annual reports, press releases, Dzongkha)
            title_lower = title.lower()
            if "annual report" in title_lower:
                continue
            if "press release" in title_lower:
                continue
            if "(dzongkha)" in title_lower or title_lower.endswith("dzongkha"):
                continue

            seen_urls.add(full_url)
            judgments.append({
                "title": title.strip(),
                "pdf_url": full_url,
                "doc_type": "case_law",
            })

        return judgments

    def _make_id(self, doc: Dict[str, str]) -> str:
        """Generate a stable ID from title."""
        title = doc.get("title", "")
        slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")[:80]
        return f"BT-{doc['doc_type']}-{slug}"

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        doc_type = raw.get("doc_type", "legislation")
        _type = "case_law" if doc_type == "case_law" else "legislation"
        return {
            "_id": self._make_id(raw),
            "_source": "BT/Judiciary",
            "_type": _type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": self._extract_year(raw.get("title", "")),
            "url": raw.get("pdf_url", ""),
            "doc_type": doc_type,
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all acts, rules, and judgments."""
        all_docs = []

        # Fetch acts
        logger.info("Fetching acts listing...")
        resp = self._request(ACTS_URL)
        if resp:
            acts = self._parse_acts_page(resp.text)
            logger.info(f"Found {len(acts)} acts")
            all_docs.extend(acts)

        # Fetch rules
        logger.info("Fetching rules listing...")
        resp = self._request(RULES_URL)
        if resp:
            rules = self._parse_rules_page(resp.text)
            logger.info(f"Found {len(rules)} rules")
            all_docs.extend(rules)

        # Fetch judgments
        logger.info("Fetching judgments listing...")
        resp = self._request(JUDGMENTS_URL)
        if resp:
            judgments = self._parse_judgments_page(resp.text)
            logger.info(f"Found {len(judgments)} judgments")
            all_docs.extend(judgments)

        logger.info(f"Total documents to process: {len(all_docs)}")

        count = 0
        for doc in all_docs:
            pdf_resp = self._request(doc["pdf_url"])
            if pdf_resp is None:
                logger.warning(f"Failed to download: {doc['title'][:60]}")
                continue

            text = self._extract_pdf_text(pdf_resp.content)
            if len(text) < 100:
                logger.warning(f"Insufficient text ({len(text)} chars): {doc['title'][:60]}")
                continue

            doc["text"] = text
            count += 1
            yield doc

        logger.info(f"Completed: {count} documents with full text")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent documents (all judgments + check for new acts)."""
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        resp = self._request(ACTS_URL)
        if resp is None:
            logger.error("Cannot reach judiciary.gov.bt acts page")
            return False

        acts = self._parse_acts_page(resp.text)
        if not acts:
            logger.error("No acts found on listing page")
            return False

        logger.info(f"Acts listing OK: {len(acts)} acts found")

        # Test one PDF download
        pdf_resp = self._request(acts[0]["pdf_url"])
        if pdf_resp is None:
            logger.error("Cannot download PDF")
            return False

        text = self._extract_pdf_text(pdf_resp.content)
        logger.info(f"PDF extraction OK: {acts[0]['title'][:60]} ({len(text)} chars)")
        return len(text) > 100


def main():
    import argparse

    parser = argparse.ArgumentParser(description="BT/Judiciary data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample (for validation)",
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = BTJudiciaryScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
