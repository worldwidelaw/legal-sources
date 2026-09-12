#!/usr/bin/env python3
"""
IR/ABCenterLaws -- Abdorrahman Boroumand Center Iranian Laws (English)

Fetches ~35 English-translated Iranian law documents from iranrights.org.
Full text is embedded in HTML pages (not PDFs).

Strategy:
  - Scrape collection page for document links
  - Fetch each document page and extract full text from HTML

Usage:
  python bootstrap.py bootstrap --sample
  python bootstrap.py bootstrap --full
  python bootstrap.py test
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IR.ABCenterLaws")

BASE_URL = "https://www.iranrights.org"
COLLECTION_URL = f"{BASE_URL}/library/collection/112/laws"


class _HTMLTextExtractor(HTMLParser):
    """Extract clean text from HTML content areas."""

    def __init__(self):
        super().__init__()
        self.parts: List[str] = []
        self.skip_tags = {"script", "style", "nav", "header", "footer"}
        self.in_skip = 0

    def handle_starttag(self, tag, attrs):
        if tag in self.skip_tags:
            self.in_skip += 1
        if not self.in_skip:
            if tag in ("br",):
                self.parts.append("\n")
            elif tag in ("p", "div", "h1", "h2", "h3", "h4", "h5", "h6", "li", "tr"):
                self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag in self.skip_tags:
            self.in_skip = max(0, self.in_skip - 1)
        if not self.in_skip and tag in ("p", "div", "h1", "h2", "h3", "h4", "h5", "h6"):
            self.parts.append("\n")

    def handle_data(self, data):
        if not self.in_skip:
            self.parts.append(data)

    def get_text(self) -> str:
        text = "".join(self.parts).strip()
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r" {2,}", " ", text)
        return text


class ABCenterScraper(BaseScraper):
    """Scraper for IR/ABCenterLaws."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

    def _request(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
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

    def _parse_collection_page(self, html: str) -> List[Dict[str, str]]:
        """Extract document links, titles, and dates from the collection page."""
        docs = []
        seen_ids = set()

        # Find all /library/document/NNNN/ links (single or double quotes)
        for m in re.finditer(
            r"""href=['"](/library/document/(\d+)/?)['"]""",
            html,
        ):
            url_path, doc_id = m.group(1), m.group(2)
            if doc_id in seen_ids:
                continue
            seen_ids.add(doc_id)

            # Try to extract title from surrounding context
            # Look for <span> or text near the link
            context_start = max(0, m.start() - 50)
            context_end = min(len(html), m.end() + 500)
            context = html[m.start():context_end]

            title = ""
            # Try <span> inside <a>
            span_match = re.search(r'<span>(.*?)</span>', context, re.DOTALL)
            if span_match:
                raw_title = re.sub(r'<[^>]+>', '', span_match.group(1)).strip()
                # Split date from title
                date_match = re.search(r'\s*(\w+ \d{1,2}, \d{4})\s*$', raw_title)
                if date_match:
                    title = raw_title[:date_match.start()].strip()
                else:
                    title = raw_title

            date = ""
            # Extract date from context
            date_match = re.search(r'(\w+ \d{1,2}, \d{4})', context)
            if date_match:
                try:
                    dt = datetime.strptime(date_match.group(1), "%B %d, %Y")
                    date = dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass

            docs.append({
                "doc_id": doc_id,
                "url": f"{BASE_URL}{url_path}",
                "title": title,
                "date": date,
            })

        return docs

    def _extract_document_text(self, html: str) -> Dict[str, str]:
        """Extract full text and metadata from a document page."""
        result = {"text": "", "title": "", "date": ""}

        # Extract title from <title> tag (after :: separator) or content <h2>
        m = re.search(r"<title>[^:]*::\s*(.*?)</title>", html, re.DOTALL)
        if m:
            result["title"] = re.sub(r"<[^>]+>", "", m.group(1)).strip()

        if not result["title"] or result["title"] == "Abdorrahman Boroumand Center":
            # Look for the document title in h2 tags inside content area
            content_start = html.find("page_section_content")
            if content_start >= 0:
                h2s = re.findall(r"<h2[^>]*>(.*?)</h2>", html[content_start:], re.DOTALL)
                for h2 in h2s:
                    clean = re.sub(r"<[^>]+>", "", h2).strip()
                    if clean and clean != "Library search" and len(clean) > 5:
                        result["title"] = clean
                        break

        # Extract date from page metadata
        for pattern in [
            r'<time[^>]*datetime="([^"]*)"',
            r'(\w+ \d{1,2}, \d{4})',
            r'(\d{4}-\d{2}-\d{2})',
        ]:
            m = re.search(pattern, html)
            if m:
                raw_date = m.group(1)
                if re.match(r"\d{4}-\d{2}-\d{2}", raw_date):
                    result["date"] = raw_date[:10]
                    break
                try:
                    dt = datetime.strptime(raw_date, "%B %d, %Y")
                    result["date"] = dt.strftime("%Y-%m-%d")
                    break
                except ValueError:
                    pass

        # Extract main content — look for library-document-full or body class
        content_html = ""
        for class_name in ["library-document-full", "body"]:
            m = re.search(
                rf"""<div[^>]+class=['"]{class_name}['"][^>]*>(.*?)(?:</div>)""",
                html,
                re.DOTALL | re.IGNORECASE,
            )
            if m and len(m.group(1)) > 200:
                content_html = m.group(1)
                break

        # Fallback: find the largest content block between nav and footer
        if not content_html:
            m = re.search(
                r"<div[^>]+id=['\"]page_section_content['\"][^>]*>(.*?)(?:<footer|<div[^>]+id=['\"]page_section_footer)",
                html,
                re.DOTALL | re.IGNORECASE,
            )
            if m:
                content_html = m.group(1)

        if not content_html:
            m = re.search(r"<body[^>]*>(.*?)</body>", html, re.DOTALL | re.IGNORECASE)
            if m:
                content_html = m.group(1)

        parser = _HTMLTextExtractor()
        parser.feed(content_html)
        result["text"] = parser.get_text()

        return result

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw.get("doc_id", ""),
            "_source": "IR/ABCenterLaws",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("url", ""),
        }

    def fetch_all(self, max_records: int = None) -> Generator[Dict[str, Any], None, None]:
        count = 0

        # Fetch collection page
        resp = self._request(COLLECTION_URL)
        if resp is None:
            logger.error("Cannot fetch collection page")
            return

        docs = self._parse_collection_page(resp.text)
        logger.info(f"Found {len(docs)} documents in collection")

        if not docs:
            logger.error("No documents found on collection page")
            return

        for doc in docs:
            if max_records and count >= max_records:
                return

            resp = self._request(doc["url"])
            if resp is None:
                logger.warning(f"Failed to fetch: {doc['doc_id']} ({doc['title'][:50]})")
                continue

            extracted = self._extract_document_text(resp.text)
            text = extracted["text"]

            if not text or len(text) < 200:
                logger.warning(
                    f"Insufficient text ({len(text)} chars): {doc['doc_id']} ({doc['title'][:50]})"
                )
                continue

            raw = {
                "doc_id": doc["doc_id"],
                "title": extracted["title"] or doc["title"],
                "text": text,
                "date": extracted["date"] or doc.get("date", ""),
                "url": doc["url"],
            }
            count += 1
            yield raw

        logger.info(f"Completed: {count} documents fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all(max_records=10)

    def test(self) -> bool:
        resp = self._request(COLLECTION_URL)
        if resp is None:
            logger.error("Cannot reach collection page")
            return False

        docs = self._parse_collection_page(resp.text)
        logger.info(f"Collection page OK: {len(docs)} documents found")

        if docs:
            resp = self._request(docs[0]["url"])
            if resp:
                extracted = self._extract_document_text(resp.text)
                logger.info(
                    f"Document OK: {docs[0]['doc_id']} "
                    f"({len(extracted['text'])} chars, title={extracted['title'][:60]})"
                )
            else:
                logger.warning("Could not fetch sample document")

        return len(docs) > 0


def main():
    parser = argparse.ArgumentParser(description="IR/ABCenterLaws data fetcher")
    parser.add_argument(
        "command",
        # "bootstrap-fast" is the VPS pipeline alias for a full bootstrap.
        choices=["bootstrap", "bootstrap-fast", "update", "test"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = ABCenterScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command in ("bootstrap", "bootstrap-fast"):
        # Sample mode only when explicitly requested; the VPS pipeline runs the
        # full path and ingests data/records.jsonl.
        if args.sample:
            sample_dir = Path(__file__).parent / "sample"
            sample_dir.mkdir(exist_ok=True)
            count = 0
            for raw in scraper.fetch_all(max_records=15):
                record = scraper.normalize(raw)
                out_path = sample_dir / f"record_{count:04d}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                logger.info(
                    f"[{count + 1}] {record.get('title', '?')[:80]} "
                    f"({len(record.get('text', '')):,} chars)"
                )
                count += 1
            logger.info(f"Bootstrap complete: {count} records saved to sample/")
        else:
            # Stream normalized full records to data/records.jsonl for VPS ingest.
            data_dir = Path(__file__).parent / "data"
            data_dir.mkdir(exist_ok=True)
            jsonl_path = data_dir / "records.jsonl"
            count = 0
            with open(jsonl_path, "w", encoding="utf-8") as f:
                for raw in scraper.fetch_all():
                    record = scraper.normalize(raw)
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    count += 1
                    if count % 10 == 0:
                        logger.info(f"Progress: {count} records written")
            logger.info(f"Full bootstrap complete: {count} records -> {jsonl_path}")

    elif args.command == "update":
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for raw in scraper.fetch_updates():
            record = scraper.normalize(raw)
            out_path = sample_dir / f"update_{count:04d}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
        logger.info(f"Update complete: {count} records")


if __name__ == "__main__":
    main()
