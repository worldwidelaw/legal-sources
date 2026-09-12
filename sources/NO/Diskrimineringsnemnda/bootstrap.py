#!/usr/bin/env python3
"""
NO/Diskrimineringsnemnda — Norwegian Equality and Anti-Discrimination Tribunal

Fetches discrimination, harassment, and equality decisions from Diskrimineringsnemnda
via the Umbraco Content Delivery API v2.

Older decisions (pre-2021) have inline HTML text via the `tekst` property.
Newer decisions have PDF attachments that require text extraction.

API endpoint: /umbraco/delivery/api/v2/content?filter=contentType:vedtak
License: NLOD 2.0
"""

import argparse
import io
import json
import logging
import re
import sys
import time
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://www.diskrimineringsnemnda.no"
API_BASE = f"{BASE_URL}/umbraco/delivery/api/v2/content"
SOURCE_ID = "NO/Diskrimineringsnemnda"
SAMPLE_DIR = Path(__file__).parent / "sample"
PAGE_SIZE = 100


def strip_html(html: str) -> str:
    """Strip HTML tags and clean up whitespace."""
    text = re.sub(r"<br\s*/?>", "\n", html)
    text = re.sub(r"</(p|div|h[1-6]|li|tr|blockquote)>", "\n\n", text)
    text = re.sub(r"<[^>]+>", "", text)
    text = unescape(text)
    text = re.sub(r"\xa0", " ", text)
    lines = [line.strip() for line in text.splitlines()]
    text = "\n".join(lines)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def extract_pdf_text(pdf_bytes: bytes) -> Optional[str]:
    """Extract text from PDF bytes using pdfplumber."""
    try:
        import pdfplumber
        pages_text = []
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    pages_text.append(page_text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
        text = "\n\n".join(pages_text).strip()
        return text if len(text) > 50 else None
    except Exception as e:
        logger.warning(f"PDF extraction failed: {e}")
        return None


class DiskrimineringsnemndaFetcher:
    """Fetcher for Norwegian Equality and Anti-Discrimination Tribunal decisions."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "application/json",
        })

    def _fetch_page(self, skip: int) -> Dict[str, Any]:
        """Fetch a page of vedtak from the API."""
        url = f"{API_BASE}?filter=contentType%3Avedtak&skip={skip}&take={PAGE_SIZE}&fields=properties%5B%24all%5D"
        resp = self.session.get(url, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def _download_pdf(self, pdf_path: str) -> Optional[str]:
        """Download a PDF and extract its text."""
        url = BASE_URL + pdf_path
        try:
            resp = self.session.get(url, timeout=60, headers={"Accept": "*/*"})
            if resp.status_code != 200:
                logger.warning(f"PDF download failed ({resp.status_code}): {url}")
                return None
            return extract_pdf_text(resp.content)
        except requests.RequestException as e:
            logger.warning(f"PDF download error: {e}")
            return None

    def _extract_text(self, props: Dict[str, Any]) -> Optional[str]:
        """Extract text from inline HTML or PDF attachment."""
        tekst = props.get("tekst")
        if tekst and isinstance(tekst, dict) and tekst.get("markup"):
            text = strip_html(tekst["markup"])
            if len(text) > 50:
                return text

        pdf_path = props.get("pDFVedlegg")
        if pdf_path:
            text = self._download_pdf(pdf_path)
            if text:
                return text

        return None

    def normalize(self, item: Dict[str, Any], text: str) -> Dict[str, Any]:
        """Normalize an API item into the standard schema."""
        props = item["properties"]
        name = item.get("name", "")
        saksnummer = props.get("saksnummer") or name
        vedtaksdato = props.get("vedtaksdato")
        date = None
        if vedtaksdato:
            date = vedtaksdato[:10]

        route = item.get("route", {})
        path = route.get("path", "")
        url = f"{BASE_URL}{path}" if path else f"{BASE_URL}/klagesaker-og-statistikk"

        vedtakstype = props.get("vedtakstype", "Vedtak")
        ingress = props.get("ingress")
        summary = ""
        if ingress and isinstance(ingress, dict) and ingress.get("markup"):
            summary = strip_html(ingress["markup"])

        doc_id = item.get("id", saksnummer)

        return {
            "_id": f"NO-DISKR-{doc_id}",
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.utcnow().isoformat() + "Z",
            "title": f"Sak {saksnummer}" if saksnummer else name,
            "text": text,
            "date": date,
            "url": url,
            "case_number": saksnummer,
            "decision_type": vedtakstype,
            "summary": summary,
            "language": "nob",
        }

    def fetch_all(self) -> Iterator[Dict[str, Any]]:
        """Fetch all decisions, paginating through the API."""
        skip = 0
        total = None
        count = 0

        while True:
            logger.info(f"Fetching vedtak skip={skip}...")
            data = self._fetch_page(skip)

            if total is None:
                total = data.get("total", 0)
                logger.info(f"Total vedtak: {total}")

            items = data.get("items", [])
            if not items:
                break

            for item in items:
                props = item.get("properties", {})
                text = self._extract_text(props)
                if not text:
                    logger.debug(f"No text for {item.get('name')}")
                    continue
                record = self.normalize(item, text)
                count += 1
                if count % 50 == 0:
                    logger.info(f"[{count}/{total}] {record['title'][:60]} ({len(text)} chars)")
                yield record

            skip += PAGE_SIZE
            if skip >= total:
                break
            time.sleep(1.0)

        logger.info(f"Fetch complete: {count} decisions with text out of {total} total")

    def fetch_updates(self, since: str) -> Iterator[Dict[str, Any]]:
        """Fetch decisions updated after a given date."""
        for record in self.fetch_all():
            if record.get("date") and record["date"] >= since:
                yield record

    def bootstrap_sample(self, count: int = 15) -> list:
        """Fetch a sample of decisions for testing."""
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        samples = []
        skip = 0
        # Fetch first batch (older records with inline text)
        data = self._fetch_page(0)
        total = data.get("total", 0)
        items = data.get("items", [])

        for item in items:
            if len(samples) >= count // 2:
                break
            props = item.get("properties", {})
            text = self._extract_text(props)
            if text:
                record = self.normalize(item, text)
                samples.append(record)
                fn = f"sample_{len(samples):03d}.json"
                with open(SAMPLE_DIR / fn, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                logger.info(f"  Saved {fn}: {record['title'][:50]} ({len(text)} chars)")
            time.sleep(0.5)

        # Fetch newer records (likely PDF-only) from the end
        newer_skip = max(0, total - 100)
        data2 = self._fetch_page(newer_skip)
        items2 = data2.get("items", [])

        for item in items2:
            if len(samples) >= count:
                break
            props = item.get("properties", {})
            text = self._extract_text(props)
            if text:
                record = self.normalize(item, text)
                samples.append(record)
                fn = f"sample_{len(samples):03d}.json"
                with open(SAMPLE_DIR / fn, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
                logger.info(f"  Saved {fn}: {record['title'][:50]} ({len(text)} chars)")
            time.sleep(1.5)

        return samples


def main():
    parser = argparse.ArgumentParser(
        description="NO/Diskrimineringsnemnda — Norwegian Anti-Discrimination Tribunal Fetcher"
    )
    parser.add_argument("command", choices=["bootstrap", "fetch", "updates"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample data only")
    parser.add_argument("--since", type=str, help="Fetch updates since date (YYYY-MM-DD)")
    parser.add_argument("--count", type=int, default=15, help="Number of sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")

    args = parser.parse_args()
    fetcher = DiskrimineringsnemndaFetcher()

    if args.command == "bootstrap":
        if args.sample:
            samples = fetcher.bootstrap_sample(count=args.count)
            print(f"\nBootstrap complete: {len(samples)} sample decisions saved to {SAMPLE_DIR}")
            texts = [s for s in samples if s.get("text") and len(s["text"]) > 50]
            dates = [s for s in samples if s.get("date")]
            print(f"  With full text: {len(texts)}/{len(samples)}")
            print(f"  With dates: {len(dates)}/{len(samples)}")
            if texts:
                avg_len = sum(len(s["text"]) for s in texts) // len(texts)
                print(f"  Average text length: {avg_len} chars")
        else:
            count = 0
            for record in fetcher.fetch_all():
                count += 1
            print(f"\nFetch complete: {count} decisions")

    elif args.command == "updates":
        if not args.since:
            print("Error: --since is required for updates command")
            sys.exit(1)
        count = 0
        for record in fetcher.fetch_updates(args.since):
            count += 1
        print(f"\nUpdates complete: {count} decisions since {args.since}")

    elif args.command == "fetch":
        count = 0
        for record in fetcher.fetch_all():
            count += 1
        print(f"\nFetch complete: {count} decisions")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
