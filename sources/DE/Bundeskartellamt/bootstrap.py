#!/usr/bin/env python3
"""
German Federal Cartel Office (Bundeskartellamt) Decision Fetcher

Extracts competition-law decisions and case reports from bundeskartellamt.de:
- Kartellverbot (cartel prohibition)
- Missbrauchsaufsicht (abuse of dominance)
- Fusionskontrolle (merger control)
- Vergaberecht (public procurement — Vergabekammern)
- Digitale Wirtschaft (digital economy)

Discovery walks the official "Entscheidungsdatenbank" (decision database) search
form and paginates through every result via the GSB `gtp` list parameter
(~2,760 entries). Each result links to a decision detail page whose full text is
published as a born-digital PDF (same path, `.html` -> `.pdf`). Full text is
extracted with the shared PDF extractor; the HTML page is used as a fallback.

Data source: https://www.bundeskartellamt.de
License: Official German government publication (public domain under § 5 UrhG)
"""

import argparse
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, List, Optional, Dict, Any
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Add project root to path for common imports
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("DE/Bundeskartellamt")

SOURCE_ID = "DE/Bundeskartellamt"
BASE_URL = "https://www.bundeskartellamt.de"
SEARCH_URL = (
    f"{BASE_URL}/SiteGlobals/Forms/Suche/Entscheidungsdatenbanksuche_Formular.html"
)
RESULTS_PER_PAGE = 50

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
}

GERMAN_MONTHS = {
    "januar": 1, "februar": 2, "märz": 3, "maerz": 3, "april": 4, "mai": 5,
    "juni": 6, "juli": 7, "august": 8, "september": 9, "oktober": 10,
    "november": 11, "dezember": 12,
}


def extract_pdf_text(pdf_content: bytes) -> str:
    """Extract text from PDF using the centralized extractor."""
    return extract_pdf_markdown(
        source=SOURCE_ID,
        source_id="",
        pdf_bytes=pdf_content,
        table="doctrine",
    ) or ""


def _parse_de_date(text: str) -> Optional[str]:
    """Parse a German date (DD.MM.YYYY or 'D. Monat YYYY') -> ISO YYYY-MM-DD."""
    if not text:
        return None
    m = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", text)
    if m:
        d, mo, y = m.groups()
        return f"{y}-{int(mo):02d}-{int(d):02d}"
    m = re.search(r"(\d{1,2})\.\s*([A-Za-zäöüÄÖÜ]+)\s+(\d{4})", text)
    if m:
        d, mon, y = m.groups()
        mo = GERMAN_MONTHS.get(mon.lower())
        if mo:
            return f"{y}-{mo:02d}-{int(d):02d}"
    return None


class BundeskartellamtFetcher:
    """Fetcher for Bundeskartellamt decisions via the official decision database."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _get(self, url: str, timeout: int = 40) -> Optional[requests.Response]:
        for attempt in range(4):
            try:
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 200:
                    return resp
                if resp.status_code in (429, 500, 502, 503, 504):
                    wait = min(2 ** attempt * 2, 60)
                    logger.warning(f"HTTP {resp.status_code} for {url}; retry in {wait}s")
                    time.sleep(wait)
                    continue
                logger.warning(f"HTTP {resp.status_code} for {url}")
                return None
            except requests.RequestException as e:
                wait = min(2 ** attempt * 2, 60)
                logger.warning(f"Request error {e} for {url}; retry in {wait}s")
                time.sleep(wait)
        return None

    def _discover_page_context(self) -> Dict[str, Any]:
        """Fetch page 1 to learn the total count and the pagination component id."""
        resp = self._get(f"{SEARCH_URL}?resultsPerPage={RESULTS_PER_PAGE}")
        if resp is None:
            raise RuntimeError(
                "Bundeskartellamt decision database unreachable "
                "(Entscheidungsdatenbanksuche returned no page 0) — likely an IP block."
            )
        html = resp.text
        total_m = re.search(r"([0-9][0-9.]*)\s*Einträge", html)
        total = int(total_m.group(1).replace(".", "")) if total_m else None
        comp_m = re.search(r"gtp=(\d+)_list", html)
        comp_id = comp_m.group(1) if comp_m else None
        if not comp_id:
            raise RuntimeError(
                "Could not locate pagination component id (gtp=NNN_list) on the "
                "Bundeskartellamt search page — layout may have changed."
            )
        if not total:
            # Fall back: keep paging until an empty page.
            logger.warning("Could not read total 'Einträge' count; will page until empty.")
        logger.info(f"Decision DB: total={total} entries, component={comp_id}")
        return {"first_html": html, "total": total, "comp_id": comp_id}

    def _parse_results(self, html: str) -> List[Dict[str, Any]]:
        """Parse the 50 result rows on a search page into decision descriptors."""
        soup = BeautifulSoup(html, "html.parser")
        out = []
        for a in soup.select("a.c-searchresult-table__title"):
            href = a.get("href")
            if not href:
                continue
            detail_url = urljoin(BASE_URL, href)
            case_number = a.get_text(strip=True)

            tw = a.find_parent("div", class_="c-searchresult-table__text-wrapper")
            fmt = area = excerpt = ""
            if tw:
                spans = tw.select("p.c-topline .c-topline__item")
                if len(spans) >= 1:
                    fmt = spans[0].get_text(strip=True)
                if len(spans) >= 2:
                    area = spans[1].get_text(strip=True)
                exc = tw.select_one(".c-searchresult-table__excerpt")
                if exc:
                    excerpt = exc.get_text(" ", strip=True)

            date_iso = subject = decision_type = ""
            tr = a.find_parent("tr")
            if tr:
                tds = tr.find_all("td", recursive=False)
                if len(tds) >= 2:
                    date_iso = _parse_de_date(tds[1].get_text(" ", strip=True)) or ""
                if len(tds) >= 3:
                    subject = tds[2].get_text(" ", strip=True)
                if len(tds) >= 4:
                    decision_type = tds[3].get_text(" ", strip=True)

            out.append({
                "detail_url": detail_url,
                "case_number": case_number,
                "format": fmt,
                "area": area,
                "excerpt": excerpt,
                "date": date_iso,
                "subject": subject,
                "decision_type": decision_type,
            })
        return out

    def _discover_all(self) -> Iterator[Dict[str, Any]]:
        """Walk every page of the decision database, yielding result descriptors."""
        ctx = self._discover_page_context()
        comp_id = ctx["comp_id"]
        total = ctx["total"]
        last_page = ((total - 1) // RESULTS_PER_PAGE + 1) if total else 10_000

        seen = set()
        empty_pages = 0
        for page in range(1, last_page + 1):
            if page == 1:
                html = ctx["first_html"]
            else:
                url = f"{SEARCH_URL}?gtp={comp_id}_list%253D{page}&resultsPerPage={RESULTS_PER_PAGE}"
                resp = self._get(url)
                if resp is None:
                    logger.warning(f"Page {page} failed; skipping")
                    continue
                html = resp.text
            rows = self._parse_results(html)
            if not rows:
                empty_pages += 1
                if empty_pages >= 2:
                    break
                continue
            empty_pages = 0
            for row in rows:
                if row["detail_url"] in seen:
                    continue
                seen.add(row["detail_url"])
                yield row
            logger.info(f"Discovered page {page}/{last_page if total else '?'} "
                        f"({len(seen)} unique so far)")
            time.sleep(1.0)

    @staticmethod
    def _pdf_url_for(detail_url: str) -> Optional[str]:
        if not detail_url.endswith(".html"):
            return None
        return detail_url[:-5] + ".pdf?__blob=publicationFile"

    def _fetch_html_text(self, detail_url: str) -> str:
        """Fallback: extract the main article body text from the HTML page."""
        resp = self._get(detail_url)
        if resp is None:
            return ""
        soup = BeautifulSoup(resp.content, "html.parser")
        main = soup.select_one("article, main, .l-content-wrapper") or soup
        for tag in main.select("script, style, nav, header, footer"):
            tag.decompose()
        return re.sub(r"\n{3,}", "\n\n", main.get_text("\n", strip=True))

    def fetch_decision(self, info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        detail_url = info["detail_url"]
        case_number = info["case_number"]

        full_text = ""
        pdf_url = self._pdf_url_for(detail_url)
        source_url = detail_url
        if pdf_url:
            resp = self._get(pdf_url, timeout=120)
            if resp is not None and resp.content[:4] == b"%PDF":
                full_text = extract_pdf_text(resp.content)
                source_url = pdf_url

        if not full_text or len(full_text.strip()) < 500:
            html_text = self._fetch_html_text(detail_url)
            if len(html_text.strip()) > len(full_text.strip()):
                full_text = html_text
                source_url = detail_url

        if not full_text or len(full_text.strip()) < 300:
            logger.info(f"  Insufficient text for {case_number} "
                        f"({len(full_text.strip())} chars) — skipping")
            return None

        # Derive a unique id from the URL path: a Fallbericht and an Entscheidung
        # can share the same case number, so the case number alone is not unique.
        slug_m = re.search(r"/Entscheidung/DE/(.+?)\.html$", detail_url)
        slug = slug_m.group(1) if slug_m else case_number
        doc_id = ("DE_BKartA_" + slug).replace("/", "_").replace("-", "_").replace(" ", "").replace(".", "")
        date = info.get("date") or ""
        if not date:
            ym = re.search(r"/(\d{4})/", detail_url)
            if ym:
                date = f"{ym.group(1)}-01-01"

        parts = [p for p in [info.get("format"), info.get("area"), info.get("subject")] if p]
        title = f"Bundeskartellamt {case_number}"
        if parts:
            title += " — " + " / ".join(parts)

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "case_number": case_number,
            "title": title,
            "text": full_text,
            "date": date or None,
            "decision_date": date or None,
            "decision_type": info.get("decision_type", ""),
            "procedure_type": info.get("area", ""),
            "document_format": info.get("format", ""),
            "subject_market": info.get("subject", ""),
            "summary": info.get("excerpt", ""),
            "url": source_url,
            "html_url": detail_url,
            "authority": "Bundeskartellamt",
            "country": "DE",
            "language": "de",
            "text_length": len(full_text),
        }

    def fetch_all(self, limit: int = 0) -> Iterator[Dict[str, Any]]:
        count = 0
        for info in self._discover_all():
            doc = self.fetch_decision(info)
            if doc:
                yield doc
                count += 1
                if count % 25 == 0:
                    logger.info(f"Fetched {count} decisions...")
            if limit and count >= limit:
                break
            time.sleep(0.5)
        logger.info(f"Total decisions fetched: {count}")

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Yield decisions newer than `since` (DB is sorted newest-first)."""
        since_iso = since.date().isoformat() if isinstance(since, datetime) else str(since)
        for info in self._discover_all():
            if info.get("date") and info["date"] < since_iso:
                break
            doc = self.fetch_decision(info)
            if doc:
                yield doc
            time.sleep(0.5)

    def normalize(self, raw_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Records are already normalized in fetch_decision."""
        return raw_doc


def bootstrap(sample: bool = False) -> int:
    fetcher = BundeskartellamtFetcher()
    base = Path(__file__).parent
    count = 0

    if sample:
        sample_dir = base / "sample"
        sample_dir.mkdir(exist_ok=True)
        for doc in fetcher.fetch_all(limit=12):
            count += 1
            path = sample_dir / f"{doc['_id']}.json"
            path.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
            logger.info(f"Sample {count}: {doc['case_number']} | "
                        f"text={doc['text_length']:,} chars")
    else:
        data_dir = base / "data"
        data_dir.mkdir(exist_ok=True)
        out_path = data_dir / "records.jsonl"
        with open(out_path, "w", encoding="utf-8") as f:
            for doc in fetcher.fetch_all(limit=0):
                f.write(json.dumps(doc, ensure_ascii=False) + "\n")
                count += 1
        logger.info(f"Wrote {count} records to {out_path}")

    logger.info(f"Done. Total records: {count}")
    return count


def main():
    parser = argparse.ArgumentParser(description="Bundeskartellamt Decision Fetcher")
    parser.add_argument("command", nargs="?", default="bootstrap",
                        choices=["bootstrap", "bootstrap-fast", "test"],
                        help="Command to run")
    parser.add_argument("--sample", action="store_true", help="Fetch sample only (12 docs)")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    parser.add_argument("--limit", type=int, default=0, help="Maximum documents to fetch")
    args, _unknown = parser.parse_known_args()

    if args.command == "test":
        fetcher = BundeskartellamtFetcher()
        n = 0
        for doc in fetcher.fetch_all(limit=3):
            n += 1
            print(f"\n--- Document {n} ---")
            print(f"ID: {doc['_id']}")
            print(f"Case: {doc['case_number']}")
            print(f"Title: {doc['title'][:90]}")
            print(f"Date: {doc['date']}  Type: {doc['decision_type']}")
            print(f"Text length: {doc['text_length']:,} chars")
            print(f"Preview: {doc['text'][:250]}")
        return

    n = bootstrap(sample=args.sample)
    if args.sample and n < 10:
        logger.error(f"Only {n} samples (need 10+). Check the Bundeskartellamt DB.")
        sys.exit(1)


if __name__ == "__main__":
    main()
