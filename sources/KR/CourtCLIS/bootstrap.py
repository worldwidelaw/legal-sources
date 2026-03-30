#!/usr/bin/env python3
"""
KR/CourtCLIS -- Korean Court Decisions via law.go.kr DRF API

Fetches Korean court precedents (판례) from the official law.go.kr DRF API.
~171K decisions from Supreme Court, lower courts, and specialized courts.
Full text including holdings, summaries, and complete opinions.

API endpoints:
  - GET /DRF/lawSearch.do?OC=test&target=prec&type=XML  (paginated listing)
  - GET /DRF/lawService.do?OC=test&target=prec&type=XML&ID=...  (full text)

Usage:
  python bootstrap.py bootstrap            # Full initial pull (~171K decisions)
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py update               # Same as bootstrap (no date filter)
  python bootstrap.py test-api             # Quick connectivity test
"""

import sys
import json
import logging
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KR.CourtCLIS")

BASE_URL = "https://www.law.go.kr/DRF"
OC = "test"
PAGE_SIZE = 100  # Max allowed by API


class CourtCLISScraper(BaseScraper):
    """
    Scraper for KR/CourtCLIS -- Korean court decisions via law.go.kr DRF API.
    Country: KR
    URL: https://www.law.go.kr/
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url="",
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
                "Accept": "application/xml",
            },
            timeout=120,
        )

    def _search_precedents(self, page: int = 1, display: int = PAGE_SIZE, sort: str = "date") -> tuple:
        """Fetch a page of precedent listings. Returns (total_count, list of prec dicts).
        sort='date' prioritizes entries from courts (with full text) over tax system entries."""
        url = (
            f"{BASE_URL}/lawSearch.do?OC={OC}&target=prec&type=XML"
            f"&display={display}&page={page}&sort={sort}&mobileYn=Y"
        )
        self.rate_limiter.wait()
        resp = self.client.get(url)
        resp.raise_for_status()

        root = ET.fromstring(resp.content)
        total = int(root.findtext("totalCnt", "0"))

        precs = []
        for el in root.findall("prec"):
            precs.append({
                "판례일련번호": el.findtext("판례일련번호", ""),
                "사건명": el.findtext("사건명", ""),
                "사건번호": el.findtext("사건번호", ""),
                "선고일자": el.findtext("선고일자", ""),
                "법원명": el.findtext("법원명", ""),
                "법원종류코드": el.findtext("법원종류코드", ""),
                "사건종류명": el.findtext("사건종류명", ""),
                "사건종류코드": el.findtext("사건종류코드", ""),
                "판결유형": el.findtext("판결유형", ""),
                "선고": el.findtext("선고", ""),
                "데이터출처명": el.findtext("데이터출처명", ""),
            })

        return total, precs

    def _fetch_precedent_detail(self, prec_id: str) -> Optional[bytes]:
        """Fetch full text XML for a precedent by its 판례일련번호."""
        url = (
            f"{BASE_URL}/lawService.do?OC={OC}&target=prec&type=XML"
            f"&ID={prec_id}&mobileYn=Y"
        )
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            logger.warning(f"  Failed to fetch detail for ID={prec_id}: {e}")
            return None

    @staticmethod
    def _clean_html(text: str) -> str:
        """Strip HTML tags and decode entities from text."""
        if not text:
            return ""
        text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'&lt;', '<', text)
        text = re.sub(r'&gt;', '>', text)
        text = re.sub(r'&amp;', '&', text)
        text = re.sub(r'&nbsp;', ' ', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _extract_from_xml(self, xml_bytes: bytes) -> tuple:
        """Extract full text and metadata from precedent detail XML.
        Returns (text, metadata_dict)."""
        try:
            root = ET.fromstring(xml_bytes)
        except ET.ParseError as e:
            logger.warning(f"  XML parse error: {e}")
            return "", {}

        metadata = {
            "판례정보일련번호": root.findtext("판례정보일련번호", ""),
            "사건명": root.findtext("사건명", ""),
            "사건번호": root.findtext("사건번호", ""),
            "선고일자": root.findtext("선고일자", ""),
            "법원명": root.findtext("법원명", ""),
            "사건종류명": root.findtext("사건종류명", ""),
            "판결유형": root.findtext("판결유형", ""),
            "판시사항": root.findtext("판시사항", ""),
            "판결요지": root.findtext("판결요지", ""),
            "참조조문": root.findtext("참조조문", ""),
            "참조판례": root.findtext("참조판례", ""),
        }

        # Full text is in 판례내용
        full_text = self._clean_html(root.findtext("판례내용", ""))

        # Also clean HTML from metadata text fields
        for key in ("판시사항", "판결요지", "참조조문", "참조판례"):
            if metadata.get(key):
                metadata[key] = self._clean_html(metadata[key])

        return full_text, metadata

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all precedents with full text."""
        page = 1
        total = None
        fetched_count = 0
        skipped = 0

        while True:
            total_cnt, precs = self._search_precedents(page=page, display=PAGE_SIZE)
            if total is None:
                total = total_cnt
                logger.info(f"Total precedents in API: {total}")

            if not precs:
                break

            for prec in precs:
                prec_id = prec.get("판례일련번호", "")
                if not prec_id:
                    continue

                xml_bytes = self._fetch_precedent_detail(prec_id)
                if xml_bytes is None:
                    skipped += 1
                    continue

                full_text, detail_meta = self._extract_from_xml(xml_bytes)
                if not full_text:
                    logger.warning(f"  ID={prec_id}: no text extracted, skipping")
                    skipped += 1
                    continue

                yield {
                    "prec_id": prec_id,
                    "search_meta": prec,
                    "detail_meta": detail_meta,
                    "full_text": full_text,
                }
                fetched_count += 1

            if page * PAGE_SIZE >= total:
                break
            page += 1
            if page % 10 == 0:
                logger.info(f"  Page {page}, {fetched_count} precedents fetched, {skipped} skipped")

        logger.info(f"Done: {fetched_count} precedents fetched, {skipped} skipped")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """No incremental update available — re-fetches all."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw precedent data into standard schema."""
        search = raw.get("search_meta", {})
        detail = raw.get("detail_meta", {})
        prec_id = raw.get("prec_id", "")

        title = detail.get("사건명", "") or search.get("사건명", "")
        case_number = detail.get("사건번호", "") or search.get("사건번호", "")
        court = detail.get("법원명", "") or search.get("법원명", "")

        # Format date as ISO 8601
        raw_date = detail.get("선고일자", "") or search.get("선고일자", "")
        date = None
        if raw_date and len(raw_date) == 8:
            date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"

        # Build comprehensive text: holdings + summary + full opinion
        text_parts = []
        holdings = detail.get("판시사항", "")
        if holdings:
            text_parts.append(f"[판시사항]\n{holdings}")
        summary = detail.get("판결요지", "")
        if summary:
            text_parts.append(f"[판결요지]\n{summary}")
        full_text = raw.get("full_text", "")
        if full_text:
            text_parts.append(f"[판례내용]\n{full_text}")

        combined_text = "\n\n".join(text_parts)

        return {
            "_id": f"KR-PREC-{prec_id}",
            "_source": "KR/CourtCLIS",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": combined_text,
            "date": date,
            "url": f"https://www.law.go.kr/판례/{case_number}",
            "case_number": case_number,
            "court": court,
            "case_type": detail.get("사건종류명", "") or search.get("사건종류명", ""),
            "judgment_type": detail.get("판결유형", "") or search.get("판결유형", ""),
        }


# ── CLI entrypoint ────────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = CourtCLISScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "bootstrap":
        result = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        print(json.dumps(result, indent=2, default=str))

    elif command == "update":
        result = scraper.bootstrap(sample_mode=False)
        print(json.dumps(result, indent=2, default=str))

    elif command == "test-api":
        print("Testing law.go.kr precedent DRF API...")
        try:
            total, precs = scraper._search_precedents(page=1, display=3)
            print(f"  Search OK: {total} total precedents, got {len(precs)} in page 1")
            if precs:
                pid = precs[0]["판례일련번호"]
                xml_bytes = scraper._fetch_precedent_detail(pid)
                if xml_bytes:
                    text, meta = scraper._extract_from_xml(xml_bytes)
                    print(f"  Detail OK: '{meta.get('사건명', '')}' — {len(text)} chars of text")
                else:
                    print("  Detail FAILED")
        except Exception as e:
            print(f"  FAILED: {e}")
            sys.exit(1)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
