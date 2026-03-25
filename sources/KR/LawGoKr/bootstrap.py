#!/usr/bin/env python3
"""
KR/LawGoKr -- Korean Law Information Center (KLIC) DRF API Data Fetcher

Fetches all Korean statutes from the official law.go.kr DRF API.
Covers acts, presidential decrees, ministerial ordinances, treaties, etc.

API endpoints:
  - GET /DRF/lawSearch.do?OC=test&target=law&type=XML&mobileYn=Y  (paginated listing)
  - GET /DRF/lawService.do?OC=test&target=law&type=XML&MST=...&mobileYn=Y  (full text)

Usage:
  python bootstrap.py bootstrap            # Full initial pull (~5,500 laws)
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py update               # Same as bootstrap (no date filter)
  python bootstrap.py test-api             # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KR.LawGoKr")

BASE_URL = "https://www.law.go.kr/DRF"
OC = "test"
PAGE_SIZE = 20  # API default page size


class LawGoKrScraper(BaseScraper):
    """
    Scraper for KR/LawGoKr -- Korean Law Information Center DRF API.
    Country: KR
    URL: https://open.law.go.kr/
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

    def _search_laws(self, page: int = 1, display: int = PAGE_SIZE) -> tuple:
        """Fetch a page of law listings. Returns (total_count, list of law dicts)."""
        url = (
            f"{BASE_URL}/lawSearch.do?OC={OC}&target=law&type=XML"
            f"&display={display}&page={page}&mobileYn=Y"
        )
        self.rate_limiter.wait()
        resp = self.client.get(url)
        resp.raise_for_status()

        root = ET.fromstring(resp.content)
        total = int(root.findtext("totalCnt", "0"))

        laws = []
        for law_el in root.findall("law"):
            laws.append({
                "법령일련번호": law_el.findtext("법령일련번호", ""),
                "법령ID": law_el.findtext("법령ID", ""),
                "법령명한글": law_el.findtext("법령명한글", ""),
                "법령약칭명": law_el.findtext("법령약칭명", ""),
                "공포일자": law_el.findtext("공포일자", ""),
                "공포번호": law_el.findtext("공포번호", ""),
                "시행일자": law_el.findtext("시행일자", ""),
                "법령구분명": law_el.findtext("법령구분명", ""),
                "소관부처명": law_el.findtext("소관부처명", ""),
                "현행연혁코드": law_el.findtext("현행연혁코드", ""),
            })

        return total, laws

    def _fetch_law_detail(self, mst: str) -> Optional[str]:
        """Fetch full text XML for a law by its 법령일련번호 (MST)."""
        url = (
            f"{BASE_URL}/lawService.do?OC={OC}&target=law&type=XML"
            f"&MST={mst}&mobileYn=Y"
        )
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            logger.warning(f"  Failed to fetch law detail for MST={mst}: {e}")
            return None

    def _extract_text_from_xml(self, xml_bytes: bytes) -> tuple:
        """Extract full text and metadata from the law detail XML.
        Returns (text, metadata_dict)."""
        try:
            root = ET.fromstring(xml_bytes)
        except ET.ParseError as e:
            logger.warning(f"  XML parse error: {e}")
            return "", {}

        # Extract metadata from 기본정보
        info = root.find("기본정보")
        metadata = {}
        if info is not None:
            metadata = {
                "법령ID": info.findtext("법령ID", ""),
                "법령명_한글": info.findtext("법령명_한글", ""),
                "법령명약칭": info.findtext("법령명약칭", ""),
                "공포일자": info.findtext("공포일자", ""),
                "공포번호": info.findtext("공포번호", ""),
                "시행일자": info.findtext("시행일자", ""),
                "제개정구분": info.findtext("제개정구분", ""),
                "소관부처": info.findtext("소관부처", ""),
            }
            # Get law type from 법종구분
            law_type_el = info.find("법종구분")
            if law_type_el is not None:
                metadata["법종구분"] = law_type_el.text or ""
            else:
                metadata["법종구분"] = ""

        # Extract full text from 조문 (articles)
        text_parts = []
        articles = root.find("조문")
        if articles is not None:
            for article in articles.findall("조문단위"):
                article_num = article.findtext("조문번호", "")
                article_title = article.findtext("조문제목", "")
                article_content = article.findtext("조문내용", "")

                if article_content:
                    text_parts.append(article_content.strip())

                # Extract 항 (paragraphs)
                for hang in article.findall(".//항"):
                    hang_content = hang.findtext("항내용", "")
                    if hang_content:
                        text_parts.append(hang_content.strip())

                    # Extract 호 (clauses)
                    for ho in hang.findall("호"):
                        ho_content = ho.findtext("호내용", "")
                        if ho_content:
                            text_parts.append(ho_content.strip())

                        # Extract 목 (sub-clauses)
                        for mok in ho.findall("목"):
                            mok_content = mok.findtext("목내용", "")
                            if mok_content:
                                text_parts.append(mok_content.strip())

        # Also extract 부칙 (supplementary provisions)
        for buchiik in root.findall("부칙"):
            for article in buchiik.findall("조문단위"):
                content = article.findtext("조문내용", "")
                if content:
                    text_parts.append(content.strip())
                for hang in article.findall(".//항"):
                    hang_content = hang.findtext("항내용", "")
                    if hang_content:
                        text_parts.append(hang_content.strip())

        full_text = "\n".join(text_parts)
        # Clean up excessive whitespace
        full_text = re.sub(r'\n{3,}', '\n\n', full_text)
        full_text = re.sub(r'[ \t]+', ' ', full_text)

        return full_text.strip(), metadata

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all laws with full text."""
        page = 1
        total = None
        fetched_count = 0

        while True:
            total_cnt, laws = self._search_laws(page=page, display=PAGE_SIZE)
            if total is None:
                total = total_cnt
                logger.info(f"Total laws in API: {total}")

            if not laws:
                break

            for law in laws:
                mst = law.get("법령일련번호", "")
                if not mst:
                    continue

                # Skip non-current laws
                status = law.get("현행연혁코드", "")
                if status and status != "현행":
                    continue

                xml_bytes = self._fetch_law_detail(mst)
                if xml_bytes is None:
                    continue

                full_text, detail_meta = self._extract_text_from_xml(xml_bytes)
                if not full_text:
                    logger.warning(f"  MST={mst}: no text extracted, skipping")
                    continue

                yield {
                    "mst": mst,
                    "search_meta": law,
                    "detail_meta": detail_meta,
                    "full_text": full_text,
                }
                fetched_count += 1

            # Check if we've fetched all pages
            if page * PAGE_SIZE >= total:
                break
            page += 1
            logger.info(f"  Page {page}, {fetched_count} laws fetched so far")

        logger.info(f"Done: {fetched_count} laws fetched")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """No incremental update available — re-fetches all."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw law.go.kr data into standard schema."""
        search = raw.get("search_meta", {})
        detail = raw.get("detail_meta", {})
        mst = raw.get("mst", "")

        title = detail.get("법령명_한글", "") or search.get("법령명한글", "")
        law_id = detail.get("법령ID", "") or search.get("법령ID", "")

        # Format date as ISO 8601
        raw_date = detail.get("공포일자", "") or search.get("공포일자", "")
        date = None
        if raw_date and len(raw_date) == 8:
            date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"

        law_type = detail.get("법종구분", "") or search.get("법령구분명", "")

        return {
            "_id": f"KR-LAW-{law_id}",
            "_source": "KR/LawGoKr",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("full_text", ""),
            "date": date,
            "url": f"https://law.go.kr/LSW/lsInfoP.do?lsiSeq={mst}",
            "law_id": law_id,
            "law_type": law_type,
            "promulgation_number": detail.get("공포번호", "") or search.get("공포번호", ""),
        }


# ── CLI entrypoint ────────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = LawGoKrScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "bootstrap":
        result = scraper.bootstrap(sample_mode=sample_mode, sample_size=12)
        print(json.dumps(result, indent=2, default=str))

    elif command == "update":
        result = scraper.bootstrap(sample_mode=False)
        print(json.dumps(result, indent=2, default=str))

    elif command == "test-api":
        print("Testing law.go.kr DRF API connectivity...")
        try:
            total, laws = scraper._search_laws(page=1, display=2)
            print(f"  Search OK: {total} total laws, got {len(laws)} in page 1")
            if laws:
                mst = laws[0]["법령일련번호"]
                xml_bytes = scraper._fetch_law_detail(mst)
                if xml_bytes:
                    text, meta = scraper._extract_text_from_xml(xml_bytes)
                    print(f"  Detail OK: '{meta.get('법령명_한글', '')}' — {len(text)} chars of text")
                else:
                    print("  Detail FAILED")
        except Exception as e:
            print(f"  FAILED: {e}")
            sys.exit(1)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
