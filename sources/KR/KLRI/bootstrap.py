#!/usr/bin/env python3
"""
KR/KLRI -- Korea Legislation Research Institute English Translations

Fetches English translations of Korean statutes from the law.go.kr DRF API.
~3,000 laws with full English article text.

API endpoints:
  - GET /DRF/lawSearch.do?OC=test&target=elaw&type=XML  (paginated listing)
  - GET /DRF/lawService.do?OC=test&target=elaw&type=XML&MST=...  (full text)

Usage:
  python bootstrap.py bootstrap            # Full initial pull (~3,000 laws)
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py update               # Same as bootstrap
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
logger = logging.getLogger("legal-data-hunter.KR.KLRI")

BASE_URL = "https://www.law.go.kr/DRF"
OC = "test"
PAGE_SIZE = 20


class KLRIScraper(BaseScraper):
    """
    Scraper for KR/KLRI -- English translations of Korean laws via law.go.kr DRF API.
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
        """Fetch a page of English law listings. Returns (total_count, list of law dicts)."""
        url = (
            f"{BASE_URL}/lawSearch.do?OC={OC}&target=elaw&type=XML"
            f"&display={display}&page={page}&mobileYn=Y"
        )
        self.rate_limiter.wait()
        resp = self.client.get(url)
        resp.raise_for_status()

        root = ET.fromstring(resp.content)
        total = int(root.findtext("totalCnt", "0"))

        laws = []
        for el in root.findall("law"):
            laws.append({
                "법령일련번호": el.findtext("법령일련번호", ""),
                "법령ID": el.findtext("법령ID", ""),
                "법령명한글": el.findtext("법령명한글", ""),
                "법령명영문": el.findtext("법령명영문", ""),
                "공포일자": el.findtext("공포일자", ""),
                "공포번호": el.findtext("공포번호", ""),
                "현행연혁코드": el.findtext("현행연혁코드", ""),
                "법령구분명": el.findtext("법령구분명", ""),
                "시행일자": el.findtext("시행일자", ""),
            })

        return total, laws

    def _fetch_law_detail(self, mst: str) -> Optional[bytes]:
        """Fetch full English text XML for a law by its 법령일련번호 (MST)."""
        url = (
            f"{BASE_URL}/lawService.do?OC={OC}&target=elaw&type=XML"
            f"&MST={mst}&mobileYn=Y"
        )
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            logger.warning(f"  Failed to fetch detail for MST={mst}: {e}")
            return None

    def _extract_text_from_xml(self, xml_bytes: bytes) -> tuple:
        """Extract English full text and metadata from law detail XML.
        Returns (text, metadata_dict)."""
        try:
            root = ET.fromstring(xml_bytes)
        except ET.ParseError as e:
            logger.warning(f"  XML parse error: {e}")
            return "", {}

        # Extract metadata from InfSection
        inf = root.find("InfSection")
        metadata = {}
        if inf is not None:
            metadata = {
                "법령ID": inf.findtext("lsId", ""),
                "공포일자": inf.findtext("ancYd", ""),
                "공포번호": inf.findtext("ancNo", ""),
                "법령명영문": inf.findtext("lsNmEng", ""),
            }

        # Extract full text from JoSection (articles)
        text_parts = []
        jo_section = root.find("JoSection")
        if jo_section is not None:
            for jo in jo_section.findall("Jo"):
                jo_cts = jo.findtext("joCts", "")
                if jo_cts:
                    text_parts.append(jo_cts.strip())

                # Extract hang (paragraphs) - may be nested within joCts already
                for hang in jo.findall(".//hang"):
                    hang_cts = hang.findtext("hangCts", "")
                    if hang_cts:
                        text_parts.append(hang_cts.strip())

                    for ho in hang.findall("ho"):
                        ho_cts = ho.findtext("hoCts", "")
                        if ho_cts:
                            text_parts.append(ho_cts.strip())

        # Extract addenda from ArSection
        ar_section = root.find("ArSection")
        if ar_section is not None:
            for ar in ar_section.findall("Ar"):
                ar_cts = ar.findtext("arCts", "")
                if ar_cts:
                    text_parts.append(ar_cts.strip())

        full_text = "\n\n".join(text_parts)
        full_text = re.sub(r'\n{3,}', '\n\n', full_text)

        return full_text.strip(), metadata

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all English-translated laws with full text."""
        page = 1
        total = None
        fetched_count = 0

        while True:
            total_cnt, laws = self._search_laws(page=page, display=PAGE_SIZE)
            if total is None:
                total = total_cnt
                logger.info(f"Total English laws in API: {total}")

            if not laws:
                break

            for law in laws:
                mst = law.get("법령일련번호", "")
                if not mst:
                    continue

                # Only fetch current (현행) laws
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

            if page * PAGE_SIZE >= total:
                break
            page += 1
            if page % 10 == 0:
                logger.info(f"  Page {page}, {fetched_count} laws fetched so far")

        logger.info(f"Done: {fetched_count} English laws fetched")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """No incremental update available — re-fetches all."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw English law data into standard schema."""
        search = raw.get("search_meta", {})
        detail = raw.get("detail_meta", {})
        mst = raw.get("mst", "")

        title_en = detail.get("법령명영문", "") or search.get("법령명영문", "")
        title_kr = search.get("법령명한글", "")
        title = title_en or title_kr
        law_id = detail.get("법령ID", "") or search.get("법령ID", "")

        raw_date = detail.get("공포일자", "") or search.get("공포일자", "")
        date = None
        if raw_date and len(raw_date) == 8:
            date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"

        return {
            "_id": f"KR-ELAW-{law_id}",
            "_source": "KR/KLRI",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("full_text", ""),
            "date": date,
            "url": f"https://law.go.kr/LSW/lsInfoP.do?lsiSeq={mst}",
            "law_id": law_id,
            "language": "en",
        }


# ── CLI entrypoint ────────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = KLRIScraper()

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
        print("Testing law.go.kr English law DRF API...")
        try:
            total, laws = scraper._search_laws(page=1, display=2)
            print(f"  Search OK: {total} total English laws, got {len(laws)} in page 1")
            if laws:
                # Find a current law
                for law in laws:
                    if law.get("현행연혁코드") == "현행":
                        mst = law["법령일련번호"]
                        break
                else:
                    mst = laws[0]["법령일련번호"]
                xml_bytes = scraper._fetch_law_detail(mst)
                if xml_bytes:
                    text, meta = scraper._extract_text_from_xml(xml_bytes)
                    print(f"  Detail OK: '{meta.get('법령명영문', '')}' — {len(text)} chars of text")
                else:
                    print("  Detail FAILED")
        except Exception as e:
            print(f"  FAILED: {e}")
            sys.exit(1)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
