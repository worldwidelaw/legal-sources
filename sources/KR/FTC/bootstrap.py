#!/usr/bin/env python3
"""
KR/FTC -- Korea Fair Trade Commission (KFTC) Decisions via law.go.kr DRF API

Fetches all KFTC decisions from the official law.go.kr DRF API (target=ftc).
8,100+ decisions covering resolutions, corrective orders, consent decrees (2008+).

API endpoints:
  - GET /DRF/lawSearch.do?OC=test&target=ftc&type=XML&sort=ddes&mobileYn=Y  (paginated listing)
  - GET /DRF/lawService.do?OC=test&target=ftc&ID=...&type=XML&mobileYn=Y    (full text)

Usage:
  python bootstrap.py bootstrap            # Full initial pull (~8,100 decisions)
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
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

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.KR.FTC")

BASE_URL = "https://www.law.go.kr/DRF"
OC = "test"
PAGE_SIZE = 100


class FTCScraper(BaseScraper):
    """
    Scraper for KR/FTC -- Korea Fair Trade Commission decisions via law.go.kr DRF API.
    Country: KR
    URL: https://www.ftc.go.kr/
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

    def _search_decisions(self, page: int = 1, display: int = PAGE_SIZE) -> tuple:
        """Fetch a page of FTC decision listings. Returns (total_count, list of dicts)."""
        url = (
            f"{BASE_URL}/lawSearch.do?OC={OC}&target=ftc&type=XML"
            f"&display={display}&page={page}&sort=ddes&mobileYn=Y"
        )
        self.rate_limiter.wait()
        resp = self.client.get(url)
        resp.raise_for_status()

        root = ET.fromstring(resp.content)
        total = int(root.findtext("totalCnt", "0"))

        decisions = []
        for ftc_el in root.findall("ftc"):
            decisions.append({
                "결정문일련번호": ftc_el.findtext("결정문일련번호", "").strip(),
                "사건명": ftc_el.findtext("사건명", "").strip(),
                "사건번호": ftc_el.findtext("사건번호", "").strip(),
                "문서유형": ftc_el.findtext("문서유형", "").strip(),
                "회의종류": ftc_el.findtext("회의종류", "").strip(),
                "결정번호": ftc_el.findtext("결정번호", "").strip(),
                "결정일자": ftc_el.findtext("결정일자", "").strip(),
            })

        return total, decisions

    def _fetch_detail(self, serial_no: str) -> Optional[bytes]:
        """Fetch full text XML for a decision by its 결정문일련번호."""
        url = (
            f"{BASE_URL}/lawService.do?OC={OC}&target=ftc&type=XML"
            f"&ID={serial_no}&mobileYn=Y"
        )
        self.rate_limiter.wait()
        try:
            resp = self.client.get(url)
            resp.raise_for_status()
            return resp.content
        except Exception as e:
            logger.warning(f"  Failed to fetch detail for ID={serial_no}: {e}")
            return None

    def _extract_detail(self, xml_bytes: bytes) -> dict:
        """Extract full text and metadata from FtcService XML."""
        try:
            root = ET.fromstring(xml_bytes)
        except ET.ParseError as e:
            logger.warning(f"  XML parse error: {e}")
            return {}

        def get_text(tag):
            val = root.findtext(tag, "")
            return val.strip() if val and val.strip() != "null" else ""

        # Build full text from all content sections
        text_sections = []
        section_tags = [
            ("결정요지", "결정요지"),
            ("주문", "주문"),
            ("신청취지", "신청취지"),
            ("이유", "이유"),
            ("의결문", "의결문"),
            ("시정명령사항", "시정명령사항"),
            ("시정권고사항", "시정권고사항"),
            ("시정권고이유", "시정권고이유"),
            ("법위반내용", "법위반내용"),
        ]

        for label, tag in section_tags:
            content = get_text(tag)
            if content:
                text_sections.append(f"[{label}]\n{content}")

        # Also check 피심정보 (respondent info)
        respondent_el = root.find("피심정보")
        respondent_info = ""
        if respondent_el is not None:
            name = respondent_el.findtext("피심정보명", "").strip()
            content = respondent_el.findtext("피심정보내용", "").strip()
            if name and content:
                respondent_info = f"{name}\n{content}"

        full_text = "\n\n".join(text_sections)
        # Clean up img tags and excessive whitespace
        full_text = re.sub(r'<img[^>]*>', '', full_text)
        full_text = re.sub(r'<[^>]+>', '', full_text)
        full_text = re.sub(r'\n{3,}', '\n\n', full_text)
        full_text = full_text.strip()

        return {
            "사건명": get_text("사건명"),
            "사건번호": get_text("사건번호"),
            "문서유형": get_text("문서유형"),
            "결정일자": get_text("결정일자"),
            "결정번호": get_text("결정번호"),
            "회의종류": get_text("회의종류"),
            "피심정보": respondent_info,
            "full_text": full_text,
        }

    def _parse_date(self, raw_date: str) -> Optional[str]:
        """Parse Korean date formats to ISO 8601."""
        if not raw_date:
            return None
        # Remove trailing dots and whitespace
        raw_date = raw_date.strip().rstrip(".")
        # Try YYYY.M.D format
        m = re.match(r'(\d{4})\.(\d{1,2})\.(\d{1,2})', raw_date)
        if m:
            return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
        # Try YYYYMMDD format
        m = re.match(r'(\d{4})(\d{2})(\d{2})', raw_date)
        if m:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all FTC decisions with full text."""
        page = 1
        total = None
        fetched_count = 0
        skipped_no_text = 0

        while True:
            total_cnt, decisions = self._search_decisions(page=page, display=PAGE_SIZE)
            if total is None:
                total = total_cnt
                logger.info(f"Total FTC decisions in API: {total}")

            if not decisions:
                break

            for dec in decisions:
                serial = dec.get("결정문일련번호", "")
                if not serial:
                    continue

                xml_bytes = self._fetch_detail(serial)
                if xml_bytes is None:
                    continue

                detail = self._extract_detail(xml_bytes)
                if not detail.get("full_text"):
                    skipped_no_text += 1
                    logger.debug(f"  ID={serial}: no text extracted, skipping")
                    continue

                # Merge search metadata with detail
                yield {
                    "serial_no": serial,
                    "search_meta": dec,
                    "detail": detail,
                }
                fetched_count += 1

            if page * PAGE_SIZE >= total:
                break
            page += 1
            logger.info(f"  Page {page}, {fetched_count} decisions fetched so far")

        logger.info(f"Done: {fetched_count} decisions fetched, {skipped_no_text} skipped (no text)")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """No incremental update available — re-fetches all."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw FTC data into standard schema."""
        search = raw.get("search_meta", {})
        detail = raw.get("detail", {})
        serial = raw.get("serial_no", "")

        title = detail.get("사건명") or search.get("사건명", "")
        case_number = detail.get("사건번호") or search.get("사건번호", "")
        decision_number = detail.get("결정번호") or search.get("결정번호", "")
        doc_type = detail.get("문서유형") or search.get("문서유형", "")
        raw_date = detail.get("결정일자") or search.get("결정일자", "")
        date = self._parse_date(raw_date)

        # Determine _type: 의결서/심결서 are case_law, others are doctrine
        _type = "case_law"
        if doc_type in ("시정권고서", "동의의결서"):
            _type = "doctrine"

        return {
            "_id": f"KR-FTC-{serial}",
            "_source": "KR/FTC",
            "_type": _type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": detail.get("full_text", ""),
            "date": date,
            "url": f"https://www.law.go.kr/LSW/ftcInfoP.do?ftcSeq={serial}",
            "case_number": case_number,
            "decision_number": decision_number,
            "document_type": doc_type,
            "respondent": detail.get("피심정보", ""),
        }


# ── CLI entrypoint ────────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = FTCScraper()

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
        print("Testing law.go.kr FTC DRF API connectivity...")
        try:
            total, decisions = scraper._search_decisions(page=1, display=3)
            print(f"  Search OK: {total} total decisions, got {len(decisions)} in page 1")
            if decisions:
                serial = decisions[0]["결정문일련번호"]
                xml_bytes = scraper._fetch_detail(serial)
                if xml_bytes:
                    detail = scraper._extract_detail(xml_bytes)
                    text = detail.get("full_text", "")
                    print(f"  Detail OK: '{detail.get('사건명', '')}' — {len(text)} chars of text")
                else:
                    print("  Detail FAILED")
        except Exception as e:
            print(f"  FAILED: {e}")
            sys.exit(1)

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
