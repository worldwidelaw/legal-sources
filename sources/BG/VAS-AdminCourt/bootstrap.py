"""
Legal Data Hunter - Bulgarian Supreme Administrative Court (VAS) Scraper

Fetches case law from the Bulgarian Supreme Administrative Court (Върховен административен съд).
Data source: EPEP (Unified Portal for Electronic Justice) JSON API at ecase.justice.bg
Method: JSON API with POST search + file download for full text
Coverage: 2010 onwards (~63,500 cases with full text decisions)
"""

import re
import sys
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

from bs4 import BeautifulSoup

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("BG/VAS-AdminCourt")


class BulgarianVASAdminCourtScraper(BaseScraper):
    """
    Scraper for: Bulgarian Supreme Administrative Court (Върховен административен съд)
    Country: BG
    URL: https://ecase.justice.bg

    Data types: case_law
    Auth: none
    """

    VAS_COURT_ID = "113"
    BASE_URL = "https://ecase.justice.bg"
    PAGE_SIZE = 50
    # Years to cover (2010 to current)
    START_YEAR = 2010

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=self.BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
                "Content-Type": "application/json",
            },
        )

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all VAS decisions by iterating through years."""
        current_year = datetime.now().year
        for year in range(self.START_YEAR, current_year + 1):
            logger.info(f"Fetching cases for year {year}")
            yield from self._fetch_year(year)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield decisions from years >= since."""
        start_year = since.year
        current_year = datetime.now().year
        for year in range(start_year, current_year + 1):
            logger.info(f"Fetching updates for year {year}")
            for doc in self._fetch_year(year):
                date_str = doc.get("date", "")
                if date_str:
                    try:
                        doc_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                        if doc_date.replace(tzinfo=None) >= since.replace(tzinfo=None):
                            yield doc
                    except Exception:
                        yield doc
                else:
                    yield doc

    def _fetch_year(self, year: int) -> Generator[dict, None, None]:
        """Fetch all cases for a given year, yielding raw dicts with full text."""
        page = 1
        total_yielded = 0

        while True:
            cases = self._search_cases(year, page)
            if not cases:
                break

            for case in cases:
                raw = self._build_raw(case)
                if raw and raw.get("full_text"):
                    total_yielded += 1
                    yield raw

            page += 1
            time.sleep(0.5)

        logger.info(f"Year {year}: yielded {total_yielded} decisions with full text")

    def _search_cases(self, year: int, page: int) -> list:
        """Search for VAS cases in a given year/page."""
        try:
            self.rate_limiter.wait()
            payload = {
                "data": json.dumps({"CourtId": self.VAS_COURT_ID, "RegYear": str(year)}),
                "page": page,
                "size": self.PAGE_SIZE,
            }
            resp = self.client.post("/Case/LoadData", json_data=payload)
            data = resp.json()
            return data.get("data", [])
        except Exception as e:
            logger.error(f"Failed to search cases year={year} page={page}: {e}")
            return []

    def _build_raw(self, case: dict) -> Optional[dict]:
        """Build a raw dict with case metadata, act info, and full text."""
        case_gid = case.get("gid", "")
        if not case_gid:
            return None

        # Get acts for this case
        acts = self._get_acts(case_gid)
        if not acts:
            return None

        # Use the first (most recent) act
        act = acts[0]
        act_gid = act.get("gid", "")

        # Download full text
        full_text = self._get_full_text(act_gid) if act_gid else ""

        if not full_text:
            return None

        # Combine into single raw dict for normalize()
        raw = dict(case)
        raw["act"] = act
        raw["full_text"] = full_text
        return raw

    def _get_acts(self, case_gid: str) -> list:
        """Get acts (decisions) for a case."""
        try:
            self.rate_limiter.wait()
            payload = {
                "data": json.dumps({"gid": case_gid}),
                "page": 1,
                "size": 10,
            }
            resp = self.client.post("/Case/ActsLoadData", json_data=payload)
            data = resp.json()
            return data.get("data", [])
        except Exception as e:
            logger.debug(f"Failed to get acts for case {case_gid}: {e}")
            return []

    def _get_full_text(self, act_gid: str) -> str:
        """Download full text for an act via preview page → file download."""
        try:
            self.rate_limiter.wait()
            # Step 1: Get preview page to find download link
            preview_url = f"/case/preview?type=7&gid={act_gid}"
            resp = self.client.get(preview_url)
            html = resp.text

            soup = BeautifulSoup(html, "html.parser")

            # Find the download link
            download_link = None
            for a_tag in soup.find_all("a", href=True):
                href = a_tag["href"]
                if "/api/file/download/" in href:
                    download_link = href
                    break

            if not download_link:
                # Try extracting text directly from preview
                text = soup.get_text(separator=" ", strip=True)
                if len(text) > 100:
                    return self._clean_text(text)
                return ""

            # Step 2: Download the file
            self.rate_limiter.wait()
            file_resp = self.client.get(download_link)
            content = file_resp.content

            # Decode - files are typically UTF-16 encoded HTML
            decoded = None
            for enc in ["utf-16", "utf-8", "utf-16-le", "windows-1251"]:
                try:
                    decoded = content.decode(enc)
                    if len(decoded) > 50:
                        break
                except Exception:
                    continue

            if not decoded:
                return ""

            # Parse and extract text
            file_soup = BeautifulSoup(decoded, "html.parser")
            text = file_soup.get_text(separator=" ", strip=True)
            return self._clean_text(text)

        except Exception as e:
            logger.debug(f"Failed to get full text for act {act_gid}: {e}")
            return ""

    def _clean_text(self, text: str) -> str:
        """Clean extracted text."""
        # Collapse multiple whitespace
        text = re.sub(r"\s+", " ", text).strip()
        # Remove any zero-width characters
        text = re.sub(r"[\u200b\u200c\u200d\ufeff]", "", text)
        return text

    def normalize(self, raw: dict) -> dict:
        """Transform raw combined dict into standard schema."""
        act = raw.get("act", {})
        full_text = raw.get("full_text", "")
        case_gid = raw.get("gid", "")
        act_gid = act.get("gid", "")
        reg_number = raw.get("regNumber", "")
        reg_year = raw.get("regYear", "")
        case_kind = raw.get("caseKindName", "")
        act_kind = act.get("actKindName", "")
        act_number = act.get("number", "")

        # Build title
        title_parts = []
        if act_kind:
            title_parts.append(act_kind)
        if act_number:
            title_parts.append(f"№ {act_number}")
        title_parts.append(f"по {case_kind} № {reg_number}/{reg_year}")
        title = " ".join(title_parts)

        # Parse date
        date_signed = act.get("dateSigned", "")
        date_str = ""
        if date_signed:
            try:
                dt = datetime.fromisoformat(date_signed)
                date_str = dt.strftime("%Y-%m-%d")
            except Exception:
                pass

        # Judges
        preparators = act.get("preparators", [])
        judges = ", ".join(
            p.get("judgeName", "") for p in preparators if p.get("judgeName")
        )

        return {
            "_id": act_gid or case_gid,
            "_source": "BG/VAS-AdminCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": date_str,
            "url": f"{self.BASE_URL}/Case/LoadData?gid={case_gid}",
            "case_gid": case_gid,
            "act_gid": act_gid,
            "case_number": f"{reg_number}/{reg_year}",
            "case_year": reg_year,
            "case_kind": case_kind,
            "act_type": act_kind,
            "act_number": str(act_number) if act_number else "",
            "judges": judges,
            "reporter": raw.get("judgeReporter", ""),
            "department": raw.get("departmentName", ""),
            "panel": raw.get("panelName", ""),
            "plaintiff": raw.get("sideLeft", ""),
            "defendant": raw.get("sideRight", ""),
        }


# ── CLI Entry Point ──────────────────────────────────────────────

def main():
    scraper = BulgarianVASAdminCourtScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, {stats['records_updated']} updated, {stats['records_skipped']} skipped")
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
