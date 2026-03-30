#!/usr/bin/env python3
"""
PE/TribunalConstitucional -- Peru Constitutional Court Decisions

Fetches decisions from Peru's Tribunal Constitucional via the case search
interface at tc.gob.pe/consultas-de-causas/ and downloads full text from
HTML versions of decisions.

Strategy:
  - Search cases by year via the consultas-de-causas page (paginated, 20/page)
  - For each case, fetch the detail page to find HTML decision links
  - Download the HTML decision and extract plain text
  - Covers sentencias, autos, admisibilidad, inadmisibilidad, etc.

Source: https://tc.gob.pe/jurisprudencia/
Rate limit: 1 req/sec

Usage:
  python bootstrap.py bootstrap            # Full pull (all years)
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import html as html_mod
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.PE.TribunalConstitucional")

SEARCH_URL = "https://www.tc.gob.pe/consultas-de-causas/"
DETAIL_URL = "https://www.tc.gob.pe/consultas-de-causas/detalles-consulta"
JURIS_BASE = "https://www.tc.gob.pe/jurisprudencia/"

# Years to cover (TC has decisions from ~1996 onwards)
YEARS = list(range(2026, 1995, -1))


class TribunalConstitucionalScraper(BaseScraper):
    """
    Scraper for PE/TribunalConstitucional -- Peru Constitutional Court.
    Country: PE
    URL: https://tc.gob.pe/jurisprudencia/

    Data types: case_law
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            headers={
                "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml",
            },
            timeout=60,
        )

    def _clean_html(self, text: str) -> str:
        """Strip HTML tags and clean whitespace from decision HTML."""
        if not text:
            return ""
        text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<(?:p|div|br|h[1-6]|li|tr)[^>]*/?>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', '', text)
        text = html_mod.unescape(text)
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n[ \t]+', '\n', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _get_case_ids_for_year(self, year: int) -> list[str]:
        """Get all case id_exp values for a given year by paginating search results."""
        all_ids = []
        page = 1

        while True:
            self.rate_limiter.wait()
            params = f"?bus=tc&action=search&n_exp=&a_exp={year}&tip_exp=&demdt=&demdo=&ponte=&pagina={page}"
            resp = self.client.get(SEARCH_URL + params, timeout=30)

            if resp is None or resp.status_code != 200:
                break

            html = resp.text
            ids = re.findall(r'id_exp=(\d+)', html)

            if not ids:
                break

            all_ids.extend(ids)
            page += 1

            if len(ids) < 20:
                break

        return all_ids

    def _get_case_detail(self, id_exp: str) -> Optional[dict]:
        """Fetch case detail page and extract metadata + HTML decision links."""
        self.rate_limiter.wait()
        resp = self.client.get(f"{DETAIL_URL}?id_exp={id_exp}", timeout=30)

        if resp is None or resp.status_code != 200:
            return None

        html = resp.text

        # Extract case number from the page (e.g., "00001-2024-CC")
        case_match = re.search(r'(\d{5}-\d{4}-(?:AA|AI|CC|HC|HD|AC|PA|Q|PI))', html)
        if not case_match:
            return None
        case_number = case_match.group(1)

        # Extract case type from the case number suffix
        type_match = re.search(r'-(\w+)$', case_number)
        case_type = type_match.group(1) if type_match else ""

        # Extract metadata from first data row (date, court, ponente)
        rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL)
        date_filed = ""
        ponente = ""
        court = ""

        for row in rows:
            tds = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
            clean_tds = [re.sub(r'<[^>]+>', '', td).strip() for td in tds]
            clean_tds = [c for c in clean_tds if c]

            if len(clean_tds) >= 3 and re.match(r'\d{2}/\d{2}/\d{4}', clean_tds[0]):
                date_filed = clean_tds[0]
                court = clean_tds[1] if len(clean_tds) > 1 else ""
                ponente = clean_tds[2] if len(clean_tds) > 2 else ""
                break

        # Find HTML decision links (prefer sentencia/final decision)
        html_links = re.findall(
            r'href=["\']([^"\']*jurisprudencia[^"\']*\.(?:html?|htm))["\']',
            html, re.IGNORECASE
        )

        if not html_links:
            return None

        # Extract resolution info from rows
        resolutions = []
        for row in rows:
            tds = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL)
            clean_tds = [re.sub(r'<[^>]+>', '', td).strip() for td in tds]
            links_in_row = re.findall(
                r'href=["\']([^"\']*jurisprudencia[^"\']*\.(?:html?|htm))["\']',
                row, re.IGNORECASE
            )
            if links_in_row:
                res_type = clean_tds[0] if clean_tds else "Unknown"
                res_date = ""
                for td in clean_tds:
                    m = re.match(r'(\d{2}/\d{2}/\d{4})', td)
                    if m:
                        res_date = m.group(1)
                        break
                resolutions.append({
                    "type": res_type,
                    "date": res_date,
                    "html_url": links_in_row[-1],  # Last link = HTML version
                })

        # Prefer "Sentencia" resolution, otherwise take the last one
        best = None
        for r in resolutions:
            if "sentencia" in r["type"].lower():
                best = r
                break
        if not best:
            best = resolutions[-1] if resolutions else {"type": "Unknown", "date": "", "html_url": html_links[-1]}

        return {
            "id_exp": id_exp,
            "case_number": case_number,
            "case_type": case_type,
            "date_filed": date_filed,
            "ponente": ponente,
            "court": court,
            "resolution_type": best["type"],
            "resolution_date": best["date"],
            "html_url": best["html_url"],
        }

    def _fetch_decision_text(self, html_url: str) -> str:
        """Download HTML decision and extract plain text."""
        self.rate_limiter.wait()

        # URL-encode spaces in the filename
        if " " in html_url:
            parts = html_url.rsplit("/", 1)
            if len(parts) == 2:
                html_url = parts[0] + "/" + quote(parts[1])

        resp = self.client.get(html_url, timeout=60)
        if resp is None or resp.status_code != 200:
            return ""

        return self._clean_html(resp.text)

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Convert dd/mm/yyyy to ISO 8601."""
        if not date_str:
            return None
        m = re.match(r'(\d{2})/(\d{2})/(\d{4})', date_str)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
        return None

    # -- Core scraper methods ------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all decisions by searching each year."""
        total = 0
        for year in YEARS:
            logger.info(f"Fetching cases for year {year}...")
            ids = self._get_case_ids_for_year(year)
            logger.info(f"Year {year}: {len(ids)} cases found")

            for id_exp in ids:
                detail = self._get_case_detail(id_exp)
                if not detail:
                    continue

                text = self._fetch_decision_text(detail["html_url"])
                if not text or len(text) < 100:
                    continue

                detail["text"] = text
                total += 1

                if total % 50 == 0:
                    logger.info(f"Progress: {total} decisions fetched")

                yield detail

        logger.info(f"Fetch complete: {total} decisions")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Fetch recent decisions (current year and previous)."""
        current_year = datetime.now().year
        since_iso = since.strftime("%Y-%m-%d")

        for year in [current_year, current_year - 1]:
            ids = self._get_case_ids_for_year(year)
            for id_exp in ids:
                detail = self._get_case_detail(id_exp)
                if not detail:
                    continue

                res_date = self._parse_date(detail.get("resolution_date", ""))
                if res_date and res_date < since_iso:
                    continue

                text = self._fetch_decision_text(detail["html_url"])
                if not text or len(text) < 100:
                    continue

                detail["text"] = text
                yield detail

    def _get_case_ids_page(self, year: int, page: int) -> list[str]:
        """Get case id_exp values for a single page of search results."""
        self.rate_limiter.wait()
        params = f"?bus=tc&action=search&n_exp=&a_exp={year}&tip_exp=&demdt=&demdo=&ponte=&pagina={page}"
        resp = self.client.get(SEARCH_URL + params, timeout=30)
        if resp is None or resp.status_code != 200:
            return []
        return re.findall(r'id_exp=(\d+)', resp.text)

    def fetch_sample(self, count: int = 15) -> Generator[dict, None, None]:
        """Fetch sample decisions from recent years (paginate lazily)."""
        found = 0

        for year in [2025, 2024, 2023]:
            if found >= count:
                break

            logger.info(f"Searching year {year} for samples...")
            page = 1

            while found < count:
                ids = self._get_case_ids_page(year, page)
                if not ids:
                    break

                for id_exp in ids:
                    if found >= count:
                        break

                    detail = self._get_case_detail(id_exp)
                    if not detail:
                        continue

                    text = self._fetch_decision_text(detail["html_url"])
                    if not text or len(text) < 100:
                        continue

                    detail["text"] = text
                    found += 1

                    title = detail.get("case_number", "?")
                    logger.info(
                        f"Sample {found}/{count}: {title} "
                        f"[{detail.get('resolution_type', '?')}] "
                        f"({len(text)} chars)"
                    )

                    yield detail

                page += 1

        logger.info(f"Sample complete: {found} records")

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw decision record to standard schema."""
        case_number = raw.get("case_number", "unknown")
        res_type = raw.get("resolution_type", "")

        # Build title from case info
        title = f"TC {case_number}"
        if res_type:
            title += f" - {res_type}"

        # Use resolution date if available, else filing date
        date_iso = self._parse_date(raw.get("resolution_date", ""))
        if not date_iso:
            date_iso = self._parse_date(raw.get("date_filed", ""))

        # Build a unique ID
        _id = f"PE-TC-{case_number}"
        if res_type and res_type.lower() != "sentencia":
            safe_type = re.sub(r'[^a-zA-Z]', '', res_type)[:20]
            _id += f"-{safe_type}"

        web_url = raw.get("html_url", f"https://tc.gob.pe/jurisprudencia/")

        return {
            "_id": _id,
            "_source": "PE/TribunalConstitucional",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": date_iso,
            "url": web_url,
            "case_number": case_number,
            "case_type": raw.get("case_type", ""),
            "ponente": raw.get("ponente", ""),
            "court": raw.get("court", ""),
            "resolution_type": res_type,
            "language": "es",
        }

    def test_api(self) -> bool:
        """Test connectivity to TC website."""
        logger.info("Testing TC website access...")

        # Test search page
        self.rate_limiter.wait()
        resp = self.client.get(
            SEARCH_URL + "?bus=tc&action=search&n_exp=00001&a_exp=2024&tip_exp=&demdt=&demdo=&ponte=&pagina=1",
            timeout=30,
        )
        if resp is None or resp.status_code != 200:
            logger.error("Search page failed")
            return False

        ids = re.findall(r'id_exp=(\d+)', resp.text)
        if not ids:
            logger.error("No case IDs found in search results")
            return False
        logger.info(f"Search: OK ({len(ids)} cases found)")

        # Test detail page
        detail = self._get_case_detail(ids[0])
        if not detail:
            logger.error("Detail page failed")
            return False
        logger.info(f"Detail: OK ({detail['case_number']})")

        # Test HTML decision fetch
        text = self._fetch_decision_text(detail["html_url"])
        if not text or len(text) < 100:
            logger.error("HTML decision fetch failed or too short")
            return False
        logger.info(f"Decision text: OK ({len(text)} chars)")

        logger.info("All tests passed!")
        return True


# ── CLI ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    scraper = TribunalConstitucionalScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample] [--count N]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test-api":
        ok = scraper.test_api()
        sys.exit(0 if ok else 1)

    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        count = 15
        for i, arg in enumerate(sys.argv):
            if arg == "--count" and i + 1 < len(sys.argv):
                count = int(sys.argv[i + 1])

        if sample_mode:
            gen = scraper.fetch_sample(count=count)
        else:
            gen = scraper.fetch_all()

        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in gen:
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1
            logger.info(f"Saved: {out_path.name}")

        logger.info(f"Bootstrap complete: {saved} records saved to {sample_dir}")

    elif command == "update":
        since = datetime.now(timezone.utc).replace(month=1, day=1)
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        saved = 0
        for record in scraper.fetch_updates(since):
            normalized = scraper.normalize(record)
            out_path = sample_dir / f"{normalized['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(normalized, f, ensure_ascii=False, indent=2)
            saved += 1

        logger.info(f"Update complete: {saved} records saved")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
