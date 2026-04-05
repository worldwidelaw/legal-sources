#!/usr/bin/env python3
"""
US/VA-Law -- Virginia Law Portal (Code of Virginia + Admin Code + Constitution)

Fetches full text of Virginia legislation via the official REST JSON API at
law.lis.virginia.gov. Covers:
  - Code of Virginia (66 titles, ~30K sections)
  - Virginia Administrative Code (24 titles)
  - Constitution of Virginia (12 articles)

Strategy:
  1. Code of Virginia: Titles → Chapters → Section list → Section detail (Body)
  2. Admin Code: Titles → Agencies → Chapters → Section list → Section detail (Body)
  3. Constitution: Articles → Section detail (Body)

Data: Public domain. No authentication required.

Usage:
  python bootstrap.py bootstrap            # Full pull (all collections)
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample sections
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import re
import time
import json
import logging
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.VA-Law")

API_BASE = "https://law.lis.virginia.gov/api"
DELAY = 1.0  # seconds between requests


def strip_html(html_text: str) -> str:
    """Strip HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r'<style[^>]*>.*?</style>', '', html_text, flags=re.DOTALL)
    text = re.sub(r'<script[^>]*>.*?</script>', '', html_text, flags=re.DOTALL)
    text = re.sub(r'<br\s*/?>', '\n', text)
    text = re.sub(r'</p>', '\n', text)
    text = re.sub(r'</div>', '\n', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = html_module.unescape(text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n[ \t]+', '\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


class VALawScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
                "Accept": "application/json",
            },
            timeout=60,
        )

    def _get_json(self, url: str):
        """Fetch URL and parse JSON, with rate limiting."""
        time.sleep(DELAY)
        resp = self.http.get(url)
        return resp.json()

    # ── Code of Virginia ──────────────────────────────────────────────

    def fetch_cov_titles(self) -> list:
        """Get list of all Code of Virginia titles."""
        data = self._get_json(f"{API_BASE}/CoVTitlesGetListOfJson/")
        return data if isinstance(data, list) else []

    def fetch_cov_chapters(self, title_num: str) -> list:
        """Get chapters for a CoV title."""
        data = self._get_json(f"{API_BASE}/CoVChaptersGetListOfJson/{title_num}/")
        if isinstance(data, dict):
            return data.get("ChapterList", []) or []
        return []

    def fetch_cov_sections(self, title_num: str, chapter_num: str) -> list:
        """Get section list for a CoV title/chapter."""
        data = self._get_json(f"{API_BASE}/CoVSectionsGetListOfJson/{title_num}/{chapter_num}/")
        sections = []
        if isinstance(data, dict):
            for article in (data.get("ArticleList") or []):
                for subpart in (article.get("SubPartList") or []):
                    for sec in (subpart.get("SectionList") or []):
                        sections.append(sec)
        return sections

    def fetch_cov_section_detail(self, section_number: str) -> dict:
        """Get full section detail (with Body) for a CoV section."""
        data = self._get_json(f"{API_BASE}/CoVSectionsGetSectionDetailsJson/{section_number}/")
        if isinstance(data, dict):
            for ch in (data.get("ChapterList") or []):
                return ch  # First chapter entry contains the section detail
        return {}

    def iter_cov(self, max_sections: int = 0) -> Generator[dict, None, None]:
        """Iterate all Code of Virginia sections with full text."""
        titles = self.fetch_cov_titles()
        logger.info(f"Code of Virginia: {len(titles)} titles")
        count = 0
        seen_titles = set()

        for title in titles:
            t_num = title["TitleNumber"]
            t_name = title["TitleName"]
            # Deduplicate (API sometimes returns duplicates)
            if t_num in seen_titles:
                continue
            seen_titles.add(t_num)

            chapters = self.fetch_cov_chapters(t_num)
            logger.info(f"  Title {t_num} ({t_name}): {len(chapters)} chapters")

            for ch in chapters:
                ch_num = ch["ChapterNum"]
                sections = self.fetch_cov_sections(t_num, ch_num)

                for sec in sections:
                    sec_num = sec["SectionNumber"]
                    detail = self.fetch_cov_section_detail(sec_num)
                    body = detail.get("Body") or ""
                    text = strip_html(body)

                    if not text or len(text) < 10:
                        continue

                    yield {
                        "collection": "CoV",
                        "section_id": f"CoV-{sec_num}",
                        "title_num": t_num,
                        "title_name": t_name,
                        "chapter_num": ch_num,
                        "chapter_name": ch.get("ChapterName", ""),
                        "section_number": sec_num,
                        "section_title": sec.get("SectionTitle", detail.get("SectionTitle", "")),
                        "text": text,
                        "url": f"https://law.lis.virginia.gov/vacode/title{t_num}/chapter{ch_num}/section{sec_num}/",
                    }
                    count += 1
                    if count % 100 == 0:
                        logger.info(f"    CoV progress: {count} sections")
                    if max_sections and count >= max_sections:
                        return

    # ── Administrative Code ───────────────────────────────────────────

    def fetch_admin_titles(self) -> list:
        """Get list of all Administrative Code titles."""
        data = self._get_json(f"{API_BASE}/AdministrativeCodeGetTitleListOfJson/")
        return data if isinstance(data, list) else []

    def fetch_admin_agencies(self, title_num: str) -> list:
        """Get agencies for an admin code title."""
        data = self._get_json(f"{API_BASE}/AdministrativeCodeGetAgencyListOfJson/{title_num}/")
        if isinstance(data, dict):
            return data.get("AgencyList", []) or []
        return []

    def fetch_admin_chapters(self, title_num: str, agency_num: str) -> list:
        """Get chapters for an admin code agency."""
        data = self._get_json(f"{API_BASE}/AdministrativeCodeChapterListOfJson/{title_num}/{agency_num}/")
        if isinstance(data, dict):
            for ag in (data.get("AgencyList") or []):
                return ag.get("ChapterList", []) or []
        return []

    def fetch_admin_sections(self, title_num: str, agency_num: str, chapter_num: str) -> list:
        """Get section list for an admin code chapter."""
        data = self._get_json(f"{API_BASE}/AdministrativeCodeGetSectionListOfJson/{title_num}/{agency_num}/{chapter_num}/")
        sections = []
        if isinstance(data, dict):
            for ag in (data.get("AgencyList") or []):
                for ch in (ag.get("ChapterList") or []):
                    for sec in (ch.get("Sections") or []):
                        sections.append(sec)
        return sections

    def fetch_admin_section_detail(self, title_num: str, agency_num: str,
                                    chapter_num: str, section_num: str) -> dict:
        """Get full detail for an admin code section.

        The section number may contain dots or colons:
          - "10" → sectionNumber=10, point=0, colon=0
          - "10.1" → sectionNumber=10, point=1, colon=0
          - "10:1" → sectionNumber=10, point=0, colon=1
        """
        # Parse the section number into parts
        if ":" in section_num:
            parts = section_num.split(":", 1)
            base = parts[0]
            colon_part = parts[1]
            # Base might still have a dot
            if "." in base:
                dot_parts = base.split(".", 1)
                s_num = dot_parts[0]
                s_point = dot_parts[1]
            else:
                s_num = base
                s_point = "0"
            s_colon = colon_part
        elif "." in section_num:
            dot_parts = section_num.split(".", 1)
            s_num = dot_parts[0]
            s_point = dot_parts[1]
            s_colon = "0"
        else:
            s_num = section_num
            s_point = "0"
            s_colon = "0"

        url = f"{API_BASE}/AdministrativeCodeGetSectionDetailsJson/{title_num}/{agency_num}/{chapter_num}/{s_num}/{s_point}/{s_colon}/"
        data = self._get_json(url)
        if isinstance(data, dict):
            for ag in (data.get("AgencyList") or []):
                for ch in (ag.get("ChapterList") or []):
                    for sec in (ch.get("Sections") or []):
                        return sec
        return {}

    def iter_admin(self, max_sections: int = 0) -> Generator[dict, None, None]:
        """Iterate all Administrative Code sections with full text."""
        titles = self.fetch_admin_titles()
        logger.info(f"Administrative Code: {len(titles)} titles")
        count = 0

        for title in titles:
            t_num = title["TitleNumber"]
            t_name = title["TitleName"]
            agencies = self.fetch_admin_agencies(t_num)
            logger.info(f"  Admin Title {t_num} ({t_name}): {len(agencies)} agencies")

            for agency in agencies:
                a_num = agency["AgencyNumber"]
                a_name = agency["AgencyName"]
                chapters = self.fetch_admin_chapters(t_num, a_num)

                for ch in chapters:
                    ch_num = ch["ChapterNumber"]
                    if ch_num == "Preface":
                        continue  # Skip preface entries

                    sections = self.fetch_admin_sections(t_num, a_num, ch_num)

                    for sec in sections:
                        sec_num = sec["SectionNumber"]
                        detail = self.fetch_admin_section_detail(t_num, a_num, ch_num, sec_num)
                        body = detail.get("Body") or ""
                        text = strip_html(body)

                        if not text or len(text) < 10:
                            continue

                        vac_id = f"VAC-{t_num}-{a_num}-{ch_num}-{sec_num}"
                        yield {
                            "collection": "VAC",
                            "section_id": vac_id,
                            "title_num": t_num,
                            "title_name": t_name,
                            "agency_num": a_num,
                            "agency_name": a_name,
                            "chapter_num": ch_num,
                            "chapter_name": ch.get("ChapterName", ""),
                            "section_number": sec_num,
                            "section_title": sec.get("SectionTitle", detail.get("SectionTitle", "")),
                            "text": text,
                            "url": f"https://law.lis.virginia.gov/admincode/title{t_num}/agency{a_num}/chapter{ch_num}/section{sec_num}/",
                        }
                        count += 1
                        if count % 100 == 0:
                            logger.info(f"    VAC progress: {count} sections")
                        if max_sections and count >= max_sections:
                            return

    # ── Constitution ──────────────────────────────────────────────────

    def fetch_constitution_articles(self) -> list:
        """Get list of all Constitution articles."""
        data = self._get_json(f"{API_BASE}/ConstitutionArticlesGetListOfJson/")
        return data if isinstance(data, list) else []

    def fetch_constitution_section_detail(self, article_num: str, section_num: str) -> dict:
        """Get full detail for a Constitution section."""
        data = self._get_json(f"{API_BASE}/ConstitutionSectionDetailsJson/{article_num}/{section_num}/")
        if isinstance(data, dict):
            for sec in (data.get("Sections") or []):
                return sec
        return {}

    def fetch_constitution_sections_list(self, article_num: str) -> list:
        """Get sections list for a Constitution article."""
        # The XML endpoint actually returns JSON with sections list
        data = self._get_json(f"{API_BASE}/ConstitutionSectionsGetListOfXml/{article_num}/")
        if isinstance(data, dict):
            return data.get("Sections") or []
        return []

    def iter_constitution(self, max_sections: int = 0) -> Generator[dict, None, None]:
        """Iterate all Constitution sections with full text."""
        articles = self.fetch_constitution_articles()
        logger.info(f"Constitution: {len(articles)} articles")
        count = 0

        for article in articles:
            a_num = article["ArticleNumber"]
            a_name = article["ArticleName"]

            sections = self.fetch_constitution_sections_list(a_num)
            logger.info(f"  Article {a_num} ({a_name}): {len(sections)} sections")

            for sec in sections:
                sec_num = sec["SectionNumber"]
                detail = self.fetch_constitution_section_detail(a_num, sec_num)
                body = detail.get("Body") or ""
                text = strip_html(body)

                if not text or len(text) < 10:
                    continue

                yield {
                    "collection": "Constitution",
                    "section_id": f"CONST-{a_num}-{sec_num}",
                    "article_num": a_num,
                    "article_name": a_name,
                    "section_number": sec_num,
                    "section_name": detail.get("SectionName", sec.get("SectionName", "")),
                    "text": text,
                    "url": f"https://law.lis.virginia.gov/constitution/article{a_num}/section{sec_num}/",
                }
                count += 1
                if max_sections and count >= max_sections:
                    return

    # ── Normalize ─────────────────────────────────────────────────────

    def normalize(self, raw: dict) -> dict:
        """Transform raw section data into standard schema."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        collection = raw["collection"]
        section_id = raw["section_id"]

        if collection == "CoV":
            title = f"Code of Virginia § {raw['section_number']} — {raw.get('section_title', '')}"
        elif collection == "VAC":
            title = f"VAC {raw['title_num']}-{raw['agency_num']}-{raw['chapter_num']}-{raw['section_number']} — {raw.get('section_title', '')}"
        else:
            title = f"VA Constitution Art. {raw['article_num']} § {raw['section_number']} — {raw.get('section_name', '')}"

        return {
            "_id": section_id,
            "_source": "US/VA-Law",
            "_type": "legislation",
            "_fetched_at": now,
            "section_id": section_id,
            "title": title,
            "text": raw["text"],
            "date": today,
            "url": raw["url"],
            "collection": collection,
        }

    # ── Scraper interface ─────────────────────────────────────────────

    def test_api(self):
        """Test connectivity to Virginia Law API."""
        logger.info("Testing Virginia Law API...")
        try:
            titles = self.fetch_cov_titles()
            logger.info(f"  CoV titles: {len(titles)}")
            detail = self.fetch_cov_section_detail("1-200")
            body = detail.get("Body", "")
            if body and "common law" in body.lower():
                logger.info("  CoV section detail: OK (§ 1-200)")
            else:
                logger.error("  CoV section detail: unexpected content")
                return False

            articles = self.fetch_constitution_articles()
            logger.info(f"  Constitution articles: {len(articles)}")

            admin_titles = self.fetch_admin_titles()
            logger.info(f"  Admin Code titles: {len(admin_titles)}")

            logger.info("API test PASSED")
            return True
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all raw sections across all Virginia law collections."""
        total = 0
        for record in self.iter_cov():
            yield record
            total += 1
        logger.info(f"CoV complete: {total} sections")

        admin_count = 0
        for record in self.iter_admin():
            yield record
            admin_count += 1
        logger.info(f"VAC complete: {admin_count} sections")
        total += admin_count

        const_count = 0
        for record in self.iter_constitution():
            yield record
            const_count += 1
        logger.info(f"Constitution complete: {const_count} sections")
        total += const_count

        logger.info(f"Total Virginia sections: {total}")

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch all sections (no incremental update supported)."""
        yield from self.fetch_all()

    def fetch_sample(self) -> Generator[dict, None, None]:
        """Fetch a small sample: 5 CoV + 5 VAC + 5 Constitution sections."""
        count = 0
        for record in self.iter_cov(max_sections=5):
            yield record
            count += 1
        for record in self.iter_admin(max_sections=5):
            yield record
            count += 1
        for record in self.iter_constitution(max_sections=5):
            yield record
            count += 1
        logger.info(f"Sample complete: {count} sections")


if __name__ == "__main__":
    scraper = VALawScraper()
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "test-api":
            scraper.test_api()
        elif cmd == "bootstrap":
            sample = "--sample" in sys.argv
            if sample:
                scraper.bootstrap(sample_mode=True)
            else:
                scraper.bootstrap(sample_mode=False)
        else:
            print(f"Unknown command: {cmd}")
            print("Usage: python bootstrap.py [test-api|bootstrap [--sample]]")
    else:
        print("Usage: python bootstrap.py [test-api|bootstrap [--sample]]")
