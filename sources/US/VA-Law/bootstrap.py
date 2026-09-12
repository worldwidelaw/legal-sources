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

Incremental refresh (#1502):
  The portal exposes no upstream modified stamp anywhere — the whole JSON API
  surface is enumerated at /jsonapi/ and carries no date facet, the responses
  come back `Cache-Control: no-cache` with no `Last-Modified`/`ETag`, and the
  bulk CSVs under /CSV/ share one batch mtime that has not moved since
  2025-08-13 even though the API already serves 2026 amendments. So the only
  honest availability comparator is the upstream body itself: `fetch_updates`
  walks the corpus and yields only the sections whose text hash differs from
  the one recorded in data/va_law_state.json. That is the `availability`
  comparator in common.base_scraper — an upstream content SHA, not a date read
  out of the document.

Usage:
  python bootstrap.py bootstrap            # Full pull (all collections)
  python bootstrap.py bootstrap-fast       # Alias for the full pull
  python bootstrap.py bootstrap --sample   # Fetch ~15 sample sections
  python bootstrap.py update               # Incremental refresh (changed only)
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import re
import time
import json
import hashlib
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

#: Flush the content-hash state this often so a torn-down fleet slot keeps the
#: work it already did instead of restarting from an empty comparator.
STATE_FLUSH_EVERY = 500

#: Amendment history citation at the foot of a section body: "2005, c. 839.",
#: "2019, cc. 12, 34.", "Code 1919, § 2". Used only to date the record — never
#: as the refresh comparator.
_CITATION_YEAR_RE = re.compile(r"\b(1[6-9]\d\d|20\d\d)\s*,\s*(?:cc?\.|§)")

#: Virginia Register historical note on an Administrative Code section:
#: "eff. July 1, 1993", "eff. February 1, 2010".
_EFFECTIVE_YEAR_RE = re.compile(r"eff\.\s+(?:[A-Z][a-z]+\s+\d{1,2},\s*)?(19\d\d|20\d\d)")

#: The current Constitution of Virginia took effect on this date. Constitution
#: sections carry no citation block, and a crawl-date fallback would re-date
#: every one of them on every pass.
CONSTITUTION_EFFECTIVE_DATE = "1971-07-01"


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

    #: law.lis.virginia.gov publishes no modified/published stamp at any level
    #: (see the module docstring), so the refresh narrows on the upstream body
    #: hash rather than on `since`.
    incremental_comparator = "availability"

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
        self.state_path = Path(source_dir) / "data" / "va_law_state.json"
        self._state = None
        self._state_dirty = 0

    # ── Upstream content-hash state ───────────────────────────────────

    @property
    def state(self) -> dict:
        """Lazily loaded `{section_id: sha1(text)}` map of the last seen bodies."""
        if self._state is None:
            try:
                with open(self.state_path, encoding="utf-8") as fh:
                    loaded = json.load(fh)
                self._state = loaded.get("hashes", {})
                logger.info(
                    "Loaded content-hash state: %d sections (updated %s)",
                    len(self._state), loaded.get("updated_at", "unknown"),
                )
            except FileNotFoundError:
                self._state = {}
                logger.info("No content-hash state yet — first run builds it")
            except (json.JSONDecodeError, OSError) as e:
                # A truncated state file must not silently become "nothing
                # changed"; drop it and rebuild rather than skip the corpus.
                self._state = {}
                logger.warning("Unreadable state at %s (%s) — rebuilding", self.state_path, e)
        return self._state

    @staticmethod
    def _body_hash(raw: dict) -> str:
        return hashlib.sha1(raw["text"].encode("utf-8")).hexdigest()

    def _remember(self, raw: dict) -> None:
        """Record a section's upstream body hash, flushing periodically."""
        self.state[raw["section_id"]] = self._body_hash(raw)
        self._state_dirty += 1
        if self._state_dirty >= STATE_FLUSH_EVERY:
            self._save_state()

    def _save_state(self) -> None:
        if self._state is None:
            return
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state_path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(
                {
                    "version": 1,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                    "hashes": self._state,
                },
                fh,
            )
        tmp.replace(self.state_path)
        self._state_dirty = 0

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

    @staticmethod
    def _enacted_date(text: str, collection: str) -> str:
        """Date the section from the newest year in its enactment history.

        The old code stamped every record with the crawl date, so an unchanged
        section looked freshly dated on every pass. Code sections carry an
        acts-of-assembly citation ("2019, cc. 12, 34."), Administrative Code
        sections a Virginia Register note ("eff. February 1, 2010"); both are
        stable across crawls. Where neither exists the fallback still has to be
        stable, so Constitution sections take the current Constitution's
        effective date rather than today's.
        """
        years = [int(y) for y in _CITATION_YEAR_RE.findall(text)]
        years += [int(y) for y in _EFFECTIVE_YEAR_RE.findall(text)]
        if years:
            return f"{max(years)}-01-01"
        if collection == "Constitution":
            return CONSTITUTION_EFFECTIVE_DATE
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def normalize(self, raw: dict) -> dict:
        """Transform raw section data into standard schema."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

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
            "date": self._enacted_date(raw["text"], collection),
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

    def _walk(self) -> Generator[dict, None, None]:
        """Walk every section of every collection once, in publication order."""
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

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all raw sections, recording each body hash for later refreshes."""
        try:
            for raw in self._walk():
                self._remember(raw)
                yield raw
        finally:
            self._save_state()

    def fetch_updates(self, since=None) -> Generator[dict, None, None]:
        """Yield only the sections whose upstream body changed.

        `since` is a crawl timestamp and there is nothing upstream to compare it
        against — the portal serves no modified stamp on any endpoint, and the
        bulk CSVs carry a batch mtime that lags the API by a year. So the
        comparator is the upstream body hash recorded by the previous run: a
        section comes through when law.lis.virginia.gov is serving text we have
        not seen, which is exactly when it became available to us, and never
        because of a date printed inside it (#1502).
        """
        # `since` is deliberately unread: reporting this as a date-narrowed
        # refresh would be a false positive in classify_fetch_updates, and the
        # declared `availability` comparator is what actually narrows it.
        logger.info(
            "Incremental refresh: comparing upstream body hashes against "
            "%d recorded sections", len(self.state),
        )
        if not self.state:
            logger.warning(
                "No content-hash state on disk — this refresh yields the whole "
                "corpus once to establish the baseline, then narrows."
            )

        examined = changed = added = 0
        try:
            for raw in self._walk():
                examined += 1
                digest = self._body_hash(raw)
                previous = self.state.get(raw["section_id"])
                if previous == digest:
                    continue
                if previous is None:
                    added += 1
                else:
                    changed += 1
                self._remember(raw)
                yield raw
        finally:
            self._save_state()
            logger.info(
                "Refresh done: %d sections examined, %d changed, %d new, "
                "%d unchanged and skipped",
                examined, changed, added, examined - changed - added,
            )

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


USAGE = ("Usage: python bootstrap.py "
         "[test-api|bootstrap [--sample]|bootstrap-fast|update]")


if __name__ == "__main__":
    scraper = VALawScraper()
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "test-api":
            scraper.test_api()
        elif cmd in ("bootstrap", "bootstrap-fast"):
            # The fleet wrapper invokes `bootstrap-fast`; without the alias it
            # falls back to re-ingesting sample/ (#1113 class).
            scraper.bootstrap(sample_mode="--sample" in sys.argv)
        elif cmd == "update":
            scraper.update()
        else:
            print(f"Unknown command: {cmd}")
            print(USAGE)
    else:
        print(USAGE)
