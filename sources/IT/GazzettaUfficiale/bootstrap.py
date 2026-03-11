#!/usr/bin/env python3
"""
IT/GazzettaUfficiale -- Italian Official Gazette Data Fetcher

Fetches Italian legislation from the Gazzetta Ufficiale (Official Gazette).

Strategy:
  - Uses the Gazzetta Ufficiale website to browse gazette issues by date.
  - Each gazette issue lists multiple acts with their codiceRedazionale (editorial code).
  - Full text is fetched via the caricaArticolo endpoint which returns article HTML.
  - For each act, we fetch all articles and combine them into full text.

Endpoints:
  - Gazette issue listing: /eli/gu/{yyyy}/{mm}/{dd}/{issue}/sg/html
  - Act detail: /atto/serie_generale/caricaDettaglioAtto/originario
  - Article text: /atto/serie_generale/caricaArticolo

Data:
  - Serie Generale (general series) from 1986 to present
  - Languages: Italian
  - Rate limit: conservative 1 request/second

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update (recent issues only)
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Generator, Optional, Dict, Any, List

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IT.GazzettaUfficiale")

# Base URLs
GU_BASE_URL = "https://www.gazzettaufficiale.it"

# Act type mappings from Italian
ACT_TYPES = {
    "LEGGE": "legge",
    "DECRETO LEGISLATIVO": "decreto.legislativo",
    "DECRETO-LEGGE": "decreto.legge",
    "DECRETO DEL PRESIDENTE DELLA REPUBBLICA": "dpr",
    "DECRETO DEL PRESIDENTE DEL CONSIGLIO DEI MINISTRI": "dpcm",
    "DECRETO MINISTERIALE": "dm",
    "DECRETO": "decreto",
    "REGOLAMENTO": "regolamento",
    "COMUNICATO": "comunicato",
    "AVVISO": "avviso",
}


class GazzettaUfficialeScraper(BaseScraper):
    """
    Scraper for IT/GazzettaUfficiale -- Italian Official Gazette.
    Country: IT
    URL: https://www.gazzettaufficiale.it

    Data types: legislation
    Auth: none (Open Government Data)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=GU_BASE_URL,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept-Language": "it,en",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            timeout=60,
        )

    def _get_gazette_issues_for_year(self, year: int) -> List[Dict[str, Any]]:
        """
        Get list of gazette issues for a given year.

        The Gazzetta Ufficiale Serie Generale is published multiple times per week.
        We'll check for issues by iterating through dates and issue numbers.

        Returns list of dicts with: date, issue_number
        """
        issues = []

        # Start from most recent date and work backwards
        if year == datetime.now().year:
            end_date = datetime.now()
        else:
            end_date = datetime(year, 12, 31)

        start_date = datetime(year, 1, 1)

        # Check the last few days of December from previous year for sample
        current_date = end_date

        # For sample mode, we'll start from recent dates
        # The gazette issues are numbered sequentially within the year
        # We can try to access specific issue numbers

        # Try to get recent issues by checking the ELI endpoint
        for issue_num in range(300, 0, -1):  # Start from high issue number
            if len(issues) >= 10:  # Get enough issues for discovery
                break

            try:
                # Try to find which date has this issue number
                # The pattern is /eli/gu/{yyyy}/{mm}/{dd}/{issue}/sg/html
                # We need to find dates with gazette issues
                pass
            except:
                pass

        return issues

    def _get_acts_from_gazette_issue(self, date_str: str, issue_num: int) -> List[Dict[str, Any]]:
        """
        Parse a gazette issue page to get all acts.

        Args:
            date_str: Date in YYYY-MM-DD format
            issue_num: Gazette issue number

        Returns list of dicts with: code, title, act_type, date
        """
        acts = []

        try:
            # Parse date
            year, month, day = date_str.split("-")

            # Fetch gazette issue page
            url = f"/eli/gu/{year}/{month}/{day}/{issue_num}/sg/html"

            self.rate_limiter.wait()
            resp = self.client.get(url)

            if resp.status_code == 404:
                logger.debug(f"Gazette issue {date_str}/{issue_num} not found")
                return []

            resp.raise_for_status()
            content = resp.text

            # Check if gazette is still loading
            if "fase di caricamento" in content.lower():
                logger.debug(f"Gazette issue {date_str}/{issue_num} is still loading")
                return []

            # Parse acts from the page
            # The structure is:
            # <a href="...caricaDettaglioAtto...codiceRedazionale=XXX...">
            #   <span class="data">LEGGE 30 settembre 2024, n. 148</span>
            # </a>
            # <a href="...same link...">Title of the act (CODE)</a>

            # First, extract all code/date pairs
            code_pattern = re.compile(
                r'atto\.dataPubblicazioneGazzetta=([^&"]+)[^"]*'
                r'atto\.codiceRedazionale=([^&"]+)',
                re.IGNORECASE
            )

            # Then find title blocks - they appear after the second link
            # Pattern: >Title text (CODE)<
            title_pattern = re.compile(
                r'>([^<>]{20,500})\s*\((\d{2}[A-Z]\d{5})\)\s*<',
                re.DOTALL
            )

            seen_codes = set()
            code_to_pubdate = {}

            # First pass: collect all codes and their pub dates
            for match in code_pattern.finditer(content):
                pub_date = match.group(1).strip()
                code = match.group(2).strip()
                if code not in code_to_pubdate:
                    code_to_pubdate[code] = pub_date

            # Second pass: find titles with codes
            for match in title_pattern.finditer(content):
                title_text = match.group(1).strip()
                code = match.group(2).strip()

                if code in seen_codes:
                    continue
                seen_codes.add(code)

                pub_date = code_to_pubdate.get(code, pub_date)

                # Clean title
                title = html.unescape(title_text)
                title = re.sub(r'\s+', ' ', title).strip()

                if not title or len(title) < 10:
                    continue

                # Determine act type from the code prefix
                # Codes like 24G00164 - G typically means LEGGE
                act_type = "unknown"
                if code:
                    prefix = code[2:3].upper() if len(code) > 2 else ""
                    if prefix == "G":
                        act_type = "legge"
                    elif prefix == "A":
                        act_type = "decreto"
                    elif prefix == "E":
                        act_type = "decreto.legislativo"

                acts.append({
                    "code": code,
                    "title": title,
                    "act_type": act_type,
                    "pub_date": pub_date,
                    "gazette_date": date_str,
                    "gazette_number": issue_num,
                })

            logger.info(f"Found {len(acts)} acts in gazette {date_str}/{issue_num}")
            return acts

        except Exception as e:
            logger.warning(f"Failed to parse gazette issue {date_str}/{issue_num}: {e}")
            return []

    def _fetch_act_full_text(self, code: str, pub_date: str) -> tuple:
        """
        Fetch full text of an act from the Gazzetta Ufficiale.

        The full text is loaded via the vediMenuHTML endpoint which shows
        all articles, then we fetch each article's text.

        Returns (full_text, act_type, title, doc_date) tuple.
        """
        try:
            # First, get the act menu page which lists all articles
            menu_url = (
                f"/atto/vediMenuHTML?"
                f"atto.dataPubblicazioneGazzetta={pub_date}&"
                f"atto.codiceRedazionale={code}&"
                f"tipoSerie=serie_generale&"
                f"tipoVigenza=originario"
            )

            self.rate_limiter.wait()
            resp = self.client.get(menu_url)

            if resp.status_code == 404:
                logger.debug(f"Act {code} not found")
                return "", "", "", ""

            resp.raise_for_status()
            content = resp.text

            # Extract metadata from the page
            # Title is in <h3 class="consultazione">
            title = ""
            title_match = re.search(
                r'<h3[^>]*class="consultazione"[^>]*>([^<]+(?:<[^>]+>[^<]+)*)</h3>',
                content, re.DOTALL | re.IGNORECASE
            )
            if title_match:
                title = self._clean_html_text(title_match.group(1))

            # Act type from the page
            act_type = ""
            type_match = re.search(
                r'<p[^>]*class="grassetto"[^>]*>\s*([A-Z][A-Z\s]+?)(?:\s*\d|<)',
                content, re.IGNORECASE
            )
            if type_match:
                act_type = type_match.group(1).strip()
                act_type = ACT_TYPES.get(act_type.upper(), act_type.lower().replace(" ", "."))

            # Extract document date
            doc_date = ""
            date_match = re.search(
                r'(\d{1,2})\s+(gennaio|febbraio|marzo|aprile|maggio|giugno|'
                r'luglio|agosto|settembre|ottobre|novembre|dicembre)\s+(\d{4})',
                content, re.IGNORECASE
            )
            if date_match:
                doc_date = self._parse_italian_date(
                    date_match.group(1),
                    date_match.group(2),
                    date_match.group(3)
                )

            # Find all article links
            article_pattern = re.compile(
                r'caricaArticolo\?([^"\']+)',
                re.IGNORECASE
            )

            article_params = []
            for match in article_pattern.finditer(content):
                params = match.group(1)
                if params not in article_params:
                    article_params.append(params)

            # Fetch each article's text
            text_parts = []

            for params in article_params[:50]:  # Limit to prevent huge downloads
                article_text = self._fetch_article_text(params)
                if article_text:
                    text_parts.append(article_text)

            full_text = "\n\n".join(text_parts)

            return full_text, act_type, title, doc_date

        except Exception as e:
            logger.warning(f"Failed to fetch act {code}: {e}")
            return "", "", "", ""

    def _fetch_article_text(self, params: str) -> str:
        """
        Fetch text of a single article.

        Returns the cleaned text content.
        """
        try:
            url = f"/atto/serie_generale/caricaArticolo?{params}"

            self.rate_limiter.wait()
            resp = self.client.get(url)

            if resp.status_code != 200:
                return ""

            content = resp.text

            # Extract text from <pre> tags (main content)
            text_parts = []

            # Primary content is in <pre> tags within dettaglio_atto_testo
            pre_pattern = re.compile(
                r'<pre[^>]*>([^<]+(?:<[^>]+>[^<]+)*)</pre>',
                re.DOTALL | re.IGNORECASE
            )

            for match in pre_pattern.finditer(content):
                text = match.group(1)
                text = self._clean_html_text(text)
                if text and len(text) > 10:
                    text_parts.append(text)

            # If no <pre> found, try other text containers
            if not text_parts:
                span_pattern = re.compile(
                    r'<span[^>]*class="dettaglio_atto_testo"[^>]*>([^<]+(?:<[^>]+>[^<]+)*)</span>',
                    re.DOTALL | re.IGNORECASE
                )
                for match in span_pattern.finditer(content):
                    text = self._clean_html_text(match.group(1))
                    if text and len(text) > 10:
                        text_parts.append(text)

            return "\n".join(text_parts)

        except Exception as e:
            logger.debug(f"Failed to fetch article: {e}")
            return ""

    def _parse_italian_date(self, day: str, month_name: str, year: str) -> str:
        """Parse Italian date to ISO 8601."""
        months = {
            "gennaio": 1, "febbraio": 2, "marzo": 3, "aprile": 4,
            "maggio": 5, "giugno": 6, "luglio": 7, "agosto": 8,
            "settembre": 9, "ottobre": 10, "novembre": 11, "dicembre": 12
        }

        month = months.get(month_name.lower(), 1)

        try:
            return f"{year}-{month:02d}-{int(day):02d}"
        except:
            return ""

    def _clean_html_text(self, text: str) -> str:
        """Clean HTML from text."""
        if not text:
            return ""

        # Remove HTML tags but preserve line breaks
        text = re.sub(r'<br\s*/?\s*>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', ' ', text)

        # Unescape HTML entities
        text = html.unescape(text)

        # Normalize whitespace but keep paragraph breaks
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n\s*\n+', '\n\n', text)
        text = text.strip()

        return text

    def _get_acts_from_rss(self, series: str = "SG") -> List[Dict[str, Any]]:
        """
        Get acts from the RSS feed.

        The RSS feed returns the latest gazette issue with all acts.

        Args:
            series: Series code (SG=General, S1=Constitutional Court, etc.)

        Returns list of dicts with: code, title, pub_date, link
        """
        acts = []

        try:
            url = f"/rss/{series}"
            import time
            time.sleep(1)  # Rate limiting
            resp = self.client.get(url)

            if resp.status_code != 200:
                logger.warning(f"RSS feed {series} returned {resp.status_code}")
                return []

            content = resp.text

            # Parse RSS items
            # <item>
            #   <title>DECRETO LEGISLATIVO 3 febbraio 2026, n.17</title>
            #   <link>http://www.gazzettaufficiale.it/eli/id/2026/02/12/26G00033/SG</link>
            #   <content:encoded>Description text (CODE)</content:encoded>
            # </item>

            import xml.etree.ElementTree as ET

            # Parse XML
            root = ET.fromstring(content)

            # Handle namespaces
            namespaces = {
                'content': 'http://purl.org/rss/1.0/modules/content/'
            }

            for item in root.findall('.//item'):
                title_elem = item.find('title')
                link_elem = item.find('link')
                desc_elem = item.find('content:encoded', namespaces)

                if title_elem is None or link_elem is None:
                    continue

                title = title_elem.text or ""
                link = link_elem.text or ""
                description = desc_elem.text if desc_elem is not None else ""

                # Extract code from link
                # Link format: http://www.gazzettaufficiale.it/eli/id/2026/02/12/26G00033/SG
                code_match = re.search(r'/eli/id/(\d{4})/(\d{2})/(\d{2})/([^/]+)/[A-Z]+', link)
                if code_match:
                    year = code_match.group(1)
                    month = code_match.group(2)
                    day = code_match.group(3)
                    code = code_match.group(4)
                    pub_date = f"{year}-{month}-{day}"

                    acts.append({
                        "code": code,
                        "title": title,
                        "description": description,
                        "pub_date": pub_date,
                        "link": link,
                    })

            logger.info(f"Found {len(acts)} acts in RSS feed {series}")
            return acts

        except Exception as e:
            logger.warning(f"Failed to parse RSS feed {series}: {e}")
            return []

    def _discover_recent_gazettes(self, max_days: int = 30, start_year: int = None) -> List[Dict[str, Any]]:
        """
        Discover gazette issues by checking dates.

        Returns list of dicts with: date, issue_number
        """
        issues = []

        # Start from current date and work backwards
        if start_year is None:
            current_date = datetime.now()
        else:
            current_date = datetime(start_year, 12, 31)

        for day_offset in range(max_days):
            if len(issues) >= 15:  # Enough for sample mode
                break

            check_date = current_date - timedelta(days=day_offset)
            date_str = check_date.strftime("%Y-%m-%d")
            year, month, day = date_str.split("-")

            # Try a range of issue numbers for this date
            # Issues are typically numbered 1-300 per year
            day_of_year = check_date.timetuple().tm_yday
            estimated_issue = min(305, max(1, day_of_year))

            # Check a few issue numbers around the estimate
            for issue_offset in range(-3, 4):
                issue_num = estimated_issue + issue_offset
                if issue_num < 1:
                    continue

                try:
                    url = f"/eli/gu/{year}/{month}/{day}/{issue_num}/sg/html"

                    import time
                    time.sleep(1)  # Rate limiting
                    resp = self.client.get(url)

                    if resp.status_code == 200:
                        # Check if gazette is still loading
                        if "fase di caricamento" in resp.text.lower():
                            continue

                        # Found a valid, loaded issue
                        issues.append({
                            "date": date_str,
                            "issue_number": issue_num,
                        })
                        logger.info(f"Found gazette issue: {date_str} #{issue_num}")
                        break  # Move to next date

                except Exception as e:
                    continue

        return issues

    def _get_issues_from_archive_year(self, year: int) -> List[Dict[str, Any]]:
        """
        Get all gazette issues from the archive page for a specific year.

        The archive listing at /ricercaArchivioCompleto/serie_generale/{year}
        contains links to all gazette issues published that year.

        Returns list of dicts with: date, issue_number
        """
        issues = []

        try:
            url = f"/ricercaArchivioCompleto/serie_generale/{year}"
            self.rate_limiter.wait()
            resp = self.client.get(url)

            if resp.status_code != 200:
                logger.warning(f"Archive page for {year} returned {resp.status_code}")
                return []

            content = resp.text

            # Parse issue links from the archive page
            # Pattern: /gazzetta/serie_generale/caricaDettaglio?dataPubblicazioneGazzetta=YYYY-MM-DD&numeroGazzetta=N
            pattern = re.compile(
                r'dataPubblicazioneGazzetta=(\d{4}-\d{2}-\d{2})[^"]*numeroGazzetta=(\d+)',
                re.IGNORECASE
            )

            seen = set()
            for match in pattern.finditer(content):
                date_str = match.group(1)
                issue_num = int(match.group(2))
                key = f"{date_str}_{issue_num}"

                if key in seen:
                    continue
                seen.add(key)

                issues.append({
                    "date": date_str,
                    "issue_number": issue_num,
                })

            # Sort by issue number
            issues.sort(key=lambda x: x["issue_number"])

            logger.info(f"Found {len(issues)} gazette issues for {year}")
            return issues

        except Exception as e:
            logger.warning(f"Failed to get archive for {year}: {e}")
            return []

    def _get_acts_from_gazette_page(self, year: int, month: int, day: int, issue_num: int) -> List[Dict[str, Any]]:
        """
        Get acts from a gazette issue page.

        Returns list of dicts with: code, pub_date, title
        """
        acts = []

        try:
            url = f"/eli/gu/{year:04d}/{month:02d}/{day:02d}/{issue_num}/sg/html"
            import time
            time.sleep(1)  # Rate limiting
            resp = self.client.get(url)

            if resp.status_code != 200:
                return []

            content = resp.text

            # Check if gazette is still loading
            if "fase di caricamento" in content.lower():
                return []

            # Extract codes from the page
            # Looking for patterns like: codiceRedazionale=26G00033
            code_pattern = re.compile(
                r'dataPubblicazioneGazzetta=([^&"]+)[^"]*'
                r'codiceRedazionale=([^&"]+)',
                re.IGNORECASE
            )

            seen_codes = set()
            pub_date = f"{year:04d}-{month:02d}-{day:02d}"

            for match in code_pattern.finditer(content):
                code = match.group(2).strip()
                if code in seen_codes:
                    continue
                seen_codes.add(code)

                # Only include actual legislation codes (G = laws, E = legislative decrees)
                # Skip A codes (announcements) and C codes (constitutional court - different series)
                if len(code) > 2 and code[2:3].upper() in ["G", "E"]:
                    acts.append({
                        "code": code,
                        "pub_date": pub_date,
                        "title": "",
                    })

            logger.info(f"Found {len(acts)} legislative acts in gazette {pub_date}#{issue_num}")
            return acts

        except Exception as e:
            logger.warning(f"Failed to get gazette {year}-{month:02d}-{day:02d}: {e}")
            return []

    def fetch_all(self, start_year: int = None, end_year: int = None) -> Generator[dict, None, None]:
        """
        Yield all documents from the Gazzetta Ufficiale.

        Iterates through archive years from start_year to end_year (default: 2020 to current).
        For each year, fetches the list of gazette issues from the archive page,
        then processes each issue to extract legislation acts.

        Args:
            start_year: First year to fetch (default: 2020)
            end_year: Last year to fetch (default: current year)
        """
        documents_yielded = 0
        seen_codes = set()

        # Default year range
        current_year = datetime.now().year
        if start_year is None:
            start_year = 2020  # Start from 2020 by default
        if end_year is None:
            end_year = current_year

        logger.info(f"Fetching Gazzetta Ufficiale from {start_year} to {end_year}")

        # Process each year
        for year in range(end_year, start_year - 1, -1):  # Most recent first
            logger.info(f"Processing year {year}...")

            # Get all issues for this year from archive
            issues = self._get_issues_from_archive_year(year)

            if not issues:
                logger.warning(f"No issues found for {year}, trying direct date scan...")
                # Fallback: try direct date scanning for recent years
                if year >= current_year - 1:
                    issues = self._discover_recent_gazettes(max_days=180, start_year=year)

            # Process each gazette issue
            for issue in issues:
                date_str = issue["date"]
                issue_num = issue["issue_number"]

                # Parse date
                try:
                    year_p, month_p, day_p = map(int, date_str.split("-"))
                except ValueError:
                    continue

                # Get acts from this gazette issue
                gazette_acts = self._get_acts_from_gazette_page(year_p, month_p, day_p, issue_num)

                for act in gazette_acts:
                    code = act["code"]
                    pub_date = act["pub_date"]

                    if code in seen_codes:
                        continue
                    seen_codes.add(code)

                    # Fetch full text
                    logger.info(f"Fetching full text for {code}...")
                    full_text, act_type, title, doc_date = self._fetch_act_full_text(code, pub_date)

                    if not full_text:
                        logger.debug(f"No full text for act {code}, skipping")
                        continue

                    if not doc_date:
                        doc_date = pub_date

                    yield {
                        "code": code,
                        "title": title,
                        "act_type": act_type,
                        "doc_date": doc_date,
                        "full_text": full_text,
                        "gazette_date": pub_date,
                        "gazette_number": issue_num,
                        "pub_date": pub_date,
                    }

                    documents_yielded += 1

                    if documents_yielded % 100 == 0:
                        logger.info(f"Progress: {documents_yielded} documents yielded")

        logger.info(f"Fetch complete: {documents_yielded} total documents")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents updated since the given date.

        Uses RSS feed which returns the most recent gazette issue.
        """
        logger.info(f"Checking for updates since {since}...")

        # Get acts from RSS feed
        acts = self._get_acts_from_rss("SG")

        for act in acts:
            code = act["code"]
            pub_date = act["pub_date"]
            rss_title = act["title"]

            # Check if this act is after the since date
            try:
                act_date = datetime.strptime(pub_date, "%Y-%m-%d")
                if act_date.replace(tzinfo=timezone.utc) < since:
                    continue
            except:
                pass

            full_text, act_type, title, doc_date = self._fetch_act_full_text(code, pub_date)

            if not full_text:
                continue

            if not title:
                title = rss_title

            if not doc_date:
                doc_date = pub_date

            yield {
                "code": code,
                "title": title,
                "act_type": act_type,
                "doc_date": doc_date,
                "full_text": full_text,
                "gazette_date": pub_date,
                "gazette_number": 0,
                "pub_date": pub_date,
            }

    def normalize(self, raw: dict) -> dict:
        """
        Transform raw document data into standard schema.

        CRITICAL: Includes full text in the 'text' field.
        """
        code = raw.get("code", "")
        title = raw.get("title", "")
        full_text = raw.get("full_text", "")
        act_type = raw.get("act_type", "unknown")
        doc_date = raw.get("doc_date", "")
        gazette_date = raw.get("gazette_date", "")
        gazette_number = raw.get("gazette_number", 0)
        pub_date = raw.get("pub_date", "")

        # Create unique document ID
        doc_id = code if code else f"unknown_{datetime.now().timestamp()}"

        # Extract year from document date or gazette date
        year = 0
        if doc_date and len(doc_date) >= 4:
            year = int(doc_date[:4])
        elif gazette_date and len(gazette_date) >= 4:
            year = int(gazette_date[:4])

        # Build URL
        url = f"{GU_BASE_URL}/atto/serie_generale/caricaDettaglioAtto/originario?atto.dataPubblicazioneGazzetta={pub_date}&atto.codiceRedazionale={code}"

        # Extract act number from code (e.g., 24G00164 -> 164)
        act_number = 0
        if code and len(code) >= 5:
            try:
                act_number = int(code[-5:].lstrip("0") or "0")
            except:
                pass

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "IT/GazzettaUfficiale",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": doc_date or gazette_date,
            "url": url,
            # Additional metadata
            "doc_id": doc_id,
            "year": year,
            "act_type": act_type,
            "act_number": act_number,
            "gazette_date": gazette_date,
            "gazette_number": gazette_number,
            "language": "it",
            "eli_code": code,
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Gazzetta Ufficiale endpoints...")

        # Test home page
        print("\n1. Testing home page...")
        try:
            resp = self.client.get("/home")
            print(f"   Status: {resp.status_code}")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test RSS feed
        print("\n2. Testing RSS feed...")
        acts = []
        try:
            acts = self._get_acts_from_rss("SG")
            print(f"   Found {len(acts)} acts in RSS feed")
            if acts:
                print(f"   Sample act: {acts[0]['code']} - {acts[0]['title'][:50]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test fetching full text
        if acts:
            print("\n3. Testing full text fetch...")
            try:
                act = acts[0]
                full_text, act_type, title, doc_date = self._fetch_act_full_text(
                    act["code"], act["pub_date"]
                )
                print(f"   Text length: {len(full_text)} characters")
                print(f"   Act type: {act_type}")
                print(f"   Title: {title[:80]}..." if title else "   No title")
                print(f"   Date: {doc_date}")
                if full_text:
                    print(f"   Sample text: {full_text[:200]}...")
            except Exception as e:
                print(f"   ERROR: {e}")

        print("\nTest complete!")


def main():
    scraper = GazzettaUfficialeScraper()

    if len(sys.argv) < 2:
        print(
            "Usage: python bootstrap.py [bootstrap|update|test] "
            "[--sample] [--sample-size N]"
        )
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 12
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "test":
        scraper.test_connection()

    elif command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(
                f"\nSample complete: "
                f"{stats.get('sample_records_saved', 0)} records saved to sample/"
            )
        else:
            stats = scraper.bootstrap()
            print(
                f"\nBootstrap complete: {stats['records_new']} new, "
                f"{stats['records_updated']} updated, "
                f"{stats['records_skipped']} skipped"
            )
        print(json.dumps(stats, indent=2))

    elif command == "update":
        stats = scraper.update()
        print(
            f"\nUpdate complete: {stats['records_new']} new, "
            f"{stats['records_updated']} updated"
        )
        print(json.dumps(stats, indent=2))

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
