"""
World Wide Law - Serbian Constitutional Court Scraper

Fetches case law from the Serbian Constitutional Court (Ustavni sud).
Data source: https://ustavni.sud.rs
Method: HTML scraping via search pagination and detail pages
Coverage: 1998 onwards (~21,000 decisions)
"""

import re
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin

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
logger = logging.getLogger("RS/ConstitutionalCourt")


class SerbianConstitutionalCourtScraper(BaseScraper):
    """
    Scraper for: Serbian Constitutional Court (Ustavni sud)
    Country: RS
    URL: https://ustavni.sud.rs

    Data types: case_law
    Auth: none
    """

    # Serbian month names (both Latin and Cyrillic)
    SERBIAN_MONTHS = {
        # Latin
        "januar": 1, "februar": 2, "mart": 3, "april": 4,
        "maj": 5, "jun": 6, "jul": 7, "avgust": 8,
        "septembar": 9, "oktobar": 10, "novembar": 11, "decembar": 12,
        # Cyrillic
        "јануар": 1, "фебруар": 2, "март": 3, "април": 4,
        "мај": 5, "јун": 6, "јул": 7, "август": 8,
        "септембар": 9, "октобар": 10, "новембар": 11, "децембар": 12,
    }

    # Case type codes
    CASE_TYPES = {
        "Уж": "constitutional_complaint",
        "IУ": "constitutionality_general",
        "IУз": "constitutionality_laws",
        "IУм": "constitutionality_treaties",
        "IУо": "constitutionality_regulations",
        "IУа": "constitutionality_ap_acts",
        "IУл": "constitutionality_local_acts",
        "IУп": "constitutionality_decrees",
        "IIУ": "pre_promulgation_review",
        "IIIУ": "jurisdiction_conflict",
        "IVУ": "presidential_impeachment",
        "VУ": "electoral_dispute",
        "VIУ": "mandate_confirmation",
        "VIIУ": "prohibition_org",
        "VIIIУ": "judge_prosecutor_appeal",
        "IXУ": "individual_act_appeal",
        "XУ": "other",
    }

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=self.config.get("api", {}).get("base_url", "https://ustavni.sud.rs"),
            headers=self._auth_headers,
            verify=False,  # Site has SSL cert issues
        )
        self.page_size = 20

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Yield all documents by paginating through search results.
        """
        offset = 0
        total_fetched = 0
        consecutive_empty_pages = 0  # Track consecutive empty pages to detect issues

        # First request to get total count
        total_count = self._get_total_count()
        logger.info(f"Total decisions in database: {total_count}")

        if total_count == 0:
            logger.error("Total count is 0 - this indicates the site may be blocking requests or has changed structure")
            # Still try to fetch the first page as a fallback
            predmet_ids = self._fetch_search_page(0)
            if not predmet_ids:
                logger.error("First search page also returned 0 results. Aborting.")
                return
            # If first page has results despite total count being 0, continue

        while True:
            # Stop if we've fetched enough records (server returns results indefinitely)
            if total_count > 0 and offset >= total_count:
                logger.info(f"Reached total count ({total_count}), stopping pagination")
                break

            logger.info(f"Fetching page at offset {offset}")
            predmet_ids = self._fetch_search_page(offset)

            if not predmet_ids:
                consecutive_empty_pages += 1
                if offset == 0:
                    logger.error("First page returned 0 results! Cannot proceed.")
                    logger.error("This may indicate IP blocking, captcha, or site structure change.")
                    break
                elif consecutive_empty_pages >= 3:
                    logger.warning(f"Got {consecutive_empty_pages} consecutive empty pages at offset {offset}, stopping")
                    break
                else:
                    logger.warning(f"Empty page at offset {offset}, but continuing to check for more...")
                    offset += self.page_size
                    continue
            else:
                consecutive_empty_pages = 0  # Reset counter on successful page

            for predmet_id in predmet_ids:
                try:
                    doc = self._fetch_decision(predmet_id)
                    if doc:
                        total_fetched += 1
                        yield doc
                except Exception as e:
                    logger.warning(f"Failed to fetch decision {predmet_id}: {e}")
                    continue

            offset += len(predmet_ids)
            logger.info(f"Progress: {total_fetched}/{total_count} decisions fetched")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """
        Yield documents published since the given datetime.
        Results are sorted by date descending, so we stop when we hit older documents.
        """
        offset = 0

        while True:
            predmet_ids = self._fetch_search_page(offset, sort="dateDESC")

            if not predmet_ids:
                break

            found_old = False
            for predmet_id in predmet_ids:
                try:
                    doc = self._fetch_decision(predmet_id)
                    if doc:
                        date_str = doc.get("date", "")
                        if date_str:
                            try:
                                doc_date = datetime.strptime(date_str, "%d.%m.%Y.")
                                doc_date = doc_date.replace(tzinfo=timezone.utc)
                                if doc_date < since:
                                    found_old = True
                                    break
                            except ValueError:
                                pass
                        yield doc
                except Exception as e:
                    logger.warning(f"Failed to fetch decision {predmet_id}: {e}")
                    continue

            if found_old:
                break

            offset += len(predmet_ids)

    def _get_total_count(self) -> int:
        """Get total number of decisions from search page."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(
                "/sudska-praksa/baza-sudske-prakse",
                params={"limit": 1, "startfrom": 0, "sortBy": "dateDESC", "action": 1}
            )

            # Log response status for debugging
            logger.info(f"Search page response: status={resp.status_code}, length={len(resp.text)}")

            # Check for blocking/captcha pages
            if resp.status_code != 200:
                logger.error(f"Search page returned non-200 status: {resp.status_code}")
                return 0

            if len(resp.text) < 1000:
                logger.error(f"Search page response suspiciously short ({len(resp.text)} bytes), may be blocked")
                logger.debug(f"Response content: {resp.text[:500]}")
                return 0

            soup = BeautifulSoup(resp.text, "html.parser")

            # Look for "Укупно пронађено XXXXX предмета"
            h1 = soup.find("h1", string=re.compile(r"Укупно пронађено"))
            if h1:
                match = re.search(r"(\d+)", h1.get_text())
                if match:
                    return int(match.group(1))

            # Fallback: log what we found for debugging
            all_h1 = soup.find_all("h1")
            logger.warning(f"Could not find total count header. H1 tags found: {[h.get_text()[:50] for h in all_h1[:5]]}")

            return 0
        except Exception as e:
            logger.error(f"Failed to get total count: {e}")
            return 0

    def _fetch_search_page(self, offset: int = 0, sort: str = "dateDESC") -> list[str]:
        """
        Fetch a page of search results and extract PredmetId values.
        Returns list of predmet_ids.
        """
        try:
            self.rate_limiter.wait()
            resp = self.client.get(
                "/sudska-praksa/baza-sudske-prakse",
                params={
                    "limit": self.page_size,
                    "startfrom": offset,
                    "sortBy": sort,
                    "action": 1
                }
            )

            # Check for blocking/error responses
            if resp.status_code != 200:
                logger.error(f"Search page at offset {offset} returned status {resp.status_code}")
                return []

            if len(resp.text) < 1000:
                logger.error(f"Search page at offset {offset} response too short ({len(resp.text)} bytes)")
                logger.debug(f"Response content: {resp.text[:500]}")
                return []

            soup = BeautifulSoup(resp.text, "html.parser")

            # Find all detail page links
            links = soup.find_all("a", href=re.compile(r"pregled-dokumenta\?PredmetId="))
            predmet_ids = []

            for link in links:
                href = link.get("href", "")
                match = re.search(r"PredmetId=(\d+)", href)
                if match:
                    predmet_id = match.group(1)
                    if predmet_id not in predmet_ids:
                        predmet_ids.append(predmet_id)

            if not predmet_ids and offset == 0:
                # First page returned no results - this is unusual, log details
                logger.warning(f"No PredmetId links found on first page. Logging page structure...")
                logger.warning(f"Total links on page: {len(soup.find_all('a'))}")
                all_links = soup.find_all("a", href=True)[:10]
                logger.warning(f"First 10 links: {[a.get('href')[:50] for a in all_links]}")

            logger.info(f"Found {len(predmet_ids)} decisions at offset {offset}")
            return predmet_ids

        except Exception as e:
            logger.error(f"Failed to fetch search page at offset {offset}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []

    def _fetch_decision(self, predmet_id: str) -> Optional[dict]:
        """
        Fetch a single decision by PredmetId and extract all content.
        Returns raw document dict with full text.
        """
        self.rate_limiter.wait()
        url = f"/sudska-praksa/baza-sudske-prakse/pregled-dokumenta?PredmetId={predmet_id}"
        resp = self.client.get(url)
        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract case reference from title
        # Format: "Преглед документа - Пракса Уставног суда - Уж-11232/2017"
        title_tag = soup.find("title")
        case_reference = ""
        if title_tag:
            title_text = title_tag.get_text()
            match = re.search(r"-\s*([A-Za-zА-Яа-яЁё]+[-‐][0-9/]+)\s*$", title_text)
            if match:
                case_reference = match.group(1)

        # Extract main content from text-content div
        content_div = soup.find("div", class_="text-content")
        if not content_div:
            content_div = soup.find("div", class_="doc")

        if not content_div:
            logger.warning(f"No content found for decision {predmet_id}")
            return None

        # Extract full text
        full_text = self._extract_clean_text(content_div)

        if not full_text or len(full_text) < 100:
            logger.warning(f"Insufficient text content for decision {predmet_id}")
            return None

        # Extract metadata from sidebar or inline
        metadata = self._extract_metadata(soup)

        # Try to extract date from content if not in metadata
        date_str = metadata.get("date", "")
        if not date_str:
            date_str = self._extract_date_from_text(full_text)

        # Try to extract case reference from content if not from title
        if not case_reference:
            case_reference = metadata.get("case_reference", "")
        if not case_reference:
            case_reference = self._extract_case_reference_from_text(full_text)

        # Determine case type from reference
        case_type = self._determine_case_type(case_reference)

        return {
            "predmet_id": predmet_id,
            "case_reference": case_reference,
            "date": date_str,
            "outcome": metadata.get("outcome", ""),
            "case_type": case_type,
            "legal_area": metadata.get("legal_area", ""),
            "constitutional_articles": metadata.get("constitutional_articles", ""),
            "applicant": metadata.get("applicant", ""),
            "notes": metadata.get("notes", ""),
            "full_text": full_text,
            "url": f"https://ustavni.sud.rs{url}",
        }

    def _extract_metadata(self, soup: BeautifulSoup) -> dict:
        """Extract metadata from the decision page."""
        metadata = {}

        # Look for metadata in the details section or list items
        # Format: <li><label>Датум доношења одлуке:</label> 11.12.2025.</li>
        for li in soup.find_all("li"):
            label = li.find("label")
            if not label:
                continue

            label_text = label.get_text(strip=True)
            value_text = li.get_text(strip=True).replace(label_text, "").strip()

            if "Датум доношења" in label_text or "Датум" in label_text:
                metadata["date"] = value_text
            elif "Предмет" in label_text:
                metadata["case_reference"] = value_text
            elif "Исход" in label_text:
                metadata["outcome"] = value_text
            elif "Правна област" in label_text:
                metadata["legal_area"] = value_text
            elif "Члан" in label_text and "Устава" in label_text:
                metadata["constitutional_articles"] = value_text
            elif "Подносилац" in label_text:
                metadata["applicant"] = value_text
            elif "Напомена" in label_text:
                metadata["notes"] = value_text

        return metadata

    def _extract_clean_text(self, content_div) -> str:
        """Extract clean text from the document, removing HTML artifacts."""
        if not content_div:
            return ""

        # Remove script, style, and nav elements
        for element in content_div.find_all(["script", "style", "img", "nav", "aside"]):
            element.decompose()

        # Get text with line breaks preserved
        text = content_div.get_text(separator="\n", strip=True)

        # Clean up excessive whitespace
        text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
        text = re.sub(r" +", " ", text)
        text = re.sub(r"^\s+", "", text, flags=re.MULTILINE)

        # Remove HTML entities
        text = text.replace("\xa0", " ")
        text = text.replace("&nbsp;", " ")

        return text.strip()

    def _extract_date_from_text(self, text: str) -> str:
        """Extract decision date from text content."""
        # Look for patterns like "11. децембра 2025. године" or "11.12.2025."
        # Serbian format: DD. месец YYYY. године
        patterns = [
            r"(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})\.",  # 11.12.2025.
            r"(\d{1,2})\.\s*([а-яА-Яa-zA-Z]+)\s*(\d{4})\.\s*године",  # 11. децембра 2025. године
        ]

        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                groups = match.groups()
                if len(groups) == 3:
                    if groups[1].isdigit():
                        # DD.MM.YYYY format
                        return f"{groups[0]}.{groups[1]}.{groups[2]}."
                    else:
                        # DD. месец YYYY format
                        month = self.SERBIAN_MONTHS.get(groups[1].lower())
                        if month:
                            return f"{groups[0]}.{month}.{groups[2]}."

        return ""

    def _extract_case_reference_from_text(self, text: str) -> str:
        """Extract case reference number from text content."""
        # Match patterns like Уж-11232/2017, IУз-53/2004, etc.
        patterns = [
            r"(Уж[-‐]\d+/\d+)",
            r"(I+У[зомалп]?[-‐]\d+/\d+)",
            r"(V+У[-‐]\d+/\d+)",
            r"(X+У[-‐]\d+/\d+)",
        ]

        for pattern in patterns:
            match = re.search(pattern, text)
            if match:
                return match.group(1)

        return ""

    def _determine_case_type(self, case_reference: str) -> str:
        """Determine case type from reference number."""
        if not case_reference:
            return "unknown"

        # Extract the prefix (e.g., "Уж" from "Уж-11232/2017")
        match = re.match(r"([A-Za-zА-Яа-яIVX]+)", case_reference)
        if match:
            prefix = match.group(1)
            return self.CASE_TYPES.get(prefix, "other")

        return "unknown"

    def _parse_serbian_date(self, date_str: str) -> Optional[str]:
        """
        Parse Serbian date format and return ISO format.
        Input formats:
            - "11.12.2025." (DD.MM.YYYY)
            - "11. децембра 2025. године"
        """
        if not date_str:
            return None

        # DD.MM.YYYY. format
        match = re.match(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", date_str)
        if match:
            day, month, year = match.groups()
            try:
                dt = datetime(int(year), int(month), int(day))
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        # DD. месец YYYY format
        match = re.search(r"(\d{1,2})\.\s*([а-яА-Яa-zA-Z]+)\s*(\d{4})", date_str)
        if match:
            day, month_name, year = match.groups()
            month = self.SERBIAN_MONTHS.get(month_name.lower())
            if month:
                try:
                    dt = datetime(int(year), month, int(day))
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass

        return None

    def normalize(self, raw: dict) -> dict:
        """
        Transform a raw document into the standard schema.

        CRITICAL: Includes FULL TEXT from document content.
        """
        predmet_id = raw.get("predmet_id", "")
        case_reference = raw.get("case_reference", "")

        # Parse date to ISO format
        date_iso = self._parse_serbian_date(raw.get("date", ""))

        # Build title
        case_type = raw.get("case_type", "")
        outcome = raw.get("outcome", "")
        if case_reference:
            title = f"Constitutional Court Decision {case_reference}"
            if outcome:
                title += f" - {outcome}"
        else:
            title = f"Constitutional Court Decision (ID: {predmet_id})"

        # Get full text
        full_text = raw.get("full_text", "")

        return {
            "_id": f"RS/ConstitutionalCourt/{predmet_id}",
            "_source": "RS/ConstitutionalCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),

            # Standard required fields
            "title": title,
            "text": full_text,  # MANDATORY FULL TEXT
            "date": date_iso,
            "url": raw.get("url"),

            # Source-specific fields
            "predmet_id": predmet_id,
            "case_reference": case_reference,
            "case_type": case_type,
            "outcome": outcome,
            "legal_area": raw.get("legal_area"),
            "constitutional_articles": raw.get("constitutional_articles"),
            "applicant": raw.get("applicant"),
            "notes": raw.get("notes"),

            # Keep raw data for debugging
            "_raw": raw,
        }


# ── CLI Entry Point ───────────────────────────────────────────────

def main():
    scraper = SerbianConstitutionalCourtScraper()

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
