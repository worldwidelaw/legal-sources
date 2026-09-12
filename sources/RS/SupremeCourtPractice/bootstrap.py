"""
Legal Data Hunter - Serbian Supreme Court of Cassation (VKS) Scraper

Fetches case law from the former Supreme Court of Cassation (Vrhovni kasacioni sud).
Data source: https://vrh.sud.rs (court_type=vks)
Method: SOLR search + Drupal 7 detail page scraping
Coverage: ~29,500+ decisions (pre-2022, when the court was renamed to Supreme Court)
Complements RS/SupremeCourt which covers court_type=sc (post-2022 decisions).
"""

import re
import sys
import json
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urlparse

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
logger = logging.getLogger("RS/SupremeCourtPractice")


class SerbianVKSScraper(BaseScraper):
    """
    Scraper for: Serbian Supreme Court of Cassation (Vrhovni kasacioni sud)
    Country: RS
    URL: https://vrh.sud.rs
    Court type: vks (pre-2022 decisions)

    Data types: case_law
    Auth: none
    """

    CASE_TYPES = {
        "Kzz": "criminal_legality_protection",
        "Kž": "criminal_appeal",
        "Rev": "civil_revision",
        "Rev2": "civil_revision_second",
        "Prev": "pre_revision",
        "Gzz": "general_legality_protection",
        "Uzp": "unified_position",
        "Rsz": "request_legality",
        "Gz": "civil_general",
        "P": "civil_first_instance",
        "K": "criminal_first_instance",
        "Kd": "criminal_discipline",
        "Up": "administrative",
    }

    LEGAL_MATTERS = {
        "krivična": "criminal",
        "građanska": "civil",
        "upravna": "administrative",
        "radnopravna": "labor",
        "stečajna": "bankruptcy",
        "intelektualna": "intellectual_property",
        "stambena": "housing",
    }

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=self.config.get("api", {}).get("base_url", "https://vrh.sud.rs"),
            headers=self._auth_headers,
            verify=False,
        )
        self.page_size = 50

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all VKS decisions by paginating through SOLR search results."""
        page = 0
        total_fetched = 0

        while True:
            logger.info(f"Fetching search page {page}")
            decision_links = self._fetch_search_page(page)

            if not decision_links:
                logger.info(f"No more results at page {page}, stopping")
                break

            for link_info in decision_links:
                try:
                    doc = self._fetch_decision(link_info["url"], link_info["node_id"])
                    if doc:
                        total_fetched += 1
                        yield doc
                except Exception as e:
                    logger.warning(f"Failed to fetch decision {link_info['url']}: {e}")
                    continue

            page += 1
            logger.info(f"Progress: {total_fetched} decisions fetched so far")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield documents published since the given datetime."""
        page = 0

        while True:
            decision_links = self._fetch_search_page(page)

            if not decision_links:
                break

            found_old = False
            for link_info in decision_links:
                try:
                    doc = self._fetch_decision(link_info["url"], link_info["node_id"])
                    if doc:
                        published_at = doc.get("published_at", "")
                        if published_at:
                            try:
                                doc_date = datetime.fromisoformat(published_at.replace("+02:00", "+00:00"))
                                if doc_date < since:
                                    found_old = True
                                    break
                            except ValueError:
                                pass
                        yield doc
                except Exception as e:
                    logger.warning(f"Failed to fetch decision {link_info['url']}: {e}")
                    continue

            if found_old:
                break

            page += 1

    def _fetch_search_page(self, page: int = 0) -> list[dict]:
        """Fetch a page of SOLR search results for court_type=vks."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(
                "/sr-lat/solr-search-page/results",
                params={
                    "court_type": "vks",
                    "matter": "_none",
                    "registrant": "_none",
                    "sorting": "by_date_down",
                    "results": str(self.page_size),
                    "page": str(page),
                }
            )
            soup = BeautifulSoup(resp.text, "html.parser")

            decision_links = []
            base_url = "https://vrh.sud.rs"

            for link in soup.find_all("a", href=True):
                href = link.get("href", "")

                if any(skip in href for skip in [
                    "solr-search-page", "sites/default", "sites/all",
                    "textsize", "page=", "#", "javascript",
                    "sudska-vlast", "o-nama", "javnost", "korisni",
                    "galerija", "mapa-sajta", "kontakt", "informator",
                    "javne-nabavke", "publikacije", "stru%C4%8Dno",
                    "me%C4%91unarodna", "savetovanje", "pridru%C5%BEivanje",
                    "css", "js", "png", "jpg", "pdf"
                ]):
                    continue

                if href.startswith("http"):
                    if "vrh.sud.rs/sr-lat/" not in href:
                        continue
                    full_url = href
                elif href.startswith("/sr-lat/"):
                    full_url = base_url + href
                else:
                    continue

                node_id = None

                if not any(d["url"] == full_url for d in decision_links):
                    path = urlparse(full_url).path
                    if re.search(r"(kzz|rev|gzz|uzp|prev|rsz|gz|kž|kd)", path, re.IGNORECASE):
                        decision_links.append({
                            "url": full_url,
                            "node_id": node_id,
                        })

            logger.info(f"Found {len(decision_links)} decisions at page {page}")
            return decision_links

        except Exception as e:
            logger.error(f"Failed to fetch search page {page}: {e}")
            return []

    def _fetch_decision(self, url: str, node_id: Optional[str] = None) -> Optional[dict]:
        """Fetch a single decision by URL and extract all content."""
        self.rate_limiter.wait()
        resp = self.client.get(url.replace("https://vrh.sud.rs", ""))
        soup = BeautifulSoup(resp.text, "html.parser")

        if not node_id:
            body = soup.find("body")
            if body:
                classes = body.get("class", [])
                for cls in classes:
                    match = re.search(r"page-node-(\d+)", cls)
                    if match:
                        node_id = match.group(1)
                        break

            if not node_id:
                shortlink = soup.find("link", rel="shortlink")
                if shortlink:
                    href = shortlink.get("href", "")
                    match = re.search(r"node/(\d+)", href)
                    if match:
                        node_id = match.group(1)

        if not node_id:
            logger.warning(f"Could not extract node ID from {url}")
            return None

        title = ""
        og_title = soup.find("meta", property="og:title")
        if og_title:
            title = og_title.get("content", "")

        case_reference = self._extract_case_reference(title)

        published_at = ""
        pub_meta = soup.find("meta", property="article:published_time")
        if pub_meta:
            published_at = pub_meta.get("content", "")

        decision_date = ""

        article = soup.find("article", class_="node-court-practice")
        full_text = ""

        if article:
            body_field = article.find("div", class_="field-name-body")
            if body_field:
                full_text = self._extract_clean_text(body_field)
                decision_date = self._extract_date_from_text(full_text)

        if not full_text or len(full_text) < 100:
            logger.warning(f"Insufficient text content for decision {node_id}")
            return None

        case_type = self._determine_case_type(case_reference)
        matter = self._determine_matter(full_text, title)

        return {
            "node_id": node_id,
            "case_reference": case_reference,
            "title": title,
            "full_text": full_text,
            "decision_date": decision_date,
            "published_at": published_at,
            "case_type": case_type,
            "matter": matter,
            "url": url,
        }

    def _extract_case_reference(self, title: str) -> str:
        """Extract case reference number from title."""
        if not title:
            return ""

        patterns = [
            r"(Kzz\s*\d+/\d+)",
            r"(Kž\d?\s*\d+/\d+)",
            r"(Rev\d?\s*\d+/\d+)",
            r"(Prev\s*\d+/\d+)",
            r"(Gzz\s*\d+/\d+)",
            r"(Uzp\s*\d+/\d+)",
            r"(Rsz\s*\d+/\d+)",
            r"(Gz\s*\d+/\d+)",
            r"(Kd\s*\d+/\d+)",
        ]

        for pattern in patterns:
            match = re.search(pattern, title, re.IGNORECASE)
            if match:
                return match.group(1)

        return ""

    def _extract_date_from_text(self, text: str) -> str:
        """Extract decision date from text content."""
        if not text:
            return ""

        patterns = [
            r"(\d{1,2})\.(\d{1,2})\.(\d{4})\.\s*godina?",
            r"(\d{1,2})\.(\d{1,2})\.(\d{4})\.",
        ]

        for pattern in patterns:
            match = re.search(pattern, text[:500])
            if match:
                day, month, year = match.groups()
                try:
                    dt = datetime(int(year), int(month), int(day))
                    return dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass

        return ""

    def _extract_clean_text(self, content_div) -> str:
        """Extract clean text from the document, removing HTML artifacts."""
        if not content_div:
            return ""

        for element in content_div.find_all(["script", "style", "img", "nav", "aside"]):
            element.decompose()

        text = content_div.get_text(separator="\n", strip=True)

        text = re.sub(r"\n\s*\n\s*\n+", "\n\n", text)
        text = re.sub(r" +", " ", text)
        text = re.sub(r"^\s+", "", text, flags=re.MULTILINE)

        text = text.replace("\xa0", " ")
        text = text.replace("&nbsp;", " ")

        text = re.sub(r"Preuzmite dokument u PDF formatu\s*$", "", text, flags=re.IGNORECASE)

        return text.strip()

    def _determine_case_type(self, case_reference: str) -> str:
        """Determine case type from reference number."""
        if not case_reference:
            return "unknown"

        match = re.match(r"([A-Za-zžŽ]+)\d?", case_reference)
        if match:
            prefix = match.group(1)
            return self.CASE_TYPES.get(prefix, "other")

        return "unknown"

    def _determine_matter(self, text: str, title: str) -> str:
        """Determine legal matter from text content."""
        combined = (text[:2000] + " " + title).lower()

        for keyword, matter in self.LEGAL_MATTERS.items():
            if keyword in combined:
                return matter

        if "kzz" in title.lower() or "kž" in title.lower() or "kd" in title.lower():
            return "criminal"
        elif "rev" in title.lower() or "prev" in title.lower():
            return "civil"

        return "unknown"

    def normalize(self, raw: dict) -> dict:
        """Transform a raw document into the standard schema."""
        node_id = raw.get("node_id", "")
        case_reference = raw.get("case_reference", "")
        title = raw.get("title", "")

        date_iso = raw.get("decision_date", "")
        if not date_iso:
            pub = raw.get("published_at", "")
            if pub:
                try:
                    dt = datetime.fromisoformat(pub.replace("+02:00", "+00:00"))
                    date_iso = dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass

        if not title and case_reference:
            title = f"VKS Decision {case_reference}"

        full_text = raw.get("full_text", "")

        return {
            "_id": f"RS/SupremeCourtPractice/{node_id}",
            "_source": "RS/SupremeCourtPractice",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),

            "title": title,
            "text": full_text,
            "date": date_iso,
            "url": raw.get("url"),

            "node_id": node_id,
            "case_reference": case_reference,
            "case_type": raw.get("case_type"),
            "matter": raw.get("matter"),
            "published_at": raw.get("published_at"),

            "_raw": raw,
        }


# ── CLI Entry Point ───────────────────────────────────────────────

def main():
    scraper = SerbianVKSScraper()

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
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
