#!/usr/bin/env python3
"""
INTL/IAEASafetyStandards -- IAEA Safety Standards Database

Fetches the full text of IAEA Safety Standards from two sources:
1. NSS-OUI (Nuclear Safety and Security Online User Interface) — HTML full text
   for Fundamentals, Requirements, and select legacy guides (~20 publications)
2. INIS API + www-pub.iaea.org — PDF downloads for Safety Guides (SSG/GSG)

All PDFs are freely downloadable from www-pub.iaea.org without authentication.
NSS-OUI is publicly accessible at nucleus-apps.iaea.org/nss-oui.

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py update               # Same as bootstrap
  python bootstrap.py test-api             # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
from html.parser import HTMLParser
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown, preload_existing_ids

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.IAEASafetyStandards")

SOURCE_ID = "INTL/IAEASafetyStandards"
NSS_OUI_BASE = "https://nucleus-apps.iaea.org/nss-oui"
INIS_API = "https://inis.iaea.org/api/records"

# ── NSS-OUI published collections (Fundamentals + Requirements + legacy) ─────
# These 20 publications have full HTML text on the NSS-OUI platform.
NSS_OUI_STANDARDS = [
    {"standard_id": "SF-1", "title": "Fundamental Safety Principles", "year": 2006,
     "pub_type": "Safety Fundamentals",
     "collection_id": "m_a46feae7-82b7-4d40-887d-466f7e874594"},
    {"standard_id": "GSR Part 1 (Rev. 1)", "title": "Governmental, Legal and Regulatory Framework for Safety", "year": 2016,
     "pub_type": "General Safety Requirements",
     "collection_id": "m_8fd5e365-0c72-4e28-9ec7-3a2452df2b43"},
    {"standard_id": "GSR Part 2", "title": "Leadership and Management for Safety", "year": 2016,
     "pub_type": "General Safety Requirements",
     "collection_id": "m_f816f75b-4a2f-47a0-8578-91ceb427bc8b"},
    {"standard_id": "GSR Part 3", "title": "Radiation Protection and Safety of Radiation Sources: International Basic Safety Standards", "year": 2014,
     "pub_type": "General Safety Requirements",
     "collection_id": "m_3761d926-c16f-4477-a63b-741a9db1c16c"},
    {"standard_id": "GSR Part 4 (Rev. 1)", "title": "Safety Assessment for Facilities and Activities", "year": 2016,
     "pub_type": "General Safety Requirements",
     "collection_id": "m_0d27280c-1967-4e9c-aa22-4394ea3acb5b"},
    {"standard_id": "GSR Part 5", "title": "Predisposal Management of Radioactive Waste", "year": 2009,
     "pub_type": "General Safety Requirements",
     "collection_id": "m_a973cd6d-a90c-46dc-89a7-a4e712237a6c"},
    {"standard_id": "GSR Part 6", "title": "Decommissioning of Facilities", "year": 2014,
     "pub_type": "General Safety Requirements",
     "collection_id": "m_fdfbc892-74a3-4654-a9ca-df41de556ee4"},
    {"standard_id": "GSR Part 7", "title": "Preparedness and Response for a Nuclear or Radiological Emergency", "year": 2015,
     "pub_type": "General Safety Requirements",
     "collection_id": "m_0fce2a40-ac12-4db9-ad3b-e4bfc93f678e"},
    {"standard_id": "SSR-1", "title": "Site Evaluation for Nuclear Installations", "year": 2019,
     "pub_type": "Specific Safety Requirements",
     "collection_id": "m_75270619-c2e0-47e5-8e36-8dcd4b688221"},
    {"standard_id": "SSR-2/1 (Rev. 1)", "title": "Safety of Nuclear Power Plants: Design", "year": 2016,
     "pub_type": "Specific Safety Requirements",
     "collection_id": "m_daad92bb-3bb0-4445-a6ec-a287e55e9664"},
    {"standard_id": "SSR-2/2 (Rev. 1)", "title": "Safety of Nuclear Power Plants: Commissioning and Operation", "year": 2016,
     "pub_type": "Specific Safety Requirements",
     "collection_id": "m_75d87c76-77bf-4e42-b516-3b55ca01bd2b"},
    {"standard_id": "SSR-3", "title": "Safety of Research Reactors", "year": 2016,
     "pub_type": "Specific Safety Requirements",
     "collection_id": "m_d7cd9e00-83d6-4a0a-9101-a5b72a7550f7"},
    {"standard_id": "SSR-4", "title": "Safety of Nuclear Fuel Cycle Facilities", "year": 2017,
     "pub_type": "Specific Safety Requirements",
     "collection_id": "m_6ed2bf4f-b00e-4436-bde0-abf0d5a94e9d"},
    {"standard_id": "SSR-5", "title": "Disposal of Radioactive Waste", "year": 2011,
     "pub_type": "Specific Safety Requirements",
     "collection_id": "m_7eeb23f5-bdc4-4672-ad64-39821db04214"},
    {"standard_id": "SSR-6 (Rev. 2)", "title": "Regulations for the Safe Transport of Radioactive Material, 2025 Edition", "year": 2025,
     "pub_type": "Specific Safety Requirements",
     "collection_id": "m_b03c4897-38e3-41f4-a2d4-f3670bd8b300"},
    {"standard_id": "GS-G-2.1", "title": "Arrangements for Preparedness for a Nuclear or Radiological Emergency", "year": 2007,
     "pub_type": "Safety Guide (Legacy)",
     "collection_id": "m_969f7d33-ffba-4255-af1d-cf9280a70fa3"},
    {"standard_id": "GS-G-3.1", "title": "Application of the Management System for Facilities and Activities", "year": 2006,
     "pub_type": "Safety Guide (Legacy)",
     "collection_id": "m_3a0f5d6c-9f82-4344-9621-5217be2e46b7"},
    {"standard_id": "GS-G-3.5", "title": "The Management System for Nuclear Installations", "year": 2009,
     "pub_type": "Safety Guide (Legacy)",
     "collection_id": "m_9e9c0b0c-815b-4ac8-9d13-d8aeea366a12"},
    {"standard_id": "GSG-1", "title": "Classification of Radioactive Waste", "year": 2009,
     "pub_type": "General Safety Guide",
     "collection_id": "m_8ab0b2ed-f483-4b86-96e9-53d2ba323700"},
    {"standard_id": "GSG-2", "title": "Criteria for Use in Preparedness and Response for a Nuclear or Radiological Emergency", "year": 2011,
     "pub_type": "General Safety Guide",
     "collection_id": "m_76287e92-e1c2-4d80-bc8a-a02e1b5a2131"},
]


class _HTMLTextExtractor(HTMLParser):
    """Strip HTML tags and extract clean text."""

    SKIP_TAGS = {"script", "style", "nav", "header", "footer", "noscript"}

    def __init__(self):
        super().__init__()
        self.parts: List[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag, attrs):
        if tag in self.SKIP_TAGS:
            self._skip_depth += 1
        if tag in ("br", "p", "div", "li", "h1", "h2", "h3", "h4", "h5", "h6", "tr"):
            self.parts.append("\n")

    def handle_endtag(self, tag):
        if tag in self.SKIP_TAGS:
            self._skip_depth = max(0, self._skip_depth - 1)
        if tag in ("p", "div", "li", "h1", "h2", "h3", "h4", "h5", "h6", "tr"):
            self.parts.append("\n")

    def handle_data(self, data):
        if self._skip_depth == 0:
            self.parts.append(data)


def _html_to_text(html: str) -> str:
    """Convert HTML to clean text."""
    extractor = _HTMLTextExtractor()
    extractor.feed(html)
    raw = "".join(extractor.parts)
    # Collapse whitespace
    lines = []
    for line in raw.split("\n"):
        cleaned = " ".join(line.split())
        if cleaned:
            lines.append(cleaned)
    return "\n".join(lines)


class IAEASafetyStandardsScraper(BaseScraper):
    """Scraper for INTL/IAEASafetyStandards."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/131.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

    def _fetch_nss_oui_text(self, collection_id: str) -> Optional[str]:
        """Fetch full text from NSS-OUI HTML page."""
        url = (f"{NSS_OUI_BASE}/Content/Index"
               f"?CollectionId={collection_id}&type=PublishedCollection")
        try:
            resp = self.session.get(url, timeout=60)
            resp.raise_for_status()
            text = _html_to_text(resp.text)
            # Remove navigation boilerplate from start/end
            # The content starts after "Content" header and the ToC
            return text.strip()
        except Exception as e:
            logger.error(f"Failed to fetch NSS-OUI collection {collection_id}: {e}")
            return None

    def _search_inis_pdf_standards(self, page: int = 1, size: int = 50) -> List[Dict[str, Any]]:
        """Search INIS API for Safety Standards with PDF URLs."""
        params = {
            "q": '"IAEA Safety Standards Series" "www-pub.iaea.org"',
            "size": size,
            "page": page,
            "sort": "newest",
        }
        try:
            resp = self.session.get(INIS_API, params=params, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            return data.get("hits", {}).get("hits", [])
        except Exception as e:
            logger.error(f"INIS API search failed: {e}")
            return []

    def _extract_pdf_url(self, record: Dict) -> Optional[str]:
        """Extract www-pub.iaea.org PDF URL from INIS record identifiers."""
        identifiers = record.get("metadata", {}).get("identifiers", [])
        for ident in identifiers:
            raw = ident.get("identifier", "")
            # May contain comma-separated URLs
            for url in raw.split(","):
                url = url.strip()
                if "www-pub.iaea.org" in url and url.lower().endswith(".pdf"):
                    return url.replace("http://", "https://")
        return None

    def _extract_inis_title(self, record: Dict) -> str:
        """Get clean title from INIS record."""
        title = record.get("metadata", {}).get("title", "")
        # Strip HTML tags
        return re.sub(r"<[^>]+>", "", title).strip()

    def _extract_standard_id_from_title(self, title: str) -> Optional[str]:
        """Try to extract standard ID like SSG-14, GSG-8 from title."""
        # Look for patterns like "No. SSG-14" or just "SSG-14"
        m = re.search(r"(?:No\.\s*)?((?:SSG|GSG|SSR|GSR|SF|GS-[GR])-?\d[\w./]*)", title)
        if m:
            return m.group(1)
        return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw record into standard schema."""
        std_id = raw.get("standard_id", "unknown")
        doc_id = f"IAEA-SS-{std_id}".replace(" ", "-").replace("/", "-")

        title = raw.get("title", "")
        if std_id and std_id not in title:
            title = f"IAEA {std_id}: {title}"

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": raw.get("date", None),
            "url": raw.get("url", ""),
            "standard_id": std_id,
            "publication_type": raw.get("pub_type", ""),
            "year": raw.get("year"),
        }

    def _fetch_nss_oui_standards(self, sample: bool = False) -> Generator[Dict, None, None]:
        """Fetch standards from NSS-OUI (HTML full text)."""
        items = NSS_OUI_STANDARDS[:15] if sample else NSS_OUI_STANDARDS
        existing = preload_existing_ids(SOURCE_ID, table="legislation")

        for entry in items:
            std_id = entry["standard_id"]
            doc_id = f"IAEA-SS-{std_id}".replace(" ", "-").replace("/", "-")

            if doc_id in existing:
                logger.info(f"Skipping {std_id} (already in Neon)")
                continue

            self.rate_limiter.wait()
            text = self._fetch_nss_oui_text(entry["collection_id"])

            if not text or len(text) < 100:
                logger.warning(f"Insufficient text for {std_id}: {len(text) if text else 0} chars")
                continue

            url = (f"{NSS_OUI_BASE}/Content/Index"
                   f"?CollectionId={entry['collection_id']}&type=PublishedCollection")

            yield {
                "standard_id": std_id,
                "title": entry["title"],
                "text": text,
                "date": f"{entry['year']}-01-01",
                "url": url,
                "pub_type": entry["pub_type"],
                "year": entry["year"],
            }

    def _fetch_inis_pdf_standards(self, existing: set, sample: bool = False) -> Generator[Dict, None, None]:
        """Fetch additional standards from INIS API (PDF download + extraction)."""
        if sample:
            return  # NSS-OUI provides enough for sample mode

        # Track standard IDs we already have from NSS-OUI
        nss_ids = {e["standard_id"] for e in NSS_OUI_STANDARDS}
        seen_titles = set()
        total = 0

        for page in range(1, 20):  # Max 20 pages
            hits = self._search_inis_pdf_standards(page=page, size=50)
            if not hits:
                break

            for record in hits:
                title = self._extract_inis_title(record)
                pdf_url = self._extract_pdf_url(record)

                if not pdf_url or not title:
                    continue

                # Skip non-English editions
                title_lower = title.lower()
                if any(lang in title_lower for lang in
                       ("chinese edition", "russian edition", "arabic edition",
                        "french edition", "spanish edition", "(édition")):
                    continue

                # Deduplicate by title
                title_key = re.sub(r"\s+", " ", title.lower().strip())
                if title_key in seen_titles:
                    continue
                seen_titles.add(title_key)

                std_id = self._extract_standard_id_from_title(title)
                if std_id and std_id in nss_ids:
                    continue  # Already fetched from NSS-OUI

                doc_id = f"IAEA-SS-{std_id or title[:40]}".replace(" ", "-").replace("/", "-")
                if doc_id in existing:
                    continue

                self.rate_limiter.wait()
                text = extract_pdf_markdown(
                    source=SOURCE_ID,
                    source_id=doc_id,
                    pdf_url=pdf_url,
                    table="legislation",
                )

                if not text or len(text) < 100:
                    logger.warning(f"Insufficient PDF text for {title[:60]}: "
                                   f"{len(text) if text else 0} chars")
                    continue

                pub_date = record.get("metadata", {}).get("publication_date", "")
                year_match = re.match(r"(\d{4})", pub_date)
                year = int(year_match.group(1)) if year_match else None

                yield {
                    "standard_id": std_id or title[:60],
                    "title": title,
                    "text": text,
                    "date": f"{pub_date}-01-01" if len(pub_date) == 4 else pub_date,
                    "url": pdf_url,
                    "pub_type": "Safety Guide",
                    "year": year,
                }

                total += 1
                logger.info(f"[PDF {total}] {std_id or title[:40]}: {len(text)} chars")

            if not hits or len(hits) < 50:
                break

    def _fetch_documents(self, sample: bool = False) -> Generator[Dict, None, None]:
        """Core fetcher combining NSS-OUI and INIS sources."""
        total = 0

        # Phase 1: NSS-OUI full text (Fundamentals + Requirements)
        logger.info("Phase 1: Fetching from NSS-OUI (HTML full text)...")
        for raw in self._fetch_nss_oui_standards(sample=sample):
            total += 1
            logger.info(f"[{total}] {raw['standard_id']}: {len(raw['text'])} chars")
            yield raw

        if sample:
            logger.info(f"Sample mode: {total} records from NSS-OUI")
            return

        # Phase 2: INIS API + PDF downloads (Safety Guides)
        logger.info("Phase 2: Fetching Safety Guides from INIS/PDF...")
        existing = preload_existing_ids(SOURCE_ID, table="legislation")
        for raw in self._fetch_inis_pdf_standards(existing, sample=False):
            total += 1
            yield raw

        logger.info(f"TOTAL: {total} records")

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        yield from self._fetch_documents(sample=False)

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self._fetch_documents(sample=False)

    def test_api(self):
        """Quick connectivity test."""
        logger.info("Testing NSS-OUI connectivity...")
        text = self._fetch_nss_oui_text(NSS_OUI_STANDARDS[0]["collection_id"])
        if text and len(text) > 100:
            logger.info(f"NSS-OUI OK: SF-1 has {len(text)} chars")
            logger.info(f"Preview: {text[:200]}...")
        else:
            logger.error("NSS-OUI failed")
            return

        logger.info("Testing INIS API...")
        hits = self._search_inis_pdf_standards(page=1, size=5)
        if hits:
            logger.info(f"INIS API OK: {len(hits)} results")
            for h in hits[:2]:
                title = self._extract_inis_title(h)
                pdf = self._extract_pdf_url(h)
                logger.info(f"  {title[:60]} -> PDF: {pdf}")
        else:
            logger.error("INIS API returned no results")


def main():
    scraper = IAEASafetyStandardsScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test-api":
        scraper.test_api()
    elif command in ("bootstrap", "update"):
        stats = scraper.bootstrap(sample_mode=sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
