#!/usr/bin/env python3
"""
SK/UOOU -- Slovak Data Protection Authority (ÚOOÚ) Data Fetcher

Fetches methodological guidelines, EDPB guidance translations, and annual reports
from the Slovak Office for Personal Data Protection (Úrad na ochranu osobných údajov).

Note: The Slovak DPA does NOT publish enforcement decisions publicly (confirmed by
GDPRhub). This source covers their published doctrine content.

Strategy:
  - Scrape office guidelines listing page for document links
  - Scrape EDPB guidelines listing page for PDF links
  - Scrape annual reports page for PDF links
  - Download PDF/DOCX files and extract full text

Endpoints:
  - Office guidelines: https://dataprotection.gov.sk/en/legislation/guidelines-faq/office-guidelines/
  - EDPB guidelines: https://dataprotection.gov.sk/sk/legislativa-metodiky/metodiky-faq/metodiky-edpb/
  - Annual reports: https://dataprotection.gov.sk/files/annual-reports/

Data:
  - ~14 office methodological guidelines
  - ~97 EDPB guideline PDFs (SK + EN versions)
  - Annual reports (PDF)
  - Languages: Slovak (SK), some English (EN)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 10+ sample records for validation
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import io
import html
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

# PDF extraction
try:
    import pypdf
    HAS_PYPDF = True
except ImportError:
    HAS_PYPDF = False

# DOCX extraction
try:
    import docx
    HAS_DOCX = True
except ImportError:
    HAS_DOCX = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SK.uoou")

BASE_URL = "https://dataprotection.gov.sk"
OFFICE_GUIDELINES_URL = "/en/legislation/guidelines-faq/office-guidelines/"
EDPB_GUIDELINES_URL = "/sk/legislativa-metodiky/metodiky-faq/metodiky-edpb/"

# Known office guideline pages with their file download paths
# Scraped from the English site on 2026-03-22
OFFICE_GUIDELINE_PAGES = [
    {
        "slug": "methodological-guideline-1-2023-monitoring-camera-devices-natural-persons-family-home",
        "title": "Methodological guideline no. 1-2023 – Monitoring by camera devices of natural persons in a family home",
        "title_sk": "Metodické usmernenie č. 1-2023 – Monitorovanie kamerovými zariadeniami fyzických osôb v rodinnom dome",
    },
    {
        "slug": "obligations-shop-operator-from-point-view-personal-data-protection-updated-version-from-02/18/2020",
        "title": "Obligations of the e-shop operator from the point of view of personal data protection",
        "title_sk": "Povinnosti prevádzkovateľa e-shopu z pohľadu ochrany osobných údajov",
    },
    {
        "slug": "list-processing-operations-that-are-subject-an-impact-assessment",
        "title": "List of processing operations that are subject to an impact assessment",
        "title_sk": "Zoznam spracovateľských operácií, ktoré podliehajú posúdeniu vplyvu",
    },
    {
        "slug": "legal-processing-personal-data-during-clinical-trials",
        "title": "Legal processing of personal data during clinical trials",
        "title_sk": "Zákonné spracúvanie osobných údajov pri klinických skúšaniach",
    },
    {
        "slug": "compliance-methodology-processing-personal-data-school-environment",
        "title": "Compliance methodology for processing personal data in the school environment",
        "title_sk": "Metodika súladu pre spracúvanie osobných údajov v školskom prostredí",
    },
    {
        "slug": "methodological-guideline-2-2018-lawfulness-processing-updated-version-from-01/22/2019",
        "title": "Methodological guideline no. 2-2018 – Lawfulness of processing",
        "title_sk": "Metodické usmernenie č. 2-2018 – Zákonnosť spracúvania",
    },
    {
        "slug": "opinion-uoou-sr-on-delivery-documents-administrative-proceedings-inspection-administrative-file",
        "title": "Opinion of the ÚOOÚ SR on the delivery of documents in administrative proceedings",
        "title_sk": "Stanovisko ÚOOÚ SR k doručovaniu písomností v správnom konaní a nazeraniu do správneho spisu",
    },
    {
        "slug": "status-legal-entities-natural-persons-entrepreneurs-from-point-view-personal-data-protection",
        "title": "Status of legal entities and natural persons-entrepreneurs from the point of view of personal data protection",
        "title_sk": "Postavenie právnických osôb a fyzických osôb-podnikateľov z pohľadu ochrany osobných údajov",
    },
    {
        "slug": "the-concepts-basic-rights-freedoms-versus-rights-freedoms-how-identify-them",
        "title": "The concepts of 'basic rights and freedoms' versus 'rights and freedoms' – and how to identify them",
        "title_sk": "Pojmy 'základné práva a slobody' versus 'práva a slobody' – a ako ich identifikovať",
    },
    {
        "slug": "administrator-fan-page-located-on-social-network-position-operator",
        "title": "Administrator of a fan page located on a social network in the position of operator",
        "title_sk": "Správca fanúšikovskej stránky na sociálnej sieti v pozícii prevádzkovateľa",
    },
    {
        "slug": "to-notify-office-responsible-person-his-contact-details-according-new-legislation",
        "title": "To notify the office of the responsible person and his contact details according to the new legislation",
        "title_sk": "Oznámenie zodpovednej osoby a jej kontaktných údajov úradu podľa novej legislatívy",
    },
    {
        "slug": "authorized-person-instruction-authorized-person-according-new-legislation",
        "title": "Authorized person and instruction of the authorized person according to the new legislation",
        "title_sk": "Oprávnená osoba a poučenie oprávnenej osoby podľa novej legislatívy",
    },
    {
        "slug": "30-steps-compliance-new-legal-regulation-personal-data-protection",
        "title": "30 steps of compliance with the new legal regulation of personal data protection",
        "title_sk": "30 krokov súladu s novou právnou úpravou ochrany osobných údajov",
    },
    {
        "slug": "methodologies-office-cities-municipalities",
        "title": "Methodologies of the office for cities and municipalities",
        "title_sk": "Metodiky úradu pre mestá a obce",
    },
]


class SlovakDPAScraper(BaseScraper):
    """
    Scraper for SK/UOOU -- Slovak Data Protection Authority.
    Country: SK
    URL: https://dataprotection.gov.sk

    Data types: doctrine
    Auth: none (Open public access)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "WorldWideLaw/1.0 (Open Data Research)",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "sk,en;q=0.5",
            },
            timeout=60,
        )

    def _extract_pdf_text(self, pdf_bytes: bytes, max_size: int = 20_000_000) -> str:
        """Extract text from PDF bytes using pypdf."""
        if not HAS_PYPDF:
            logger.warning("pypdf not available, cannot extract PDF text")
            return ""
        if len(pdf_bytes) > max_size:
            logger.warning(f"PDF too large ({len(pdf_bytes)} bytes), skipping")
            return ""

        try:
            reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
            texts = []
            for page in reader.pages:
                text = page.extract_text()
                if text:
                    texts.append(text.strip())
            return "\n\n".join(texts)
        except Exception as e:
            logger.warning(f"Failed to extract PDF text: {e}")
            return ""

    def _extract_docx_text(self, docx_bytes: bytes) -> str:
        """Extract text from DOCX bytes using python-docx."""
        if not HAS_DOCX:
            logger.warning("python-docx not available, cannot extract DOCX text")
            return ""

        try:
            doc = docx.Document(io.BytesIO(docx_bytes))
            texts = []
            for para in doc.paragraphs:
                if para.text.strip():
                    texts.append(para.text.strip())
            return "\n\n".join(texts)
        except Exception as e:
            logger.warning(f"Failed to extract DOCX text: {e}")
            return ""

    def _download_and_extract(self, file_path: str) -> str:
        """Download a file and extract text based on extension."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(file_path)
            resp.raise_for_status()
            data = resp.content

            lower = file_path.lower()
            if lower.endswith(".pdf"):
                return self._extract_pdf_text(data)
            elif lower.endswith(".docx"):
                return self._extract_docx_text(data)
            else:
                # Try as text
                return resp.text.strip() if resp.text else ""
        except Exception as e:
            logger.warning(f"Failed to download {file_path}: {e}")
            return ""

    def _get_office_guideline_list(self) -> List[Dict[str, str]]:
        """Scrape the office guidelines listing page for guideline page links."""
        results = []
        try:
            self.rate_limiter.wait()
            resp = self.client.get(OFFICE_GUIDELINES_URL)
            resp.raise_for_status()
            content = resp.text

            # Pattern: <li><a href="/en/legislation/.../slug/">Title</a></li>
            pattern = r'<a\s+href="(/en/legislation/guidelines-faq/office-guidelines/[^"]+/)"[^>]*>([^<]+)</a>'
            matches = re.findall(pattern, content)
            for href, title in matches:
                clean_title = html.unescape(title).strip()
                results.append({"href": href, "title": clean_title})

            logger.info(f"Found {len(results)} office guidelines on listing page")
            return results
        except Exception as e:
            logger.warning(f"Failed to scrape office guidelines listing: {e}")
            return []

    def _scrape_office_guideline_page(self, page_url: str) -> Optional[str]:
        """Scrape an individual office guideline page to find the download link."""
        try:
            self.rate_limiter.wait()
            resp = self.client.get(page_url)
            resp.raise_for_status()
            content = resp.text

            # Look for download links: /files/metod-urad/... or /files/directives/...
            # HTML may use single or double quotes and may contain newlines inside href
            patterns = [
                r"""href=['"](/?files/[^'"]*\.(?:pdf|docx?))['"]""",
                r"""href=['"]([^'"]*dataprotection\.gov\.sk/files/[^'"]*\.(?:pdf|docx?))['"]""",
            ]
            for pattern in patterns:
                # Use DOTALL to match across newlines within href
                match = re.search(pattern, content, re.IGNORECASE | re.DOTALL)
                if match:
                    link = match.group(1)
                    # Clean newlines and whitespace from URL
                    link = re.sub(r'\s+', '', link)
                    if link.startswith("http"):
                        link = link.replace(BASE_URL, "")
                    if not link.startswith("/"):
                        link = "/" + link
                    return link

            logger.warning(f"No download link found on {page_url}")
            return None
        except Exception as e:
            logger.warning(f"Failed to scrape {page_url}: {e}")
            return None

    def _get_edpb_pdf_links(self) -> List[Dict[str, str]]:
        """Scrape the EDPB guidelines page for all PDF download links."""
        results = []
        try:
            self.rate_limiter.wait()
            resp = self.client.get(EDPB_GUIDELINES_URL)
            resp.raise_for_status()
            content = resp.text

            # HTML structure: <tr><td class="fN">Title</td><td ...><a href='/files/...pdf'>Stiahnuť</a></td></tr>
            # Parse table rows to get title + href pairs
            row_pattern = r"""<td\s+class="fN">([^<]+)</td>\s*<td[^>]*><a[^>]*href=['"](/files/metod-edpb/[^'"]+\.pdf)['"]"""
            row_matches = re.findall(row_pattern, content, re.IGNORECASE | re.DOTALL)

            seen = set()
            for title, href in row_matches:
                if href not in seen:
                    seen.add(href)
                    clean_title = html.unescape(title).strip()
                    if clean_title:
                        results.append({"href": href, "title": clean_title})

            # Fallback: find any PDF links not yet seen
            fallback_pattern = r"""href=['"](/files/metod-edpb/[^'"]+\.pdf)['"]"""
            for href in re.findall(fallback_pattern, content, re.IGNORECASE):
                if href not in seen:
                    seen.add(href)
                    filename = href.rsplit("/", 1)[-1]
                    title = filename.replace(".pdf", "").replace("_", " ").replace("-", " ").strip()
                    title = title[0].upper() + title[1:] if title else filename
                    results.append({"href": href, "title": title})

            logger.info(f"Found {len(results)} EDPB guideline PDFs")
            return results
        except Exception as e:
            logger.warning(f"Failed to scrape EDPB guidelines page: {e}")
            return []

    def _detect_language(self, file_path: str, title: str) -> str:
        """Detect language from filename or title patterns."""
        lower = file_path.lower() + " " + title.lower()
        # Slovak indicators
        sk_indicators = ["usmerneni", "odporúčan", "stanovisk", "tykajuc", "podla", "ochran",
                         "spracuvan", "nariadeni", "predpisov", "osobnych", "udajov"]
        for ind in sk_indicators:
            if ind in lower:
                return "sk"
        # English indicators
        en_indicators = ["guidelines", "recommendation", "opinion", "processing",
                         "regulation", "certification", "transfer"]
        for ind in en_indicators:
            if ind in lower:
                return "en"
        return "sk"  # default

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all doctrine documents from the Slovak DPA."""
        logger.info("Starting full bootstrap of SK/UOOU documents")

        # 1. Office guidelines
        logger.info("Phase 1: Fetching office guidelines")
        office_guidelines = self._get_office_guideline_list()
        for idx, guideline in enumerate(office_guidelines):
            logger.info(f"  [{idx+1}/{len(office_guidelines)}] {guideline['title'][:60]}...")

            file_path = self._scrape_office_guideline_page(guideline["href"])
            if not file_path:
                logger.warning(f"  Skipping (no download link): {guideline['title'][:60]}")
                continue

            text = self._download_and_extract(file_path)
            if not text or len(text) < 100:
                logger.warning(f"  Skipping (no text extracted): {guideline['title'][:60]}")
                continue

            doc_id = f"UOOU/office/{idx+1:03d}"
            yield {
                "doc_id": doc_id,
                "title": guideline["title"],
                "title_sk": "",
                "text": text,
                "date": "",
                "url": f"{BASE_URL}{guideline['href']}",
                "file_url": f"{BASE_URL}{file_path}",
                "category": "office_guideline",
                "language": "sk",
            }

        # 2. EDPB guidelines
        logger.info("Phase 2: Fetching EDPB guidelines")
        edpb_links = self._get_edpb_pdf_links()
        for idx, link in enumerate(edpb_links):
            logger.info(f"  [{idx+1}/{len(edpb_links)}] {link['title'][:60]}...")

            text = self._download_and_extract(link["href"])
            if not text or len(text) < 100:
                logger.warning(f"  Skipping (no text extracted): {link['title'][:60]}")
                continue

            lang = self._detect_language(link["href"], link["title"])
            doc_id = f"UOOU/edpb/{idx+1:03d}"
            yield {
                "doc_id": doc_id,
                "title": link["title"],
                "title_sk": "",
                "text": text,
                "date": "",
                "url": f"{BASE_URL}{EDPB_GUIDELINES_URL}",
                "file_url": f"{BASE_URL}{link['href']}",
                "category": "edpb_guideline",
                "language": lang,
            }

        logger.info("Bootstrap complete")

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """Fetch updates since a given date. Re-fetches all (small corpus)."""
        logger.info(f"Fetching updates since {since} (re-scanning all)")
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw document data into standard schema."""
        doc_id = raw.get("doc_id", "")
        title = raw.get("title", "")
        text = raw.get("text", "")
        date_str = raw.get("date", "")
        url = raw.get("url", "")

        return {
            # Required base fields
            "_id": doc_id,
            "_source": "SK/UOOU",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            # Standard fields
            "title": title,
            "text": text,
            "date": date_str if date_str else None,
            "url": url,
            # Additional metadata
            "doc_id": doc_id,
            "title_sk": raw.get("title_sk", ""),
            "category": raw.get("category", ""),
            "language": raw.get("language", "sk"),
            "file_url": raw.get("file_url", ""),
            "authority": "Úrad na ochranu osobných údajov Slovenskej republiky",
            "authority_en": "Office for Personal Data Protection of the Slovak Republic",
        }

    def test_connection(self):
        """Quick connectivity test."""
        print("Testing Slovak DPA endpoints...")

        # Test main site
        print("\n1. Testing main site...")
        try:
            resp = self.client.get("/en/")
            print(f"   Status: {resp.status_code}")
            print(f"   Page length: {len(resp.text)} chars")
        except Exception as e:
            print(f"   ERROR: {e}")
            return

        # Test office guidelines page
        print("\n2. Testing office guidelines page...")
        try:
            resp = self.client.get(OFFICE_GUIDELINES_URL)
            print(f"   Status: {resp.status_code}")
            print(f"   Page length: {len(resp.text)} chars")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test EDPB guidelines page
        print("\n3. Testing EDPB guidelines page...")
        try:
            resp = self.client.get(EDPB_GUIDELINES_URL)
            print(f"   Status: {resp.status_code}")
            print(f"   Page length: {len(resp.text)} chars")
        except Exception as e:
            print(f"   ERROR: {e}")

        # Test a sample PDF download
        print("\n4. Testing sample PDF download...")
        try:
            test_path = "/files/metod-urad/13/30_krokov_suladu_0.pdf"
            self.rate_limiter.wait()
            resp = self.client.get(test_path)
            print(f"   Status: {resp.status_code}")
            print(f"   Content-Type: {resp.headers.get('Content-Type', 'unknown')}")
            print(f"   Size: {len(resp.content)} bytes")
            if resp.status_code == 200:
                text = self._extract_pdf_text(resp.content)
                print(f"   Extracted text: {len(text)} chars")
                if text:
                    print(f"   First 200 chars: {text[:200]}...")
        except Exception as e:
            print(f"   ERROR: {e}")

        print("\nTest complete.")


if __name__ == "__main__":
    scraper = SlovakDPAScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    elif command == "update":
        scraper.bootstrap(sample_mode=False)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
