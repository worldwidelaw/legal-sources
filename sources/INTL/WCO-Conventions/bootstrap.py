#!/usr/bin/env python3
"""
INTL/WCO-Conventions -- World Customs Organization Legal Instruments

Fetches WCO conventions, declarations, resolutions, and recommendations
with full text extracted from PDFs or HTML pages.

Strategy:
  - Conventions: hardcoded PDF URLs from wcoomd.org (verified)
  - Declarations: scrape listing page for PDF links
  - Resolutions: scrape listing page for PDF links
  - Recommendations: scrape 4 sub-category pages for PDF links
  - Download PDFs and extract text via pdfplumber
  - ~130 legal instruments total

Endpoints:
  - Conventions:  https://www.wcoomd.org/en/about-us/legal-instruments/conventions.aspx
  - Declarations:  https://www.wcoomd.org/en/about-us/legal-instruments/declarations.aspx
  - Resolutions:   https://www.wcoomd.org/en/about-us/legal-instruments/resolutions.aspx
  - Recommendations: 4 sub-pages (HS, PF, IT, EC)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py update             # Fetch all
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import io
import hashlib
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from html import unescape

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.WCO-Conventions")

BASE_URL = "https://www.wcoomd.org"
SOURCE_ID = "INTL/WCO-Conventions"

# ── Hardcoded conventions (PDF paths verified) ────────────────────────
CONVENTIONS = [
    ("WCO-CCC-Convention", "Convention establishing a Customs Co-operation Council", "/-/media/wco/public/global/pdf/about-us/legal-instruments/conventions-and-agreements/ccc/convccc.pdf", "1950-12-15"),
    ("WCO-HS-Convention", "International Convention on the Harmonized Commodity Description and Coding System", "/-/media/wco/public/global/pdf/about-us/legal-instruments/conventions-and-agreements/conventions/ng0300ea.pdf", "1983-06-14"),
    ("WCO-Nomenclature-Convention", "Convention on Nomenclature for the classification of goods in Customs tariffs", "/-/media/wco/public/global/pdf/about-us/legal-instruments/conventions-and-agreements/conventions/nom_conv_bil.pdf", "1950-12-15"),
    ("WCO-ECS-Carnets", "Customs Convention on ECS carnets for commercial samples", "/-/media/wco/public/global/pdf/about-us/legal-instruments/conventions-and-agreements/conventions/conv_ecs_carnet_en.pdf", "1956-11-01"),
    ("WCO-TempImport-Packings", "Customs Convention on the temporary importation of packings", "/-/media/wco/public/global/pdf/about-us/legal-instruments/conventions-and-agreements/conventions/conv_packing_en.pdf", "1960-06-06"),
    ("WCO-TempImport-ProfEquip", "Customs Convention on the temporary importation of professional equipment", "/-/media/wco/public/global/pdf/about-us/legal-instruments/conventions-and-agreements/conventions/conv_prof_equip_en.pdf", "1961-06-08"),
    ("WCO-Exhibitions-Convention", "Customs Convention concerning facilities for the importation of goods for display or use at exhibitions, fairs, meetings or similar events", "/-/media/wco/public/global/pdf/about-us/legal-instruments/conventions-and-agreements/conventions/conv_fairequip_en.pdf", "1961-06-08"),
    ("WCO-ATA-Convention", "Customs Convention on the ATA carnet for the temporary admission of goods", "/-/media/wco/public/global/pdf/about-us/legal-instruments/conventions-and-agreements/ata/pf_ata_conv_text.pdf", "1961-12-06"),
    ("WCO-Seafarers-Welfare", "Customs Convention concerning welfare material for seafarers", "/-/media/wco/public/global/pdf/about-us/legal-instruments/conventions-and-agreements/conventions/conv_seaf_en.pdf", "1964-12-01"),
    ("WCO-TempImport-SciEquip", "Customs Convention on the temporary importation of scientific equipment", "/-/media/wco/public/global/pdf/about-us/legal-instruments/conventions-and-agreements/conventions/conv_sci_equip_en.pdf", "1968-09-11"),
    ("WCO-TempImport-PedagEquip", "Customs Convention on the temporary importation of pedagogic material", "/-/media/wco/public/global/pdf/about-us/legal-instruments/conventions-and-agreements/conventions/conv_pedag_equip_en.pdf", "1970-06-08"),
    ("WCO-ITI-Convention", "Customs Convention on the international transit of goods (ITI Convention)", "/-/media/wco/public/global/pdf/about-us/legal-instruments/conventions-and-agreements/conventions/iti-convention-text-e.pdf", "1971-06-07"),
    ("WCO-Kyoto-Convention-1973", "International Convention on the simplification and harmonization of Customs procedures (Kyoto Convention)", "/-/media/wco/public/global/pdf/about-us/legal-instruments/conventions-and-agreements/conventions/kyoto-conv-1973_en.pdf", "1973-05-18"),
    ("WCO-Kyoto-Convention-Revised", "International Convention on the simplification and harmonization of Customs procedures (Revised Kyoto Convention)", "/-/media/wco/public/global/pdf/about-us/legal-instruments/conventions-and-agreements/revised-kyoto/pg0350ea.pdf", "1999-06-26"),
    ("WCO-Nairobi-Convention", "International Convention on mutual administrative assistance for the prevention, investigation and repression of Customs offences (Nairobi Convention)", "/-/media/wco/public/global/pdf/about-us/legal-instruments/conventions-and-agreements/nairobi/naireng1.pdf", "1977-06-09"),
    ("WCO-Johannesburg-Convention", "International Convention on mutual administrative assistance in Customs matters (Johannesburg Convention)", "/-/media/wco/public/global/pdf/about-us/legal-instruments/conventions-and-agreements/conventions/johann_conv_en.pdf", "2003-06-27"),
    ("WCO-Istanbul-Convention", "Convention on Temporary Admission (Istanbul Convention)", "/-/media/wco/public/global/pdf/about-us/legal-instruments/conventions-and-agreements/istanbul/istanbul_legal_text_eng.pdf", "1990-06-26"),
    ("WCO-Containers-Convention", "Customs Convention on Containers, 1972", "/-/media/wco/public/global/pdf/about-us/legal-instruments/conventions-and-agreements/containers/pf_txt_containers_contract.pdf", "1972-12-02"),
    ("WCO-BDV-Valuation", "Convention on the Valuation of Goods for Customs Purposes (BDV)", "/-/media/wco/public/global/pdf/about-us/legal-instruments/conventions-and-agreements/conventions/conv_val_bil.pdf", "1950-12-15"),
]

# ── Pages to scrape for PDF links ─────────────────────────────────────
SCRAPE_PAGES = [
    ("declaration", f"{BASE_URL}/en/about-us/legal-instruments/declarations.aspx"),
    ("resolution", f"{BASE_URL}/en/about-us/legal-instruments/resolutions.aspx"),
    ("recommendation-ec", f"{BASE_URL}/en/about-us/legal-instruments/recommendations/ec_recommendations.aspx"),
    ("recommendation-it", f"{BASE_URL}/en/about-us/legal-instruments/recommendations/it_recommendations.aspx"),
    ("recommendation-hs", f"{BASE_URL}/en/about-us/legal-instruments/recommendations/hs_recommendations.aspx"),
]


def _strip_html(text: str) -> str:
    """Remove HTML tags and decode entities."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = unescape(text)
    text = text.replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _make_id(category: str, title: str) -> str:
    """Create a stable ID from category and title."""
    slug = re.sub(r"[^a-z0-9]+", "-", title.lower())[:60].strip("-")
    h = hashlib.sha256(f"{category}:{title}".encode()).hexdigest()[:8]
    return f"WCO-{category}-{h}-{slug}"


def _extract_date(title: str) -> Optional[str]:
    """Try to extract a date from the title text."""
    # Match patterns like "(June 2024)", "(22 June 2023)", "(1 July 2006)"
    m = re.search(r"\((\d{1,2}\s+)?(\w+)\s+(\d{4})\)", title)
    if m:
        months = {
            "january": "01", "february": "02", "march": "03", "april": "04",
            "may": "05", "june": "06", "july": "07", "august": "08",
            "september": "09", "october": "10", "november": "11", "december": "12",
        }
        day = m.group(1).strip() if m.group(1) else "01"
        month = months.get(m.group(2).lower(), "01")
        year = m.group(3)
        return f"{year}-{month}-{int(day):02d}"
    return None


class WCOConventionsScraper(BaseScraper):
    SOURCE_ID = SOURCE_ID

    def __init__(self):
        super().__init__()
        self.http = HttpClient()

    def _get(self, url: str) -> Optional[str]:
        try:
            resp = self.http.get(url)
            if resp and resp.status_code == 200:
                return resp.text
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
        return None

    def _get_bytes(self, url: str) -> Optional[bytes]:
        try:
            resp = self.http.get(url)
            if resp and resp.status_code == 200:
                return resp.content
        except Exception as e:
            logger.warning(f"Failed to fetch bytes {url}: {e}")
        return None

    def _extract_pdf_text(self, pdf_bytes: bytes) -> str:
        try:
            import pdfplumber
            pages_text = []
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:
                        pages_text.append(text)
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass
            return "\n\n".join(pages_text)
        except Exception as e:
            logger.warning(f"PDF extraction failed: {e}")
            return ""

    def _fetch_pdf_record(self, doc_id: str, title: str, pdf_url: str,
                          date: Optional[str], page_url: str,
                          category: str) -> Optional[Dict[str, Any]]:
        """Download a PDF and return a normalized record."""
        if not pdf_url.startswith("http"):
            pdf_url = BASE_URL + pdf_url

        # Strip query params for cleaner URL, then add ?la=en
        clean_url = pdf_url.split("?")[0]
        fetch_url = clean_url + "?la=en"

        pdf_bytes = self._get_bytes(fetch_url)
        if not pdf_bytes:
            logger.warning(f"Could not download PDF: {fetch_url}")
            return None

        text = self._extract_pdf_text(pdf_bytes)
        if not text or len(text) < 100:
            logger.warning(f"Insufficient text for {doc_id}: {len(text)} chars")
            return None

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": date,
            "url": page_url,
            "pdf_url": clean_url,
            "category": category,
            "language": "en",
        }

    def _scrape_pdf_links(self, page_url: str) -> List[Dict[str, str]]:
        """Scrape a WCO listing page for PDF links with titles."""
        html = self._get(page_url)
        if not html:
            return []

        results = []
        seen = set()

        # Pattern: link text followed by or containing a PDF href
        # WCO pages use <a href="/-/media/...pdf">Title</a> pattern
        for m in re.finditer(
            r'<a[^>]*href="([^"]*\.pdf[^"]*)"[^>]*>(.*?)</a>',
            html, re.DOTALL
        ):
            pdf_path = m.group(1)
            link_text = _strip_html(m.group(2))

            # Skip non-English PDFs (Russian, Arabic, etc. are parallel)
            if "/ru/" in pdf_path or "/ar/" in pdf_path or "/zh/" in pdf_path:
                continue
            # Skip if it looks like a language-only link
            if link_text.lower() in ("arabic", "russian", "chinese", "spanish",
                                      "french", "portuguese"):
                continue
            if pdf_path in seen:
                continue
            seen.add(pdf_path)

            # Try to get a better title from surrounding context
            title = link_text if len(link_text) > 10 else ""
            if not title:
                # Look backwards for a heading or strong text
                pos = m.start()
                preceding = html[max(0, pos - 500):pos]
                heading = re.findall(r"<(?:h[2-4]|strong|b)>([^<]+)</", preceding)
                if heading:
                    title = _strip_html(heading[-1])

            if not title or len(title) < 5:
                # Use filename as title
                fname = pdf_path.split("/")[-1].replace(".pdf", "").replace("?la=en", "")
                title = fname.replace("-", " ").replace("_", " ").title()

            results.append({"pdf_path": pdf_path, "title": title})

        return results

    def _yield_conventions(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all hardcoded conventions."""
        page_url = f"{BASE_URL}/en/about-us/legal-instruments/conventions.aspx"
        for doc_id, title, pdf_path, date in CONVENTIONS:
            logger.info(f"Convention: {title[:70]}...")
            record = self._fetch_pdf_record(
                doc_id=doc_id, title=title,
                pdf_url=BASE_URL + pdf_path,
                date=date, page_url=page_url,
                category="convention"
            )
            if record:
                yield record
            time.sleep(1.5)

    def _yield_page_instruments(self, category: str,
                                 page_url: str) -> Generator[Dict[str, Any], None, None]:
        """Scrape a listing page and yield PDF-based records."""
        logger.info(f"Scraping {category} page: {page_url}")
        links = self._scrape_pdf_links(page_url)
        logger.info(f"Found {len(links)} PDF links for {category}")

        for item in links:
            title = item["title"]
            doc_id = _make_id(category, title)
            date = _extract_date(title)

            logger.info(f"  {category}: {title[:70]}...")
            record = self._fetch_pdf_record(
                doc_id=doc_id, title=title,
                pdf_url=item["pdf_path"],
                date=date, page_url=page_url,
                category=category
            )
            if record:
                yield record
            time.sleep(1.5)

    def fetch_all(
        self, sample: bool = False
    ) -> Generator[Dict[str, Any], None, None]:
        count = 0
        target = 15 if sample else 999

        # 1. Conventions (hardcoded PDFs)
        for record in self._yield_conventions():
            count += 1
            yield record
            if count >= target:
                return

        # 2. Declarations, Resolutions, Recommendations
        for category, page_url in SCRAPE_PAGES:
            for record in self._yield_page_instruments(category, page_url):
                count += 1
                yield record
                if count >= target:
                    return

        logger.info(f"Total records fetched: {count}")

    def fetch_updates(
        self, since: str
    ) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all(sample=False)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return raw


def bootstrap(sample: bool = False):
    scraper = WCOConventionsScraper()
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    count = 0
    for record in scraper.fetch_all(sample=sample):
        count += 1
        fname = f"{record['_id']}.json"
        with open(sample_dir / fname, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2)
        logger.info(
            f"[{count}] Saved {fname} — {record['title'][:60]}... "
            f"({len(record['text'])} chars)"
        )

    logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")
    return count


def test():
    scraper = WCOConventionsScraper()
    html = scraper._get(f"{BASE_URL}/en/about-us/legal-instruments/conventions.aspx")
    if html and "conventions" in html.lower():
        logger.info("PASS: WCO conventions page accessible")
        return True
    else:
        logger.error("FAIL: Could not access WCO conventions page")
        return False


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    import argparse

    parser = argparse.ArgumentParser(description="INTL/WCO-Conventions bootstrap")
    parser.add_argument("command", choices=["bootstrap", "update", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    args = parser.parse_args()

    if args.command == "test":
        success = test()
        sys.exit(0 if success else 1)
    elif args.command == "bootstrap":
        count = bootstrap(sample=args.sample)
        sys.exit(0 if count > 0 else 1)
    elif args.command == "update":
        count = bootstrap(sample=False)
        sys.exit(0 if count > 0 else 1)
