#!/usr/bin/env python3
"""
TG/JournalOfficiel -- Journal Officiel de la République Togolaise

Fetches Togo's Official Gazette texts with full content.

Strategy:
  - Paginate the search listing at /recherche?page=N (~30 items/page, ~402 pages)
  - Extract metadata (nature, number, date, institution) from listing rows
  - Fetch each node page to get the full text from inline HTML
  - Fall back to PDF extraction if inline text is insufficient

Usage:
  python bootstrap.py bootstrap          # Fetch all legislation
  python bootstrap.py bootstrap --sample # Fetch 10 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import logging
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from urllib.parse import urljoin, unquote

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TG.JournalOfficiel")

BASE_URL = "https://jo.gouv.tg"
LISTING_URL = f"{BASE_URL}/recherche"
MAX_PAGES = 500  # ~402 pages expected

# French month mapping for date parsing
FRENCH_MONTHS = {
    "janvier": "01", "février": "02", "mars": "03", "avril": "04",
    "mai": "05", "juin": "06", "juillet": "07", "août": "08",
    "septembre": "09", "octobre": "10", "novembre": "11", "décembre": "12",
}


def _parse_french_date(text: str) -> str:
    """Parse a French date like 'Mercredi, 8 octobre 2025' to ISO format."""
    if not text:
        return ""
    # Try DD/MM/YYYY format first
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{4})", text)
    if m:
        day, month, year = m.group(1), m.group(2), m.group(3)
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    # Try French long form: "8 octobre 2025"
    m = re.search(r"(\d{1,2})\s+(\w+)\s+(\d{4})", text)
    if m:
        day = m.group(1).zfill(2)
        month_name = m.group(2).lower()
        year = m.group(3)
        month = FRENCH_MONTHS.get(month_name, "")
        if month:
            return f"{year}-{month}-{day}"
    # Try ISO datetime from content attribute
    m = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    if m:
        return m.group(1)
    return ""


class JournalOfficielScraper(BaseScraper):
    """Scraper for TG/JournalOfficiel -- Togo Official Gazette."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.5",
        })

    def _request(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        """HTTP GET with rate limiting and retry."""
        for attempt in range(3):
            try:
                time.sleep(1.5)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(5)
        return None

    def _parse_listing_page(self, html: str) -> List[Dict[str, str]]:
        """Parse a search listing page for document entries."""
        soup = BeautifulSoup(html, "html.parser")
        documents = []

        rows = soup.select(".views-row")
        for row in rows:
            doc = {}

            # Nature (Loi, Décret, Arrêté, etc.)
            nature_el = row.select_one(".field_nature")
            doc["nature"] = nature_el.get_text(strip=True) if nature_el else ""

            # Document number and node link
            title_el = row.select_one(".field_title a")
            if title_el:
                doc["number"] = title_el.get_text(strip=True)
                href = title_el.get("href", "")
                doc["node_url"] = urljoin(BASE_URL, href)
                # Extract node ID
                node_match = re.search(r"/node/(\d+)", href)
                doc["node_id"] = node_match.group(1) if node_match else ""
            else:
                continue  # Skip rows without a link

            # Date from listing
            date_el = row.select_one(".field_date_signature")
            if date_el:
                # Check for ISO date in content attribute of nested span
                date_span = date_el.select_one("[content]")
                if date_span:
                    doc["date"] = _parse_french_date(date_span.get("content", ""))
                else:
                    doc["date"] = _parse_french_date(date_el.get_text(strip=True))
            else:
                doc["date"] = ""

            # Institution
            inst_el = row.select_one(".entite_emettrice")
            doc["institution"] = inst_el.get_text(strip=True) if inst_el else ""

            # Title/description from field-en-tete
            header_el = row.select_one(".views-field-field-en-tete .field-content")
            doc["description"] = header_el.get_text(strip=True) if header_el else ""

            # PDF link
            pdf_el = row.select_one(".views-field-field-fichier a")
            doc["pdf_url"] = pdf_el.get("href", "") if pdf_el else ""

            if doc.get("node_id"):
                documents.append(doc)

        return documents

    def _extract_node_text(self, html: str, node_id: str) -> Dict[str, str]:
        """Extract full text and metadata from a node page.

        Strategy: use BeautifulSoup to find structured fields and separate
        the metadata from the actual legal text content.
        """
        soup = BeautifulSoup(html, "html.parser")
        result = {"text": "", "date": "", "title": "", "jo_number": "", "jo_type": ""}

        # Find the article content
        article = soup.find("article")
        if not article:
            article = soup.find("div", class_="node")
        if not article:
            return result

        # Extract title from the first heading
        header = article.find(["h1", "h2"])
        if header:
            title_text = header.get_text(strip=True)
            if "JOURNAL OFFICIEL" not in title_text.upper():
                result["title"] = title_text

        # Extract structured metadata from field divs
        for field_div in article.find_all("div", class_=re.compile(r"field-name-")):
            label_el = field_div.find("div", class_="field-label")
            items_el = field_div.find("div", class_="field-items")
            if not label_el:
                continue
            label = label_el.get_text(strip=True).rstrip(":")
            value = items_el.get_text(strip=True) if items_el else ""

            if "date" in label.lower() or "signature" in label.lower():
                # Check for ISO date in content attribute
                date_span = field_div.find("[content]") if field_div else None
                if date_span and date_span.get("content"):
                    result["date"] = _parse_french_date(date_span["content"])
                else:
                    result["date"] = _parse_french_date(value)
            elif "numéro jo" in label.lower():
                result["jo_number"] = value
            elif "type de jo" in label.lower():
                result["jo_type"] = value

        # Remove structured field divs and navigation to isolate legal text
        # Clone the article to avoid modifying the soup
        import copy
        text_article = copy.copy(article)

        # Remove field divs (metadata), navigation, headers
        for el in text_article.find_all("div", class_=re.compile(r"field-name-")):
            el.decompose()
        for el in text_article.find_all(["nav", "footer", "header"]):
            el.decompose()
        for el in text_article.find_all("div", class_=re.compile(r"links|tabs")):
            el.decompose()

        # Get remaining text (the actual legal content)
        raw_text = text_article.get_text("\n", strip=True)
        lines = [l.strip() for l in raw_text.split("\n") if l.strip()]

        # Filter out navigation items and non-breaking spaces
        nav_labels = {
            "Journal Officiel", "Lois", "Actes du Gouvernement", "Nominations",
            "Associations", "Partis Politiques", "Avis", "Annonces",
            "Sites Public", "Jurisprudence", "Open data",
            "Portail de la République Togolaise", "Présidence", "Primature",
            "Service Public", "Assemblée Nationale", "Droit International",
            "CEDEAO", "OHADA", "UEMOA", "Union Africaine",
            "Vous êtes ici", "Accueil",
        }
        meta_labels = {
            "Domaine du texte", "Date de signature", "Numéro JO",
            "Type de JO", "Numéro de la page dans le JO", "Fichier",
            "Visas sans lien", "Visas avec lien",
        }

        clean_lines = []
        skip_next = False
        for line in lines:
            # Skip non-breaking spaces
            if line.replace("\xa0", "").strip() == "":
                continue
            if line in nav_labels:
                continue
            # Skip metadata labels and their values
            if any(line.startswith(label) for label in meta_labels):
                skip_next = True
                continue
            if skip_next:
                skip_next = False
                continue
            # Skip the title (already extracted)
            if result["title"] and line == result["title"]:
                continue
            clean_lines.append(line)

        result["text"] = "\n".join(clean_lines).strip()
        return result

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        node_id = raw.get("node_id", "")
        doc_id = f"TG-JO-{node_id}"

        # Build title from nature + number + description
        parts = []
        if raw.get("nature"):
            parts.append(raw["nature"])
        if raw.get("number"):
            parts.append(f"n° {raw['number']}")
        title = " ".join(parts)
        if raw.get("description"):
            title = f"{title} {raw['description']}" if title else raw["description"]
        if raw.get("extracted_title") and not title:
            title = raw["extracted_title"]

        return {
            "_id": doc_id,
            "_source": "TG/JournalOfficiel",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": raw.get("text", ""),
            "date": raw.get("date", ""),
            "url": raw.get("node_url", ""),
            "nature": raw.get("nature", ""),
            "number": raw.get("number", ""),
            "institution": raw.get("institution", ""),
            "jo_number": raw.get("jo_number", ""),
            "jo_type": raw.get("jo_type", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all documents from paginated listing."""
        count = 0
        seen_nodes = set()

        for page_num in range(MAX_PAGES):
            url = f"{LISTING_URL}?page={page_num}"
            resp = self._request(url)
            if resp is None:
                logger.warning(f"Failed to fetch listing page {page_num}")
                break

            docs = self._parse_listing_page(resp.text)
            if not docs:
                logger.info(f"No documents on page {page_num}, stopping")
                break

            logger.info(f"Page {page_num}: {len(docs)} documents")

            for doc in docs:
                node_id = doc.get("node_id", "")
                if not node_id or node_id in seen_nodes:
                    continue
                seen_nodes.add(node_id)

                # Fetch the detail page for full text
                node_resp = self._request(doc["node_url"])
                if node_resp is None:
                    logger.warning(f"Failed to fetch node {node_id}")
                    continue

                extracted = self._extract_node_text(node_resp.text, node_id)

                text = extracted["text"]

                # Fall back to PDF if inline text is too short
                if (not text or len(text) < 100) and doc.get("pdf_url"):
                    logger.info(f"Inline text too short for node {node_id}, trying PDF")
                    try:
                        pdf_text = extract_pdf_markdown(
                            source="TG/JournalOfficiel",
                            source_id=f"TG-JO-{node_id}",
                            pdf_url=doc["pdf_url"],
                            table="legislation",
                        )
                        if pdf_text and len(pdf_text) >= 100:
                            text = pdf_text
                    except Exception as e:
                        logger.warning(f"PDF extraction failed for node {node_id}: {e}")

                if not text or len(text) < 50:
                    logger.warning(f"Insufficient text for node {node_id}: {len(text) if text else 0} chars")
                    continue

                raw = {
                    "node_id": node_id,
                    "node_url": doc["node_url"],
                    "nature": doc.get("nature", ""),
                    "number": doc.get("number", ""),
                    "date": extracted.get("date") or doc.get("date", ""),
                    "institution": doc.get("institution", ""),
                    "description": doc.get("description", ""),
                    "extracted_title": extracted.get("title", ""),
                    "text": text,
                    "jo_number": extracted.get("jo_number", ""),
                    "jo_type": extracted.get("jo_type", ""),
                    "pdf_url": doc.get("pdf_url", ""),
                }
                count += 1
                yield raw

        logger.info(f"Completed: {count} documents fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent documents (first 3 pages)."""
        count = 0
        seen_nodes = set()

        for page_num in range(3):
            url = f"{LISTING_URL}?page={page_num}"
            resp = self._request(url)
            if resp is None:
                continue

            docs = self._parse_listing_page(resp.text)
            for doc in docs:
                node_id = doc.get("node_id", "")
                if not node_id or node_id in seen_nodes:
                    continue
                seen_nodes.add(node_id)

                node_resp = self._request(doc["node_url"])
                if node_resp is None:
                    continue

                extracted = self._extract_node_text(node_resp.text, node_id)
                text = extracted["text"]

                if (not text or len(text) < 100) and doc.get("pdf_url"):
                    try:
                        pdf_text = extract_pdf_markdown(
                            source="TG/JournalOfficiel",
                            source_id=f"TG-JO-{node_id}",
                            pdf_url=doc["pdf_url"],
                            table="legislation",
                        )
                        if pdf_text and len(pdf_text) >= 100:
                            text = pdf_text
                    except Exception as e:
                        logger.warning(f"PDF extraction failed: {e}")

                if not text or len(text) < 50:
                    continue

                raw = {
                    "node_id": node_id,
                    "node_url": doc["node_url"],
                    "nature": doc.get("nature", ""),
                    "number": doc.get("number", ""),
                    "date": extracted.get("date") or doc.get("date", ""),
                    "institution": doc.get("institution", ""),
                    "description": doc.get("description", ""),
                    "extracted_title": extracted.get("title", ""),
                    "text": text,
                    "jo_number": extracted.get("jo_number", ""),
                    "jo_type": extracted.get("jo_type", ""),
                    "pdf_url": doc.get("pdf_url", ""),
                }
                count += 1
                yield raw

        logger.info(f"Updates: {count} documents fetched")

    def test(self) -> bool:
        """Quick connectivity test."""
        resp = self._request(f"{LISTING_URL}?page=0")
        if resp is None:
            logger.error("Cannot reach JO listing page")
            return False

        docs = self._parse_listing_page(resp.text)
        if not docs:
            logger.error("No documents found on listing page")
            return False

        logger.info(f"Listing OK: {len(docs)} documents on page 0")

        # Test one document
        doc = docs[0]
        node_resp = self._request(doc["node_url"])
        if node_resp:
            extracted = self._extract_node_text(node_resp.text, doc.get("node_id", ""))
            logger.info(
                f"Doc OK: {doc.get('nature', '')} {doc.get('number', '')} "
                f"({len(extracted['text'])} chars)"
            )
            return True

        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="TG/JournalOfficiel data fetcher")
    parser.add_argument(
        "command",
        choices=["bootstrap", "update", "test"],
        help="Command to run",
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Only fetch a small sample (for validation)",
    )
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = JournalOfficielScraper()

    if args.command == "test":
        success = scraper.test()
        sys.exit(0 if success else 1)

    elif args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=15)
        fetched = stats.get("records_fetched", 0) or stats.get("sample_records_saved", 0)
        logger.info(f"Bootstrap complete: {fetched} records — {stats}")
        if fetched == 0:
            sys.exit(1)

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {stats}")


if __name__ == "__main__":
    main()
