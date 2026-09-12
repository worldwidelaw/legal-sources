#!/usr/bin/env python3
"""
TZ/TCRA — Tanzania Communications Regulatory Authority

Fetches regulatory documents (laws, regulations, guidelines, rules, codes, sector plans)
from the TCRA website. All documents are PDFs; text extracted via pdfplumber.

Categories scraped:
  - legislation   (Sheria — Acts of Parliament)
  - legislation-1 (Sera — Policy documents)
  - regulations   (Kanuni — Subsidiary regulations)
  - rules         (Kanuni — Rules)
  - guidelines    (Miongozo — Guidelines)
  - codes         (Kanuni — Codes of conduct)
  - communication-sector-guidebooks-and-plans

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import json
import time
import logging
import hashlib
import io
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import urljoin, unquote

import requests
import pdfplumber
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.TZ.TCRA")

BASE_URL = "https://www.tcra.go.tz"
SOURCE_ID = "TZ/TCRA"

# Categories to scrape: (slug, label, data_type)
CATEGORIES = [
    ("legislation-1", "legislation", "legislation"),
    ("legislation", "policy", "doctrine"),
    ("regulations", "regulation", "legislation"),
    ("rules", "rules", "legislation"),
    ("guidelines", "guideline", "doctrine"),
    ("codes", "code", "legislation"),
    ("communication-sector-guidebooks-and-plans", "sector_plan", "doctrine"),
]

MAX_PAGES_PER_CATEGORY = 10


class TCRAScraper(BaseScraper):
    """
    Scraper for TZ/TCRA — Tanzania Communications Regulatory Authority.
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (open-data research project)",
        })

    def _normalize_pdf_key(self, url: str) -> str:
        """Extract the core filename from view or download URLs for dedup."""
        for prefix in ["/uploads/documents/", "/download/"]:
            if prefix in url:
                return unquote(url.split(prefix)[-1]).strip()
        return url

    def _collect_pdfs_from_category(self, slug: str, label: str) -> list[dict]:
        """Paginate through a category and collect all PDF entries."""
        results = []
        seen_keys = set()

        for page_num in range(MAX_PAGES_PER_CATEGORY):
            url = f"{BASE_URL}/documents/{slug}"
            if page_num > 0:
                url += f"?page={page_num + 1}"

            try:
                resp = self.session.get(url, timeout=30)
                resp.raise_for_status()
            except Exception as e:
                logger.warning(f"Failed to fetch {url}: {e}")
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            found = 0

            for link in soup.find_all("a", href=True):
                href = link["href"]
                if "/download/" not in href and "/uploads/documents/" not in href:
                    continue

                # Normalize to download URL
                if "/uploads/documents/" in href:
                    filename = href.split("/uploads/documents/")[-1]
                    pdf_url = f"{BASE_URL}/download/{filename}"
                else:
                    pdf_url = href if href.startswith("http") else urljoin(BASE_URL, href)

                # Dedup by core filename
                key = self._normalize_pdf_key(pdf_url)
                if key in seen_keys:
                    continue
                seen_keys.add(key)

                # Skip "Tazama" (View) / "Pakua" (Download) link text
                link_text = link.get_text(strip=True).lower()
                if link_text in ("tazama", "pakua", "view", "download", "open"):
                    title = self._title_from_filename(pdf_url)
                else:
                    title = self._extract_title_near_link(link, soup)
                    if not title or len(title) < 5:
                        title = self._title_from_filename(pdf_url)

                results.append({
                    "pdf_url": pdf_url,
                    "title": title,
                    "category": label,
                })
                found += 1

            if found == 0:
                break
            time.sleep(0.5)

        logger.info(f"  {slug}: {len(results)} PDFs found")
        return results

    def _extract_title_near_link(self, link_tag, soup) -> str:
        """Try to find the document title near the link element."""
        # Check for h4/h5/h3 sibling or parent
        parent = link_tag.parent
        for _ in range(4):
            if parent is None:
                break
            for heading in parent.find_all(["h2", "h3", "h4", "h5"], limit=1):
                text = heading.get_text(strip=True)
                if len(text) > 5:
                    return text
            parent = parent.parent

        # Use link text itself
        text = link_tag.get_text(strip=True)
        if text and len(text) > 5 and text.lower() not in ("view", "download", "open"):
            return text

        return ""

    def _title_from_filename(self, url: str) -> str:
        """Extract a readable title from the PDF filename."""
        filename = unquote(url.split("/")[-1])
        # Remove prefix like "sw-1234567890-"
        filename = re.sub(r"^sw-\d+-", "", filename)
        # Remove .pdf extension
        filename = re.sub(r"\.pdf$", "", filename, flags=re.IGNORECASE)
        # Replace underscores and hyphens
        filename = filename.replace("_", " ").replace("-", " ")
        return filename.strip()

    def _extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download PDF and extract text via pdfplumber."""
        try:
            resp = self.session.get(pdf_url, timeout=120)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to download PDF {pdf_url}: {e}")
            return None

        if len(resp.content) < 500:
            return None

        # Skip very large PDFs (>50MB)
        if len(resp.content) > 50 * 1024 * 1024:
            logger.warning(f"Skipping oversized PDF ({len(resp.content)} bytes): {pdf_url}")
            return None

        try:
            pdf = pdfplumber.open(io.BytesIO(resp.content))
            pages_text = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages_text.append(text)
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
            pdf.close()
            full_text = "\n\n".join(pages_text)
            return full_text if len(full_text) >= 100 else None
        except Exception as e:
            logger.warning(f"PDF extraction failed for {pdf_url}: {e}")
            return None

    def _extract_date(self, title: str) -> Optional[str]:
        """Extract date from title text."""
        if not title:
            return None

        # Try year patterns like "2025", "2024"
        # First look for month+year
        month_map = {
            "january": "01", "february": "02", "march": "03", "april": "04",
            "may": "05", "june": "06", "july": "07", "august": "08",
            "september": "09", "october": "10", "november": "11", "december": "12",
        }
        for month_name, month_num in month_map.items():
            m = re.search(
                rf"{month_name}\s+(20\d{{2}}|19\d{{2}})",
                title.lower(),
            )
            if m:
                return f"{m.group(1)}-{month_num}-01"

        # Swahili months
        sw_months = {
            "januari": "01", "februari": "02", "machi": "03", "aprili": "04",
            "mei": "05", "juni": "06", "julai": "07", "agosti": "08",
            "septemba": "09", "oktoba": "10", "novemba": "11", "desemba": "12",
        }
        for month_name, month_num in sw_months.items():
            m = re.search(
                rf"{month_name}\s+(20\d{{2}}|19\d{{2}})",
                title.lower(),
            )
            if m:
                return f"{m.group(1)}-{month_num}-01"

        # Fall back to year in title
        years = re.findall(r"\b(20\d{2}|19\d{2})\b", title)
        if years:
            return f"{years[-1]}-01-01"

        return None

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw document into standard schema."""
        text = raw.get("text", "").strip()
        if not text or len(text) < 100:
            return None

        title = raw.get("title", "").strip()
        if not title:
            return None

        category = raw.get("category", "other")
        _type = "legislation" if category in ("legislation", "regulation", "rules", "code") else "doctrine"

        slug = raw["pdf_url"].split("/")[-1]
        doc_id = f"TZ-TCRA-{hashlib.md5(slug.encode()).hexdigest()[:12]}"

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": _type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": self._extract_date(title),
            "url": raw["pdf_url"],
            "category": category,
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all TCRA documents with full PDF text."""
        all_entries = []
        for slug, label, _dtype in CATEGORIES:
            entries = self._collect_pdfs_from_category(slug, label)
            all_entries.extend(entries)

        # Deduplicate by core filename
        seen = set()
        unique = []
        for entry in all_entries:
            key = self._normalize_pdf_key(entry["pdf_url"])
            if key not in seen:
                seen.add(key)
                unique.append(entry)

        logger.info(f"Total unique PDFs to process: {len(unique)}")

        yielded = 0
        skipped = 0

        for i, entry in enumerate(unique):
            logger.info(f"[{i+1}/{len(unique)}] {entry['title'][:60]}")

            text = self._extract_pdf_text(entry["pdf_url"])
            if not text:
                skipped += 1
                logger.info(f"  Skipped (no extractable text)")
                continue

            entry["text"] = text
            normalized = self.normalize(entry)
            if normalized:
                yielded += 1
                yield normalized

            time.sleep(1.5)

        logger.info(f"Done. Yielded: {yielded}, Skipped: {skipped}")

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """Fetch recently added documents."""
        yield from self.fetch_all()

    def test(self) -> dict:
        """Quick connectivity test."""
        try:
            resp = self.session.get(f"{BASE_URL}/documents/regulations", timeout=30)
            soup = BeautifulSoup(resp.text, "html.parser")
            pdf_links = [
                a for a in soup.find_all("a", href=True)
                if "/download/" in a["href"] or "/uploads/documents/" in a["href"]
            ]
            return {
                "status": "ok" if resp.status_code == 200 else "error",
                "http_status": resp.status_code,
                "pdf_links_on_page": len(pdf_links),
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = TCRAScraper()

    if len(sys.argv) < 2:
        print("Usage: bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        result = scraper.test()
        print(json.dumps(result, indent=2))
    elif command in ("bootstrap", "update"):
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        limit = 15 if sample_mode else 99999

        gen = scraper.fetch_all() if command == "bootstrap" else scraper.fetch_updates()

        for record in gen:
            count += 1
            if sample_mode:
                outpath = sample_dir / f"{count:04d}.json"
                outpath.write_text(json.dumps(record, indent=2, ensure_ascii=False))
                print(f"[{count}] {record['title'][:60]} ({len(record['text'])} chars)")
            else:
                print(json.dumps(record, ensure_ascii=False))

            if count >= limit:
                break

        print(f"\nTotal records: {count}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
