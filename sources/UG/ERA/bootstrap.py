#!/usr/bin/env python3
"""
UG/ERA -- Uganda Electricity Regulatory Authority

Fetches tariff orders, licensing decisions, quarterly tariff review reports,
gazette notices and other regulatory documents published by ERA.

The era.go.ug site is WordPress + WP Download Manager (WPDM). Every published
document is a `wpdmpro` package listed in the WordPress sitemap. Each package
landing page (/download/{slug}/) carries the title and a `?wpdmdl={id}` link
that streams the underlying PDF. Scanned/image-only PDFs are skipped.

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
import warnings
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests
import pdfplumber
from bs4 import BeautifulSoup

warnings.filterwarnings("ignore", category=requests.packages.urllib3.exceptions.InsecureRequestWarning)

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UG.ERA")

BASE_URL = "https://www.era.go.ug"
SITEMAP_URL = f"{BASE_URL}/wp-sitemap-posts-wpdmpro-1.xml"


class ERAScraper(BaseScraper):
    """
    Scraper for UG/ERA -- Uganda Electricity Regulatory Authority.
    Country: UG
    URL: https://www.era.go.ug/orders/

    Data types: doctrine, legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (LegalDataHunter open-data research project)",
        })

    # ------------------------------------------------------------------
    # Data collection
    # ------------------------------------------------------------------

    def _get_all_packages(self) -> list[dict]:
        """Parse the WPDM sitemap into [{url, lastmod}], newest first."""
        try:
            resp = self.session.get(SITEMAP_URL, timeout=40)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch sitemap: {e}")
            return []

        packages = []
        for m in re.finditer(r"<url>\s*<loc>([^<]+)</loc>\s*(?:<lastmod>([^<]+)</lastmod>)?", resp.text):
            url = m.group(1).strip()
            lastmod = (m.group(2) or "").strip()
            if "/download/" in url:
                packages.append({"url": url, "lastmod": lastmod})

        # Newest first -> recent (born-digital, text) PDFs sample first.
        packages.sort(key=lambda p: p["lastmod"], reverse=True)
        logger.info(f"Sitemap lists {len(packages)} WPDM packages")
        return packages

    def _get_pkg_info(self, pkg: dict) -> Optional[dict]:
        """Fetch a package landing page; extract title and wpdmdl download id."""
        url = pkg["url"]
        try:
            resp = self.session.get(url, timeout=40)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to fetch {url}: {e}")
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        h1 = soup.find("h1")
        title = h1.get_text(strip=True) if h1 else ""
        if not title or len(title) < 5:
            return None

        m = re.search(r"wpdmdl=(\d+)", resp.text)
        if not m:
            return None
        wpdmdl = m.group(1)
        download_url = f"{url}?wpdmdl={wpdmdl}"

        return {
            "page_url": url,
            "download_url": download_url,
            "title": title,
            "lastmod": pkg.get("lastmod", ""),
        }

    # ------------------------------------------------------------------
    # PDF extraction
    # ------------------------------------------------------------------

    def _extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download PDF and extract text via pdfplumber."""
        try:
            resp = self.session.get(pdf_url, timeout=180)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to download PDF {pdf_url}: {e}")
            return None

        if len(resp.content) < 500 or b"%PDF" not in resp.content[:1024]:
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
            return full_text if len(full_text) >= 200 else None
        except Exception as e:
            logger.warning(f"PDF extraction failed for {pdf_url}: {e}")
            return None

    # ------------------------------------------------------------------
    # Normalize
    # ------------------------------------------------------------------

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform raw document into standard schema."""
        text = raw.get("text", "").strip()
        if not text or len(text) < 200:
            return None

        title = raw.get("title", "").strip()
        if not title:
            return None

        # Most ERA outputs are regulatory doctrine; flag primary legislation.
        tl = title.lower()
        if re.search(r"\b(act|regulations?|statutory instrument|by-?laws?)\b", tl):
            _type = "legislation"
        else:
            _type = "doctrine"

        doc_id = f"UG-ERA-{hashlib.md5(raw['page_url'].encode()).hexdigest()[:12]}"

        return {
            "_id": doc_id,
            "_source": "UG/ERA",
            "_type": _type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": _extract_date(title, raw.get("lastmod", "")),
            "url": raw["page_url"],
            "document_type": _classify(title),
        }

    # ------------------------------------------------------------------
    # Main fetch methods
    # ------------------------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all ERA documents with full PDF text."""
        packages = self._get_all_packages()

        yielded = 0
        skipped = 0
        for i, pkg in enumerate(packages):
            info = self._get_pkg_info(pkg)
            if not info:
                skipped += 1
                continue

            logger.info(f"[{i+1}/{len(packages)}] {info['title'][:60]}")
            text = self._extract_pdf_text(info["download_url"])
            if not text:
                skipped += 1
                logger.info(f"  Skipped (no extractable text): {info['title'][:50]}")
                continue

            info["text"] = text
            # Yield RAW per BaseScraper contract; the framework calls normalize().
            # (Previously yielded self.normalize(...) → double-normalize → 0 records.)
            yielded += 1
            yield info

            time.sleep(1.5)

        logger.info(f"Done. Yielded: {yielded}, Skipped: {skipped}")

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """Sitemap is sorted newest-first; re-scan and rely on dedup downstream."""
        yield from self.fetch_all()

    def test(self) -> dict:
        """Quick connectivity test."""
        try:
            pkgs = self._get_all_packages()
            return {
                "status": "ok" if pkgs else "error",
                "packages": len(pkgs),
                "newest": pkgs[0]["url"] if pkgs else None,
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _classify(title: str) -> str:
    tl = title.lower()
    if "tariff order" in tl or re.search(r"\border\b", tl):
        return "order"
    if "licen" in tl:
        return "licensing"
    if "tariff" in tl and ("review" in tl or "report" in tl):
        return "tariff_review"
    if "tariff" in tl:
        return "tariff"
    if "decision" in tl:
        return "decision"
    if "notice" in tl or "gazette" in tl:
        return "notice"
    if "report" in tl:
        return "report"
    if re.search(r"\b(act|regulations?|statutory instrument)\b", tl):
        return "legislation"
    return "other"


def _extract_date(title: str, lastmod: str = "") -> Optional[str]:
    """Date from the title where possible, else the sitemap lastmod."""
    if title:
        month_map = {
            "january": "01", "february": "02", "march": "03", "april": "04",
            "may": "05", "june": "06", "july": "07", "august": "08",
            "september": "09", "october": "10", "november": "11", "december": "12",
        }
        for name, num in month_map.items():
            m = re.search(rf"(\d{{1,2}})\s+{name}\s+(20\d{{2}}|19\d{{2}})", title.lower())
            if m:
                return f"{m.group(2)}-{num}-{m.group(1).zfill(2)}"
        # quarter pattern
        qmap = {"first": 3, "second": 6, "third": 9, "fourth": 12}
        for qname, qmonth in qmap.items():
            if qname in title.lower() and re.search(r"20\d{2}", title):
                yr = re.search(r"(20\d{2})", title).group(1)
                return f"{yr}-{str(qmonth).zfill(2)}-01"
        years = re.findall(r"\b(20\d{2}|19\d{2})\b", title)
        if years:
            return f"{max(years)}-01-01"
    if lastmod:
        m = re.match(r"(\d{4}-\d{2}-\d{2})", lastmod)
        if m:
            return m.group(1)
    return None


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    scraper = ERAScraper()

    if len(sys.argv) < 2:
        print("Usage: bootstrap.py [bootstrap|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        print(json.dumps(scraper.test(), indent=2))
    elif command in ("bootstrap", "update"):
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        limit = 15 if sample_mode else 99999
        gen = scraper.fetch_all() if command == "bootstrap" else scraper.fetch_updates()

        for raw in gen:
            # fetch_all() yields RAW docs; normalize here to match the VPS
            # bootstrap_fast() path and write proper sample records.
            record = scraper.normalize(raw)
            if not record:
                continue
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
