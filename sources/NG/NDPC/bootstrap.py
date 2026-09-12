#!/usr/bin/env python3
"""
NG/NDPC -- Nigeria Data Protection Commission

Fetches publications from the NDPC via two channels:
1. WordPress REST API (/wp-json/wp/v2/posts) — news, enforcement
   announcements, compliance guidance, press releases.
2. Resources-page PDFs — NDP Act, GAID, regulations, annual reports,
   guidance notices, white papers.

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py update             # Fetch recent records
  python bootstrap.py test               # Quick connectivity test
"""

import io
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

import pdfplumber
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.NG.NDPC")

BASE_URL = "https://ndpc.gov.ng"
WP_API = f"{BASE_URL}/wp-json/wp/v2"
RESOURCES_PAGE_ID = 1475

# Known PDF resources from the Resources page (stable wp-content URLs).
# These contain the substantive regulatory documents.
RESOURCE_PDFS = [
    {
        "slug": "NDP-Act-2023",
        "title": "Nigeria Data Protection Act, 2023",
        "url": f"{BASE_URL}/wp-content/uploads/2024/03/Nigeria_Data_Protection_Act_2023.pdf",
        "date": "2023-06-14",
    },
    {
        "slug": "NDP-Act-GAID-2025",
        "title": "NDP Act General Application and Implementation Directive (GAID) 2025",
        "url": f"{BASE_URL}/wp-content/uploads/2025/07/NDP-ACT-GAID-2025-MARCH-20TH.pdf",
        "date": "2025-03-20",
    },
    {
        "slug": "NDPR-2019",
        "title": "Nigeria Data Protection Regulation (NDPR) 2019",
        "url": f"{BASE_URL}/wp-content/uploads/2024/03/NigeriaDataProtectionRegulation.pdf",
        "date": "2019-01-25",
    },
    {
        "slug": "NDPR-Implementation-Guidelines",
        "title": "Guidelines for Implementation of NDPR in Public Institutions",
        "url": f"{BASE_URL}/wp-content/uploads/2024/03/GuidelinesForImplementationOfNDPRInPublicInstitutionsFinal11.pdf",
        "date": "2020-01-01",
    },
    {
        "slug": "NDPR-Performance-Report-2019-2020",
        "title": "NDPR Lite Performance Report 2019-2020",
        "url": f"{BASE_URL}/wp-content/uploads/2024/03/NDPR-Lite-Performance-Report-2O19-2O2O.pdf",
        "date": "2020-01-01",
    },
    {
        "slug": "DPA-Report",
        "title": "Data Protection Audit Report",
        "url": f"{BASE_URL}/wp-content/uploads/2024/03/DPA-Report-2.pdf",
        "date": "2023-01-01",
    },
    {
        "slug": "SRAP-2023-2027",
        "title": "NDPC Strategic Roadmap and Action Plan (SRAP) 2023-2027",
        "url": f"{BASE_URL}/wp-content/uploads/2024/03/Srap.pdf",
        "date": "2023-01-01",
    },
    {
        "slug": "National-Digital-Economy-Policy",
        "title": "National Digital Economy Policy and Strategy",
        "url": f"{BASE_URL}/wp-content/uploads/2024/03/Policy-National_Digital_Economy_Policy_and_Strategy.pdf",
        "date": "2020-01-01",
    },
    {
        "slug": "Public-Sector-Compliance-Circular",
        "title": "Public Sector Data Protection Compliance Circular",
        "url": f"{BASE_URL}/wp-content/uploads/2024/03/Public_Sector_Data_Protection_Compliance_Circular.pdf",
        "date": "2022-01-01",
    },
    {
        "slug": "Code-of-Conduct",
        "title": "NDPC Code of Conduct",
        "url": f"{BASE_URL}/wp-content/uploads/2024/03/coc.pdf",
        "date": "2023-01-01",
    },
    {
        "slug": "Guidance-Notice",
        "title": "NDPC Guidance Notice",
        "url": f"{BASE_URL}/wp-content/uploads/2024/03/guidance_notice.pdf",
        "date": "2023-01-01",
    },
    {
        "slug": "Registration-Guidance",
        "title": "NDPC Registration Guidance for Data Controllers and Processors",
        "url": f"{BASE_URL}/wp-content/uploads/2024/03/registration.pdf",
        "date": "2023-01-01",
    },
    {
        "slug": "Registration-Guidance-2024-Updated",
        "title": "Updated Guidance Notice on Registration of Data Controllers and Data Processors of Major Importance 2024",
        "url": f"{BASE_URL}/wp-content/uploads/2025/07/Updated-Guidance-Notice-on-Registtration-2024.pdf",
        "date": "2024-01-01",
    },
    {
        "slug": "NDPR-Draft-2020-2021",
        "title": "NDPR Compiled Draft 2020-2021",
        "url": f"{BASE_URL}/wp-content/uploads/2024/03/hhNITDA_Compiled-NDPR-Draft-2020-2021_0701.pdf",
        "date": "2021-07-01",
    },
    {
        "slug": "Annual-Report-2023",
        "title": "NDPC Annual Report 2023",
        "url": f"{BASE_URL}/wp-content/uploads/2024/07/AnnualReport2023.pdf",
        "date": "2023-12-31",
    },
    {
        "slug": "Annual-Report-2024",
        "title": "NDPC Annual Report 2024",
        "url": f"{BASE_URL}/wp-content/uploads/2025/01/NDPC-Annual-Report-2024.pdf",
        "date": "2024-12-31",
    },
    {
        "slug": "NDPC-Bulletin",
        "title": "NDPC Bulletin",
        "url": f"{BASE_URL}/wp-content/uploads/2024/11/BULLETIN-NDPC.pdf",
        "date": "2024-11-01",
    },
    {
        "slug": "NDPC-Journal-Data-Privacy",
        "title": "NDPC International Journal of Data Privacy and Protection",
        "url": f"{BASE_URL}/wp-content/uploads/2025/01/NDPC-International-Journal-of-Data-Privacy-and-Protection.pdf",
        "date": "2025-01-01",
    },
    {
        "slug": "NDPC-DIAL-White-Paper",
        "title": "Privacy by Design in Early-Stage Innovation (NDPC-DIAL White Paper)",
        "url": f"{BASE_URL}/wp-content/uploads/2025/11/NDPC_DIAL-White-Paper-Privacy-by-Design-in-Early-Stage-Innovation-.pdf",
        "date": "2025-11-01",
    },
    {
        "slug": "NDP-Act-Yoruba",
        "title": "NDP Act, 2023 (Yoruba Translation)",
        "url": f"{BASE_URL}/wp-content/uploads/2025/12/Book_NDPAct_Yoruba.pdf",
        "date": "2023-06-14",
    },
    {
        "slug": "NDP-Act-Hausa",
        "title": "NDP Act, 2023 (Hausa Translation)",
        "url": f"{BASE_URL}/wp-content/uploads/2025/12/Book_NDPAct_Hausa.pdf",
        "date": "2023-06-14",
    },
    {
        "slug": "NDP-Act-Igbo",
        "title": "NDP Act, 2023 (Igbo Translation)",
        "url": f"{BASE_URL}/wp-content/uploads/2025/12/Book_NDPAct_Igbo.pdf",
        "date": "2023-06-14",
    },
]


class NDPCScraper(BaseScraper):
    """
    Scraper for NG/NDPC -- Nigeria Data Protection Commission.

    Country: NG
    URL: https://www.ndpc.gov.ng/
    Data types: doctrine
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        })

    # ------------------------------------------------------------------
    # WordPress API
    # ------------------------------------------------------------------

    def _fetch_wp_posts(self) -> list[dict]:
        """Fetch all posts from the WordPress REST API."""
        posts = []
        page = 1
        per_page = 50

        while True:
            url = f"{WP_API}/posts?per_page={per_page}&page={page}&_fields=id,title,content,date,link,categories"
            try:
                resp = self.session.get(url, timeout=30)
                if resp.status_code == 400:
                    break
                resp.raise_for_status()
            except Exception as e:
                logger.error(f"WP API page {page} failed: {e}")
                break

            data = resp.json()
            if not data:
                break

            for post in data:
                title = _html_decode(post["title"]["rendered"])
                raw_html = post["content"]["rendered"]
                text = _strip_html(raw_html)

                if not text or len(text) < 50:
                    continue

                posts.append({
                    "wp_id": post["id"],
                    "title": title,
                    "text": text,
                    "date": post["date"][:10] if post.get("date") else None,
                    "url": post.get("link", ""),
                    "document_type": "news",
                    "source_channel": "wp_api",
                })

            total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
            logger.info(f"WP API page {page}/{total_pages}: {len(data)} posts")

            if page >= total_pages:
                break
            page += 1
            time.sleep(1.0)

        logger.info(f"Total WP posts fetched: {len(posts)}")
        return posts

    # ------------------------------------------------------------------
    # PDF extraction
    # ------------------------------------------------------------------

    def _extract_pdf_text(self, pdf_url: str) -> Optional[str]:
        """Download PDF and extract text via pdfplumber."""
        try:
            resp = self.session.get(pdf_url, timeout=120)
            resp.raise_for_status()
        except Exception as e:
            logger.warning(f"Failed to download PDF {pdf_url}: {e}")
            return None

        ctype = resp.headers.get("content-type", "")
        if "pdf" not in ctype.lower() and not resp.content[:5].startswith(b"%PDF"):
            logger.warning(f"Not a PDF ({ctype}): {pdf_url}")
            return None

        if len(resp.content) < 500:
            return None

        try:
            pdf = pdfplumber.open(io.BytesIO(resp.content))
            pages_text = []
            for pg in pdf.pages:
                text = pg.extract_text()
                if text:
                    pages_text.append(text)
                try:
                    pg.flush_cache(); pg.get_textmap.cache_clear()
                except Exception:
                    pass
            pdf.close()
            full_text = "\n\n".join(pages_text)
            full_text = _clean_text(full_text)
            return full_text if len(full_text) >= 100 else None
        except Exception as e:
            logger.warning(f"PDF extraction failed for {pdf_url}: {e}")
            return None

    def _fetch_resource_pdfs(self) -> list[dict]:
        """Download and extract text from known resource PDFs."""
        records = []
        for info in RESOURCE_PDFS:
            logger.info(f"Downloading PDF: {info['title'][:60]}")
            text = self._extract_pdf_text(info["url"])
            if not text:
                logger.warning(f"Skipped (no text): {info['title'][:60]}")
                continue

            records.append({
                "pdf_slug": info["slug"],
                "title": info["title"],
                "text": text,
                "date": info["date"],
                "url": info["url"],
                "document_type": "regulation",
                "source_channel": "pdf_resource",
            })
            time.sleep(1.5)

        logger.info(f"Total PDF resources fetched: {len(records)}")
        return records

    # ------------------------------------------------------------------
    # Normalize
    # ------------------------------------------------------------------

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform a raw document into the standard schema."""
        text = (raw.get("text") or "").strip()
        if not text or len(text) < 100:
            return None

        title = (raw.get("title") or "").strip()
        if not title:
            return None

        if raw.get("source_channel") == "pdf_resource":
            doc_id = f"NG-NDPC-PDF-{raw['pdf_slug']}"
        else:
            doc_id = f"NG-NDPC-WP-{raw['wp_id']}"

        return {
            "_id": doc_id,
            "_source": "NG/NDPC",
            "_type": "doctrine",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "document_type": raw.get("document_type", ""),
        }

    # ------------------------------------------------------------------
    # Main fetch methods
    # ------------------------------------------------------------------

    def fetch_all(self) -> Generator[dict, None, None]:
        """Fetch all NDPC publications (WP posts + resource PDFs).

        Yields RAW records per the BaseScraper contract; the framework calls
        normalize(). (Previously yielded already-normalized records, which the
        framework's bootstrap would double-normalize → KeyError on wp_id.)
        """
        # Fetch PDFs first (higher-value regulatory content)
        yield from self._fetch_resource_pdfs()

        # Then fetch WP posts
        yield from self._fetch_wp_posts()

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """Fetch recent WP posts (incremental by date)."""
        # `update()` passes a datetime; this body treats `since` as a date string (#1512).
        since = as_date_str(since)
        if since:
            # WP API supports after= parameter
            posts = []
            page = 1
            per_page = 50
            while True:
                url = (
                    f"{WP_API}/posts?per_page={per_page}&page={page}"
                    f"&after={since}T00:00:00"
                    f"&_fields=id,title,content,date,link,categories"
                )
                try:
                    resp = self.session.get(url, timeout=30)
                    if resp.status_code == 400:
                        break
                    resp.raise_for_status()
                except Exception as e:
                    logger.error(f"WP API update page {page} failed: {e}")
                    break

                data = resp.json()
                if not data:
                    break

                for post in data:
                    title = _html_decode(post["title"]["rendered"])
                    raw_html = post["content"]["rendered"]
                    text = _strip_html(raw_html)
                    if not text or len(text) < 50:
                        continue
                    posts.append({
                        "wp_id": post["id"],
                        "title": title,
                        "text": text,
                        "date": post["date"][:10] if post.get("date") else None,
                        "url": post.get("link", ""),
                        "document_type": "news",
                        "source_channel": "wp_api",
                    })

                total_pages = int(resp.headers.get("X-WP-TotalPages", 1))
                if page >= total_pages:
                    break
                page += 1
                time.sleep(1.0)

            # Yield RAW records; the framework normalizes.
            yield from posts
        else:
            yield from self.fetch_all()

    def test(self) -> dict:
        """Quick connectivity test."""
        results = {}

        # Test WP API
        try:
            resp = self.session.get(f"{WP_API}/posts?per_page=1", timeout=15)
            total = int(resp.headers.get("X-WP-Total", 0))
            results["wp_api"] = {
                "status": resp.status_code,
                "ok": resp.status_code == 200 and total > 0,
                "total_posts": total,
            }
        except Exception as e:
            results["wp_api"] = {"status": "error", "ok": False, "error": str(e)}

        # Test one PDF
        try:
            test_pdf = RESOURCE_PDFS[0]
            resp = self.session.head(test_pdf["url"], timeout=15)
            results["pdf_resources"] = {
                "status": resp.status_code,
                "ok": resp.status_code == 200,
                "total_pdfs": len(RESOURCE_PDFS),
            }
        except Exception as e:
            results["pdf_resources"] = {"status": "error", "ok": False, "error": str(e)}

        all_ok = all(r.get("ok") for r in results.values())
        return {"status": "ok" if all_ok else "partial", "endpoints": results}


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _strip_html(html: str) -> str:
    """Remove HTML tags and clean whitespace."""
    import html as html_mod
    text = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</(p|div|h[1-6]|li|tr)>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = html_mod.unescape(text)
    return _clean_text(text)


def _html_decode(text: str) -> str:
    """Decode HTML entities in a string."""
    import html as html_mod
    return html_mod.unescape(text)


def _clean_text(text: str) -> str:
    """Collapse excessive whitespace while preserving paragraph breaks."""
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


if __name__ == "__main__":
    scraper = NDPCScraper()

    if len(sys.argv) < 2:
        print("Usage: bootstrap.py [bootstrap|bootstrap-fast|update|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        result = scraper.test()
        print(json.dumps(result, indent=2))
    elif command == "bootstrap":
        # Route through the framework so records are normalized and persisted
        # to data/records.jsonl (previously full runs only printed to stdout →
        # nothing ingested on the VPS).
        if sample_mode:
            stats = scraper.run_sample(n=15)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, "
                  f"{stats['records_updated']} updated, {stats['errors']} errors")
    elif command == "bootstrap-fast":
        stats = scraper.bootstrap_fast()
        print(json.dumps(stats, indent=2, default=str))
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate: {stats['records_new']} new, {stats['records_updated']} updated")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
