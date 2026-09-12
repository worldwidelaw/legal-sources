#!/usr/bin/env python3
"""
EU/ECDC — European Centre for Disease Prevention and Control

Fetches technical guidance, risk assessments, surveillance reports, and scientific
advice from ECDC.

Approach:
1. Parse ECDC sitemap to discover all publication URLs
2. Filter out data-visualization / dashboard pages (COVID maps, notification rates)
3. For each publication page: extract metadata (title, date, type) from HTML
4. Download linked PDF and extract full text via pdfplumber
5. Fall back to HTML body text if PDF unavailable

Data type: doctrine
Coverage: 2005–present
"""

import json
import hashlib
import io
import logging
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterator, Optional
from xml.etree import ElementTree

import requests

# Optional PDF extraction
try:
    import pdfplumber
    HAS_PDFPLUMBER = True
except ImportError:
    HAS_PDFPLUMBER = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

SOURCE_ID = "EU/ECDC"
BASE_URL = "https://www.ecdc.europa.eu"
SITEMAP_INDEX = f"{BASE_URL}/sitemap.xml"
HEADERS = {
    "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
DELAY = 3  # seconds between page requests

# URL slug substrings that indicate data-visualization / dashboard / map pages
# These rarely have substantive text content
SKIP_SLUGS = [
    "notification-rate",
    "cases-maps",
    "distribution-confirmed",
    "vaccine-uptake",
    "vaccination-coverage",
    "testing-rate",
    "hospital-occupancy",
    "intensive-care",
    "positivity-rate",
    "subnational-14-day",
    "country-overview",
    "colour-blind",
    "-maps-",
    "atlas-surveillance",
    "download-todays-data",
    "download-historical-data",
    "data-collection",
    "case-definition",
]


class ECDCFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

    def _get(self, url: str, **kwargs) -> Optional[requests.Response]:
        for attempt in range(3):
            try:
                resp = self.session.get(url, timeout=30, **kwargs)
                if resp.status_code == 429:
                    wait = min(30, 5 * (attempt + 1))
                    logger.warning(f"Rate limited (429), waiting {wait}s... {url}")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                logger.warning(f"Request failed ({attempt+1}/3) {url}: {e}")
                if attempt < 2:
                    time.sleep(3 * (attempt + 1))
        return None

    # ------------------------------------------------------------------
    # Discovery: parse sitemap for publication URLs
    # ------------------------------------------------------------------
    def discover_publication_urls(self) -> list[str]:
        """Parse the ECDC sitemap index to collect all /en/publications-data/ URLs."""
        logger.info("Fetching sitemap index...")
        resp = self._get(SITEMAP_INDEX)
        if not resp:
            raise RuntimeError("Cannot fetch sitemap index")

        root = ElementTree.fromstring(resp.content)
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        sub_urls = [loc.text for loc in root.findall(".//sm:loc", ns) if loc.text]

        all_pub_urls = []
        for sub_url in sub_urls:
            logger.info(f"Parsing {sub_url}...")
            resp = self._get(sub_url)
            if not resp:
                continue
            sub_root = ElementTree.fromstring(resp.content)
            for loc in sub_root.findall(".//sm:loc", ns):
                url = loc.text
                if not url or "/en/publications-data/" not in url:
                    continue
                path = url.replace(BASE_URL, "")
                parts = path.strip("/").split("/")
                if len(parts) < 3:
                    continue
                url_lower = url.lower()
                if any(pat in url_lower for pat in SKIP_SLUGS):
                    continue
                all_pub_urls.append(url)
            time.sleep(1)

        # Reverse sort so newest publications (later alphabetically) come first
        all_pub_urls = sorted(set(all_pub_urls), reverse=True)
        logger.info(f"Discovered {len(all_pub_urls)} publication URLs (after filtering)")
        return all_pub_urls

    # ------------------------------------------------------------------
    # Scrape a single publication page
    # ------------------------------------------------------------------
    def scrape_publication(self, url: str) -> Optional[Dict[str, Any]]:
        """Scrape a single publication page for metadata and content."""
        resp = self._get(url)
        if not resp:
            return None

        html = resp.text

        # Extract title
        title = self._extract_meta(html, "og:title") or self._extract_tag(html, "title")
        if not title:
            return None
        title = re.sub(r"\s*\|\s*European Centre.*$", "", title).strip()

        # Extract date
        date_str = self._extract_date(html)

        # Extract description/summary
        description = self._extract_meta(html, "og:description") or ""

        # Extract article body text from HTML
        body_text = self._extract_article_text(html)

        # Find PDF link
        pdf_url = self._find_pdf_link(html)

        # Extract full text from PDF if available
        full_text = ""
        if pdf_url and HAS_PDFPLUMBER:
            full_text = self._extract_pdf_text(pdf_url)
            time.sleep(DELAY)  # rate limit after PDF download

        # Use best available text
        if full_text and len(full_text) > len(body_text):
            text = full_text
            text_source = "pdf"
        elif body_text and len(body_text) >= 200:
            text = body_text
            text_source = "html"
        elif description and len(description) >= 100:
            text = description
            text_source = "description"
        else:
            text = "\n\n".join(filter(None, [body_text, description])).strip()
            text_source = "combined"

        if not text or len(text) < 1000:
            logger.debug(f"Skipping {url}: insufficient text ({len(text) if text else 0} chars)")
            return None

        pub_type = self._classify_publication(url, title)

        return {
            "url": url,
            "title": title,
            "date": date_str,
            "text": text,
            "text_source": text_source,
            "description": description,
            "publication_type": pub_type,
            "pdf_url": f"{BASE_URL}{pdf_url}" if pdf_url and pdf_url.startswith("/") else pdf_url,
        }

    def _extract_meta(self, html: str, prop: str) -> str:
        m = re.search(rf'<meta\s+(?:property|name)="{prop}"\s+content="([^"]*)"', html)
        if not m:
            m = re.search(rf'content="([^"]*)"\s+(?:property|name)="{prop}"', html)
        return m.group(1).strip() if m else ""

    def _extract_tag(self, html: str, tag: str) -> str:
        m = re.search(rf"<{tag}[^>]*>([^<]+)</{tag}>", html, re.I)
        return m.group(1).strip() if m else ""

    def _extract_date(self, html: str) -> Optional[str]:
        d = self._extract_meta(html, "article:published_time")
        if d:
            m = re.match(r"(\d{4}-\d{2}-\d{2})", d)
            if m:
                return m.group(1)

        m = re.search(
            r"(\d{1,2})\s+(January|February|March|April|May|June|July|"
            r"August|September|October|November|December)\s+(\d{4})",
            html,
        )
        if m:
            day, month_name, year = m.groups()
            try:
                dt = datetime.strptime(f"{day} {month_name} {year}", "%d %B %Y")
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                pass

        m = re.search(r'"datePublished"\s*:\s*"(\d{4}-\d{2}-\d{2})"', html)
        if m:
            return m.group(1)

        return None

    def _extract_article_text(self, html: str) -> str:
        """Extract text from the article/main content area."""
        import html as html_mod

        m = re.search(r"<article[^>]*>(.*?)</article>", html, re.S)
        if not m:
            m = re.search(
                r'class="[^"]*layout__region--content[^"]*"[^>]*>(.*?)</div>\s*</div>',
                html,
                re.S,
            )
        if not m:
            return ""

        content = m.group(1)
        content = re.sub(
            r"<(script|style|nav|footer|header)[^>]*>.*?</\1>",
            "",
            content,
            flags=re.S | re.I,
        )
        content = re.sub(r"<[^>]+>", " ", content)
        content = html_mod.unescape(content)
        content = re.sub(r"\s+", " ", content).strip()
        return content

    def _find_pdf_link(self, html: str) -> Optional[str]:
        """Find the primary PDF download link."""
        pdfs = re.findall(r'href="([^"]+\.pdf)"', html, re.I)
        if not pdfs:
            return None
        for pdf in pdfs:
            if "/sites/default/files/" in pdf:
                return pdf
        return pdfs[0]

    def _extract_pdf_text(self, pdf_url: str) -> str:
        """Download PDF and extract text."""
        if pdf_url.startswith("/"):
            pdf_url = f"{BASE_URL}{pdf_url}"

        try:
            resp = self.session.get(pdf_url, timeout=60, stream=True)
            resp.raise_for_status()

            content_length = int(resp.headers.get("Content-Length", 0))
            if content_length > 50 * 1024 * 1024:
                logger.warning(f"PDF too large ({content_length} bytes): {pdf_url}")
                return ""

            pdf_bytes = resp.content
            if len(pdf_bytes) > 50 * 1024 * 1024:
                return ""

            text_parts = []
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text_parts.append(page_text)
                    try:
                        page.flush_cache(); page.get_textmap.cache_clear()
                    except Exception:
                        pass

            return "\n\n".join(text_parts)

        except Exception as e:
            logger.warning(f"PDF extraction failed for {pdf_url}: {e}")
            return ""

    def _classify_publication(self, url: str, title: str) -> str:
        """Classify publication type from URL slug and title."""
        slug = url.lower()
        title_l = title.lower()

        if "risk-assessment" in slug or "rapid-risk-assessment" in slug:
            return "risk_assessment"
        if "communicable-disease-threats-report" in slug or "cdtr" in title_l:
            return "threat_report"
        if "epidemiological-update" in slug or "epidemiological update" in title_l:
            return "epidemiological_update"
        if "surveillance-report" in slug or "surveillance report" in title_l:
            return "surveillance_report"
        if "annual-epidemiological-report" in slug:
            return "annual_report"
        if "guidance" in slug or "guidance" in title_l:
            return "guidance"
        if "technical-report" in slug or "technical report" in title_l:
            return "technical_report"
        if "scientific-advice" in slug or "scientific advice" in title_l:
            return "scientific_advice"
        if "measles" in slug or "rubella" in slug:
            return "monitoring_report"
        return "publication"

    # ------------------------------------------------------------------
    # Normalize
    # ------------------------------------------------------------------
    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        url = raw["url"]
        slug = url.replace(BASE_URL, "").strip("/")
        doc_id = hashlib.sha256(slug.encode()).hexdigest()[:16]

        return {
            "_id": f"ecdc-{doc_id}",
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.utcnow().isoformat() + "Z",
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "publication_type": raw.get("publication_type", "publication"),
            "description": raw.get("description", ""),
            "pdf_url": raw.get("pdf_url"),
            "text_source": raw.get("text_source", ""),
        }

    # ------------------------------------------------------------------
    # Public entry points
    # ------------------------------------------------------------------
    def fetch_all(self, max_docs: int = None) -> Iterator[Dict[str, Any]]:
        urls = self.discover_publication_urls()
        count = 0
        skipped = 0
        for url in urls:
            if max_docs and count >= max_docs:
                break
            raw = self.scrape_publication(url)
            if raw:
                yield self.normalize(raw)
                count += 1
                skipped = 0
                if count % 50 == 0:
                    logger.info(f"Fetched {count} documents...")
            else:
                skipped += 1
            time.sleep(DELAY)

    def fetch_updates(self, since: datetime) -> Iterator[Dict[str, Any]]:
        """Fetch recent publications (last N from sitemap, filtered by date)."""
        urls = self.discover_publication_urls()
        count = 0
        for url in urls:
            raw = self.scrape_publication(url)
            if not raw:
                continue
            if raw.get("date"):
                try:
                    pub_date = datetime.strptime(raw["date"], "%Y-%m-%d")
                    if pub_date < since:
                        continue
                except ValueError:
                    pass
            yield self.normalize(raw)
            count += 1
            time.sleep(DELAY)
        logger.info(f"Updates since {since.date()}: {count} documents")


# ==================================================================
# CLI
# ==================================================================
def main():
    import argparse

    parser = argparse.ArgumentParser(description="EU/ECDC bootstrap")
    sub = parser.add_subparsers(dest="command")

    boot = sub.add_parser("bootstrap", help="Fetch and save samples")
    boot.add_argument("--sample", action="store_true", help="Sample mode (15 docs)")
    boot.add_argument("--full", action="store_true", help="Fetch all documents")
    boot.add_argument("--max", type=int, default=None, help="Max documents")

    args = parser.parse_args()

    if args.command != "bootstrap":
        parser.print_help()
        sys.exit(1)

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    fetcher = ECDCFetcher()

    if args.sample:
        limit = 15
    elif args.max:
        limit = args.max
    elif args.full:
        limit = None
    else:
        limit = 15

    count = 0
    for doc in fetcher.fetch_all(max_docs=limit):
        out_path = sample_dir / f"{doc['_id']}.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)
        count += 1
        text_len = len(doc.get("text", ""))
        logger.info(
            f"[{count}] {doc['title'][:80]} — {text_len} chars ({doc.get('text_source', '?')})"
        )

    logger.info(f"Done. Saved {count} documents to {sample_dir}")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
