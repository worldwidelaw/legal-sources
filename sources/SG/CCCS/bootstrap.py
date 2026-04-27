#!/usr/bin/env python3
"""
SG/CCCS -- Singapore Competition & Consumer Commission

Fetches decisions from the CCCS public register (case-register/public-register).
Full text extracted from inline page content (Next.js/Isomer RSC payload).

Strategy:
  - Discover all case URLs from the sitemap (sitemap.xml)
  - For each case page, fetch HTML and parse React Server Component chunks
  - Extract title, date, category, status, summary/decision text, and PDF links
  - Text is embedded in RSC "children" arrays within table rows
  - Rate limited to 0.5 req/s

API:
  - Base: https://www.ccs.gov.sg
  - Sitemap: /sitemap.xml
  - Case pages: /case-register/public-register/{category}/{slug}/
  - No auth required

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch ~15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import html as htmlmod
import io
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List
from hashlib import sha256

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.SG.CCCS")

BASE_URL = "https://www.ccs.gov.sg"
SITEMAP_URL = f"{BASE_URL}/sitemap.xml"
REGISTER_PREFIX = "/case-register/public-register/"

CATEGORY_MAP = {
    "agreements-and-collaborations": "Agreements & Collaborations",
    "mergers-and-acquisitions": "Mergers & Acquisitions",
    "dominance-and-conduct": "Dominance & Conduct",
    "consumer-protection--fair-trading-": "Consumer Protection (Fair Trading)",
    "commitments-and-remedies": "Commitments & Remedies",
    "government-advisories": "Government Advisories",
    "market-studies": "Market Studies",
}

# Map categories to data types
CATEGORY_DATA_TYPE = {
    "agreements-and-collaborations": "case_law",
    "mergers-and-acquisitions": "case_law",
    "dominance-and-conduct": "case_law",
    "consumer-protection--fair-trading-": "case_law",
    "commitments-and-remedies": "case_law",
    "government-advisories": "doctrine",
    "market-studies": "doctrine",
}


def _fix_mojibake(text: str) -> str:
    """Fix common UTF-8 double-encoding artifacts in RSC payloads."""
    # Replace orphaned lead bytes from UTF-8 smart quotes/dashes
    replacements = {
        "\u00e2\u0080\u009c": "\u201c",  # "
        "\u00e2\u0080\u009d": "\u201d",  # "
        "\u00e2\u0080\u0098": "\u2018",  # '
        "\u00e2\u0080\u0099": "\u2019",  # '
        "\u00e2\u0080\u0093": "\u2013",  # –
        "\u00e2\u0080\u0094": "\u2014",  # —
        "\u00e2\u0080\u00a6": "\u2026",  # …
        "\u00c2\u00a0": "\u00a0",        # non-breaking space
        "\u00c3\u00a9": "\u00e9",        # é
    }
    for bad, good in replacements.items():
        text = text.replace(bad, good)
    # Also strip leftover orphan bytes that didn't match a pattern
    text = re.sub(r"[\x80-\x9f]", "", text)
    return text


def extract_rsc_text(html_content: str) -> Dict[str, Any]:
    """
    Parse Next.js RSC payload chunks from the HTML to extract
    title, date, status, summary text, and PDF links.
    """
    result = {
        "title": "",
        "date": "",
        "status": "",
        "text_parts": [],
        "pdf_links": [],
    }

    # Extract RSC push chunks
    pushes = re.findall(
        r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', html_content, re.DOTALL
    )

    all_decoded = []
    for push in pushes:
        try:
            decoded = push.encode("utf-8").decode("unicode_escape")
            # Fix double-encoded UTF-8 (latin-1 bytes that are actually UTF-8)
            try:
                decoded = decoded.encode("latin-1").decode("utf-8")
            except (UnicodeDecodeError, UnicodeEncodeError):
                pass
        except Exception:
            decoded = push
        all_decoded.append(decoded)

    full_payload = "\n".join(all_decoded)

    # Fix common mojibake from RSC payloads
    full_payload = _fix_mojibake(full_payload)

    # Extract title from prose-display-md heading
    title_match = re.search(
        r'"prose-display-md[^"]*"[^}]*"children":"([^"]+)"', full_payload
    )
    if title_match:
        result["title"] = htmlmod.unescape(title_match.group(1))

    # Extract date from the line after the title (prose-label-sm-medium)
    date_match = re.search(
        r'"prose-label-sm-medium[^"]*"[^}]*"children":"(\d{1,2}\s+\w+\s+\d{4})"',
        full_payload,
    )
    if date_match:
        result["date"] = date_match.group(1)

    # Extract status line (e.g., "Investigation | Closed - Infringement Decision Issued")
    status_match = re.search(
        r'"prose-body-base mb-3[^"]*"[^}]*"children":"([^"]+)"', full_payload
    )
    if status_match:
        result["status"] = status_match.group(1)

    # Extract PDF links from isomer-user-content
    pdf_links = re.findall(
        r'"href":"(https://isomer-user-content[^"]+\.pdf)"', full_payload
    )
    result["pdf_links"] = list(set(pdf_links))

    # Extract all text content from RSC payload.
    # Content appears in multiple patterns — capture all of them.
    text_parts = []
    seen = set()

    # Pattern 1: text in elements with dir="ltr"
    content_strings = re.findall(
        r'"dir":"ltr","children":\[?"([^"]{10,})"', full_payload
    )

    # Pattern 2: text in prose-body-base paragraphs (often contain the main content)
    content_strings += re.findall(
        r'prose-body-base text-base-content"[^}]*"children":\[?"([^"]{10,})"',
        full_payload,
    )

    # Pattern 3: text in any "children":"long text" that's actual content
    # These appear in table cells and standalone paragraphs
    all_children = re.findall(r'"children":"([^"]{30,})"', full_payload)
    for c in all_children:
        if "className" not in c and "focus-" not in c and "bg-" not in c:
            content_strings.append(c)

    # Filter out navigation/UI text
    skip_patterns = {
        "Skip to main content",
        "Official website links end with .gov.sg",
        "Government agencies communicate",
        "Secure websites use HTTPS",
        "Call the 24/7 ScamShield",
        "A Singapore Government Agency",
        "Report Vulnerability",
        "Open Government Products",
        "This page might have been moved",
        "Competition and Consumer Commission of Singapore",
        "Anti-competitive practices",
    }

    for text in content_strings:
        cleaned = htmlmod.unescape(text).strip()
        if len(cleaned) < 10:
            continue
        if any(skip in cleaned for skip in skip_patterns):
            continue
        # Skip field labels that appear as single-cell content
        if cleaned in (
            "Case Number",
            "Case Type",
            "Case Status",
            "Decision Date",
            "Case Summary",
            "Summary of Infringement Decision",
            "Useful Links",
            "Useful links:",
            "Parties",
            "Applicant(s)",
        ):
            continue
        text_parts.append(cleaned)

    result["text_parts"] = text_parts
    return result


def parse_date(date_str: str) -> Optional[str]:
    """Parse a date like '4 June 2010' or '31 July 2025' to ISO format."""
    if not date_str:
        return None
    for fmt in ("%d %B %Y", "%d %b %Y"):
        try:
            dt = datetime.strptime(date_str.strip(), fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _extract_pdf_text(pdf_url: str, timeout: int = 60) -> str:
    """Download a PDF and extract text using pdfplumber."""
    import requests
    try:
        import pdfplumber
    except ImportError:
        logger.warning("pdfplumber not available, skipping PDF extraction")
        return ""

    try:
        resp = requests.get(pdf_url, timeout=timeout, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"
        })
        if resp.status_code != 200:
            logger.warning("PDF download failed: HTTP %d for %s", resp.status_code, pdf_url)
            return ""
        if len(resp.content) > 50_000_000:  # Skip >50MB PDFs
            logger.warning("PDF too large (%d bytes), skipping: %s", len(resp.content), pdf_url)
            return ""
        with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
            pages = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages.append(text)
            return "\n\n".join(pages)
    except Exception as e:
        logger.warning("PDF extraction failed for %s: %s", pdf_url, e)
        return ""


class CCCSScraper(BaseScraper):
    def __init__(self):
        super().__init__(str(Path(__file__).parent))
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
            timeout=30,
            respect_robots=False,
        )

    def _get_case_urls(self) -> List[Dict[str, str]]:
        """Get all case page URLs from the sitemap."""
        logger.info("Fetching sitemap from %s", SITEMAP_URL)
        resp = self.http.get(SITEMAP_URL)
        if resp.status_code != 200:
            raise RuntimeError(f"Sitemap fetch failed: {resp.status_code}")

        root = ET.fromstring(resp.text)
        ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

        cases = []
        for url_elem in root.findall("sm:url", ns):
            loc = url_elem.find("sm:loc", ns)
            if loc is None:
                continue
            url = loc.text.strip()

            # Extract case URLs: must be under public-register with a category and slug
            path = url.replace(BASE_URL, "")
            if not path.startswith(REGISTER_PREFIX):
                continue

            remainder = path[len(REGISTER_PREFIX) :].strip("/")
            parts = remainder.split("/")
            if len(parts) != 2:
                continue  # Skip category index pages

            category_slug, case_slug = parts
            if category_slug not in CATEGORY_MAP:
                continue

            lastmod_elem = url_elem.find("sm:lastmod", ns)
            lastmod = lastmod_elem.text.strip() if lastmod_elem is not None else None

            cases.append(
                {
                    "url": url,
                    "category_slug": category_slug,
                    "case_slug": case_slug,
                    "lastmod": lastmod,
                }
            )

        logger.info("Found %d case URLs in sitemap", len(cases))
        return cases

    def _fetch_case(self, case_info: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """Fetch and parse a single case page."""
        url = case_info["url"]
        category_slug = case_info["category_slug"]

        time.sleep(2)  # Rate limit
        try:
            resp = self.http.get(url)
        except Exception as e:
            logger.warning("Failed to fetch %s: %s", url, e)
            return None

        if resp.status_code != 200:
            logger.warning("HTTP %d for %s", resp.status_code, url)
            return None

        data = extract_rsc_text(resp.text)

        # Check for "page moved or deleted"
        full_text = "\n\n".join(data["text_parts"])
        if "This page might have been moved" in full_text or not data["text_parts"]:
            logger.warning("Page not found or empty: %s", url)
            return None

        # Build document ID from URL slug
        doc_id = f"SG/CCCS/{category_slug}/{case_info['case_slug']}"

        title = data["title"] or case_info["case_slug"].replace("-", " ").title()
        date_iso = parse_date(data["date"])
        category = CATEGORY_MAP.get(category_slug, category_slug)
        data_type = CATEGORY_DATA_TYPE.get(category_slug, "case_law")

        # Try to extract full text from linked PDFs (decisions are much richer)
        pdf_text = ""
        for pdf_url in data.get("pdf_links", []):
            logger.info("Downloading PDF: %s", pdf_url[:80])
            extracted = _extract_pdf_text(pdf_url)
            if extracted and len(extracted) > len(pdf_text):
                pdf_text = extracted
            time.sleep(1)

        # Use PDF text as primary if substantially longer, otherwise use page text
        if pdf_text and len(pdf_text) > len(full_text) * 2:
            combined_text = pdf_text
        elif pdf_text:
            combined_text = full_text + "\n\n---\n\n" + pdf_text
        else:
            combined_text = full_text

        return {
            "_id": doc_id,
            "_source": "SG/CCCS",
            "_type": data_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": combined_text,
            "date": date_iso,
            "url": url,
            "category": category,
            "status": data.get("status", ""),
            "pdf_links": data.get("pdf_links", []),
            "lastmod": case_info.get("lastmod"),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        cases = self._get_case_urls()
        for i, case_info in enumerate(cases):
            logger.info(
                "[%d/%d] Fetching %s", i + 1, len(cases), case_info["case_slug"]
            )
            doc = self._fetch_case(case_info)
            if doc and len(doc.get("text", "")) > 50:
                yield doc
            else:
                logger.warning(
                    "Skipped %s (no text or too short)", case_info["case_slug"]
                )

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        yield from self.fetch_all()

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return raw


def bootstrap(sample: bool = False) -> None:
    scraper = CCCSScraper()
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    count = 0
    limit = 15 if sample else 999999

    for doc in scraper.fetch_all():
        fname = sha256(doc["_id"].encode()).hexdigest()[:16] + ".json"
        (sample_dir / fname).write_text(json.dumps(doc, indent=2, ensure_ascii=False))
        count += 1
        text_len = len(doc.get("text", ""))
        logger.info(
            "Saved %s — %s (%d chars)", doc["_id"], doc["title"][:60], text_len
        )
        if count >= limit:
            break

    logger.info("Bootstrap complete: %d documents saved to sample/", count)


def test() -> None:
    """Quick connectivity test."""
    scraper = CCCSScraper()
    cases = scraper._get_case_urls()
    logger.info("Sitemap returned %d case URLs", len(cases))
    if cases:
        doc = scraper._fetch_case(cases[0])
        if doc:
            logger.info(
                "Test OK: %s — %d chars of text", doc["title"][:60], len(doc["text"])
            )
        else:
            logger.error("Test FAILED: could not parse first case")
    else:
        logger.error("Test FAILED: no case URLs found")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "test"
    if cmd == "bootstrap":
        sample = "--sample" in sys.argv
        bootstrap(sample=sample)
    elif cmd == "test":
        test()
    else:
        print(f"Usage: {sys.argv[0]} [bootstrap [--sample] | test]")
