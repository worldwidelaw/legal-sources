#!/usr/bin/env python3
"""
CD/Leganet -- DRC (Congo) Legislation Portal Fetcher

Fetches legislation from leganet.cd, a static HTML site with full-text
legal documents covering DRC law from 1886 to present.

Strategy:
  - Crawl 8 category index pages to discover document URLs
  - Fetch each HTML document and extract full text
  - Parse title tag for metadata (date, type, description)

Usage:
  python bootstrap.py bootstrap          # Fetch all documents
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import time
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Set
from urllib.parse import urljoin, unquote, quote

import requests
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.CD.Leganet")

# The publisher moved to www.leganet.be, which every internal link on the site now
# points at and which serves a valid Let's Encrypt cert. The old www.leganet.cd
# still answers but on a self-signed cPanel cert (CN=leganetcd.kihe2179.odns.fr,
# issuer == subject) that can never verify — that was the visible half of #1593.
BASE_URL = "https://www.leganet.be"

# Both domains serve the same DRC corpus, so a link to either is ours to follow.
KNOWN_HOSTS = ("leganet.be", "www.leganet.be", "leganet.cd", "www.leganet.cd")

# .cd has no verifiable cert and an AIA repair has nothing to fetch, but it serves
# the identical static HTML over plain HTTP with no redirect. If we ever land back
# on it, downgrade rather than abort: that is no weaker than trusting a self-signed
# cert, and unlike pinning the leaf it survives the yearly AutoSSL rotation.
TLS_DOWNGRADE_HOSTS = ("leganet.cd", "www.leganet.cd")

CATEGORY_PAGES = [
    "/Legislation/Tables/droit_civil.htm",
    "/Legislation/Tables/droit_economique.htm",
    "/Legislation/Tables/droit_judiciaire.htm",
    "/Legislation/Tables/droit_penal.htm",
    "/Legislation/Tables/droit_public.htm",
    "/Legislation/Tables/droit_social.htm",
    "/Legislation/Tables/droitfiscal.htm",
    "/Legislation/Tables/provinces.htm",
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.5",
}


class LeganetScraper(BaseScraper):
    """Scraper for CD/Leganet -- DRC legislation portal."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        # Set once the self-signed cert is observed, so we don't pay the failed
        # TLS handshake on every subsequent request.
        self._downgraded = False

    @staticmethod
    def _downgrade(url: str) -> str:
        """Rewrite https:// -> http:// for the allowlisted self-signed host."""
        if not url.startswith("https://"):
            return url
        host = url.split("/")[2].lower()
        if host not in TLS_DOWNGRADE_HOSTS:
            return url
        return "http://" + url[len("https://"):]

    @staticmethod
    def _sniff_encoding(content: bytes) -> str:
        """Pick a decoding for a response whose Content-Type omits charset.

        The site serves valid UTF-8 declared only in a <meta charset> tag, but
        older pages are still windows-1252. Hardcoding windows-1252 turned every
        accent into mojibake ("dEcembre"), which in turn broke the accented
        month regex in _parse_date and silently dated those documents to
        January 1st (#1593). Trust the meta tag, then verify by decoding.
        """
        head = content[:2048]
        m = re.search(rb"charset=[\"']?([\w-]+)", head, re.I)
        declared = m.group(1).decode("ascii", "ignore").lower() if m else None

        candidates = [declared] if declared else []
        candidates += ["utf-8", "windows-1252"]

        for enc in candidates:
            if not enc:
                continue
            try:
                content.decode(enc)
                return enc
            except (UnicodeDecodeError, LookupError):
                continue
        # windows-1252 maps every byte, so this is only reached on a bad alias.
        return "windows-1252"

    def _request(self, url: str, timeout: int = 30) -> Optional[requests.Response]:
        """HTTP GET with retry and rate limiting."""
        if self._downgraded:
            url = self._downgrade(url)

        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 406:
                    logger.warning(f"ModSecurity blocked request to {url[:80]}")
                    return None
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                if 'charset' not in resp.headers.get('content-type', ''):
                    resp.encoding = self._sniff_encoding(resp.content)
                return resp
            except requests.exceptions.SSLError as e:
                downgraded = self._downgrade(url)
                if downgraded == url:
                    logger.warning(f"Attempt {attempt+1} failed for {url[:80]}: {e}")
                else:
                    if not self._downgraded:
                        logger.warning(
                            "leganet.cd TLS verification failed (self-signed cert, "
                            "issue #1593) — falling back to plain HTTP for this host"
                        )
                        self._downgraded = True
                    url = downgraded
                    continue
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url[:80]}: {e}")
                if attempt < 2:
                    time.sleep(5 * (attempt + 1))
        return None

    def _discover_documents(self, max_docs: Optional[int] = None) -> List[Dict[str, str]]:
        """Crawl category pages to discover all document URLs."""
        seen_urls: Set[str] = set()
        documents: List[Dict[str, str]] = []

        for cat_path in CATEGORY_PAGES:
            cat_url = BASE_URL + cat_path
            cat_name = cat_path.split("/")[-1].replace(".htm", "")
            logger.info(f"Crawling category: {cat_name}")

            resp = self._request(cat_url)
            if resp is None:
                logger.warning(f"Failed to fetch category page: {cat_name}")
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            for a in soup.find_all("a", href=True):
                href = a["href"]
                link_text = a.get_text(strip=True)

                # Skip navigation, category links, PDFs, and external links.
                # Links used to be relative; the site now writes them absolute
                # against www.leganet.be, so filter on the resolved host instead
                # of rejecting everything that starts with "http" (#1593).
                if not href or href.startswith(("#", "mailto:", "javascript:")):
                    continue
                if "Tables/" in href or href == "../../" or not link_text:
                    continue

                # Only HTML documents (every doc has a PDF twin we skip)
                if not (href.endswith(".htm") or href.endswith(".html")):
                    continue

                # Skip table-of-contents and index pages
                lower_href = href.lower()
                if any(skip in lower_href for skip in ["table.htm", "index.htm", "sommaire"]):
                    continue

                full_url = urljoin(cat_url, href)

                # Stay on the publisher's own hosts, and normalize the legacy
                # .cd domain onto the canonical .be one so the same document
                # reached via either link dedups to a single _id.
                if full_url.split("/")[2].lower() not in KNOWN_HOSTS:
                    continue
                full_url = re.sub(
                    r"^https?://(?:www\.)?leganet\.(?:be|cd)", BASE_URL, full_url
                )

                # Only documents under /Legislation/
                if "/Legislation/" not in full_url:
                    continue

                if full_url in seen_urls:
                    continue
                seen_urls.add(full_url)

                documents.append({
                    "url": full_url,
                    "link_text": link_text[:200],
                    "category": cat_name,
                })

                if max_docs and len(documents) >= max_docs:
                    return documents

        if not documents:
            # Fail loud: a 0-URL crawl is always a transport/layout regression,
            # never a legitimately empty corpus (issue #1593).
            raise RuntimeError(
                f"Discovered 0 document URLs across {len(CATEGORY_PAGES)} category "
                f"pages on {BASE_URL} — expected ~2,200. Transport failure or "
                "site layout change; check the category page fetches above."
            )

        logger.info(f"Discovered {len(documents)} unique document URLs")
        return documents

    def _extract_document(self, url: str) -> Optional[Dict[str, Any]]:
        """Fetch a document page and extract metadata + full text."""
        resp = self._request(url)
        if resp is None:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract title
        title = ""
        if soup.title and soup.title.string:
            title = soup.title.string.strip()
            # Clean up multi-line titles
            title = re.sub(r"\s+", " ", title)

        if not title or title.lower() in ("404 not found", "not found"):
            return None

        # Extract body text
        body = soup.find("body")
        if not body:
            return None

        # Remove script and style tags
        for tag in body.find_all(["script", "style"]):
            tag.decompose()

        text = body.get_text(separator="\n", strip=True)

        # Remove the LEGANET.CD header watermarks
        text = re.sub(r"(LEGANET\.CD\s*)+", "", text)
        # Clean excessive whitespace
        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r" {2,}", " ", text)
        text = text.strip()

        if len(text) < 100:
            return None

        # Parse date from title
        date = self._parse_date(title)

        # Generate document ID from URL path
        path = unquote(url.replace(BASE_URL, ""))
        doc_id = hashlib.md5(path.encode("utf-8")).hexdigest()[:12]

        return {
            "document_id": f"CD-LEG-{doc_id}",
            "title": title,
            "text": text,
            "date": date,
            "url": url,
            "path": path,
        }

    def _parse_date(self, title: str) -> Optional[str]:
        """Extract date from document title like '15 février 1965. - ORDONNANCE 44'."""
        # French month names, keyed on the accent-stripped form so the lookup
        # below can never miss. Keying on the accented spelling used to drop
        # "décembre" -> "decembre" out of the dict, silently dating every
        # December document to January (#1593).
        months = {
            "janvier": "01", "fevrier": "02", "mars": "03", "avril": "04",
            "mai": "05", "juin": "06", "juillet": "07", "aout": "08",
            "septembre": "09", "octobre": "10", "novembre": "11", "decembre": "12",
        }

        # Pattern: DD month YYYY
        m = re.search(
            # \s* not \s+ : titles are typed by hand and run the day into the
            # month ("DU 16juin 2011"), which otherwise fell back to Jan 1st.
            r"(\d{1,2})\s*(janvier|f[eé]vrier|mars|avril|mai|juin|juillet|ao[uû]t|"
            r"septembre|octobre|novembre|d[eé]cembre)\s+(\d{4})",
            title, re.IGNORECASE,
        )
        if m:
            day = int(m.group(1))
            month_name = m.group(2).lower().replace("é", "e").replace("û", "u")
            year = m.group(3)
            month = months.get(month_name)
            if month is None:
                logger.warning(f"Unmapped French month {month_name!r} in title: {title[:80]}")
                return None
            return f"{year}-{month}-{day:02d}"

        # Pattern: just a year
        m = re.search(r"\b(1[89]\d{2}|20[0-2]\d)\b", title)
        if m:
            return f"{m.group(1)}-01-01"

        return None

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "_id": raw.get("document_id", ""),
            "_source": "CD/Leganet",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "url": raw.get("url", ""),
            "category": raw.get("category", ""),
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Fetch all legislation documents (yields raw dicts for BaseScraper to normalize)."""
        documents = self._discover_documents()
        count = 0

        for doc_info in documents:
            doc = self._extract_document(doc_info["url"])
            if doc is None:
                continue

            doc["category"] = doc_info.get("category", "")
            if doc.get("text"):
                count += 1
                yield doc

        logger.info(f"Completed: {count} documents fetched")

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Fetch recent documents (static site, so just first pages)."""
        yield from self.fetch_all()

    def test(self) -> bool:
        """Quick connectivity test."""
        resp = self._request(BASE_URL + CATEGORY_PAGES[0])
        if resp is None:
            logger.error("Cannot reach leganet.cd")
            return False

        soup = BeautifulSoup(resp.text, "html.parser")
        links = [a for a in soup.find_all("a", href=True)
                 if a["href"].endswith((".htm", ".html"))]
        logger.info(f"Category page OK: {len(links)} links found")

        # Test a document page
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if (href.endswith((".htm", ".html")) and "Tables/" not in href
                    and not href.startswith("http") and not href.startswith("#")):
                full_url = urljoin(BASE_URL + CATEGORY_PAGES[0], href)
                if "/Legislation/" in full_url:
                    doc = self._extract_document(full_url)
                    if doc:
                        logger.info(f"Document OK: {doc['title'][:60]} ({len(doc['text'])} chars)")
                        return True

        logger.error("No document could be fetched")
        return False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="CD/Leganet data fetcher")
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

    scraper = LeganetScraper()

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
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
