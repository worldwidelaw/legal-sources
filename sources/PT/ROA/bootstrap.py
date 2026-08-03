#!/usr/bin/env python3
"""
PT/ROA -- Revista da Ordem dos Advogados Fetcher

Fetches legal doctrine articles from the official journal of the Portuguese
Bar Association (Ordem dos Advogados).

Strategy:
  - Discovery: Crawl year index pages at portal.oa.pt, then issue pages
  - Full text: Individual article PDFs at /media/{id}/{slug}.pdf
  - Text extraction: pdfminer via common.pdf_extract
  - Archive: 1941 to present, ~10-15 articles per issue, 2 issues/year

Data types:
  - Doutrina (Doctrine/Theory)
  - Jurisprudência Comentada (Annotated Case Law)
  - Editorial

License: Open access (free online, stated on journal pages)

Usage:
  python bootstrap.py bootstrap          # Full initial pull
  python bootstrap.py bootstrap --sample # Fetch 12 sample records for validation
  python bootstrap.py bootstrap-fast     # Alias for bootstrap --sample
"""

import html as html_mod
import json
import logging
import re
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import urljoin

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://portal.oa.pt"
INDEX_URL = f"{BASE_URL}/publicacoes/revista-da-ordem-dos-advogados/"
SOURCE_ID = "PT/ROA"
SOURCE_DIR = Path(__file__).resolve().parent
SAMPLE_DIR = SOURCE_DIR / "sample"

# (connect, read) timeouts — the read value caps time *between* bytes only.
DEFAULT_TIMEOUT = (10, 30)
# Hard wall-clock cap and size cap for a single PDF download so a server that
# trickles the body slowly can't wedge the whole run (see issue #1148).
PDF_DEADLINE_SECONDS = 180
MAX_PDF_BYTES = 60 * 1024 * 1024
# Hard wall-clock cap for a single article's download+extraction combined
# (see issue #1168). The #1148 download deadline can't bound PDF *text
# extraction*: opendataloader's in-process Java parser (and pdfminer) can loop
# indefinitely on a malformed PDF, wedging the run with no log output ("no
# activity ~4h, records frozen"). Running each article in a daemon thread lets
# the main sweep skip a stuck article and keep making progress; a daemon thread
# also can't block interpreter exit if the underlying native call never returns.
ARTICLE_DEADLINE_SECONDS = 300


class ROAFetcher:
    """Fetcher for Revista da Ordem dos Advogados articles."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Legal-Data-Hunter/1.0 (https://github.com/ZachLaik/LegalDataHunter)",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

    def _get(self, url: str, retries: int = 2, timeout=DEFAULT_TIMEOUT) -> Optional[requests.Response]:
        for attempt in range(retries + 1):
            try:
                resp = self.session.get(url, timeout=timeout)
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                if attempt < retries:
                    time.sleep(2 * (attempt + 1))
                else:
                    logger.error(f"Failed to fetch {url}: {e}")
                    return None

    def _get_pdf_bytes(self, url: str, retries: int = 2) -> Optional[bytes]:
        """Stream-download a PDF with a hard wall-clock deadline and size cap.

        A scalar read timeout only bounds the gap between successive bytes, so a
        server that dribbles the body can keep a socket alive indefinitely and
        wedge the whole run (issue #1148). Streaming with an explicit deadline
        guarantees a single fetch can never stall the sweep for more than
        PDF_DEADLINE_SECONDS.
        """
        for attempt in range(retries + 1):
            try:
                with self.session.get(url, timeout=DEFAULT_TIMEOUT, stream=True) as resp:
                    resp.raise_for_status()
                    ctype = resp.headers.get("Content-Type", "")
                    if not ctype.startswith("application/pdf"):
                        return None
                    deadline = time.monotonic() + PDF_DEADLINE_SECONDS
                    chunks = []
                    total = 0
                    for chunk in resp.iter_content(chunk_size=65536):
                        if chunk:
                            chunks.append(chunk)
                            total += len(chunk)
                            if total > MAX_PDF_BYTES:
                                logger.warning(f"PDF exceeds {MAX_PDF_BYTES} byte cap, skipping: {url}")
                                return None
                        if time.monotonic() > deadline:
                            raise requests.Timeout(
                                f"PDF download exceeded {PDF_DEADLINE_SECONDS}s deadline"
                            )
                    return b"".join(chunks)
            except requests.RequestException as e:
                if attempt < retries:
                    time.sleep(2 * (attempt + 1))
                else:
                    logger.error(f"Failed to download PDF {url}: {e}")
                    return None
        return None

    def _extract_text(self, pdf_bytes: bytes) -> str:
        return extract_pdf_markdown(
            source=SOURCE_ID,
            source_id="",
            pdf_bytes=pdf_bytes,
            table="doctrine",
        ) or ""

    def discover_year_urls(self) -> List[str]:
        """Get all year page URLs from the main index (all paginated pages)."""
        year_urls = []
        seen = set()

        for page in range(1, 10):
            url = f"{INDEX_URL}?page={page}" if page > 1 else INDEX_URL
            resp = self._get(url)
            if not resp:
                break

            # Extract year links: /publicacoes/revista-da-ordem-dos-advogados/ano-XXXX/
            links = re.findall(
                r'href="(/publicacoes/revista-da-ordem-dos-advogados/ano-\d{4}/)"',
                resp.text,
            )
            if not links:
                break

            for link in links:
                full = urljoin(BASE_URL, link)
                if full not in seen:
                    seen.add(full)
                    year_urls.append(full)

            time.sleep(0.5)

        logger.info(f"Found {len(year_urls)} year pages")
        return sorted(year_urls, reverse=True)

    def discover_issue_urls(self, year_url: str) -> List[Dict[str, str]]:
        """Get all issue page URLs from a year page."""
        resp = self._get(year_url)
        if not resp:
            return []

        issues = []
        # Match links to specific issue pages (exclude subscription pages)
        links = re.findall(
            r'href="(/publicacoes/revista-da-ordem-dos-advogados/ano-\d{4}/[^"]+)"',
            resp.text,
        )
        for link in links:
            full = urljoin(BASE_URL, link)
            slug = link.rstrip("/").split("/")[-1]
            # Skip subscription pages
            if "assinatura" in slug.lower():
                continue
            if full not in [i["url"] for i in issues]:
                issues.append({"url": full, "slug": slug})

        return issues

    def discover_articles(self, issue_url: str, issue_slug: str) -> List[Dict[str, Any]]:
        """Extract article metadata from an issue page.

        HTML structure per article (modern issues):
          <p><strong>Author</strong><strong> | </strong>
             <a href="/media/XXX/slug.pdf"><em>Title</em></a></p>

        Editorials may differ:
          <p><strong>Editorial</strong></p>
          <p><a href="/media/XXX/slug.pdf"><strong>Author</strong></a></p>
        """
        resp = self._get(issue_url)
        if not resp:
            return []

        html = resp.text
        articles = []
        seen_ids = set()

        year_match = re.search(r"/ano-(\d{4})/", issue_url)
        year = year_match.group(1) if year_match else ""

        # Track current section from headings like DOUTRINA, JURISPRUDÊNCIA COMENTADA
        current_section = "doutrina"

        # Primary pattern: <strong>Author</strong> ... <a href="/media/XXX/slug.pdf"> ... Title ... </a>
        # Match author + PDF link in same <p> block
        pattern = re.compile(
            r'<strong>([^<]+)</strong>'           # author in bold
            r'(?:</?strong>|\s|<br\s*/?>|\|)*'    # separator (|, whitespace, tags)
            r'<a[^>]+href="(/media/(\d+)/([^"]+\.pdf))"[^>]*>'  # PDF link
            r'[^<]*(?:<em>|<strong>)?'             # optional inner formatting
            r'([^<]+)',                            # title text
            re.DOTALL,
        )

        for m in pattern.finditer(html):
            author_raw = html_mod.unescape(m.group(1)).strip()
            pdf_path = m.group(2)
            media_id = m.group(3)
            pdf_filename = m.group(4).lower()
            title_raw = html_mod.unescape(m.group(5)).strip()

            # Skip full-issue PDFs
            if re.search(r"roa.*web|revista.*web|completa|integral", pdf_filename):
                continue
            if media_id in seen_ids:
                continue

            # Skip section headers captured as "author"
            if author_raw.upper() in ("DOUTRINA", "JURISPRUDÊNCIA COMENTADA",
                                       "INFORMAÇÃO INSTITUCIONAL", "EDITORIAL"):
                current_section = self._classify_section_name(author_raw)
                continue

            # Detect section from preceding HTML
            preceding = html[max(0, m.start() - 300):m.start()]
            section = self._detect_section(preceding, current_section)

            # Clean author: strip trailing pipes/whitespace
            author = re.sub(r'\s*\|\s*$', '', author_raw).strip()

            # Clean title
            title = title_raw.strip().rstrip('.')
            if not title or len(title) < 3:
                title = author_raw  # editorial: author is the link text

            seen_ids.add(media_id)
            pdf_url = urljoin(BASE_URL, f"/media/{media_id}/{m.group(4)}")

            articles.append({
                "id": media_id,
                "title": title,
                "author": author,
                "section": section,
                "pdf_url": pdf_url,
                "issue_url": issue_url,
                "issue_slug": issue_slug,
                "year": year,
            })

        # Fallback: find any PDF links not yet captured (older issues may differ)
        for fm in re.finditer(r'href="(/media/(\d+)/([^"]+\.pdf))"', html):
            media_id = fm.group(2)
            pdf_filename = fm.group(3).lower()
            if media_id in seen_ids:
                continue
            if re.search(r"roa.*web|revista.*web|completa|integral", pdf_filename):
                continue

            seen_ids.add(media_id)
            pdf_url = urljoin(BASE_URL, fm.group(1))
            author = self._author_from_filename(pdf_filename)
            title = self._title_from_context(html, fm.start())

            articles.append({
                "id": media_id,
                "title": title or author,
                "author": author,
                "section": "doutrina",
                "pdf_url": pdf_url,
                "issue_url": issue_url,
                "issue_slug": issue_slug,
                "year": year,
            })

        logger.info(f"  Issue {issue_slug}: {len(articles)} articles")
        return articles

    def _classify_section_name(self, name: str) -> str:
        n = name.upper().strip()
        if "JURISPRUD" in n:
            return "jurisprudencia_comentada"
        if "EDITORIAL" in n:
            return "editorial"
        if "INFORMA" in n:
            return "informacao_institucional"
        return "doutrina"

    def _detect_section(self, preceding_html: str, default: str) -> str:
        """Detect current section from preceding HTML."""
        # Look for the last section heading before this article
        sections = re.findall(
            r'<strong>\s*(DOUTRINA|EDITORIAL|JURISPRUD[ÊE]NCIA\s+COMENTADA|INFORMA[ÇC][ÃA]O\s+INSTITUCIONAL)\s*</strong>',
            preceding_html, re.I,
        )
        if sections:
            return self._classify_section_name(sections[-1])
        return default

    def _author_from_filename(self, pdf_filename: str) -> str:
        """Extract author name from PDF filename."""
        name = pdf_filename.replace(".pdf", "")
        if re.match(r"roa|editorial|indice|sumario|informac", name, re.I):
            return ""
        name = re.sub(r"-jc$", "", name)
        parts = name.split("-")
        return " ".join(
            p.lower() if p.lower() in ("de", "da", "do", "das", "dos", "e") else p.capitalize()
            for p in parts
        )

    def _title_from_context(self, html: str, link_pos: int) -> str:
        """Extract title from HTML context near a PDF link (fallback)."""
        start = max(0, link_pos - 300)
        end = min(len(html), link_pos + 300)
        ctx = html[start:end]
        # Look for <em> text (title is often in italics)
        m = re.search(r'<em>([^<]{5,200})</em>', ctx)
        if m:
            return html_mod.unescape(m.group(1)).strip()
        # Look for title= attribute
        m = re.search(r'title="([^"]{5,200})"', ctx)
        if m:
            t = html_mod.unescape(m.group(1)).strip()
            if not t.endswith('.pdf'):
                return t
        return ""

    def fetch_article(self, article: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Download a PDF and extract text for an article."""
        pdf_bytes = self._get_pdf_bytes(article["pdf_url"])
        if not pdf_bytes:
            logger.warning(f"Could not download PDF: {article['pdf_url']}")
            return None

        text = self._extract_text(pdf_bytes)
        if not text or len(text) < 100:
            logger.warning(f"Insufficient text from {article['pdf_url']}: {len(text) if text else 0} chars")
            return None

        return {**article, "text": text}

    def fetch_article_guarded(self, article: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Run fetch_article under a hard wall-clock deadline (issue #1168).

        A malformed PDF can hang text extraction (opendataloader's Java parser or
        pdfminer) with no output, wedging the whole sweep. Running the download +
        extraction in a daemon thread lets us abandon a stuck article and move on;
        the daemon thread won't block interpreter exit if the native call never
        returns.
        """
        box: Dict[str, Any] = {}

        def worker():
            try:
                box["value"] = self.fetch_article(article)
            except Exception as e:  # noqa: BLE001 — never let a worker crash wedge the run
                box["error"] = e

        t = threading.Thread(target=worker, daemon=True)
        t.start()
        t.join(ARTICLE_DEADLINE_SECONDS)
        if t.is_alive():
            logger.warning(
                f"Article fetch exceeded {ARTICLE_DEADLINE_SECONDS}s deadline, skipping: {article['pdf_url']}"
            )
            return None
        if "error" in box:
            logger.error(f"Article fetch failed {article['pdf_url']}: {box['error']}")
            return None
        return box.get("value")

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw article record."""
        year = raw.get("year", "")
        date_str = f"{year}-01-01" if year else None

        return {
            "_id": f"PT-ROA-{raw['id']}",
            "_source": SOURCE_ID,
            "_type": "doctrine",
            "_fetched_at": datetime.utcnow().isoformat() + "Z",
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "author": raw.get("author", ""),
            "date": date_str,
            "url": raw.get("pdf_url", ""),
            "section": raw.get("section", "doutrina"),
            "volume": raw.get("issue_slug", ""),
            "source_url": raw.get("issue_url", ""),
        }

    def fetch_all(self, limit: int = None) -> Iterator[Dict[str, Any]]:
        """Fetch all articles from all years."""
        count = 0
        year_urls = self.discover_year_urls()

        for year_url in year_urls:
            if limit and count >= limit:
                return

            issues = self.discover_issue_urls(year_url)
            time.sleep(0.5)

            for issue in issues:
                if limit and count >= limit:
                    return

                articles = self.discover_articles(issue["url"], issue["slug"])
                time.sleep(0.5)

                for article in articles:
                    if limit and count >= limit:
                        return

                    result = self.fetch_article_guarded(article)
                    if result:
                        yield self.normalize(result)
                        count += 1
                        logger.info(f"[{count}] {result['title'][:80]}")

                    time.sleep(1)

        logger.info(f"Total articles fetched: {count}")


def bootstrap(sample: bool = False):
    """Run the bootstrap process."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    fetcher = ROAFetcher()

    limit = 15 if sample else None
    records = []

    for record in fetcher.fetch_all(limit=limit):
        records.append(record)
        # Save individual sample
        fname = SAMPLE_DIR / f"{record['_id']}.json"
        fname.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")

    logger.info(f"Saved {len(records)} records to {SAMPLE_DIR}")

    if records:
        texts = [r["text"] for r in records if r.get("text")]
        avg_len = sum(len(t) for t in texts) / len(texts) if texts else 0
        logger.info(f"Average text length: {avg_len:.0f} chars")

    return records


def main():
    args = sys.argv[1:]

    if not args:
        print("Usage: python bootstrap.py bootstrap [--sample]")
        print("       python bootstrap.py bootstrap-fast")
        sys.exit(1)

    cmd = args[0]
    sample = "--sample" in args or cmd == "bootstrap-fast"

    if cmd in ("bootstrap", "bootstrap-fast"):
        records = bootstrap(sample=sample)
        print(f"Done. {len(records)} records.")
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)


if __name__ == "__main__":
    main()
