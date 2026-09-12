#!/usr/bin/env python3
"""
ST/TribunalContas -- Tribunal de Contas de São Tomé e Príncipe (Court of Audit)

São Tomé e Príncipe's supreme audit institution. Publishes acórdãos and decisões
of the 1st and 2nd Sections (case law on prior-visa refusals, financial liability
and account judgments), pareceres on the Conta Geral do Estado and on the RAP/INSS
accounts, audit and account-verification reports, and the Court's own normative
acts (instruções, resoluções, deliberações).

Access path
-----------
tcontas.st is a **Wix** site (NOT Google Sites — the pre-build manifest note was
wrong). Every document is a born-digital PDF served from
``<site-uuid>.usrfiles.com/ugd/27dc02_<hash>.pdf``.

Enumeration goes through the site's own ``/sitemap.xml`` index, which exposes one
``dynamic-<collection>_p_<uuid>_0_5000-sitemap.xml`` per Wix Data collection plus
``pages-sitemap.xml`` for the static pages. That is the closest thing this site has
to a structured index — there is no REST/JSON API, no ELI, no open-data portal
entry for ST.

Each page server-side-renders its repeater, so a rendered page carries every item
of its collection: one ``<div class="... wixui-repeater__item">`` per document,
holding the publication date, the title, and the anchor to the PDF. Anchors ARE
present in the HTML (contrary to the manifest note) but they point at
``usrfiles.com``, not at a ``*.pdf`` path under tcontas.st — a naive
``href="*.pdf"`` scrape finds nothing, hence the fail-loud guard below.

The same document is reachable from several pages (a dynamic detail page renders
the whole collection) and from the ``/fr/`` mirrors, so everything is deduped on
the ``27dc02_<hash>`` PDF id.

Usage:
  python bootstrap.py bootstrap          # Full pull
  python bootstrap.py bootstrap --sample # Fetch sample records
  python bootstrap.py update             # Incremental (new PDF ids only)
  python bootstrap.py discover           # Print the discovered index, no downloads
  python bootstrap.py test               # Quick connectivity test
"""

import re
import sys
import time
import html as html_mod
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional, List
from urllib.parse import urljoin, unquote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str
from common.http_client import HttpClient
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.ST.TribunalContas")

SOURCE_ID = "ST/TribunalContas"
BASE_URL = "https://www.tcontas.st"
SITEMAP_URL = f"{BASE_URL}/sitemap.xml"

# Wix sitemaps that hold no legal documents. Blog posts / events are news items,
# member profiles and store categories are site furniture.
SKIP_SITEMAPS = (
    "store-categories",
    "member-profiles",
    "blog-posts",
    "blog-categories",
    "event-pages",
)

# Static pages that are navigation furniture, not document listings.
SKIP_PATHS = {
    "", "/", "/home", "/c%C3%B3pia-home", "/cópia-home", "/o-tribunal",
    "/identidade", "/organograma", "/magistrados", "/mp", "/galeria",
    "/videos-institucionais", "/eventos", "/noticias", "/fale-connosco",
    "/form-denuncia", "/vitrine", "/cv-ricardino",
}

LOC_RE = re.compile(r"<loc>\s*([^<\s]+)\s*</loc>", re.I)
ITEM_RE = re.compile(r"<div[^>]*\bwixui-repeater__item\b[^>]*>", re.I)
PDF_RE = re.compile(
    r"https://[a-z0-9.-]*usrfiles\.com/ugd/([a-z0-9_]+)\.pdf", re.I
)
TAG_RE = re.compile(r"(?s)<[^>]+>")
SCRIPT_RE = re.compile(r"(?is)<(script|style)[^>]*>.*?</\1>")
TITLE_TAG_RE = re.compile(r"(?is)<title[^>]*>(.*?)</title>")

PT_MONTHS = {
    "janeiro": 1, "fevereiro": 2, "março": 3, "marco": 3, "abril": 4,
    "maio": 5, "junho": 6, "julho": 7, "agosto": 8, "setembro": 9,
    "outubro": 10, "novembro": 11, "dezembro": 12,
}
PT_DATE_RE = re.compile(
    r"^(\d{1,2})\s+de\s+([a-zçã]+)\s+de\s+(\d{4})$", re.I
)

# A repeater item that has lost its PDF anchor would otherwise be silently
# dropped; anything shorter than this many chars of extracted text is not a
# real Tribunal de Contas document.
MIN_TEXT_CHARS = 200

# Section prefix -> (_type, human label). Matched against the first path segment.
SECTION_TYPES = {
    "acordaos": ("case_law", "Acórdão"),
    "decisoes": ("case_law", "Decisão"),
    "instrucoes": ("legislation", "Instrução"),
    "resolu": ("legislation", "Resolução"),
    "deliberacoes": ("legislation", "Deliberação"),
}


def _pt_date_to_iso(raw: str) -> Optional[str]:
    """'9 de julho de 2026' -> '2026-07-09'."""
    m = PT_DATE_RE.match(raw.strip())
    if not m:
        return None
    day, month_name, year = m.groups()
    month = PT_MONTHS.get(month_name.lower())
    if not month:
        return None
    try:
        return datetime(int(year), month, int(day)).date().isoformat()
    except ValueError:
        return None


def _text_lines(fragment: str) -> List[str]:
    """Visible text lines of an HTML fragment, in document order."""
    stripped = SCRIPT_RE.sub(" ", fragment)
    text = TAG_RE.sub("\n", stripped)
    text = html_mod.unescape(text)
    return [line.strip() for line in text.split("\n") if line.strip()]


def _classify(path: str) -> tuple:
    """Map a tcontas.st path to (_type, label) using its first path segment."""
    segment = path.lstrip("/").split("/", 1)[0].lower()
    for prefix, value in SECTION_TYPES.items():
        if segment.startswith(prefix):
            return value
    # Pareceres, auditorias, verificações de contas, planos, relatórios,
    # manuais and the código de ética are official Court writing about the
    # public accounts rather than a decision on a case or a normative act.
    return ("doctrine", "Relatório/Parecer")


class TribunalContasScraper(BaseScraper):
    """Scraper for the São Tomé e Príncipe Court of Audit (Wix site)."""

    def __init__(self, source_dir: Optional[str] = None):
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url=BASE_URL,
            timeout=90,
            headers={
                # Wix/CloudFront serves the SSR'd repeater to browser UAs; the
                # default bot UA is worth A/B-ing again if this ever 403s.
                "User-Agent": (
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/126.0 Safari/537.36"
                ),
                "Accept-Language": "pt-PT,pt;q=0.9",
            },
        )

    # ── Discovery ────────────────────────────────────────────────────

    def _get_text(self, url: str, attempts: int = 3) -> Optional[str]:
        """Fetch a page as text.

        These pages are ~1.3 MB of Wix HTML and the origin intermittently cuts
        the chunked response ("Response ended prematurely"); urllib3's retry
        strategy only covers status codes, so retry the read here too. A page
        lost this way costs real documents, because each page renders its whole
        collection.
        """
        last_error = None
        for attempt in range(attempts):
            try:
                self.rate_limiter.wait()
                response = self.client.get(url)
                response.raise_for_status()
                return response.text
            except Exception as exc:  # noqa: BLE001 - retried, then reported
                last_error = exc
                if attempt + 1 < attempts:
                    time.sleep(2 * (attempt + 1))
        logger.warning("fetch failed after %d attempts %s: %s", attempts, url, last_error)
        return None

    def _sitemap_urls(self) -> List[str]:
        """Every content page URL, from the Wix sitemap index."""
        index = self._get_text(SITEMAP_URL)
        if not index:
            raise RuntimeError(
                f"{SOURCE_ID}: {SITEMAP_URL} unreachable — cannot enumerate the site"
            )
        sitemaps = LOC_RE.findall(index)
        if not sitemaps:
            raise RuntimeError(
                f"{SOURCE_ID}: sitemap index returned no <loc> entries "
                "(layout change or block) — refusing to report an empty corpus"
            )

        urls: List[str] = []
        for sitemap in sitemaps:
            if any(skip in sitemap for skip in SKIP_SITEMAPS):
                continue
            body = self._get_text(sitemap)
            if not body:
                continue
            found = LOC_RE.findall(body)
            logger.info("  %-70s %d urls", sitemap.rsplit("/", 1)[-1], len(found))
            urls.extend(found)

        keep, seen = [], set()
        for url in urls:
            path = url.replace(BASE_URL, "")
            # /fr/ pages mirror the Portuguese originals and point at the same PDFs.
            if path.startswith("/fr/") or unquote(path) in SKIP_PATHS or path in SKIP_PATHS:
                continue
            if url in seen:
                continue
            seen.add(url)
            keep.append(url)
        return keep

    def _items_on_page(self, url: str) -> List[Dict[str, Any]]:
        """Parse the server-rendered Wix repeater on one page into documents."""
        page = self._get_text(url)
        if not page:
            return []

        starts = [m.start() for m in ITEM_RE.finditer(page)]
        items: List[Dict[str, Any]] = []
        for i, start in enumerate(starts):
            end = starts[i + 1] if i + 1 < len(starts) else len(page)
            block = page[start:end]
            pdf_match = PDF_RE.search(block)
            if not pdf_match:
                continue
            lines = _text_lines(block)
            date_iso, title = None, None
            for line in lines[:6]:
                if date_iso is None:
                    parsed = _pt_date_to_iso(line)
                    if parsed:
                        date_iso = parsed
                        continue
                if title is None and len(line) > 3 and not line.startswith("."):
                    title = line
                if date_iso and title:
                    break
            items.append(
                {
                    "pdf_id": pdf_match.group(1),
                    "pdf_url": pdf_match.group(0),
                    "title": title,
                    "date": date_iso,
                    "page_url": url,
                }
            )

        if items:
            return items

        # Pages without a repeater (e.g. /p-estrategicos, /relatorios-atividades)
        # link their PDFs directly; fall back to the page's own <title>.
        page_title = None
        title_match = TITLE_TAG_RE.search(page)
        if title_match:
            page_title = html_mod.unescape(TAG_RE.sub("", title_match.group(1)))
            page_title = page_title.split("|")[0].strip()
        for pdf_match in PDF_RE.finditer(page):
            items.append(
                {
                    "pdf_id": pdf_match.group(1),
                    "pdf_url": pdf_match.group(0),
                    "title": page_title,
                    "date": None,
                    "page_url": url,
                }
            )
        return items

    def discover(self) -> List[Dict[str, Any]]:
        """Full document index, deduped on the PDF id."""
        logger.info("Enumerating %s via sitemap index", BASE_URL)
        pages = self._sitemap_urls()
        logger.info("%d content pages to scan", len(pages))

        docs: Dict[str, Dict[str, Any]] = {}
        for page_url in pages:
            for item in self._items_on_page(page_url):
                previous = docs.get(item["pdf_id"])
                if previous is None:
                    docs[item["pdf_id"]] = item
                elif previous.get("date") is None and item.get("date"):
                    # Prefer the copy that carries a parsed date/title.
                    docs[item["pdf_id"]] = item

        if not docs:
            raise RuntimeError(
                f"{SOURCE_ID}: scanned {len(pages)} pages and found 0 PDF documents. "
                "usrfiles.com anchors are gone or the site is blocking this vantage — "
                "failing loud rather than reporting an empty corpus."
            )
        logger.info("Discovered %d distinct documents", len(docs))
        return self._interleave_by_type(list(docs.values()))

    @staticmethod
    def _interleave_by_type(docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Round-robin the index across case_law / legislation / doctrine.

        The sitemap order is collection-by-collection, so the first 10 documents
        are all `vic2020` account verifications — a sample drawn from it says
        nothing about the acórdãos, which are this source's headline content.
        Interleaving also means a crawl that dies early still carries all three
        document types rather than one.
        """
        buckets: Dict[str, List[Dict[str, Any]]] = {
            "case_law": [], "legislation": [], "doctrine": [],
        }
        for doc in docs:
            path = doc["page_url"].replace(BASE_URL, "")
            buckets[_classify(path)[0]].append(doc)
        for bucket in buckets.values():
            bucket.sort(key=lambda d: (d.get("date") or "", d["pdf_id"]), reverse=True)

        ordered: List[Dict[str, Any]] = []
        for row in range(max(len(b) for b in buckets.values())):
            for name in ("case_law", "legislation", "doctrine"):
                if row < len(buckets[name]):
                    ordered.append(buckets[name][row])
        return ordered

    # ── BaseScraper interface ────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        yield from self.discover()

    def fetch_updates(self, since) -> Generator[dict, None, None]:
        """Yield documents published on or after `since`.

        The site exposes no per-document modification stamp, so the comparator
        is the item's own publication date as rendered in the repeater — which
        for this site *is* the date the PDF became available to us. Documents
        whose date failed to parse are always yielded so a formatting change
        upstream cannot silently freeze the corpus.
        """
        cutoff = as_date_str(since)
        for doc in self.discover():
            if not doc.get("date") or doc["date"] >= cutoff:
                yield doc

    def normalize(self, raw: dict) -> dict:
        path = raw["page_url"].replace(BASE_URL, "")
        doc_type, label = _classify(path)
        table = "case_law" if doc_type == "case_law" else doc_type

        text = extract_pdf_markdown(
            SOURCE_ID,
            raw["pdf_id"],
            pdf_url=raw["pdf_url"],
            table=table,
        )
        if not text or len(text.strip()) < MIN_TEXT_CHARS:
            logger.warning(
                "no usable text for %s (%s) — skipping",
                raw["pdf_id"],
                raw["pdf_url"],
            )
            return None

        title = raw.get("title") or f"{label} — {raw['pdf_id']}"
        return {
            "_id": f"{SOURCE_ID}/{raw['pdf_id']}",
            "_source": SOURCE_ID,
            "_type": doc_type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title.strip(),
            "text": text.strip(),
            "date": raw.get("date"),
            "url": raw["page_url"],
            "pdf_url": raw["pdf_url"],
            "document_class": label,
            "section": path.lstrip("/").split("/", 1)[0],
            "language": "pt",
            "country": "ST",
            "court": "Tribunal de Contas de São Tomé e Príncipe",
        }

    # ── CLI helpers ──────────────────────────────────────────────────

    def print_discovery(self):
        docs = self.discover()
        by_section: Dict[str, int] = {}
        for doc in docs:
            section = doc["page_url"].replace(BASE_URL, "").lstrip("/").split("/")[0]
            by_section[section] = by_section.get(section, 0) + 1
        for section, count in sorted(by_section.items(), key=lambda kv: -kv[1]):
            print(f"{count:5d}  {section}")
        print(f"{len(docs):5d}  TOTAL")

    def test_connection(self):
        try:
            docs = self.discover()
            print(f"OK: discovered {len(docs)} documents")
            sample = docs[0]
            print(f"  {sample['date']}  {sample['title']}")
            text = extract_pdf_markdown(
                SOURCE_ID, sample["pdf_id"], pdf_url=sample["pdf_url"], force=True
            )
            print(f"  text: {len(text or '')} chars")
            if not text or len(text) < MIN_TEXT_CHARS:
                print("FAIL: PDF extraction returned no usable text")
                sys.exit(1)
            print("OK: full text extraction working")
        except Exception as exc:  # noqa: BLE001
            print(f"FAIL: {exc}")
            sys.exit(1)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point (issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|discover|test] [--sample]")
        sys.exit(1)

    scraper = TribunalContasScraper()
    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        scraper.test_connection()
    elif command == "discover":
        scraper.print_discovery()
    elif command == "bootstrap":
        sample_size = 15
        for arg in sys.argv[2:]:
            if arg.startswith("--sample-size="):
                sample_size = int(arg.split("=", 1)[1])
        scraper.bootstrap(sample_mode=sample_mode, sample_size=sample_size)
    elif command == "update":
        scraper.update()
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
