#!/usr/bin/env python3
"""
HT/CONATEL-Regulations — Conseil National des Télécommunications (Haiti)

Fetches Haiti's telecom legal and regulatory corpus: the founding decree,
the organic law, the telecom law, the taxation decree, the regulatory
decisions (décisions réglementaires) of CONATEL, the national numbering
plan, the digital TV transition plan, and the licensing procedures.

Strategy:
  1. Crawl a set of seed pages (Drupal /node/ pages + the regulatory-texts
     hub) for PDF links under /sites/default/files/.
  2. Follow /node/ links discovered on the hub pages one level deep to pick
     up additional document pages.
  3. Download each PDF and extract text with pdfminer.
  4. Skip scanned PDFs with no text layer.

Usage:
  python bootstrap.py bootstrap           # Full pull
  python bootstrap.py bootstrap --sample  # Sample records for validation
  python bootstrap.py test-api            # Quick connectivity test
"""

import io
import re
import sys
import json
import logging
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional
from urllib.parse import unquote, urljoin

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.HT.CONATEL-Regulations")

BASE_URL = "https://conatel.gouv.ht"
SOURCE_ID = "HT/CONATEL-Regulations"

# Hub pages that list links to many document pages. /node/ links found on
# these are followed one level deep to discover their PDFs.
HUB_PAGES = [
    "/textes-r-glementaires",
    "/node/113",   # Décisions réglementaires du CONATEL
    "/node/115",   # Régimes de licences et procédures
    "/node/133",   # Textes légaux
    "/node/134",   # Décisions
    "/proc%C3%A9dures",
]

# Document pages known to host a single PDF (plus any discovered via hubs).
SEED_PAGES = [
    "/node/108",   # Loi organique
    "/node/109",   # Loi sur les télécommunications
    "/node/110",   # Décret sur la taxation
    "/node/111",   # Loi créant le CONATEL
    "/node/113",   # Décisions réglementaires
    "/node/114",
    "/node/117",
    "/node/119",   # Régimes de licences
    "/node/372",
    "/node/385",
    "/node/519",   # Décision réglementaire OE-CNT-DEC-20220001
    "/node/138",   # Formulaires
    "/node/139",
    "/node/140",
    "/node/141",
]

# Application forms are official telecom documents but carry little legal
# body text; keep them but classify as doctrine.
_DATE_RE = re.compile(r"(\d{1,2})\s+(janvier|f[ée]vrier|mars|avril|mai|juin|"
                      r"juillet|ao[uû]t|septembre|octobre|novembre|d[ée]cembre)"
                      r"\s+(\d{4})", re.IGNORECASE)
_MONTHS = {
    "janvier": 1, "fevrier": 2, "février": 2, "mars": 3, "avril": 4, "mai": 5,
    "juin": 6, "juillet": 7, "aout": 8, "août": 8, "septembre": 9,
    "octobre": 10, "novembre": 11, "decembre": 12, "décembre": 12,
}


def _extract_text_pdfminer(pdf_bytes: bytes) -> Optional[str]:
    """Extract text from PDF bytes using pdfminer."""
    try:
        from pdfminer.high_level import extract_text as pdfminer_extract
        text = pdfminer_extract(io.BytesIO(pdf_bytes))
        clean = text.strip().replace("\x0c", "")
        # Collapse runs of blank lines
        clean = re.sub(r"\n{3,}", "\n\n", clean)
        if len(clean) > 100:
            return clean
    except Exception as e:
        logger.warning("pdfminer extraction failed: %s", e)
    return None


def _parse_date(text: str) -> Optional[str]:
    """Find the first 'DD month YYYY' date in the text -> ISO YYYY-MM-DD."""
    m = _DATE_RE.search(text)
    if not m:
        return None
    day = int(m.group(1))
    month = _MONTHS.get(m.group(2).lower())
    year = int(m.group(3))
    if month and 1900 < year < 2100:
        try:
            return f"{year:04d}-{month:02d}-{day:02d}"
        except Exception:
            return None
    return None


class CONATELRegulationsScraper(BaseScraper):

    def __init__(self):
        super().__init__(str(Path(__file__).parent))
        self.http = HttpClient(
            base_url=BASE_URL,
            headers={
                "User-Agent": "LegalDataHunter/1.0 (open legal data research)",
                "Accept": "text/html, application/pdf, */*",
            },
            timeout=60,
        )
        self._seen_pdf = set()

    def _get_html(self, path: str) -> Optional[str]:
        url = path if path.startswith("http") else f"{BASE_URL}{path}"
        try:
            resp = self.http.get(url, timeout=60)
            if resp.status_code != 200:
                logger.warning("Failed to fetch %s: HTTP %d", url, resp.status_code)
                return None
            return resp.text
        except Exception as e:
            logger.warning("Failed to fetch %s: %s", url, e)
            return None

    def _pdf_links(self, html: str, page_url: str) -> list[str]:
        """Return absolute PDF URLs found in the HTML."""
        hrefs = re.findall(r'href="([^"]*\.pdf[^"]*)"', html, re.IGNORECASE)
        out = []
        for href in hrefs:
            if href.startswith("http"):
                pdf_url = href
            elif href.startswith("/"):
                pdf_url = f"{BASE_URL}{href}"
            else:
                pdf_url = urljoin(page_url, href)
            norm = pdf_url.split("?")[0]
            if norm in self._seen_pdf:
                continue
            self._seen_pdf.add(norm)
            out.append(pdf_url)
        return out

    def _node_links(self, html: str) -> list[str]:
        return sorted(set(re.findall(r'href="(/node/\d+)"', html)))

    def _collect_pdf_urls(self) -> list[str]:
        """Crawl hub + seed pages and follow hub /node/ links one level deep."""
        pdf_urls = []
        to_visit = list(dict.fromkeys(SEED_PAGES))

        # Follow node links discovered on hub pages.
        for hub in HUB_PAGES:
            html = self._get_html(hub)
            if not html:
                continue
            pdf_urls.extend(self._pdf_links(html, f"{BASE_URL}{hub}"))
            for node in self._node_links(html):
                if node not in to_visit:
                    to_visit.append(node)
            time.sleep(1)

        for path in to_visit:
            html = self._get_html(path)
            if not html:
                continue
            found = self._pdf_links(html, f"{BASE_URL}{path}")
            if found:
                logger.info("%s -> %d PDF(s)", path, len(found))
            pdf_urls.extend(found)
            time.sleep(1)

        logger.info("Total unique PDFs to process: %d", len(pdf_urls))
        return pdf_urls

    def _classify(self, title: str) -> tuple[str, str]:
        """Return (_type, document_type) from the title/filename."""
        t = title.lower()
        if "formulaire" in t:
            return "doctrine", "formulaire"
        if "procedure" in t or "procédure" in t:
            return "doctrine", "procedure"
        if "plan" in t and ("numerotation" in t or "pnn" in t):
            return "legislation", "plan_numerotation"
        if "concession" in t:
            return "legislation", "concession"
        if "decret" in t or "décret" in t:
            return "legislation", "decret"
        if "loi " in t or t.startswith("loi") or "loiorg" in t or "loitelecom" in t:
            return "legislation", "loi"
        if ("decision" in t or "décision" in t
                or re.search(r"oe-cnt-dec|dec\d{4,}|dec-?20\d{2}", t)):
            return "legislation", "decision_reglementaire"
        if "transition" in t or "strateg" in t or "stratég" in t:
            return "doctrine", "plan_strategique"
        return "doctrine", "texte_reglementaire"

    def _normalize_pdf(self, pdf_url: str) -> Optional[dict]:
        norm_url = pdf_url.split("?")[0]
        filename = unquote(unquote(norm_url.split("/")[-1]))
        stem = Path(filename).stem
        title = re.sub(r"\s+", " ", stem.replace("-", " ").replace("_", " ")).strip()
        doc_id = stem

        try:
            resp = self.http.get(pdf_url, timeout=120)
            if resp.status_code != 200:
                logger.warning("PDF download failed (%d): %s", resp.status_code, pdf_url)
                return None
            text = _extract_text_pdfminer(resp.content)
        except Exception as e:
            logger.warning("PDF download failed for %s: %s", pdf_url, e)
            return None

        if not text:
            logger.warning("No text extracted (likely scanned): %s", title)
            return None

        _type, doc_type = self._classify(title + " " + text[:200])

        return {
            "_id": doc_id,
            "_source": SOURCE_ID,
            "_type": _type,
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": _parse_date(text),
            "url": norm_url,
            "document_type": doc_type,
            "language": "fr",
        }

    # ── BaseScraper interface ────────────────────────────────────────

    def fetch_all(self) -> Generator[dict, None, None]:
        self._seen_pdf = set()
        for pdf_url in self._collect_pdf_urls():
            record = self._normalize_pdf(pdf_url)
            if record:
                yield record
            time.sleep(1)

    def fetch_updates(self, since: Optional[str] = None) -> Generator[dict, None, None]:
        """No date-based API — re-fetches all."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        return raw


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(description="HT/CONATEL-Regulations scraper")
    parser.add_argument("command", choices=["bootstrap", "update", "test-api"])
    parser.add_argument("--sample", action="store_true", help="Fetch only sample records")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = CONATELRegulationsScraper()

    if args.command == "test-api":
        urls = scraper._collect_pdf_urls()
        for u in urls:
            logger.info("  %s", u)
        return

    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    if args.command in ("bootstrap", "update"):
        limit = 15 if args.sample else None
        count = 0
        for record in scraper.fetch_all():
            count += 1
            if args.sample or count <= 15:
                out_path = sample_dir / f"{count:04d}.json"
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(record, f, ensure_ascii=False, indent=2)
            logger.info(
                "[%d] %s — %d chars (%s)",
                count,
                record["title"][:55],
                len(record.get("text", "")),
                record.get("_type"),
            )
            if limit and count >= limit:
                break

        logger.info("Done: %d records fetched", count)


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    main()
