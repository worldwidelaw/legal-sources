#!/usr/bin/env python3
"""
INTL/COMESA-Court -- COMESA Court of Justice

Fetches judgments and rulings from the COMESA Court of Justice via AfricanLII,
which hosts OCR'd PDFs with extractable text layers.

Strategy:
  - Walk AfricanLII's COMESA Court listing, including every per-year page, for
    metadata + PDF URLs
  - Download OCR'd PDFs from AfricanLII and extract full text
  - Fall back to comesacourt.org for any decisions not on AfricanLII
  - 45 decisions on AfricanLII, 38 on the COMESA site (heavily overlapping)

Data Coverage:
  - Trade, investment, and employment disputes from 21 COMESA member states
  - First Instance Division and Appellate Division
  - Judgments, rulings, and orders (2000-present)

Usage:
  python bootstrap.py bootstrap          # Full pull -> data/records.jsonl
  python bootstrap.py bootstrap --sample # Fetch 15 sample records -> sample/
  python bootstrap.py bootstrap-fast     # Alias for the full bootstrap (fleet entry point)
  python bootstrap.py update             # Incremental update
  python bootstrap.py test               # Quick connectivity test
"""

import io
import re
import sys
import json
import time
import hashlib
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.COMESA-Court")

AFRICANLII_LISTING = "https://africanlii.org/judgments/COMESACJ/"
AFRICANLII_BASE = "https://africanlii.org"
COMESA_DECISIONS_URL = "https://comesacourt.org/court-decisions/"
MAX_PDF_BYTES = 50 * 1024 * 1024  # 50 MB


class COMESACourtScraper(BaseScraper):
    """Scraper for COMESA Court of Justice decisions."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.last_status: Optional[int] = None
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "LegalDataHunter/1.0 (legal research; +https://github.com/worldwidelaw/legal-sources)",
            "Accept": "text/html,application/xhtml+xml,application/json",
        })

    # ── AfricanLII source (primary — OCR'd PDFs) ──────────────────────

    def _listing_pages(self) -> Generator[str, None, None]:
        """Yield the HTML of the listing root and of every per-year page.

        The root only carries the most recent judgment of each year (19 of 45),
        so the year pages linked from its sidebar are the actual corpus index.
        """
        resp = self.session.get(AFRICANLII_LISTING, timeout=30)
        resp.raise_for_status()
        root = resp.text
        yield root

        years = sorted(set(re.findall(
            r'href="(/en/judgments/COMESACJ/(?:\d{4})/)"', root,
        )))
        if not years:
            logger.warning("No year pages found on the listing root")
        for path in years:
            time.sleep(1.0)
            resp = self.session.get(f"{AFRICANLII_BASE}{path}", timeout=30)
            resp.raise_for_status()
            yield resp.text

    def _parse_africanlii_listing(self) -> list[dict]:
        """Parse the AfricanLII COMESA Court judgment listing pages."""
        entries = []
        for html in self._listing_pages():
            # Judgments live under two FRBR country prefixes -- the older
            # /akn/aa/ (26 of them are /akn/aa-au/), so match either.
            entries += re.findall(
                r'<a href="(/en/akn/[a-z-]+/judgment/comesacj/[^"]+)"[^>]*>(.*?)</a>',
                html, re.DOTALL,
            )

        decisions = []
        seen = set()
        for link, title_html in entries:
            if link in seen:
                continue
            seen.add(link)

            title = re.sub(r"<[^>]+>", "", title_html).strip()
            title = re.sub(r"\s+", " ", title)

            # Extract date from URL: eng@YYYY-MM-DD
            date_match = re.search(r"eng@(\d{4}-\d{2}-\d{2})", link)
            date = date_match.group(1) if date_match else None

            # Extract case reference from title
            ref_match = re.search(
                r"\(([^)]*(?:Reference|Appeal|Application|Taxation|Revision)[^)]*)\)",
                title,
            )
            reference = ref_match.group(1).strip() if ref_match else ""

            # Determine decision type
            title_lower = title.lower()
            if "ruling" in title_lower:
                decision_type = "Ruling"
            elif "order" in title_lower:
                decision_type = "Order"
            elif "advisory" in title_lower:
                decision_type = "Advisory Opinion"
            else:
                decision_type = "Judgment"

            # Build PDF download URL
            pdf_url = f"{AFRICANLII_BASE}{link}/source.pdf"

            # Extract parties (text before first parenthetical)
            parties_match = re.match(r"^(.+?)\s*\(", title)
            parties = parties_match.group(1).strip() if parties_match else title

            decisions.append({
                "title": title,
                "parties": parties,
                "reference_number": reference,
                "date": date,
                "pdf_url": pdf_url,
                "decision_type": decision_type,
                "akn_uri": link,
                "source_site": "africanlii",
            })

        logger.info(f"Parsed {len(decisions)} decisions from AfricanLII")
        return decisions

    # ── COMESA site source (fallback — mostly scanned) ────────────────

    def _parse_comesa_decisions(self) -> list[dict]:
        """Parse the COMESA Court decisions page for PDF links."""
        resp = self.session.get(COMESA_DECISIONS_URL, timeout=30)
        resp.raise_for_status()
        html = resp.text

        from html import unescape

        decisions = []
        seen_urls = set()
        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", html, re.DOTALL)

        for row_html in rows:
            if "<th" in row_html:
                continue
            if ".pdf" not in row_html:
                continue

            cells = re.findall(r"<td[^>]*>(.*?)</td>", row_html, re.DOTALL)
            if len(cells) < 3:
                continue

            href_match = re.search(r'href="([^"]+\.pdf)"', row_html, re.IGNORECASE)
            if not href_match:
                continue
            pdf_url = href_match.group(1)
            pdf_url = re.sub(r"^http://", "https://", pdf_url)

            if pdf_url in seen_urls:
                continue
            seen_urls.add(pdf_url)

            title = re.sub(r"<[^>]+>", "", cells[0]).strip()
            title = unescape(title)
            year = re.sub(r"<[^>]+>", "", cells[-1]).strip()

            title_lower = title.lower()
            if "judgment" in title_lower[:20] or "judgement" in title_lower[:20]:
                decision_type = "Judgment"
            elif "ruling" in title_lower[:20]:
                decision_type = "Ruling"
            elif "order" in title_lower[:20]:
                decision_type = "Order"
            else:
                decision_type = "Decision"

            parties_match = re.search(
                r"(?:Judgment|Ruling|Order|Judgement)\s*[-–—:]\s*(.+)",
                title, re.IGNORECASE,
            )
            parties = parties_match.group(1).strip() if parties_match else ""

            decisions.append({
                "title": title,
                "parties": parties,
                "reference_number": "",
                "date": self._comesa_date(year, pdf_url),
                "pdf_url": pdf_url,
                "decision_type": decision_type,
                "akn_uri": None,
                "source_site": "comesacourt",
            })

        logger.info(f"Parsed {len(decisions)} decisions from COMESA site")
        return decisions

    @staticmethod
    def _comesa_date(year: str, pdf_url: str) -> Optional[str]:
        """Best available date for a comesacourt.org row.

        The table gives only a year, but the WordPress upload path carries a
        month (/wp-content/uploads/YYYY/MM/). Use it when the two years agree,
        so a decision handed down in November stops reading as 1 January.
        """
        if not year.isdigit():
            return None
        upload = re.search(r"/uploads/(\d{4})/(\d{2})/", pdf_url)
        if upload and upload.group(1) == year:
            return f"{year}-{upload.group(2)}-01"
        return f"{year}-01-01"

    # ── PDF extraction ────────────────────────────────────────────────

    @staticmethod
    def _clean_text(text: str) -> str:
        """Remove garbage lines from scanned PDF extraction (hex hashes, etc.)."""
        if not text:
            return ""
        lines = text.split("\n")
        clean = []
        for line in lines:
            s = line.strip()
            if not s:
                clean.append(line)
                continue
            # Skip lines that are purely hex hash strings (e.g., "c4c763ea12784d00...")
            if re.match(r"^[A-Fa-f0-9]{20,}(-\d+)?$", s):
                continue
            # Skip lines like "JJ77", "FF44", "EE4488", "DDCC221133" (scanned artifacts)
            if re.match(r"^[A-Z]{2}\d{2,}$", s):
                continue
            # Skip lines like "DDJUCC22ST1133ICE" (garbled OCR)
            if re.match(r"^[A-Z]{2,}[A-Z0-9]{6,}$", s) and sum(c.isdigit() for c in s) > len(s) * 0.3:
                continue
            clean.append(line)
        return "\n".join(clean)

    def _download_pdf_text(self, url: str, doc_id: str = "") -> str:
        """Download a PDF and extract its text.

        Records the HTTP status in ``self.last_status`` so callers can tell a
        refused vantage apart from a document that simply carries no text.
        ``doc_id`` must be the same string ``normalize()`` emits as ``_id`` so
        the extractor's skip-if-already-in-Neon guard can key on it (#1480).
        """
        self.last_status = None
        try:
            time.sleep(1.5)
            resp = self.session.get(url, timeout=120)
            self.last_status = resp.status_code
            if resp.status_code != 200:
                logger.warning(f"PDF download failed ({resp.status_code}): {url}")
                return ""
            if len(resp.content) > MAX_PDF_BYTES:
                logger.warning(f"PDF too large ({len(resp.content)} bytes): {url}")
                return ""
            if len(resp.content) < 500:
                logger.warning(f"PDF too small ({len(resp.content)} bytes): {url}")
                return ""

            # Try centralized extractor first
            text = extract_pdf_markdown(
                source="INTL/COMESA-Court",
                source_id=doc_id,
                pdf_bytes=resp.content,
                table="case_law",
            )
            if text:
                text = self._clean_text(text)
                if len(text.strip()) > 200:
                    return text

            # Fallback: pdfplumber
            text = self._extract_with_pdfplumber(resp.content)
            if text:
                text = self._clean_text(text)
                if len(text.strip()) > 200:
                    return text

            # Fallback: PyMuPDF
            text = self._extract_with_fitz(resp.content)
            if text:
                text = self._clean_text(text)
            return text

        except Exception as e:
            logger.warning(f"PDF download error: {e}")
            return ""

    def _extract_with_pdfplumber(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF using pdfplumber."""
        try:
            import pdfplumber
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                pages = [p.extract_text() or "" for p in pdf.pages]
                return "\n\n".join(p for p in pages if p.strip())
        except Exception as e:
            logger.debug(f"pdfplumber extraction failed: {e}")
            return ""

    def _extract_with_fitz(self, pdf_bytes: bytes) -> str:
        """Extract text from PDF using PyMuPDF."""
        try:
            import fitz
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")
            pages = [doc[i].get_text() for i in range(len(doc))]
            doc.close()
            return "\n\n".join(p for p in pages if p.strip())
        except Exception as e:
            logger.debug(f"PyMuPDF extraction failed: {e}")
            return ""

    # ── Core scraper methods ──────────────────────────────────────────

    @staticmethod
    def _text_signature(text: str) -> str:
        """Fingerprint a decision by its opening body text.

        The two sites publish the same PDFs, but their titles disagree
        ("Agiliss Ltd v Republic of Mauritius (Reference 1 of 2022) [2023]
        COMESACJ 3" vs "Judgment - Agiliss vs The Republic of Mauritius and 4
        Others"), so only the text identifies a duplicate.
        """
        body = re.sub(r"[^a-z0-9]+", "", text.lower())[:2000]
        return hashlib.sha1(body.encode()).hexdigest()

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all decisions with full text.

        Primary source: AfricanLII (OCR'd PDFs with text layers).
        Secondary: the COMESA Court website, for decisions AfricanLII lacks.
        """
        seen_signatures: set[str] = set()
        yielded = 0

        # Phase 1: AfricanLII (high success rate)
        africanlii_decisions = self._parse_africanlii_listing()
        total = len(africanlii_decisions)
        logger.info(f"Phase 1: Processing {total} AfricanLII decisions")

        refused = 0
        for i, info in enumerate(africanlii_decisions):
            logger.info(f"[{i+1}/{total}] {info['title'][:80]}")

            text = self._download_pdf_text(info["pdf_url"], self._derive_id(info))
            if self.last_status in (403, 429):
                refused += 1
                # africanlii.org fronts its document pages (not its listings)
                # with a Cloudflare managed challenge, so a refused vantage
                # 403s every PDF while the index still looks healthy.
                if refused >= 5 and yielded == 0:
                    logger.error(
                        "AfricanLII refused the first %d document downloads "
                        "(HTTP %s) while the listing returned %d judgments — "
                        "this vantage is challenged, skipping to comesacourt.org",
                        refused, self.last_status, total,
                    )
                    break
                continue
            if not text or len(text.strip()) < 200:
                logger.warning(f"  Insufficient text ({len(text.strip()) if text else 0} chars), skipping")
                continue

            info["text"] = text
            seen_signatures.add(self._text_signature(text))
            yielded += 1
            yield info

        # Phase 2: COMESA site for decisions not on AfricanLII
        comesa_decisions = self._parse_comesa_decisions()
        logger.info(f"Phase 2: Processing {len(comesa_decisions)} COMESA site decisions")

        duplicates = 0
        for i, info in enumerate(comesa_decisions):
            logger.info(f"[COMESA {i+1}/{len(comesa_decisions)}] {info['title'][:80]}")

            text = self._download_pdf_text(info["pdf_url"], self._derive_id(info))
            if not text or len(text.strip()) < 200:
                logger.warning(f"  Insufficient text (scanned PDF), skipping")
                continue

            signature = self._text_signature(text)
            if signature in seen_signatures:
                duplicates += 1
                logger.info("  Already fetched from AfricanLII, skipping")
                continue

            info["text"] = text
            seen_signatures.add(signature)
            yielded += 1
            yield info

        logger.info(f"Phase 2: {duplicates} duplicates of AfricanLII decisions skipped")

        if yielded == 0:
            raise RuntimeError(
                "No decisions with full text from either africanlii.org or "
                "comesacourt.org — both hosts refused this vantage or changed "
                "layout. Refusing to report an empty crawl as a success."
            )

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Yield decisions newer than since date."""
        since_str = since.strftime("%Y-%m-%d") if isinstance(since, datetime) else str(since)
        logger.info(f"Fetching decisions since {since_str}")

        decisions = self._parse_africanlii_listing()
        for info in decisions:
            if info.get("date") and info["date"] >= since_str:
                text = self._download_pdf_text(info["pdf_url"], self._derive_id(info))
                if text and len(text.strip()) >= 200:
                    info["text"] = text
                    yield info

    @staticmethod
    def _derive_id(raw: dict) -> str:
        """Build the stable ``_id`` for a decision.

        Shared with ``_download_pdf_text`` so the extractor's
        skip-if-already-in-Neon guard keys on the same string ``normalize()``
        emits (#1480).
        """
        ref = raw.get("reference_number", "")
        pdf_url = raw.get("pdf_url", "")
        akn_uri = raw.get("akn_uri")

        if akn_uri:
            # e.g., /en/akn/aa/judgment/comesacj/2025/3/eng@2025-11-07
            slug = re.sub(r"[^a-zA-Z0-9]+", "-", akn_uri).strip("-")
        elif ref:
            slug = re.sub(r"[^a-zA-Z0-9]+", "-", ref.lower()).strip("-")
        else:
            slug = re.sub(r"[^a-zA-Z0-9]+", "-", Path(pdf_url).stem.lower()).strip("-")

        return f"comesa-court-{slug}"

    def normalize(self, raw: dict) -> dict:
        """Transform raw item into standard schema."""
        title = raw.get("title", "")
        ref = raw.get("reference_number", "")
        pdf_url = raw.get("pdf_url", "")
        akn_uri = raw.get("akn_uri")

        _id = self._derive_id(raw)

        # URL: prefer AfricanLII page, fall back to PDF
        if akn_uri:
            url = f"{AFRICANLII_BASE}{akn_uri}"
        else:
            url = pdf_url

        text = raw.get("text", "")

        return {
            "_id": _id,
            "_source": "INTL/COMESA-Court",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": raw.get("date"),
            "url": url,
            "reference_number": ref,
            "decision_type": raw.get("decision_type", ""),
            "court": "COMESA Court of Justice",
            "parties": raw.get("parties", ""),
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="INTL/COMESA-Court -- COMESA Court of Justice"
    )
    subparsers = parser.add_subparsers(dest="command")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Sample mode (15 records)")
    bp.add_argument("--sample-size", type=int, default=15, help="Sample size")
    bp.add_argument("--full", action="store_true", help="Fetch all records")

    subparsers.add_parser("bootstrap-fast", help="Alias for the full bootstrap")
    subparsers.add_parser("update", help="Incremental update")
    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    scraper = COMESACourtScraper()

    if args.command == "test":
        logger.info("Testing connectivity...")
        try:
            decisions = scraper._parse_africanlii_listing()
            logger.info(f"AfricanLII: {len(decisions)} decisions")
            if decisions:
                d = decisions[0]
                logger.info(f"First: {d['title'][:80]}")
                text = scraper._download_pdf_text(d["pdf_url"])
                if text and len(text.strip()) > 200:
                    logger.info(f"PDF text: {len(text)} chars")
                    logger.info(f"Preview: {text[:200]}")
                    logger.info("Test passed!")
                else:
                    logger.error("Failed to extract PDF text")
                    sys.exit(1)
        except Exception as e:
            logger.error(f"Test failed: {e}")
            sys.exit(1)

    elif args.command in ("bootstrap", "bootstrap-fast"):
        # bootstrap-fast is the fleet's entry point: it must run the full
        # crawl. Only --sample writes sample/.
        sample = getattr(args, "sample", False)
        sample_size = getattr(args, "sample_size", 15)
        stats = scraper.bootstrap(sample_mode=sample, sample_size=sample_size)
        logger.info(f"Bootstrap complete: {json.dumps(stats, indent=2)}")

    elif args.command == "update":
        stats = scraper.update()
        logger.info(f"Update complete: {json.dumps(stats, indent=2)}")


if __name__ == "__main__":
    main()
