#!/usr/bin/env python3
"""
GM/LawHubGambia -- Law Hub Gambia

Fetches case law, constitution, and legislation from lawhubgambia.com.

Strategy:
  - Curated list of content pages (case law, constitution, legislation)
  - HTML full text extraction for case law and constitution pages
  - PDF download + text extraction for legislation documents
  - BeautifulSoup for HTML parsing, pdfplumber for PDF text

Data: ~50 documents (case law, constitution, legislation)
License: Open access (non-profit legal resource)
Rate limit: 0.5 req/sec.

Usage:
  python bootstrap.py bootstrap            # Full pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import io
import json
import logging
import os
import re
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, List
from html import unescape

try:
    import requests
except ImportError:
    print("ERROR: requests not installed. Run: pip3 install requests")
    sys.exit(1)

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: beautifulsoup4 not installed. Run: pip3 install beautifulsoup4")
    sys.exit(1)

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

# Almost every PDF here is a scan of a printed act, so OCR is the normal path
# rather than a rare fallback. The shared 50-page cap and 600s budget are tuned
# for short court PDFs and would truncate the Criminal Offences Bill (142 pages)
# at a third of its text. pdf_extract reads these into module constants at
# import time, and `common/__init__` imports it, so this has to run before ANY
# `common.*` import — set after one, it silently has no effect.
os.environ.setdefault("PDF_OCR_MAX_PAGES", "400")
os.environ.setdefault("PDF_OCR_TIMEOUT", "3600")
# These are scans of printed acts whose arrangement-of-sections sets the section
# numbers in a narrow left column beside the headings. tesseract's default page
# analysis reads that as two independent blocks and emits a run of bare numbers
# followed by a run of bare headings — the "fragmented arrangement-of-sections"
# of issue #1414. psm 4 treats the page as one column and keeps each number with
# its heading; on single-column body pages it is byte-identical to the default.
os.environ.setdefault("PDF_OCR_PSM", "4")

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.GM.LawHubGambia")

BASE_URL = "https://www.lawhubgambia.com"

# ── Content catalog ──────────────────────────────────────────────────
# Case law pages with full text in HTML
CASE_LAW_PAGES = [
    # Standalone judgment pages
    {"slug": "sc-1-2002", "title": "Jammeh v Attorney-General (2001)", "date": "2001-11-29"},
    {"slug": "sc-1-2014", "title": "Gambia Press Union v The Attorney General (2018)", "date": "2018-05-09"},
    {"slug": "sc-1-2017", "title": "Bai Emil Touray v The Attorney General (2017)", "date": "2017"},
    {"slug": "jabbi-v-coma", "title": "Jabbi v Coma and Others", "date": None},
    # Pages with inline full-text judgments
    {"slug": "assembly", "title": "Ousainou Darboe & 19 Others v IGP et al (2017)", "date": "2017-11-23"},
    {"slug": "arbitrary-detention", "title": "Denton v Director-General NIA (2006)", "date": "2006-07-24"},
    {"slug": "fair-trial", "title": "Sabally v Inspector General of Police (2001)", "date": "2001"},
    {"slug": "commission-of-inquiry", "title": "M.A Kharafi & Sons v The Attorney General (2020)", "date": "2020-06-01"},
]

# Constitution pages with full text in HTML
CONSTITUTION_PAGES = [
    {"slug": "1997-constitution", "title": "The Constitution of The Republic of The Gambia, 1997", "date": "1997-01-16"},
]

# Legislation index pages that contain links to PDFs
LEGISLATION_INDEX_PAGES = [
    "criminal-law-database",
    "electoral-laws",
    "womens-rights",
    "freedom-expression-and-media",
    "persons-with-disabilities-bill-2020",
]

# Direct PDF legislation links (discovered from index pages)
LEGISLATION_PDFS = [
    # Criminal law
    {"slug": "criminal-code-1933", "pdf": "/s/Criminal-Code-Act-No-25-of-1933.pdf",
     "title": "Criminal Code Act No. 25 of 1933", "date": "1933"},
    {"slug": "criminal-procedure-code-1933", "pdf": "/s/Criminal-Procedure-Code-Act-No-26-of-1933.pdf",
     "title": "Criminal Procedure Code Act No. 26 of 1933", "date": "1933"},
    {"slug": "criminal-offences-bill-2020", "pdf": "/s/Criminal-Offences-Bill_2020.pdf",
     "title": "Criminal Offences Bill 2020", "date": "2020"},
    # Electoral laws
    {"slug": "election-act-1963", "pdf": "/s/GM1963ElectionAct.pdf",
     "title": "Election Act 1963", "date": "1963"},
    {"slug": "elections-decree-1996", "pdf": "/s/Elections-Decree-78-of-1996.pdf",
     "title": "Elections Decree No. 78 of 1996", "date": "1996"},
    {"slug": "elections-act-chapter-3-01", "pdf": "/s/Elections-Act_Decree-No-78-of-1996.pdf",
     "title": "Elections Act (Chapter 3:01)", "date": "1996"},
    {"slug": "code-of-election-campaign-1996", "pdf": "/s/Code-of-Election-Campaign-1996.pdf",
     "title": "Code of Election Campaign Ethics 1996", "date": "1996"},
    {"slug": "election-petition-rules", "pdf": "/s/Election-Petition-Rules.pdf",
     "title": "Election Petition Rules", "date": None},
    {"slug": "election-amendment-act-2017", "pdf": "/s/Election-Amendment-Act.pdf",
     "title": "Election (Amendment) Act 2017", "date": "2017"},
    # Women's rights / human rights
    {"slug": "womens-act-2010", "pdf": "/s/Womens-Act-2010.pdf",
     "title": "Women's Act 2010", "date": "2010"},
    {"slug": "womens-act-amendment-2015", "pdf": "/s/Womens-Act-Amendment-Act-2015.pdf",
     "title": "Women's (Amendment) Act 2015", "date": "2015"},
    {"slug": "sexual-offences-act-2013", "pdf": "/s/Sexual-Offences-Act-2013.pdf",
     "title": "Sexual Offences Act 2013", "date": "2013"},
    {"slug": "domestic-violence-act-2013", "pdf": "/s/Domestic-Violence-Act-2013.pdf",
     "title": "Domestic Violence Act 2013", "date": "2013"},
    {"slug": "access-to-information-bill-2020", "pdf": "/s/Access-to-Information-Bill_2020.pdf",
     "title": "Access to Information Bill 2020", "date": "2020"},
    {"slug": "persons-with-disabilities-bill-2020", "pdf": "/s/Persons-with-Disabilities-Bill_2020.pdf",
     "title": "Persons with Disabilities Bill 2020", "date": "2020"},
]

# Volumes that bind more than one instrument into a single PDF. Stored whole,
# they read as one act whose section numbering restarts partway through — the
# "duplicate section numbers" of issue #1414 are section 1 of the Criminal Code
# and section 1 of the Criminal Procedure Code sitting in the same record, not
# an upstream numbering defect. Each part becomes its own document, cut at the
# first page carrying that part's opening title.
COMPILED_PDFS = [
    {
        "pdf": "/s/1934_An-Ordinance-to-Establish-a-Code-of-Criminal-Law-An-Ordinance-to-Make-Provision-for-the-Procedu.pdf",
        "parts": [
            {"slug": "criminal-code-ordinance-1934",
             "title": "Criminal Code Ordinance (Act No. 25 of 1933)",
             "date": "1933-12-16"},
            {"slug": "criminal-procedure-code-ordinance-1934",
             "title": "Criminal Procedure Code Ordinance (Act No. 26 of 1933)",
             "date": "1933-12-16",
             # Anchored to a line of its own: the phrase also appears mid-page
             # in the Criminal Code's cross-references, which would cut the
             # volume 75 pages early.
             "starts_at": r"^\s*THE\s+CRIMINAL\s+PROCEDURE\s+CODE\W*$"},
        ],
    },
]

# Gambia Law Reports (bulk PDFs with compiled case law)
LAW_REPORT_PDFS = [
    {"slug": "gambia-law-reports-1960-1993", "pdf": "/s/The-Gambia-Law-Reports-1960-1993.pdf",
     "title": "The Gambia Law Reports 1960-1993", "date": "1993"},
    {"slug": "gambia-law-reports-1997-2001", "pdf": "/s/The-Gambia-Law-Reports-1997-2001.pdf",
     "title": "The Gambia Law Reports 1997-2001", "date": "2001"},
    {"slug": "gambia-law-reports-2002-2008-vol1", "pdf": "/s/The-Gambia-Law-Reports-2002-2008-Volume-1.pdf",
     "title": "The Gambia Law Reports 2002-2008 Volume 1", "date": "2008"},
    {"slug": "gambia-law-reports-2002-2008-vol2", "pdf": "/s/The-Gambia-Law-Reports-2002-2008-Volume-2.pdf",
     "title": "The Gambia Law Reports 2002-2008 Volume 2", "date": "2008"},
]


# Minimum extracted length for a document to be worth emitting. Below this the
# PDF is a scan whose text layer is nothing but the site's watermark, and the
# record would carry a stamp instead of a law (issue #1414).
MIN_DOC_CHARS = 500

# Per-page stamps Law Hub Gambia burns into every PDF it republishes, plus the
# LLMC scanning-programme preamble on the digitised colonial volumes. Left in,
# they repeat once per page and swamp short documents.
WATERMARK_LINE_RE = re.compile(
    r"""^(?:
          Law\s*Hub\s*Gambia\s*Digital
        | This\s+(?:document|copy)\s+is\s+courtesy\s+of\s+Law\s*hub\s+Gambia
        | Document\s+Sourced\s+from\s+www\.lawhubgambia\.com
        | \(?www\.lawhubgambia\.com\)?
        | LAW\s+HUB\s*»?
        | GAMBIA
      )\s*$""",
    re.I | re.X,
)

# On the image-only scans the stamp reaches us through OCR, which reads the page
# number, the rule beneath the stamp and the bleed-through from the facing page
# as characters on the stamp's own line: "Law Hub Gambia Digital : 7 Oo —",
# "SS Law Hub Gambia Digital", "Law Hub Gambia Digital 111". The anchored
# pattern above never matches those, so the stamp survived once per page in the
# OCR-ed acts. Drop any line carrying the stamp whose remainder is not words.
WATERMARK_STAMP_RE = re.compile(r"Law\s*Hub\s*Gambia\s*Digital", re.I)
# Longest real word that may legitimately share the line before we keep it. The
# noise runs observed are all 1-3 letters ("ss", "ian", "Ao", "Oo").
_STAMP_NOISE_MAX_LETTERS = 3


def _is_watermark_stamp_line(line: str) -> bool:
    """True if `line` is the site's per-page stamp plus OCR noise, nothing more."""
    if not WATERMARK_STAMP_RE.search(line):
        return False
    remainder = WATERMARK_STAMP_RE.sub("", line)
    return len(re.findall(r"[A-Za-z]", remainder)) <= _STAMP_NOISE_MAX_LETTERS


LLMC_PREAMBLE_RE = re.compile(
    r"This copy of a rare volume.*?(?:LLMC|Law Library Microform Consortium)[^\n]*\n"
    # The scanning credit continues onto the holding library's own line, e.g.
    # "is made available courtesy of the / Los Angeles County Law Library".
    r"(?:[ \t]*is made available courtesy of[^\n]*\n[^\n]*\n)?",
    re.I | re.S,
)

# opendataloader emits Markdown: image placeholders for image-only regions and
# ATX headings for anything it reads as a heading. Both leak into the stored
# text as `####` noise around the arrangement-of-sections (issue #1414).
MD_IMAGE_RE = re.compile(r"!\[[^\]]*\]\([^)]*\)")
MD_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s*", re.M)


def clean_extracted_text(text: str) -> str:
    """Strip site watermarks and Markdown artifacts from extracted PDF text."""
    if not text:
        return ""

    text = LLMC_PREAMBLE_RE.sub("", text)
    text = MD_IMAGE_RE.sub("", text)
    text = MD_HEADING_RE.sub("", text)

    lines = [ln.rstrip() for ln in text.splitlines()]
    kept = [
        ln
        for ln in lines
        if not WATERMARK_LINE_RE.match(ln.strip())
        and not _is_watermark_stamp_line(ln)
    ]

    out = "\n".join(kept)
    out = re.sub(r"[ \t]+\n", "\n", out)
    out = re.sub(r"\n{3,}", "\n\n", out)
    return out.strip()


def clean_html_text(html_content: str) -> str:
    """Extract clean text from HTML, removing tags and normalizing whitespace."""
    soup = BeautifulSoup(html_content, "html.parser")

    # Remove script and style elements
    for tag in soup(["script", "style", "nav", "header", "footer", "noscript"]):
        tag.decompose()

    text = soup.get_text(separator="\n")

    # Normalize whitespace
    lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped:
            lines.append(stripped)

    text = "\n".join(lines)
    # Collapse multiple blank lines
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


def extract_judgment_text(html_content: str) -> str:
    """Extract the main judgment/legal text from a Squarespace page."""
    soup = BeautifulSoup(html_content, "html.parser")

    # Remove navigation, header, footer
    for tag in soup(["script", "style", "nav", "noscript"]):
        tag.decompose()

    # Look for the main content area
    # Squarespace uses various content block classes
    main_content = None
    for selector in [
        "div.sqs-block-content",
        "article",
        "div.entry-content",
        "div.sqs-layout",
        "main",
    ]:
        blocks = soup.select(selector)
        if blocks:
            # Concatenate all content blocks
            texts = []
            for block in blocks:
                text = block.get_text(separator="\n")
                if len(text.strip()) > 100:
                    texts.append(text)
            if texts:
                main_content = "\n\n".join(texts)
                break

    if not main_content:
        # Fallback: get all text from body
        body = soup.find("body")
        if body:
            main_content = body.get_text(separator="\n")
        else:
            main_content = soup.get_text(separator="\n")

    # Clean up
    lines = []
    for line in main_content.splitlines():
        stripped = line.strip()
        if stripped:
            lines.append(stripped)

    text = "\n".join(lines)
    text = re.sub(r'\n{3,}', '\n\n', text)

    # Remove common Squarespace boilerplate
    boilerplate_patterns = [
        r'Law Hub Gambia.*?All Rights Reserved\.?',
        r'Powered by Squarespace',
        r'lawhubgambia@gmail\.com',
        r'Share\s*Facebook\s*Twitter\s*LinkedIn',
        r'Cookie Policy',
    ]
    for pattern in boilerplate_patterns:
        text = re.sub(pattern, '', text, flags=re.I | re.S)

    return text.strip()


class GMLawHubGambiaScraper(BaseScraper):
    """
    Scraper for GM/LawHubGambia.
    Country: GM
    URL: https://www.lawhubgambia.com

    Data types: case_law, legislation
    Auth: none
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        })

    def _get_with_retry(self, url: str, max_retries: int = 3, timeout: int = 60) -> Optional[requests.Response]:
        """GET with retry logic."""
        for attempt in range(max_retries):
            try:
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 200:
                    return resp
                logger.warning(f"HTTP {resp.status_code} for {url}")
            except requests.RequestException as e:
                logger.warning(f"Attempt {attempt+1}/{max_retries} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(3 * (attempt + 1))
        return None

    def _extract_pdf_text(self, pdf_url: str) -> str:
        """Extract text from PDF using the centralized extractor, de-watermarked.

        Most of this site's PDFs are scans stamped with a per-page watermark, so
        the raw extraction is either real prose plus a stamp on every page or —
        for the image-only scans — nothing but the stamp. Cleaning happens here
        so the length guards downstream measure document text, not stamps.
        """
        raw = extract_pdf_markdown(
            source="GM/LawHubGambia",
            source_id="",
            pdf_url=pdf_url,
            table="case_law",
            # The rows already in Neon hold the watermark-only text this fix
            # replaces, so the idempotent skip would preserve the defect.
            force=True,
        ) or ""
        return clean_extracted_text(raw)

    def _split_compiled_pdf(self, pdf_bytes: bytes, parts: List[dict]) -> List[tuple]:
        """Cut a multi-instrument volume into one PDF per instrument.

        Returns (part, bytes) pairs. Part boundaries are located on the text
        layer, which these colonial volumes all carry; a part whose opening
        title is never found is dropped rather than silently folded into its
        predecessor, so a layout change surfaces as a missing document instead
        of a re-merged one.
        """
        import fitz  # PyMuPDF — already a dependency of the PDF extractor

        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
        try:
            starts = [0]
            for part in parts[1:]:
                pattern = re.compile(part["starts_at"], re.I | re.M)
                page = next(
                    (i for i in range(starts[-1] + 1, doc.page_count)
                     if pattern.search(doc[i].get_text())),
                    None,
                )
                if page is None:
                    logger.warning(
                        f"Part {part['slug']} not found in volume — skipping it "
                        f"rather than merging it into {parts[len(starts)-1]['slug']}"
                    )
                    return []
                starts.append(page)

            out = []
            for i, (part, first) in enumerate(zip(parts, starts)):
                last = starts[i + 1] - 1 if i + 1 < len(starts) else doc.page_count - 1
                piece = fitz.open()
                piece.insert_pdf(doc, from_page=first, to_page=last)
                out.append((part, piece.tobytes()))
                piece.close()
                logger.info(
                    f"Volume part {part['slug']}: pages {first + 1}-{last + 1}"
                )
            return out
        finally:
            doc.close()

    def _compiled_documents(self) -> Generator[dict, None, None]:
        """Yield each instrument bound into a multi-act volume as its own doc."""
        for volume in COMPILED_PDFS:
            pdf_url = f"{BASE_URL}{volume['pdf']}"
            logger.info(f"Fetching compiled volume: {volume['pdf']}")
            resp = self._get_with_retry(pdf_url)
            if not resp:
                logger.warning(f"Failed to fetch volume {pdf_url}")
                continue

            for part, part_bytes in self._split_compiled_pdf(resp.content, volume["parts"]):
                raw = extract_pdf_markdown(
                    source="GM/LawHubGambia",
                    source_id="",
                    pdf_bytes=part_bytes,
                    table="legislation",
                    force=True,
                ) or ""
                text = clean_extracted_text(raw)
                if len(text) < MIN_DOC_CHARS:
                    logger.warning(
                        f"Skipping {part['slug']}: only {len(text)} chars after "
                        f"de-watermarking"
                    )
                    continue
                yield {
                    "slug": part["slug"],
                    "title": part["title"],
                    "text": text,
                    "date": part.get("date"),
                    "url": pdf_url,
                    "doc_type": "legislation",
                }
            time.sleep(2)

    def _discover_pdfs_from_index(self, slug: str) -> List[dict]:
        """Discover PDF links from a legislation index page."""
        url = f"{BASE_URL}/{slug}"
        resp = self._get_with_retry(url)
        if not resp:
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        pdfs = []
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "/s/" in href and href.endswith(".pdf"):
                # Normalize to relative path
                if href.startswith("http"):
                    from urllib.parse import urlparse
                    parsed = urlparse(href)
                    href = parsed.path
                title = link.get_text(strip=True) or Path(href).stem.replace("-", " ")
                pdf_slug = Path(href).stem.lower().replace(" ", "-")
                pdfs.append({
                    "slug": pdf_slug,
                    "pdf": href,
                    "title": title,
                })
        return pdfs

    def _legislation_documents(self) -> List[dict]:
        """Curated legislation PDFs plus any others the index pages link.

        The curated list carries hand-checked titles and dates, so it wins on
        conflict; discovery only adds documents nobody has catalogued yet.
        """
        docs = list(LEGISLATION_PDFS)
        # Compiled volumes are handled separately, one record per instrument;
        # discovery must not re-add them whole.
        known = {d["pdf"] for d in docs} | {v["pdf"] for v in COMPILED_PDFS}
        known_slugs = {d["slug"] for d in docs} | {
            p["slug"] for v in COMPILED_PDFS for p in v["parts"]
        }

        for slug in LEGISLATION_INDEX_PAGES:
            for found in self._discover_pdfs_from_index(slug):
                if found["pdf"] in known or found["slug"] in known_slugs:
                    continue
                known.add(found["pdf"])
                known_slugs.add(found["slug"])
                found.setdefault("date", None)
                docs.append(found)
            time.sleep(2)

        logger.info(
            f"Legislation catalog: {len(LEGISLATION_PDFS)} curated + "
            f"{len(docs) - len(LEGISLATION_PDFS)} discovered from index pages"
        )
        return docs

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all documents from Law Hub Gambia."""

        # 1. Case law pages (HTML full text)
        for case in CASE_LAW_PAGES:
            url = f"{BASE_URL}/{case['slug']}"
            logger.info(f"Fetching case law: {case['slug']}")
            resp = self._get_with_retry(url)
            if not resp:
                logger.warning(f"Failed to fetch {url}")
                continue

            text = extract_judgment_text(resp.text)
            if len(text) < 200:
                logger.warning(f"Insufficient text for {case['slug']} ({len(text)} chars)")
                continue

            yield {
                "slug": case["slug"],
                "title": case["title"],
                "text": text,
                "date": case["date"],
                "url": url,
                "doc_type": "case_law",
            }
            time.sleep(2)

        # 2. Constitution pages (HTML full text)
        for const in CONSTITUTION_PAGES:
            url = f"{BASE_URL}/{const['slug']}"
            logger.info(f"Fetching constitution: {const['slug']}")
            resp = self._get_with_retry(url)
            if not resp:
                continue

            text = extract_judgment_text(resp.text)
            if len(text) < 500:
                logger.warning(f"Insufficient text for {const['slug']}")
                continue

            yield {
                "slug": const["slug"],
                "title": const["title"],
                "text": text,
                "date": const["date"],
                "url": url,
                "doc_type": "legislation",
            }
            time.sleep(2)

        # 3. Legislation PDFs — the curated list plus whatever the index pages
        #    link that the list does not already name.
        for leg in self._legislation_documents():
            pdf_url = f"{BASE_URL}{leg['pdf']}"
            logger.info(f"Fetching legislation PDF: {leg['slug']}")
            text = self._extract_pdf_text(pdf_url)
            if len(text) < MIN_DOC_CHARS:
                logger.warning(
                    f"Skipping {leg['slug']}: only {len(text)} chars after "
                    f"de-watermarking — image-only scan that OCR could not read"
                )
                continue

            yield {
                "slug": leg["slug"],
                "title": leg["title"],
                "text": text,
                "date": leg.get("date"),
                "url": pdf_url,
                "doc_type": "legislation",
            }
            time.sleep(2)

        # 4. Multi-instrument volumes, split into one document per instrument.
        yield from self._compiled_documents()

        # 5. Gambia Law Reports (bulk PDFs — case law compilations)
        for report in LAW_REPORT_PDFS:
            pdf_url = f"{BASE_URL}{report['pdf']}"
            logger.info(f"Fetching law report PDF: {report['slug']}")
            text = self._extract_pdf_text(pdf_url)
            if len(text) < MIN_DOC_CHARS:
                logger.warning(
                    f"Skipping {report['slug']}: only {len(text)} chars after "
                    f"de-watermarking — image-only scan that OCR could not read"
                )
                continue

            yield {
                "slug": report["slug"],
                "title": report["title"],
                "text": text,
                "date": report.get("date"),
                "url": pdf_url,
                "doc_type": "case_law",
            }
            time.sleep(2)

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Law Hub Gambia is a static site — updates are infrequent."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Normalize a raw document to standard schema."""
        doc_type = raw.get("doc_type", "legislation")

        # Map doc_type to standard _type
        type_map = {
            "case_law": "case_law",
            "legislation": "legislation",
            "constitution": "legislation",
        }

        return {
            "_id": f"GM/LawHubGambia/{raw['slug']}",
            "_source": "GM/LawHubGambia",
            "_type": type_map.get(doc_type, "legislation"),
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "slug": raw["slug"],
            "title": raw["title"],
            "text": raw["text"],
            "date": raw.get("date"),
            "url": raw["url"],
            "doc_type": doc_type,
        }

    def test_api(self):
        """Test connectivity to lawhubgambia.com."""
        print("Testing connection to lawhubgambia.com...")

        # Test homepage
        resp = self._get_with_retry(f"{BASE_URL}/")
        if resp:
            print(f"  Homepage: OK ({resp.status_code})")
        else:
            print("  Homepage: FAILED")
            return

        # Test a case law page
        resp = self._get_with_retry(f"{BASE_URL}/sc-1-2002")
        if resp:
            text = extract_judgment_text(resp.text)
            print(f"  Case law page (sc-1-2002): OK ({len(text)} chars)")
        else:
            print("  Case law page: FAILED")

        # Test a PDF
        resp = self._get_with_retry(f"{BASE_URL}/s/Criminal-Code-Act-No-25-of-1933.pdf")
        if resp:
            print(f"  PDF download: OK ({len(resp.content)} bytes)")
        else:
            print("  PDF download: FAILED")

        # Test constitution page
        resp = self._get_with_retry(f"{BASE_URL}/1997-constitution")
        if resp:
            text = extract_judgment_text(resp.text)
            print(f"  Constitution: OK ({len(text)} chars)")
        else:
            print("  Constitution: FAILED")

        print("Done.")


def main():
    scraper = GMLawHubGambiaScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap --sample|bootstrap-fast|test-api]")
        sys.exit(1)

    command = sys.argv[1]

    if command == "test-api":
        scraper.test_api()
    elif command == "bootstrap-fast":
        # The fleet wrapper invokes this name; without it the wrapper falls back
        # to re-ingesting sample/ and the corpus never advances (#902, #843).
        stats = scraper.bootstrap_fast()
        print(f"\nbootstrap_fast complete: {stats['records_fetched']} fetched, "
              f"{stats.get('records_new', 0)} new, {stats['errors']} errors")
    elif command == "bootstrap":
        sample_mode = "--sample" in sys.argv
        stats = scraper.bootstrap(sample_mode=sample_mode, sample_size=15)
        print(f"\n{'='*60}")
        print(f"Bootstrap complete ({'sample' if sample_mode else 'full'}):")
        print(f"  Records fetched: {stats['records_fetched']}")
        if sample_mode:
            print(f"  Sample records saved: {stats.get('sample_records_saved', 0)}")
        else:
            print(f"  New: {stats['records_new']}")
            print(f"  Updated: {stats['records_updated']}")
            print(f"  Skipped: {stats['records_skipped']}")
        print(f"  Errors: {stats['errors']}")
        print(f"{'='*60}")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
