#!/usr/bin/env python3
"""
INTL/EAEULegalPortal -- EAEU Legal Portal (Treaties and Legislation)

Fetches legislation from the Eurasian Economic Union Legal Portal at
docs.eaeunion.org. Covers Supreme Council acts, Intergovernmental Council acts,
EEC Commission acts, international treaties, CU/CES legacy documents,
memoranda, and official announcements.

Strategy:
  - Scrape listing pages by category (pagination via PAGEN_1)
  - Fetch detail pages for metadata and file download links
  - Download DOCX files (preferred) or PDF for full text extraction
  - DOCX extraction via zipfile/xml, PDF via pdfplumber/PyMuPDF

Data Coverage:
  - ~10,000+ documents across 8 categories
  - Member states: Russia, Kazakhstan, Belarus, Armenia, Kyrgyzstan
  - Languages: Russian (primary), some translations

Usage:
  python bootstrap.py bootstrap          # Fetch all documents
  python bootstrap.py bootstrap --sample # Fetch 15 sample records
  python bootstrap.py test               # Quick connectivity test
"""

import sys
import json
import logging
import re
import io
import time
import html as html_lib
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional, Dict, Any, List, Tuple
from zipfile import ZipFile, BadZipFile
import xml.etree.ElementTree as ET

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.INTL.EAEULegalPortal")

BASE_URL = "https://docs.eaeunion.org"

# Categories to scrape (skip 161 = Court acts, covered by INTL/EAEUCourt)
CATEGORIES = {
    158: "Acts of the Supreme Eurasian Economic Council",
    159: "Intergovernmental Council acts",
    160: "Acts of the Eurasian Economic Commission",
    # 161 is excluded — covered by INTL/EAEUCourt
    162: "Documents of the CU and CES",
    163: "Internal procedures information",
    164: "International treaties",
    165: "Memoranda, statements",
    166: "Official announcements of the EEC",
}


class EAEULegalPortalScraper(BaseScraper):
    """Scraper for INTL/EAEULegalPortal."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.5",
        })

    def _request(self, url: str, timeout: int = 60) -> Optional[requests.Response]:
        """HTTP GET with delay and retry."""
        for attempt in range(3):
            try:
                time.sleep(2)
                resp = self.session.get(url, timeout=timeout)
                if resp.status_code == 429:
                    logger.warning("Rate limited, waiting 30s")
                    time.sleep(30)
                    continue
                if resp.status_code == 404:
                    return None
                resp.raise_for_status()
                return resp
            except requests.exceptions.RequestException as e:
                logger.warning(f"Attempt {attempt+1} failed for {url}: {e}")
                if attempt < 2:
                    time.sleep(10)
        return None

    def _get_listing_doc_links(self, category_id: int, max_pages: int = 200) -> List[Dict[str, str]]:
        """Scrape listing pages for a category, returning doc info dicts."""
        docs = []
        for page_num in range(1, max_pages + 1):
            url = f"{BASE_URL}/documents/{category_id}/"
            if page_num > 1:
                url += f"?PAGEN_1={page_num}"

            resp = self._request(url)
            if resp is None:
                logger.warning(f"Failed to fetch listing page {page_num} for category {category_id}")
                break

            page_html = resp.text
            # Extract document entries from listing
            entries = self._parse_listing_entries(page_html, category_id)
            if not entries:
                break

            docs.extend(entries)
            logger.info(f"Category {category_id} page {page_num}: {len(entries)} docs (total: {len(docs)})")

            # Check if there's a next page
            if f"PAGEN_1={page_num + 1}" not in page_html:
                break

        return docs

    def _parse_listing_entries(self, html: str, category_id: int) -> List[Dict[str, str]]:
        """Parse document entries from a listing page HTML."""
        entries = []

        # Split by DocSearchResult_Item divs
        items = html.split('<div class="DocSearchResult_Item">')

        for item in items[1:]:  # Skip first split (before first item)
            entry = {"category_id": str(category_id)}

            # Extract document URL and title
            link_match = re.search(
                r'href="/documents/(\d+)/(\d+)/"[^>]*class="DocSearchResult_Item__Link"[^>]*>\s*(.*?)\s*</a>',
                item, re.DOTALL
            )
            if not link_match:
                # Try alternate pattern where class comes first
                link_match = re.search(
                    r'class="DocSearchResult_Item__Link"[^>]*href="/documents/(\d+)/(\d+)/"',
                    item
                )
                if not link_match:
                    continue
                entry["section_id"] = link_match.group(1)
                entry["doc_id"] = link_match.group(2)
                # Get title separately
                title_match = re.search(
                    r'class="DocSearchResult_Item__Link"[^>]*>(.*?)</a>',
                    item, re.DOTALL
                )
                entry["title"] = self._clean_html(title_match.group(1)) if title_match else ""
            else:
                entry["section_id"] = link_match.group(1)
                entry["doc_id"] = link_match.group(2)
                entry["title"] = self._clean_html(link_match.group(3))

            # Also try to get title from the link text if we got it differently
            if not entry.get("title"):
                link_match2 = re.search(
                    r'href="/documents/\d+/\d+/"[^>]*>(.*?)</a>',
                    item, re.DOTALL
                )
                if link_match2:
                    entry["title"] = self._clean_html(link_match2.group(1))

            # Extract dates
            date_match = re.search(r'Дата принятия документа:\s*(\d{2}\.\d{2}\.\d{4})', item)
            if date_match:
                entry["adoption_date"] = date_match.group(1)
            pub_match = re.search(r'Дата опубликования документа:\s*(\d{2}\.\d{2}\.\d{4})', item)
            if pub_match:
                entry["publication_date"] = pub_match.group(1)

            # Extract file links (docx preferred, then pdf)
            file_links = re.findall(r'href="(/upload/iblock/[^"]+)"', item)
            entry["file_links"] = list(dict.fromkeys(file_links))  # deduplicate preserving order

            entry["url"] = f"{BASE_URL}/documents/{entry['section_id']}/{entry['doc_id']}/"

            if entry.get("doc_id"):
                entries.append(entry)

        return entries

    def _fetch_detail_metadata(self, doc_url: str) -> Dict[str, str]:
        """Fetch a detail page to get complete metadata and file links."""
        resp = self._request(doc_url)
        if resp is None:
            return {}

        html = resp.text
        meta = {}

        # Full title
        title_match = re.search(
            r'class="DocDetail_Col _value _full-title">\s*(.*?)\s*</div>',
            html, re.DOTALL
        )
        if title_match:
            meta["title"] = self._clean_html(title_match.group(1))

        # Document number
        rows = re.findall(
            r'<div class="DocDetail_Col _title">\s*(.*?)\s*</div>\s*'
            r'<div class="DocDetail_Col _value">\s*(.*?)\s*</div>',
            html, re.DOTALL
        )
        for label, value in rows:
            label_clean = self._clean_html(label).strip()
            value_clean = self._clean_html(value).strip()
            if "Номер документа" in label_clean:
                meta["doc_number"] = value_clean
            elif "Короткий заголовок" in label_clean:
                meta["short_title"] = value_clean
            elif "Вид документа" in label_clean:
                meta["doc_type"] = value_clean
            elif "Дата принятия" in label_clean:
                meta["adoption_date"] = value_clean
            elif "Дата опубликования" in label_clean:
                meta["publication_date"] = value_clean
            elif "Дата вступления в силу" in label_clean:
                meta["effective_date"] = value_clean

        # File download links (all)
        file_links = re.findall(r'href="(/upload/iblock/[^"]+)"', html)
        meta["file_links"] = list(dict.fromkeys(file_links))

        return meta

    def _download_and_extract_text(self, file_links: List[str]) -> Tuple[str, str]:
        """Download files and extract text. Returns (text, source_file_type).

        Preference: DOCX > ZIP (containing DOCX) > PDF > HTML > RTF.

        HTML and RTF sit at the end on purpose. Roughly 14% of the corpus --
        the CU/CES-era documents from 2010-2013 -- was published only as a
        SharePoint "save as HTML" export plus legacy binary .doc annexes, with
        no DOCX or PDF at all, so those documents used to fall through this
        chain and yield empty text (#1420). Keeping HTML *below* PDF means
        documents that already extract cleanly keep producing byte-identical
        text, so re-ingesting cannot rewrite rows that were already correct.
        """
        docx_links = [f for f in file_links if f.lower().endswith('.docx')]
        zip_links = [f for f in file_links if f.lower().endswith('.zip')]
        pdf_links = [f for f in file_links if f.lower().endswith('.pdf')]
        html_links = [f for f in file_links if f.lower().endswith(('.html', '.htm'))]
        rtf_links = [f for f in file_links if f.lower().endswith('.rtf')]

        # Try DOCX first
        for link in docx_links:
            text = self._extract_text_from_docx(BASE_URL + link)
            if text and len(text.strip()) > 50:
                return text.strip(), "docx"

        # Try ZIP (may contain DOCX)
        for link in zip_links:
            try:
                resp = self._request(BASE_URL + link, timeout=120)
                if resp:
                    text = self._extract_text_from_zip(resp.content)
                    if text and len(text.strip()) > 50:
                        return text.strip(), "zip/docx"
            except Exception as e:
                logger.debug(f"ZIP download failed for {link}: {e}")

        # Try PDF
        for link in pdf_links:
            text = self._extract_text_from_pdf(BASE_URL + link)
            if text and len(text.strip()) > 50:
                return text.strip(), "pdf"

        # Try HTML. The portal names the operative act "<stem>_doc.html" and its
        # annexes "<stem>_att.html"; the annexes carry the substantive schedules
        # and tables, so append them to the act rather than picking just one.
        if html_links:
            ordered = (
                [l for l in html_links if "_doc." in l.lower()]
                + [l for l in html_links if "_doc." not in l.lower()]
            )
            parts = []
            for link in ordered:
                part = self._extract_text_from_html(BASE_URL + link)
                if part and len(part.strip()) > 50:
                    parts.append(part.strip())
            if parts:
                return "\n\n".join(parts), "html"

        # Try RTF (rare, but stdlib-only so it costs nothing to support)
        for link in rtf_links:
            text = self._extract_text_from_rtf(BASE_URL + link)
            if text and len(text.strip()) > 50:
                return text.strip(), "rtf"

        return "", ""

    def _extract_text_from_html(self, url: str) -> str:
        """Download and extract text from an HTML document export.

        These are Microsoft Word "save as HTML" files served by SharePoint. The
        <head> carries a long docProps/<xml> metadata preamble (GUIDs, LCIDs,
        content-type ids, timestamps) that is not part of the document, so the
        body is sliced out before stripping tags -- otherwise that preamble ends
        up prepended to every record's text.
        """
        try:
            resp = self._request(url, timeout=120)
            if resp is None:
                return ""
            raw = resp.content

            # The HTTP layer reports no charset, so requests defaults to
            # ISO-8859-1 and mojibakes the Cyrillic. Trust the document's own
            # meta charset, then chardet's guess, then UTF-8.
            enc = None
            meta = re.search(rb'charset=["\']?([\w-]+)', raw[:4096], re.I)
            if meta:
                try:
                    enc = meta.group(1).decode("ascii").lower()
                    "".encode(enc)  # validate the codec name is real
                except (UnicodeDecodeError, LookupError):
                    enc = None
            enc = enc or resp.apparent_encoding or "utf-8"
            text = raw.decode(enc, errors="replace")

            body_start = text.lower().find("<body")
            if body_start != -1:
                text = text[body_start:]

            return self._html_to_text(text)
        except Exception as e:
            logger.warning(f"HTML extraction error for {url}: {e}")
            return ""

    def _extract_text_from_rtf(self, url: str) -> str:
        """Download and extract text from an RTF file (stdlib only)."""
        try:
            resp = self._request(url, timeout=120)
            if resp is None:
                return ""
            rtf = resp.content.decode("cp1251", errors="replace")
            # Decode \'xx hex escapes, drop control words and grouping braces.
            rtf = re.sub(r"\\'([0-9a-fA-F]{2})",
                         lambda m: bytes([int(m.group(1), 16)]).decode("cp1251", "replace"), rtf)
            rtf = re.sub(r'(?s)\{\\\*.*?\}', ' ', rtf)
            rtf = re.sub(r'\\par[d]?\b', '\n', rtf)
            rtf = re.sub(r'\\[a-zA-Z]+-?\d*\s?', ' ', rtf)
            rtf = rtf.replace('{', ' ').replace('}', ' ')
            rtf = re.sub(r'[ \t\xa0]+', ' ', rtf)
            return re.sub(r'\n\s*\n+', '\n\n', rtf).strip()
        except Exception as e:
            logger.warning(f"RTF extraction error for {url}: {e}")
            return ""

    @staticmethod
    def _html_to_text(markup: str) -> str:
        """Strip an HTML fragment down to readable text, preserving line breaks."""
        markup = re.sub(r'(?is)<(script|style)[^>]*>.*?</\1>', ' ', markup)
        markup = re.sub(r'(?is)<!--.*?-->', ' ', markup)
        markup = re.sub(r'(?i)<br\s*/?>', '\n', markup)
        markup = re.sub(r'(?i)</(p|div|tr|li|h[1-6]|td|th)>', '\n', markup)
        markup = re.sub(r'<[^>]+>', ' ', markup)
        markup = html_lib.unescape(markup)
        markup = markup.replace('\xa0', ' ').replace('​', '')
        # Word's export is CRLF-delimited and also wraps mid-paragraph, so
        # normalise line endings before collapsing runs of blank lines.
        markup = markup.replace('\r\n', '\n').replace('\r', '\n')
        markup = re.sub(r'[ \t]+', ' ', markup)
        markup = re.sub(r' *\n *', '\n', markup)
        return re.sub(r'\n{3,}', '\n\n', markup).strip()

    def _extract_text_from_docx(self, url: str) -> str:
        """Download and extract text from a DOCX file."""
        try:
            resp = self._request(url, timeout=120)
            if resp is None:
                return ""
            content_type = resp.headers.get("content-type", "")
            # Handle ZIP files containing docx
            if url.lower().endswith('.zip'):
                return self._extract_text_from_zip(resp.content)

            data = io.BytesIO(resp.content)
            z = ZipFile(data)
            doc_xml = z.read("word/document.xml")
            root = ET.fromstring(doc_xml)
            ns_w = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
            paragraphs = []
            current_para = []
            for elem in root.iter():
                if elem.tag == f"{ns_w}p":
                    if current_para:
                        paragraphs.append("".join(current_para))
                        current_para = []
                elif elem.tag == f"{ns_w}t":
                    if elem.text:
                        current_para.append(elem.text)
                elif elem.tag == f"{ns_w}tab":
                    current_para.append("\t")
                elif elem.tag == f"{ns_w}br":
                    current_para.append("\n")
            if current_para:
                paragraphs.append("".join(current_para))

            text = "\n".join(p for p in paragraphs if p.strip())
            return text
        except (BadZipFile, KeyError, ET.ParseError) as e:
            logger.debug(f"DOCX extraction failed for {url}: {e}")
            return ""
        except Exception as e:
            logger.warning(f"DOCX extraction error for {url}: {e}")
            return ""

    def _extract_text_from_zip(self, content: bytes) -> str:
        """Extract text from a ZIP archive containing DOCX files."""
        try:
            outer = ZipFile(io.BytesIO(content))
            for name in outer.namelist():
                if name.lower().endswith('.docx'):
                    docx_data = outer.read(name)
                    inner = ZipFile(io.BytesIO(docx_data))
                    doc_xml = inner.read("word/document.xml")
                    root = ET.fromstring(doc_xml)
                    ns_w = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"
                    paragraphs = []
                    current_para = []
                    for elem in root.iter():
                        if elem.tag == f"{ns_w}p":
                            if current_para:
                                paragraphs.append("".join(current_para))
                                current_para = []
                        elif elem.tag == f"{ns_w}t":
                            if elem.text:
                                current_para.append(elem.text)
                    if current_para:
                        paragraphs.append("".join(current_para))
                    text = "\n".join(p for p in paragraphs if p.strip())
                    if text.strip():
                        return text
        except Exception as e:
            logger.debug(f"ZIP extraction failed: {e}")
        return ""

    def _extract_text_from_pdf(self, url: str) -> str:
        """Download and extract text from a PDF file."""
        try:
            resp = self._request(url, timeout=120)
            if resp is None:
                return ""
            data = io.BytesIO(resp.content)

            # Try pdfplumber first
            try:
                import pdfplumber
                with pdfplumber.open(data) as pdf:
                    pages_text = []
                    for page in pdf.pages:
                        t = page.extract_text()
                        if t:
                            pages_text.append(t)
                        # Release per-page layout + cached textmap to avoid
                        # multi-GB peak RSS on large PDFs (OOM exit 137, #930).
                        page.flush_cache()
                        try:
                            page.get_textmap.cache_clear()
                        except AttributeError:
                            pass
                    text = "\n\n".join(pages_text)
                    if text.strip():
                        return text
            except ImportError:
                pass

            # Try PyMuPDF
            data.seek(0)
            try:
                import fitz
                doc = fitz.open(stream=data.read(), filetype="pdf")
                pages_text = []
                for page in doc:
                    t = page.get_text()
                    if t:
                        pages_text.append(t)
                doc.close()
                text = "\n\n".join(pages_text)
                if text.strip():
                    return text
            except ImportError:
                pass

            # Try pypdf
            data.seek(0)
            try:
                from pypdf import PdfReader
                reader = PdfReader(data)
                pages_text = []
                for page in reader.pages:
                    t = page.extract_text()
                    if t:
                        pages_text.append(t)
                text = "\n\n".join(pages_text)
                if text.strip():
                    return text
            except ImportError:
                pass

            return ""
        except Exception as e:
            logger.warning(f"PDF extraction error for {url}: {e}")
            return ""

    def _clean_html(self, text: str) -> str:
        """Remove HTML tags and decode entities."""
        text = re.sub(r'<[^>]+>', ' ', text)
        text = html_lib.unescape(text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def _parse_date(self, date_str: str) -> Optional[str]:
        """Convert DD.MM.YYYY to ISO 8601."""
        if not date_str:
            return None
        date_str = date_str.strip()
        m = re.match(r'(\d{2})\.(\d{2})\.(\d{4})', date_str)
        if m:
            return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
        return None

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Yield all documents from all categories."""
        for cat_id, cat_name in CATEGORIES.items():
            logger.info(f"=== Category {cat_id}: {cat_name} ===")
            doc_infos = self._get_listing_doc_links(cat_id)
            logger.info(f"Found {len(doc_infos)} documents in category {cat_id}")

            for doc_info in doc_infos:
                record = self._process_document(doc_info, cat_name)
                if record:
                    yield record

    def fetch_updates(self, since: str = None) -> Generator[Dict[str, Any], None, None]:
        """Yield recently added documents."""
        yield from self.fetch_all()

    def _process_document(self, doc_info: Dict[str, str], cat_name: str) -> Optional[Dict[str, Any]]:
        """Fetch detail page, download file, and build record."""
        doc_url = doc_info.get("url", "")
        doc_id = doc_info.get("doc_id", "")

        # Fetch detail page for complete metadata and file links
        detail = self._fetch_detail_metadata(doc_url)

        title = detail.get("title") or doc_info.get("title", "")
        if not title:
            logger.warning(f"No title for doc {doc_id}, skipping")
            return None

        # Merge file links from listing and detail page
        file_links = list(dict.fromkeys(
            doc_info.get("file_links", []) + detail.get("file_links", [])
        ))

        # Download and extract text
        text, file_type = self._download_and_extract_text(file_links)

        adoption_date = self._parse_date(
            detail.get("adoption_date") or doc_info.get("adoption_date", "")
        )
        publication_date = self._parse_date(
            detail.get("publication_date") or doc_info.get("publication_date", "")
        )
        effective_date = self._parse_date(detail.get("effective_date", ""))

        record = {
            "doc_id": doc_id,
            "section_id": doc_info.get("section_id", ""),
            "title": title,
            "short_title": detail.get("short_title", ""),
            "doc_number": detail.get("doc_number", ""),
            "doc_type": detail.get("doc_type", cat_name),
            "category": cat_name,
            "text": text,
            "text_source": file_type,
            "date": adoption_date,
            "publication_date": publication_date,
            "effective_date": effective_date,
            "url": doc_url,
        }

        if not text:
            logger.warning(f"No text extracted for doc {doc_id}: {title[:80]}")

        return record

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize a raw record into the standard schema."""
        return {
            "_id": f"EAEU-{raw['doc_id']}",
            "_source": "INTL/EAEULegalPortal",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": raw.get("title", ""),
            "short_title": raw.get("short_title", ""),
            "doc_number": raw.get("doc_number", ""),
            "doc_type": raw.get("doc_type", ""),
            "category": raw.get("category", ""),
            "text": raw.get("text", ""),
            "date": raw.get("date"),
            "publication_date": raw.get("publication_date"),
            "effective_date": raw.get("effective_date"),
            "url": raw.get("url", ""),
        }

    def run_bootstrap(self, sample: bool = False):
        """Run the bootstrap process."""
        sample_dir = self.source_dir / "sample"
        data_dir = self.source_dir / "data"
        sample_dir.mkdir(exist_ok=True)
        data_dir.mkdir(exist_ok=True)

        count = 0
        target = 15 if sample else 999999
        errors = 0
        no_text = 0

        if sample:
            # For sample: try docs from all categories, prioritize those with docx links
            for cat_id, cat_name in CATEGORIES.items():
                if count >= target:
                    break
                logger.info(f"=== Sample: Category {cat_id}: {cat_name} ===")
                doc_infos = self._get_listing_doc_links(cat_id, max_pages=1)

                # Sort: entries with .docx or .zip links first
                def has_docx(d):
                    return any(f.endswith('.docx') or f.endswith('.zip') for f in d.get('file_links', []))
                doc_infos_sorted = sorted(doc_infos, key=has_docx, reverse=True)

                # Cap per category so the sample spans all eight rather than
                # filling up from the first two. The legacy CU/CES categories
                # are the HTML-only ones (#1420), so without this the samples
                # never exercise the HTML extraction path at all.
                tried = 0
                taken = 0
                per_category = max(2, target // len(CATEGORIES) + 1)
                for doc_info in doc_infos_sorted:
                    if count >= target or tried >= 8 or taken >= per_category:
                        break
                    tried += 1
                    record = self._process_document(doc_info, cat_name)
                    if record is None:
                        errors += 1
                        continue
                    normalized = self.normalize(record)
                    if not normalized.get("text"):
                        no_text += 1
                        continue  # Skip records without text in sample mode

                    fname = f"{normalized['_id']}.json"
                    with open(sample_dir / fname, "w", encoding="utf-8") as f:
                        json.dump(normalized, f, ensure_ascii=False, indent=2)
                    count += 1
                    taken += 1
                    logger.info(
                        f"[{count}/{target}] {normalized['_id']}: "
                        f"{normalized['title'][:60]}... "
                        f"(text: {len(normalized.get('text', ''))} chars, "
                        f"via {record.get('text_source') or '?'})"
                    )
        else:
            # Stream to data/records.jsonl. This used to write one JSON file per
            # document into data/, which left the ingest side globbing ~10,000
            # files and holding them all at once (the OOM half of #1420).
            records_path = data_dir / "records.jsonl"
            no_text_path = data_dir / "no_text_docs.txt"
            with open(records_path, "w", encoding="utf-8") as out, \
                 open(no_text_path, "w", encoding="utf-8") as miss:
                for record in self.fetch_all():
                    if count >= target:
                        break
                    normalized = self.normalize(record)
                    if not normalized.get("text"):
                        no_text += 1
                        # Record which documents yielded nothing so an
                        # extraction regression can be reproduced without
                        # re-crawling the whole corpus.
                        miss.write(f"{normalized['_id']}\t{normalized.get('url','')}\n")
                        miss.flush()
                    out.write(json.dumps(normalized, ensure_ascii=False) + "\n")
                    out.flush()
                    count += 1
                    if count % 50 == 0:
                        logger.info(f"Progress: {count} documents saved ({no_text} without text)")
            logger.info(f"Wrote {count} records to {records_path}")

        logger.info(f"Bootstrap complete: {count} documents, {no_text} without text, {errors} errors")
        if not sample and count and no_text / count > 0.05:
            logger.warning(
                f"{no_text}/{count} ({100 * no_text / count:.1f}%) documents produced no text -- "
                f"see {data_dir / 'no_text_docs.txt'}"
            )
        return count

    def run_test(self):
        """Quick connectivity test."""
        logger.info("Testing connectivity to docs.eaeunion.org...")
        resp = self._request(f"{BASE_URL}/")
        if resp and resp.status_code == 200:
            logger.info("OK: Main page accessible")
        else:
            logger.error("FAIL: Cannot reach main page")
            return False

        # Test one listing page
        resp = self._request(f"{BASE_URL}/documents/158/")
        if resp and resp.status_code == 200:
            logger.info("OK: Listing page accessible")
        else:
            logger.error("FAIL: Cannot reach listing page")
            return False

        # Test one document detail page
        resp = self._request(f"{BASE_URL}/documents/464/10650/")
        if resp and resp.status_code == 200:
            logger.info("OK: Detail page accessible")
        else:
            logger.error("FAIL: Cannot reach detail page")
            return False

        logger.info("All connectivity tests passed")
        return True


if __name__ == "__main__":
    scraper = EAEULegalPortalScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|test] [--sample|--full]")
        sys.exit(1)

    command = sys.argv[1]
    sample = "--sample" in sys.argv

    if command == "test":
        success = scraper.run_test()
        sys.exit(0 if success else 1)
    # The fleet wrapper invokes `bootstrap-fast`; without this alias argparse
    # rejected it and the wrapper fell back to re-ingesting sample/.
    elif command in ("bootstrap", "bootstrap-fast"):
        count = scraper.run_bootstrap(sample=sample)
        sys.exit(0 if count > 0 else 1)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
