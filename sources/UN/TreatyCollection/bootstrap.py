#!/usr/bin/env python3
"""
UN/TreatyCollection - United Nations Treaty Collection (UNTS) Fetcher

Fetches multilateral treaties deposited with the UN Secretary-General.
~419 treaties across 29 chapters (human rights, environment, trade, etc.).

Data sources:
  - Chapter listings: treaties.un.org/Pages/Treaties.aspx
  - MTDSG XML: treaties.un.org/doc/Publication/MTDSG/Volume%20I/...
    Contains metadata + declarations, reservations, objections, participants
  - Treaty text PDFs: CTC PDFs from ViewDetails pages (actual treaty articles)

Method:
  1. Scrape chapter pages for treaty IDs (or use hardcoded sample list)
  2. Fetch MTDSG XML for each treaty -> metadata + substantial legal text
  3. Optionally fetch CTC PDF for actual treaty articles text
  4. Combine XML text + PDF text into full text field

License: UN public domain
Auth: None

Usage:
  python bootstrap.py bootstrap --sample   # Fetch ~12 sample records
  python bootstrap.py bootstrap             # Full bootstrap (all ~419 treaties)
  python bootstrap.py test                  # Test connectivity
"""

import html
import json
import re
import sys
import time
import logging
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.UN.TreatyCollection")

BASE_URL = "https://treaties.un.org"
SOURCE_ID = "UN/TreatyCollection"

NUM_CHAPTERS = 29

ROMAN_NUMERALS = {
    1: "I", 2: "II", 3: "III", 4: "IV", 5: "V",
    6: "VI", 7: "VII", 8: "VIII", 9: "IX", 10: "X",
    11: "XI", 12: "XII", 13: "XIII", 14: "XIV", 15: "XV",
    16: "XVI", 17: "XVII", 18: "XVIII", 19: "XIX", 20: "XX",
    21: "XXI", 22: "XXII", 23: "XXIII", 24: "XXIV", 25: "XXV",
    26: "XXVI", 27: "XXVII", 28: "XXVIII", 29: "XXIX",
}

# Well-known sample treaties for sample mode (skips chapter scraping)
SAMPLE_TREATIES = [
    {"mtdsg_no": "I-1", "chapter": 1, "listing_title": "Charter of the United Nations"},
    {"mtdsg_no": "IV-8", "chapter": 4, "listing_title": "Convention on the Elimination of All Forms of Discrimination against Women"},
    {"mtdsg_no": "IV-4", "chapter": 4, "listing_title": "International Covenant on Civil and Political Rights"},
    {"mtdsg_no": "IV-3", "chapter": 4, "listing_title": "International Covenant on Economic, Social and Cultural Rights"},
    {"mtdsg_no": "III-3", "chapter": 3, "listing_title": "Vienna Convention on Diplomatic Relations"},
    {"mtdsg_no": "VI-1", "chapter": 6, "listing_title": "Protocol amending the Agreements, Conventions and Protocols on Narcotic Drugs"},
    {"mtdsg_no": "X-1-a", "chapter": 10, "listing_title": "General Agreement on Tariffs and Trade"},
    {"mtdsg_no": "XVIII-10", "chapter": 18, "listing_title": "Rome Statute of the International Criminal Court"},
    {"mtdsg_no": "XXVI-2", "chapter": 26, "listing_title": "Convention on Certain Conventional Weapons"},
    {"mtdsg_no": "XXVII-7", "chapter": 27, "listing_title": "United Nations Framework Convention on Climate Change"},
    {"mtdsg_no": "IV-11", "chapter": 4, "listing_title": "Convention on the Rights of the Child"},
    {"mtdsg_no": "IV-15", "chapter": 4, "listing_title": "Convention on the Rights of Persons with Disabilities"},
]


# ---------------------------------------------------------------------------
# HTTP session
# ---------------------------------------------------------------------------

def _make_session() -> requests.Session:
    """Create a requests session with appropriate headers."""
    s = requests.Session()
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36 LegalDataHunter/1.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    return s


SESSION = _make_session()


def fetch_url(url: str, timeout: int = 60) -> Optional[requests.Response]:
    """Fetch a URL with error handling."""
    try:
        resp = SESSION.get(url, timeout=timeout, allow_redirects=True)
        resp.raise_for_status()
        return resp
    except requests.RequestException as e:
        logger.warning("Error fetching %s: %s", url, e)
        return None


# ---------------------------------------------------------------------------
# XML text extraction helpers
# ---------------------------------------------------------------------------

def _elem_text(elem: Optional[ET.Element]) -> str:
    """Extract all text content from an XML element, stripping embedded tags."""
    if elem is None:
        return ""
    raw = ET.tostring(elem, encoding="unicode", method="text")
    raw = html.unescape(raw)
    raw = re.sub(r"<[^>]+>", "", raw)
    raw = re.sub(r"\n{3,}", "\n\n", raw)
    raw = re.sub(r"[ \t]+", " ", raw)
    return raw.strip()


def _extract_participants_table(participants_elem: Optional[ET.Element]) -> str:
    """Extract participant table as readable text."""
    if participants_elem is None:
        return ""
    lines = []
    for row in participants_elem.iter("Row"):
        entries = row.findall("Entry")
        if entries:
            vals = [_elem_text(e) for e in entries]
            vals = [v for v in vals if v]
            if vals:
                lines.append(" | ".join(vals))
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Phase 1: Discover treaty IDs
# ---------------------------------------------------------------------------

def get_all_treaty_ids() -> list[dict]:
    """Fetch all treaty IDs and titles from chapter listing pages."""
    treaties = []
    for ch in range(1, NUM_CHAPTERS + 1):
        url = f"{BASE_URL}/Pages/Treaties.aspx?id={ch}&subid=A&clang=_en"
        resp = fetch_url(url)
        if not resp:
            continue

        entries = re.findall(r'mtdsg_no=([^&"]+)[^>]*>([^<]+)</a>', resp.text)
        seen = set()
        for mtdsg_no, raw_title in entries:
            if mtdsg_no in seen:
                continue
            seen.add(mtdsg_no)
            title = re.sub(r"&nbsp;", " ", raw_title).strip()
            treaties.append({"mtdsg_no": mtdsg_no, "chapter": ch, "listing_title": title})

        logger.info("Chapter %d: %d treaties", ch, len(seen))
        time.sleep(1.0)

    logger.info("Total: %d treaties", len(treaties))
    return treaties


# ---------------------------------------------------------------------------
# Phase 2: Fetch XML and extract metadata + text
# ---------------------------------------------------------------------------

def fetch_treaty_xml(mtdsg_no: str, chapter: int) -> Optional[ET.Element]:
    """Fetch and parse MTDSG XML for a treaty. Returns XML root or None."""
    roman = ROMAN_NUMERALS.get(chapter, str(chapter))
    xml_url = (
        f"{BASE_URL}/doc/Publication/MTDSG/Volume%20I/"
        f"Chapter%20{roman}/{mtdsg_no}.en.xml"
    )
    resp = fetch_url(xml_url, timeout=30)
    if not resp:
        return None
    text = resp.text.strip()
    if not text.startswith("<?xml"):
        logger.warning("XML response for %s does not start with <?xml", mtdsg_no)
        return None
    try:
        return ET.fromstring(text)
    except ET.ParseError as e:
        logger.warning("XML parse error for %s: %s", mtdsg_no, e)
        return None


def parse_xml_full(root: ET.Element, listing_title: str = "") -> dict:
    """
    Parse treaty XML into metadata dict AND full legal text.

    The MTDSG XML contains:
      - Header/ExternalData: title, conclusion, entry into force, registration, status
      - Participants: state-by-state participation table
      - Declarations: state declarations and reservations
      - Objections: objections to reservations
      - DeclarationsUnderArticle, Notifications, TerritorialApplications, EndNotes

    All of these sections contain substantial legal text.
    """
    meta = {}
    treaty = root.find("Treaty")
    if treaty is None:
        # Try without Treaty wrapper (some XMLs have it directly)
        treaty = root

    # -- Metadata from Header/ExternalData --
    chapter_name_el = root.find(".//Chapter/Name")
    if chapter_name_el is not None and chapter_name_el.text:
        meta["chapter_name"] = chapter_name_el.text.strip()

    ext = root.find(".//ExternalData")
    if ext is not None:
        title_el = ext.find("Titlesect")
        if title_el is not None and title_el.text:
            meta["title"] = re.sub(r"<[^>]+>", "", title_el.text).strip()

        conclusion_el = ext.find("Conclusion")
        if conclusion_el is not None and conclusion_el.text:
            raw = re.sub(r"<[^>]+>", "", conclusion_el.text).strip()
            meta["conclusion"] = raw
            date_match = re.search(r"(\d{1,2}\s+\w+\s+\d{4})", raw)
            if date_match:
                try:
                    dt = datetime.strptime(date_match.group(1), "%d %B %Y")
                    meta["date"] = dt.strftime("%Y-%m-%d")
                except ValueError:
                    pass

        eif = ext.find(".//EIF/Labeltext")
        if eif is not None and eif.text:
            meta["entry_into_force"] = re.sub(r"<[^>]+>", "", eif.text).strip()

        reg = ext.find(".//Registration/Labeltext")
        if reg is not None and reg.text:
            meta["registration"] = reg.text.strip()
            reg_match = re.search(r"No\.\s*(\d+)", reg.text)
            if reg_match:
                meta["registration_number"] = reg_match.group(1)

        sig = ext.find(".//Status/Signatories")
        if sig is not None and sig.text:
            try:
                meta["signatories_count"] = int(sig.text)
            except ValueError:
                pass

        parties = ext.find(".//Status/Parties")
        if parties is not None and parties.text:
            try:
                meta["parties_count"] = int(parties.text)
            except ValueError:
                pass

        text_ref = ext.find(".//TreatyText/Text")
        if text_ref is not None and text_ref.text:
            meta["text_reference"] = re.sub(r"<[^>]+>", "", text_ref.text).strip()

    # Participant list for metadata
    participants = []
    for row in root.findall(".//Participants//Row"):
        row_entries = row.findall("Entry")
        if row_entries and row_entries[0].text:
            participants.append(row_entries[0].text.strip())
    if participants:
        meta["participants"] = participants

    # Use listing title as fallback
    if not meta.get("title"):
        meta["title"] = listing_title

    # -- Build full text from all XML sections --
    text_parts = []

    # Declarations and reservations (often 10K-100K+ chars)
    declarations = _elem_text(treaty.find("Declarations") if treaty is not root else root.find(".//Declarations"))
    if declarations:
        text_parts.append(f"DECLARATIONS AND RESERVATIONS\n\n{declarations}")

    # Objections to reservations (often very large)
    objections = _elem_text(treaty.find("Objections") if treaty is not root else root.find(".//Objections"))
    if objections:
        text_parts.append(f"OBJECTIONS\n\n{objections}")

    # Declarations under specific articles
    decl_article = _elem_text(treaty.find("DeclarationsUnderArticle") if treaty is not root else root.find(".//DeclarationsUnderArticle"))
    if decl_article:
        text_parts.append(f"DECLARATIONS UNDER ARTICLE\n\n{decl_article}")

    # Notifications
    notifications = _elem_text(treaty.find("Notifications") if treaty is not root else root.find(".//Notifications"))
    if notifications:
        text_parts.append(f"NOTIFICATIONS\n\n{notifications}")

    # Territorial applications
    territory = _elem_text(treaty.find("TerritorialApplications") if treaty is not root else root.find(".//TerritorialApplications"))
    if territory:
        text_parts.append(f"TERRITORIAL APPLICATIONS\n\n{territory}")

    # Participants table
    participants_text = _extract_participants_table(
        treaty.find("Participants") if treaty is not root else root.find(".//Participants")
    )
    if participants_text:
        text_parts.append(f"PARTICIPANTS\n\n{participants_text}")

    # Endnotes
    endnotes = _elem_text(treaty.find("EndNotes") if treaty is not root else root.find(".//EndNotes"))
    if endnotes:
        text_parts.append(f"NOTES\n\n{endnotes}")

    xml_text = "\n\n---\n\n".join(text_parts)
    meta["_xml_text"] = xml_text

    return meta


# ---------------------------------------------------------------------------
# Phase 3: Optionally fetch CTC PDF for treaty articles text
# ---------------------------------------------------------------------------

def find_treaty_pdf_url(mtdsg_no: str, chapter: int) -> Optional[str]:
    """Find the CTC (Certified True Copy) PDF URL from the ViewDetails page."""
    url = (
        f"{BASE_URL}/Pages/ViewDetails.aspx"
        f"?src=TREATY&mtdsg_no={mtdsg_no}&chapter={chapter}&clang=_en"
    )
    resp = fetch_url(url, timeout=60)
    if not resp:
        return None

    # Look for CTC PDFs (certified true copies of treaty text)
    ctc_pdfs = re.findall(r'/doc/Treaties/[^"\'>\s]+\.pdf', resp.text)
    if ctc_pdfs:
        pdf_path = ctc_pdfs[0].replace("&amp;", "&")
        return f"{BASE_URL}{pdf_path}" if not pdf_path.startswith("http") else pdf_path

    # Look for CTC publication PDFs
    ctc_pub = re.findall(r'/doc/Publication/CTC/[^"\'>\s]+\.pdf', resp.text)
    if ctc_pub:
        return f"{BASE_URL}{ctc_pub[0]}"

    return None


def extract_pdf_text(pdf_url: str, mtdsg_no: str) -> str:
    """Download and extract text from treaty PDF. Returns empty string on failure."""
    try:
        from common.pdf_extract import extract_pdf_markdown
    except ImportError:
        logger.info("pdf_extract not available, skipping PDF text for %s", mtdsg_no)
        return ""

    resp = fetch_url(pdf_url, timeout=120)
    if not resp:
        return ""

    if len(resp.content) > 50_000_000:
        logger.warning("PDF too large for %s: %d bytes", mtdsg_no, len(resp.content))
        return ""

    if resp.content[:5] != b"%PDF-":
        logger.warning("Not a valid PDF for %s", mtdsg_no)
        return ""

    try:
        text = extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=mtdsg_no,
            pdf_bytes=resp.content,
            table="legislation",
        ) or ""
    except Exception as e:
        logger.warning("PDF extraction failed for %s: %s", mtdsg_no, e)
        text = ""

    return text


# ---------------------------------------------------------------------------
# Phase 4: Combined fetch for a single treaty
# ---------------------------------------------------------------------------

def fetch_single_treaty(entry: dict) -> Optional[dict]:
    """
    Fetch metadata and full text for a single treaty.

    Strategy:
      1. Fetch MTDSG XML -> metadata + declarations/reservations/objections text
      2. Optionally fetch CTC PDF -> treaty articles text
      3. Combine: PDF text (treaty articles) + XML text (declarations etc.)
    """
    mtdsg_no = entry["mtdsg_no"]
    chapter = entry["chapter"]

    # Step 1: XML metadata and text
    root = fetch_treaty_xml(mtdsg_no, chapter)
    if root is None:
        logger.warning("No XML available for %s, skipping", mtdsg_no)
        return None

    meta = parse_xml_full(root, listing_title=entry.get("listing_title", mtdsg_no))
    xml_text = meta.pop("_xml_text", "")
    logger.info("Title: %s", (meta.get("title", mtdsg_no) or mtdsg_no)[:80])
    logger.info("XML text: %d chars", len(xml_text))
    time.sleep(1.0)

    # Step 2: Try to get CTC PDF for treaty articles text
    pdf_text = ""
    pdf_url = find_treaty_pdf_url(mtdsg_no, chapter)
    if pdf_url:
        logger.info("PDF: %s", pdf_url[:100])
        pdf_text = extract_pdf_text(pdf_url, mtdsg_no)
        logger.info("PDF text: %d chars", len(pdf_text))
    time.sleep(1.0)

    # Step 3: Combine texts
    # PDF text = actual treaty articles; XML text = declarations/reservations/etc.
    parts = []
    if pdf_text:
        parts.append(pdf_text)
    if xml_text:
        parts.append(xml_text)
    full_text = "\n\n---\n\n".join(parts)

    if not full_text:
        logger.warning("No text content for treaty %s", mtdsg_no)

    return {
        "mtdsg_no": mtdsg_no,
        "chapter": chapter,
        "meta": meta,
        "full_text": full_text,
    }


# ---------------------------------------------------------------------------
# Scraper class
# ---------------------------------------------------------------------------

class TreatyCollectionScraper(BaseScraper):
    """Scraper for UN/TreatyCollection - UN Treaty Collection."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def normalize(self, raw: dict) -> Optional[dict]:
        """Normalize treaty data into standard schema."""
        meta = raw.get("meta", {})
        full_text = raw.get("full_text", "")
        mtdsg_no = raw.get("mtdsg_no", "")
        chapter = raw.get("chapter", 0)

        title = meta.get("title", f"Treaty {mtdsg_no}")
        treaty_url = (
            f"{BASE_URL}/Pages/ViewDetails.aspx"
            f"?src=TREATY&mtdsg_no={mtdsg_no}&chapter={chapter}&clang=_en"
        )

        return {
            "_id": f"UNTS-{mtdsg_no}",
            "_source": SOURCE_ID,
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": meta.get("date"),
            "url": treaty_url,
            "mtdsg_no": mtdsg_no,
            "chapter": chapter,
            "chapter_name": meta.get("chapter_name"),
            "conclusion": meta.get("conclusion"),
            "entry_into_force": meta.get("entry_into_force"),
            "registration_number": meta.get("registration_number"),
            "signatories_count": meta.get("signatories_count"),
            "parties_count": meta.get("parties_count"),
            "text_reference": meta.get("text_reference"),
            "participants": meta.get("participants"),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """
        Fetch treaties with full text.

        Streams results chapter-by-chapter: scrape chapter page for IDs,
        then immediately fetch each treaty's XML + PDF before moving to
        the next chapter. This avoids the long upfront chapter-scraping
        delay and lets the BaseScraper save results incrementally.
        """
        logger.info("Fetching treaties chapter by chapter...")

        total_yielded = 0
        for ch in range(1, NUM_CHAPTERS + 1):
            url = f"{BASE_URL}/Pages/Treaties.aspx?id={ch}&subid=A&clang=_en"
            resp = fetch_url(url)
            if not resp:
                continue

            entries = re.findall(r'mtdsg_no=([^&"]+)[^>]*>([^<]+)</a>', resp.text)
            seen = set()
            chapter_entries = []
            for mtdsg_no, raw_title in entries:
                if mtdsg_no in seen:
                    continue
                seen.add(mtdsg_no)
                title = re.sub(r"&nbsp;", " ", raw_title).strip()
                chapter_entries.append({
                    "mtdsg_no": mtdsg_no,
                    "chapter": ch,
                    "listing_title": title,
                })

            logger.info("Chapter %d: %d treaties", ch, len(chapter_entries))
            time.sleep(1.0)

            for entry in chapter_entries:
                total_yielded += 1
                logger.info("[%d] %s (Chapter %d)",
                            total_yielded, entry["mtdsg_no"], ch)
                result = fetch_single_treaty(entry)
                if result:
                    yield result

        logger.info("Finished: %d treaties yielded", total_yielded)

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        yield from self.fetch_all()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    scraper = TreatyCollectionScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|test] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "test":
        print("Testing UN Treaty Collection connectivity...")
        resp = fetch_url(f"{BASE_URL}/Pages/Treaties.aspx?id=4&subid=A&clang=_en")
        if resp:
            ids = re.findall(r'mtdsg_no=([^&"]+)', resp.text)
            print(f"  Chapter listing: OK ({len(set(ids))} treaties in Chapter IV)")
        else:
            print("  Chapter listing: FAILED")
            sys.exit(1)

        root = fetch_treaty_xml("IV-8", 4)
        if root:
            meta = parse_xml_full(root, listing_title="CEDAW")
            xml_text = meta.pop("_xml_text", "")
            print(f"  XML metadata: OK (Title: {meta.get('title', 'N/A')[:60]})")
            print(f"  XML text: {len(xml_text)} chars")
        else:
            print("  XML metadata: FAILED")
            sys.exit(1)

        print("Test PASSED")

    elif command == "bootstrap":
        scraper.bootstrap(sample_mode=sample_mode)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
