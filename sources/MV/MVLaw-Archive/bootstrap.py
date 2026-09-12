#!/usr/bin/env python3
"""
MV/MVLaw-Archive -- Maldives Attorney General old MVLaw English translations

17 English translations of key Maldivian laws from old.mvlaw.gov.mv.
PDFs downloaded and text extracted via pdfplumber.

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap-fast       # Alias for bootstrap
  python bootstrap.py test                 # Quick connectivity test
"""

import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.MV.MVLaw-Archive")

SOURCE_ID = "MV/MVLaw-Archive"
BASE_URL = "https://old.mvlaw.gov.mv"

DOCUMENTS = [
    {"id": "FIL", "title": "Law of Foreign Investments in the Republic of Maldives", "file": "FIL.pdf"},
    {"id": "PC1", "title": "Penal Code Chapter 1", "file": "PC1.pdf"},
    {"id": "PC2", "title": "Penal Code Chapter 2", "file": "PC2.pdf"},
    {"id": "Environment", "title": "Environmental Protection and Preservations Act of Maldives", "file": "Environment.pdf"},
    {"id": "Consumer", "title": "Consumer Protection Act", "file": "Consumer.pdf"},
    {"id": "Terrorism", "title": "Prevention of Terrorism Act", "file": "Terrorism.pdf"},
    {"id": "PC3", "title": "Penal Code Chapter 3", "file": "PC3.pdf"},
    # Served lowercase upstream; "Partnership.pdf" 404s.
    {"id": "Partnership", "title": "Partnership Act of Maldives", "file": "partnership.pdf"},
    {"id": "PC4", "title": "Penal Code Chapter 4", "file": "PC4.pdf"},
    {"id": "Company", "title": "The Company Act of the Maldives", "file": "Company.pdf"},
    {"id": "Corruption", "title": "Prevention and Prohibition of Corruption Act", "file": "Corruption.pdf"},
    {"id": "Family", "title": "Family Act", "file": "Family.pdf"},
    {"id": "Land", "title": "Maldivian Land Act", "file": "Land.pdf"},
    {"id": "AssocAct", "title": "Association Act", "file": "AssocAct.pdf"},
    {"id": "CabinetMinisters", "title": "Addressing of Questions to Cabinet Minister's Act", "file": "CabinetMinisters.pdf"},
    {"id": "HRC", "title": "Human Rights Commission Act", "file": "HRC.pdf"},
    {"id": "Securities", "title": "Securities Act", "file": "Securities.pdf"},
    {"id": "Constitution2008EN", "title": "Constitution of the Republic of Maldives 2008 (English)", "file": "../ganoon/QaanoonAsaasee/English-constitution.pdf"},
]


def curl_download(url: str, dest: str, max_attempts: int = 3) -> bool:
    """Download a file via curl."""
    for attempt in range(max_attempts):
        try:
            result = subprocess.run(
                ['curl', '-s', '-L', '--max-time', '60',
                 '-H', 'User-Agent: Mozilla/5.0 (compatible; LegalDataHunter/1.0)',
                 '-o', dest, url],
                capture_output=True, text=True, timeout=70
            )
            if result.returncode == 0 and os.path.getsize(dest) > 500:
                return True
        except (subprocess.TimeoutExpired, OSError):
            pass
        delay = min(5 * (2 ** attempt), 30)
        logger.warning("Download attempt %d failed for %s", attempt + 1, url)
        time.sleep(delay)
    return False


def extract_pdf_text(pdf_path: str) -> str:
    """Extract text from PDF using pdfplumber."""
    try:
        import pdfplumber
        with pdfplumber.open(pdf_path) as pdf:
            parts = []
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    text = re.sub(r'\(cid:\d+\)', ' ', text)
                    text = re.sub(r' {2,}', ' ', text)
                    parts.append(text.strip())
                try:
                    page.flush_cache(); page.get_textmap.cache_clear()
                except Exception:
                    pass
            return repair_word_export_damage("\n\n".join(parts))
    except Exception as e:
        logger.warning("PDF extraction failed for %s: %s", pdf_path, e)
        return ""


# --- Repair of Word-export damage present in the upstream PDFs -------------
#
# Some of these files were exported from Word with their automatic list
# numbering baked in as literal glyphs, and the export lost the digit: the
# PDF itself *renders* "0. Registration of Associations" (AssocAct.pdf p.5),
# so no extractor can recover the number -- it is not in the page content
# stream at all. The same export also dropped the letter from sub-clause
# labels ("a)" -> ")") and left Word hyperlink field codes ("TU UT") behind.
#
# The numbers are still recoverable from the document, just not from the
# damaged line: the table of contents lists every section with its true
# number, and the surviving (undamaged) headings pin the sequence. We only
# rewrite a run of damaged headings when the arithmetic gap between its
# neighbours matches the run length exactly, so a wrong guess cannot be
# written silently.

# A run of Word hyperlink field-code remnants occupying a whole line.
_FIELD_CODE_LINE_RE = re.compile(r"^(?:TU|UT)(?:\s+(?:TU|UT))*$")
# "12. Some Heading" at the start of a line.
_HEADING_RE = re.compile(r"^(\d+)\.[ \t]+(\S.*)$")
# A sub-clause label that kept its letter ("a) ...") or lost it (") ...").
_LETTER_RE = re.compile(r"^([a-z]?)\)[ \t]+(\S.*)$")


def _strip_field_codes(lines: List[str]) -> List[str]:
    """Drop lines that are only Word 'TU'/'UT' hyperlink field-code remnants."""
    return [ln for ln in lines if not _FIELD_CODE_LINE_RE.match(ln.strip())]


def _split_toc(lines: List[str]) -> tuple:
    """Return (section number -> title from the contents page, body start index).

    Contents entries carry dot leaders and a page number, and long titles wrap
    across lines, so an entry runs from its number until the leaders appear.
    The contents block ends at the last line carrying leaders; everything after
    that is body, which keeps wrapped body headings from being read as entries.
    """
    leader = [i for i, ln in enumerate(lines) if re.search(r"\.{4,}\s*\d+\s*$", ln.strip())]
    if not leader:
        return {}, 0
    body_start = leader[-1] + 1

    toc: Dict[int, str] = {}
    num: Optional[int] = None
    parts: List[str] = []
    for ln in lines[:body_start]:
        stripped = ln.strip()
        m = _HEADING_RE.match(stripped)
        if m:
            num, parts = int(m.group(1)), [m.group(2)]
        elif num is not None:
            parts.append(stripped)
        else:
            continue
        if "...." in parts[-1]:
            parts[-1] = re.sub(r"\.{3,}.*$", "", parts[-1])
            toc.setdefault(num, " ".join(p.strip() for p in parts if p.strip()))
            num, parts = None, []
    return toc, body_start


def _similar(a: str, b: str) -> float:
    import difflib

    norm = lambda s: re.sub(r"[^a-z0-9 ]", "", s.lower()).strip()
    return difflib.SequenceMatcher(None, norm(a), norm(b)).ratio()


def _repair_section_numbers(lines: List[str], toc: Dict[int, str], body_start: int) -> List[str]:
    """Restore section numbers that the Word export flattened to '0.'."""
    headings: List[tuple] = []  # (line_index, number, title)
    for i, ln in enumerate(lines[body_start:], start=body_start):
        m = _HEADING_RE.match(ln.strip())
        if m:
            headings.append((i, int(m.group(1)), m.group(2).strip()))

    if not any(num == 0 for _, num, _ in headings):
        return lines

    out = list(lines)
    pos = 0
    while pos < len(headings):
        if headings[pos][1] != 0:
            pos += 1
            continue
        run_start = pos
        while pos < len(headings) and headings[pos][1] == 0:
            pos += 1
        run = headings[run_start:pos]

        prev_num = headings[run_start - 1][1] if run_start > 0 else None
        next_num = headings[pos][1] if pos < len(headings) else None

        if prev_num is not None and next_num is not None:
            if next_num - prev_num - 1 != len(run):
                logger.warning(
                    "Ambiguous damaged-heading run between %d and %d (%d headings); left as-is",
                    prev_num, next_num, len(run),
                )
                continue
            start = prev_num + 1
        elif prev_num is not None:
            start = prev_num + 1
        elif next_num is not None:
            start = next_num - len(run)
            if start < 1:
                logger.warning("Damaged leading run would start at %d; left as-is", start)
                continue
        else:
            continue

        for offset, (line_idx, _, title) in enumerate(run):
            number = start + offset
            expected = toc.get(number)
            if expected and _similar(expected, title) < 0.8:
                logger.warning(
                    "Heading %d title %r does not match contents entry %r; left as-is",
                    number, title[:60], expected[:60],
                )
                continue
            out[line_idx] = re.sub(r"^(\s*)0\.", r"\g<1>%d." % number, out[line_idx], count=1)

    return out


def _repair_clause_letters(lines: List[str], body_start: int) -> List[str]:
    """Restore sub-clause letters the export dropped ('a)' -> ')').

    Lettering restarts at (a) under each section heading, so the counter is
    reset whenever a heading is crossed and advanced from the last surviving
    letter otherwise.
    """
    out = list(lines)
    current: Optional[str] = None
    for i, ln in enumerate(lines[body_start:], start=body_start):
        stripped = ln.strip()
        if _HEADING_RE.match(stripped):
            current = None
            continue
        m = _LETTER_RE.match(stripped)
        if not m:
            continue
        letter = m.group(1)
        if letter:
            current = letter
            continue
        nxt = "a" if current is None else chr(ord(current) + 1)
        if nxt > "z":
            logger.warning("Sub-clause lettering ran past 'z'; left as-is")
            current = None
            continue
        out[i] = re.sub(r"^(\s*)\)", r"\g<1>%s)" % nxt, out[i], count=1)
        current = nxt
    return out


def repair_word_export_damage(text: str) -> str:
    """Undo the Word-export damage described above. No-op on clean documents."""
    lines = text.split("\n")
    lines = _strip_field_codes(lines)
    toc, body_start = _split_toc(lines)
    lines = _repair_section_numbers(lines, toc, body_start)
    lines = _repair_clause_letters(lines, body_start)
    # Collapse blank runs left behind by removed field-code lines.
    return re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()


def normalize(doc: Dict[str, Any], text: str) -> Optional[Dict[str, Any]]:
    """Normalize a document record."""
    if not text or len(text) < 50:
        return None
    if doc['file'].startswith("../"):
        pdf_url = f"{BASE_URL}/pdf/{doc['file'][3:]}"
    else:
        pdf_url = f"{BASE_URL}/pdf/translation/{doc['file']}"
    return {
        "_id": f"mv-mvlaw-{doc['id'].lower()}",
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": doc["title"],
        "text": text,
        "date": None,
        "url": pdf_url,
        "language": "en",
    }


def fetch_all(sample: bool = False) -> Iterator[Dict[str, Any]]:
    """Fetch all documents with full text from PDFs."""
    docs = DOCUMENTS[:18] if sample else DOCUMENTS
    count = 0

    for doc in docs:
        if doc['file'].startswith("../"):
            pdf_url = f"{BASE_URL}/pdf/{doc['file'][3:]}"
        else:
            pdf_url = f"{BASE_URL}/pdf/translation/{doc['file']}"
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            logger.info("Downloading: %s", doc["title"])
            if not curl_download(pdf_url, tmp_path):
                logger.warning("Failed to download: %s", pdf_url)
                continue

            text = extract_pdf_text(tmp_path)
            if not text or len(text) < 50:
                logger.warning("No text extracted for: %s", doc["title"])
                continue

            record = normalize(doc, text)
            if record:
                count += 1
                logger.info("[%d] %s (%d chars)", count, doc["title"][:60], len(text))
                yield record
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
        time.sleep(2.0)

    logger.info("Total records: %d", count)


def main():
    args = sys.argv[1:]

    if not args:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|test] [--sample] [--full]")
        sys.exit(1)

    command = args[0]
    sample_mode = "--sample" in args

    if command == "test":
        try:
            pdf_url = f"{BASE_URL}/pdf/translation/Consumer.pdf"
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                tmp_path = tmp.name
            ok = curl_download(pdf_url, tmp_path)
            if ok:
                text = extract_pdf_text(tmp_path)
                logger.info("Test OK: %d chars from Consumer.pdf", len(text))
            os.unlink(tmp_path)
            sys.exit(0 if ok and text else 1)
        except Exception as e:
            logger.error("Test failed: %s", e)
            sys.exit(1)

    elif command in ("bootstrap", "bootstrap-fast"):
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)

        count = 0
        if sample_mode:
            for record in fetch_all(sample=True):
                fname = re.sub(r'[^\w\-]', '_', record["_id"])[:80] + ".json"
                out_file = sample_dir / fname
                out_file.write_text(json.dumps(record, ensure_ascii=False, indent=2))
                count += 1
                logger.info("Sample %d saved: %s", count, fname)
        else:
            # The fleet wrapper ingests data/records.jsonl; printing to stdout
            # leaves it empty and the run falls back to re-ingesting sample/.
            data_dir = Path(__file__).parent / "data"
            data_dir.mkdir(exist_ok=True)
            out_file = data_dir / "records.jsonl"
            with out_file.open("w", encoding="utf-8") as fh:
                for record in fetch_all(sample=False):
                    fh.write(json.dumps(record, ensure_ascii=False) + "\n")
                    count += 1
            logger.info("Wrote %d records to %s", count, out_file)

        logger.info("Done: %d records %s", count, "(sample)" if sample_mode else "(full)")
        if count == 0:
            sys.exit(1)
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
