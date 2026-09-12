#!/usr/bin/env python3
"""
US/LA-Legislation -- Louisiana statutory law (legis.la.gov)

Fetches the Louisiana Revised Statutes AND the separately-numbered codes
(Civil Code, Code of Civil Procedure, Code of Criminal Procedure, Code of
Evidence, Children's Code, Constitution Ancillaries) with full text from the
official Louisiana Legislature website.

Strategy:
  1. For each folder -- RS titles (77-130) and the codes (66-71, 73-74) --
     fetch Laws_Toc.aspx?folder=N to discover all section document IDs
     (Law.aspx?d=NNNNN links)
  2. For each section, fetch LawPrint.aspx?d=NNNNN for clean HTML
  3. Extract citation from <span id="LabelName"> and full text from
     <span id="LabelDocument">, strip HTML tags
  4. Normalize into standard schema, deriving the last-amended year from
     the section's own trailing source note ("Acts 1976, No. 307, ...")

Data: Public domain (Louisiana government works). No auth required.
Rate limit: 1 req / 1 sec.

Usage:
  python bootstrap.py bootstrap            # Full pull -> data/records.jsonl
  python bootstrap.py bootstrap-fast       # Alias used by the fleet wrapper
  python bootstrap.py bootstrap --sample   # Fetch ~18 sample sections
  python bootstrap.py test-api             # Connectivity test
"""

import sys
import json
import logging
import re
import time
import html as html_module
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.US.LA-Legislation")

BASE_URL = "https://legis.la.gov/legis"

# RS title folders (77-130) mapped to title numbers
RS_TITLE_FOLDERS = [
    (77, "1", "General Provisions"),
    (78, "2", "Aeronautics"),
    (79, "3", "Agriculture and Forestry"),
    (80, "4", "Amusements and Sports"),
    (81, "6", "Banks and Banking"),
    (82, "8", "Cemeteries"),
    (83, "9", "Civil Code-Ancillaries"),
    (84, "10", "Commercial Laws"),
    (85, "11", "Consolidated Public Retirement"),
    (86, "12", "Corporations and Associations"),
    (87, "13", "Courts and Judicial Procedure"),
    (88, "14", "Criminal Law"),
    (89, "15", "Criminal Procedure"),
    (90, "16", "District Attorneys"),
    (91, "17", "Education"),
    (92, "18", "Louisiana Election Code"),
    (93, "19", "Expropriation"),
    (94, "20", "Homesteads and Exemptions"),
    (95, "21", "Hotels and Lodging Houses"),
    (96, "22", "Insurance"),
    (97, "23", "Labor and Workers Compensation"),
    (98, "24", "Legislature and Laws"),
    (99, "25", "Libraries, Museums, and Other Scientific"),
    (100, "26", "Liquors-Alcoholic Beverages"),
    (101, "27", "Louisiana Gaming Control Law"),
    (102, "28", "Behavioral Health"),
    (103, "29", "Military, Naval, and Veterans Affairs"),
    (104, "30", "Minerals, Oil, Gas and Environmental Quality"),
    (105, "31", "Mineral Code"),
    (106, "32", "Motor Vehicles and Traffic Regulation"),
    (107, "33", "Municipalities and Parishes"),
    (108, "34", "Navigation and Shipping"),
    (109, "35", "Notaries Public and Commissioners"),
    (110, "36", "Organization of the Executive Branch"),
    (111, "37", "Professions and Occupations"),
    (112, "38", "Public Contracts, Works and Improvements"),
    (113, "39", "Public Finance"),
    (114, "40", "Public Health and Safety"),
    (115, "41", "Public Lands"),
    (116, "42", "Public Officers and Employees"),
    (117, "43", "Public Printing and Advertisements"),
    (118, "44", "Public Records and Recorders"),
    (119, "45", "Public Utilities and Carriers"),
    (120, "46", "Public Welfare and Assistance"),
    (121, "47", "Revenue and Taxation"),
    (122, "48", "Roads, Bridges and Ferries"),
    (123, "49", "State Administration"),
    (124, "50", "Surveys and Surveyors"),
    (125, "51", "Trade and Commerce"),
    (126, "52", "United States"),
    (127, "53", "War Emergency"),
    (128, "54", "Warehouses"),
    (129, "55", "Weights and Measures"),
    (130, "56", "Wildlife and Fisheries"),
]

# The codified law that sits outside the Revised Statutes. legis.la.gov serves it
# from the same Laws_Toc.aspx folder space, but with a code prefix instead of an RS
# title number ("CC 1", "CCP 1", ...) rather than the "RS 9:1851" shape.
# These were named in config.yaml from the start but never crawled -- Louisiana is
# the one civil-law state in the US, so omitting the Civil Code omitted the core of
# its private law (issue #1200).
CODE_FOLDERS = [
    (66, "CA", "Constitution Ancillaries"),
    (67, "CC", "Civil Code"),
    (68, "CCP", "Code of Civil Procedure"),
    (69, "CCRP", "Code of Criminal Procedure"),
    (70, "CE", "Code of Evidence"),
    (71, "CHC", "Children's Code"),
    (73, "HRULE", "House Rules"),
    (74, "JRULE", "Joint Rules of the Senate and House"),
]

# Sample folders for --sample mode. Codes first so the sample evidences every
# citation shape (CC/CCP/CCRP/CHC), then two representative RS titles.
SAMPLE_FOLDERS = [67, 68, 69, 71, 88, 121]

# Recognised citation prefixes, longest first so "CCP"/"CCRP" are not shadowed by "CC".
CITATION_CODES = ["CCRP", "CCP", "CHC", "HRULE", "JRULE", "RS", "CC", "CE", "CA"]

# Trailing source-note block, e.g. "Acts 1962, No. 484, §10." or
# "Added by Acts 1976, No. 307, §1, eff. Jan. 3, 1977."
ACTS_YEAR_RE = re.compile(r'\bActs?\s+(\d{4})\b')


class LALegislationScraper(BaseScraper):

    def __init__(self, source_dir: str = None):
        if source_dir is None:
            source_dir = str(Path(__file__).parent)
        super().__init__(source_dir)
        self.http = HttpClient(
            base_url="",
            headers={
                "User-Agent": "LegalDataHunter/1.0 (academic research; open data collection)",
                "Accept": "text/html,*/*",
            },
            timeout=60,
        )
        self.delay = 1.0

    def _get(self, url: str) -> str:
        """Fetch URL with rate limiting."""
        time.sleep(self.delay)
        resp = self.http.get(url)
        return resp.text

    def _strip_html(self, html_text: str) -> str:
        """Strip HTML tags and clean up text."""
        # Remove HTML tags
        text = re.sub(r'<br\s*/?>', '\n', html_text, flags=re.IGNORECASE)
        text = re.sub(r'<p[^>]*>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
        text = re.sub(r'<[^>]+>', '', text)
        # Decode HTML entities
        text = html_module.unescape(text)
        # legis.la.gov indents with runs of &nbsp; and emits CRLF inside the span,
        # which unescape leaves as U+00A0 / "\r\n" and produced "\n\r\n\n" runs.
        text = text.replace("\xa0", " ")
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        # Clean whitespace
        text = re.sub(r'[ \t]+', ' ', text)
        text = re.sub(r'\n[ \t]+', '\n', text)
        text = re.sub(r'\n{3,}', '\n\n', text)
        return text.strip()

    def _get_section_ids(self, folder_id: int) -> list:
        """Get all section document IDs from a TOC folder page.
        Returns list of (doc_id, section_text)."""
        url = f"{BASE_URL}/Laws_Toc.aspx?folder={folder_id}"
        try:
            html = self._get(url)
        except Exception as e:
            logger.warning(f"Failed to fetch TOC folder {folder_id}: {e}")
            return []

        # Extract Law.aspx?d=NNNNN links and their text
        matches = re.findall(
            r'Law\.aspx\?d=(\d+)[^>]*>([^<]+)',
            html,
        )
        seen = set()
        result = []
        for doc_id, link_text in matches:
            if doc_id not in seen:
                seen.add(doc_id)
                result.append((doc_id, link_text.strip()))
        return result

    def _fetch_section(self, doc_id: str) -> Optional[dict]:
        """Fetch full text for a single section via LawPrint.aspx."""
        url = f"{BASE_URL}/LawPrint.aspx?d={doc_id}"
        # HttpClient already retries at the transport level, but over ~52,000
        # sections a single exhausted retry cycle silently dropped the section for
        # good (#1200: 2,344 of 46,248 never reached the writer). Give the section
        # its own second chance before abandoning it.
        html = None
        for attempt in range(3):
            try:
                html = self._get(url)
                break
            except Exception as e:
                if attempt == 2:
                    logger.warning(
                        f"Failed to fetch LawPrint d={doc_id} after 3 attempts: {e}"
                    )
                    return None
                logger.debug(f"Retrying LawPrint d={doc_id} (attempt {attempt + 2}): {e}")
                time.sleep(2 ** attempt)

        # Extract citation from <span id="LabelName">
        m_name = re.search(
            r'<span\s+id="LabelName"[^>]*>([^<]+)</span>',
            html, re.IGNORECASE,
        )
        citation = m_name.group(1).strip() if m_name else ""

        # Extract full text from <span id="LabelDocument">
        m_doc = re.search(
            r'<span\s+id="LabelDocument">(.*?)</span>\s*</div>',
            html, re.IGNORECASE | re.DOTALL,
        )
        if not m_doc:
            # Try broader match
            m_doc = re.search(
                r'<span\s+id="LabelDocument">(.*?)(?:</span>)',
                html, re.IGNORECASE | re.DOTALL,
            )

        if not m_doc:
            logger.warning(f"No LabelDocument found for d={doc_id}")
            return None

        raw_html = m_doc.group(1)
        text = self._strip_html(raw_html)

        if not text or len(text) < 10:
            logger.warning(f"Text too short for d={doc_id}: {len(text) if text else 0}")
            return None

        return {
            "doc_id": doc_id,
            "citation": citation,
            "text": text,
            "url": f"{BASE_URL}/Law.aspx?d={doc_id}",
        }

    def _parse_citation(self, citation: str) -> dict:
        """Parse a citation like 'RS 1:1', 'CC 100' or 'CA 6 19.3' into components."""
        parts = {"code": "", "section_num": "", "title_num": ""}
        # Longest prefix first, so "CCP 1"/"CCRP 1" are not truncated to "CC".
        alternation = "|".join(CITATION_CODES)
        # The article part may itself contain spaces ("CA 6 19.3" = Const. art. VI
        # ancillary 19.3), so take everything after the prefix rather than one token.
        m = re.match(rf'({alternation})\s+(.+)$', citation.strip())
        if m:
            parts["code"] = m.group(1)
            parts["section_num"] = m.group(2).strip()
            # For RS, extract title number (before the colon)
            if ":" in parts["section_num"]:
                parts["title_num"] = parts["section_num"].split(":")[0]
        return parts

    def _parse_date(self, text: str) -> Optional[str]:
        """Derive the last-amended date from the trailing source-note block.

        Louisiana sections close with their legislative history, e.g.
        "Acts 1962, No. 484, §10." or "Amended by Acts 1976, No. 307, §1".
        The most recent year in that block is the consolidation's effective year.
        Only the tail is searched, so an "Acts 1898" mentioned in the body of a
        section does not masquerade as its amendment date.
        """
        tail = text[-800:] if len(text) > 800 else text
        years = [int(y) for y in ACTS_YEAR_RE.findall(tail)]
        # Guard against OCR-ish noise and forward-dated typos.
        years = [y for y in years if 1804 <= y <= datetime.now(timezone.utc).year]
        if not years:
            return None
        return f"{max(years)}-01-01"

    def test_api(self):
        """Test connectivity to legis.la.gov."""
        logger.info("Testing Louisiana Legislature website...")
        try:
            # Test TOC page
            sections = self._get_section_ids(77)
            if not sections:
                logger.error("API test FAILED: no sections found in folder 77")
                return False
            logger.info(f"  TOC folder 77: OK ({len(sections)} sections)")

            # Test LawPrint page — skip title-level entries (e.g., "RS 1")
            doc_id = None
            for sid, lt in sections:
                if ":" in lt:  # actual section like "RS 1:1"
                    doc_id = sid
                    break
            if not doc_id:
                doc_id = sections[1][0] if len(sections) > 1 else sections[0][0]
            result = self._fetch_section(doc_id)
            if result and len(result["text"]) > 50:
                logger.info(f"  LawPrint d={doc_id}: OK ({len(result['text'])} chars)")
                logger.info(f"  Citation: {result['citation']}")
                logger.info("API test PASSED")
                return True
            else:
                logger.error("API test FAILED: could not extract text")
                return False
        except Exception as e:
            logger.error(f"API test FAILED: {e}")
            return False

    def normalize(self, raw: dict, title_num: str = "", title_name: str = "") -> dict:
        """Transform raw section data into standard schema."""
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        citation = raw["citation"]
        parsed = self._parse_citation(citation)

        # Build a clean ID
        safe_citation = re.sub(r'[^A-Za-z0-9:.-]', '_', citation)
        doc_id = f"LA-{safe_citation}" if safe_citation else f"LA-{raw['doc_id']}"

        # Use citation as title, or fallback
        title = citation if citation else f"LA Doc {raw['doc_id']}"

        return {
            "_id": doc_id,
            "_source": "US/LA-Legislation",
            "_type": "legislation",
            "_fetched_at": now,
            "title": title,
            "text": raw["text"],
            # Enactment/last-amendment year from the section's own source note.
            # Falls back to the crawl date, which is the "as of" date of the
            # consolidation, so the temporal key is never null.
            "date": self._parse_date(raw["text"]) or today,
            "url": raw["url"],
            "rs_title": title_num or parsed.get("title_num", ""),
            "rs_title_name": title_name,
            "section_num": parsed.get("section_num", citation),
            "code": parsed.get("code", "RS"),
        }

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield every section of the Revised Statutes and the separate codes."""
        total = 0
        empty_folders = []
        # Skips are counted rather than merely logged: #1200 reported "46,248
        # fetched / 43,904 written" and the old counter only ever incremented on a
        # successful yield, so there was no way to tell a fetch failure from a
        # duplicate from a loss further down the pipeline.
        skipped = {"title_level": 0, "fetch_failed": 0, "duplicate_id": 0}
        seen_ids = set()

        folders = (
            [(f, t, n, True) for f, t, n in RS_TITLE_FOLDERS]
            + [(f, c, n, False) for f, c, n in CODE_FOLDERS]
        )
        for folder_id, code_or_title, group_name, is_rs in folders:
            sections = self._get_section_ids(folder_id)
            logger.info(
                f"  {'Title' if is_rs else 'Code'} {code_or_title} "
                f"({group_name}): {len(sections)} sections"
            )
            if not sections:
                empty_folders.append(f"{code_or_title} ({group_name}, folder {folder_id})")
                continue
            for doc_id, link_text in sections:
                # Skip title-level entries (e.g., "RS 1" with no section)
                if re.match(r'^(RS|TITLE)\s+\d+[A-Z]?\s*$', link_text.strip()):
                    skipped["title_level"] += 1
                    continue
                raw = self._fetch_section(doc_id)
                if not raw:
                    skipped["fetch_failed"] += 1
                    continue
                # rs_title only means anything for the Revised Statutes; the codes
                # carry their prefix in the citation instead.
                record = self.normalize(
                    raw,
                    code_or_title if is_rs else "",
                    group_name,
                )
                if record["_id"] in seen_ids:
                    # Two sections normalising to one key would be silently
                    # overwritten by an _id-keyed upsert, so say so.
                    skipped["duplicate_id"] += 1
                    logger.warning(
                        f"Duplicate _id {record['_id']} (d={doc_id}) — not re-emitted"
                    )
                    continue
                seen_ids.add(record["_id"])
                yield record
                total += 1
                if total % 100 == 0:
                    logger.info(f"  Progress: {total} sections fetched")

        logger.info(
            f"Total sections fetched: {total} "
            f"(skipped: {skipped['title_level']} title-level, "
            f"{skipped['fetch_failed']} fetch/extract failures, "
            f"{skipped['duplicate_id']} duplicate ids)"
        )
        if empty_folders:
            # A folder that stops answering is how a whole code silently vanishes
            # from the corpus; make that loud rather than a smaller record count.
            logger.warning(
                "Folders returned no sections: " + ", ".join(empty_folders)
            )
        if total == 0:
            raise RuntimeError(
                "US/LA-Legislation fetched 0 sections — legis.la.gov returned no "
                "Law.aspx links for any of the "
                f"{len(RS_TITLE_FOLDERS) + len(CODE_FOLDERS)} folders. Expected "
                "~52,000. Treat as an upstream/layout break or a block, not an "
                "empty corpus."
            )

    def fetch_updates(self, since: str) -> Generator[dict, None, None]:
        """Fetch all sections (no incremental update supported)."""
        yield from self.fetch_all()

    def fetch_sample(self) -> Generator[dict, None, None]:
        """Fetch a small sample of sections from selected titles."""
        logger.info("Fetching sample sections from selected titles and codes...")
        # Build lookup. RS folders keep their title number; code folders have none.
        folder_lookup = {f: (t, n, f"Title {t}") for f, t, n in RS_TITLE_FOLDERS}
        folder_lookup.update({f: ("", n, c) for f, c, n in CODE_FOLDERS})

        count = 0
        target = 18
        for folder_id in SAMPLE_FOLDERS:
            if count >= target:
                break
            info = folder_lookup.get(folder_id)
            if not info:
                continue
            title_num, title_name, label = info
            sections = self._get_section_ids(folder_id)
            logger.info(f"  {label} ({title_name}): {len(sections)} sections")

            # Pick first 3 actual sections (skip title-level entries)
            picked = 0
            for doc_id, link_text in sections:
                if picked >= 3 or count >= target:
                    break
                # Skip title-level or repealed entries
                lt = link_text.strip()
                if re.match(r'^(RS|TITLE)\s+\d+[A-Z]?\s*$', lt):
                    continue
                if "repealed" in lt.lower():
                    continue
                raw = self._fetch_section(doc_id)
                if raw:
                    yield self.normalize(raw, title_num, title_name)
                    count += 1
                    picked += 1
        logger.info(f"Sample complete: {count} sections fetched")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="US/LA-Legislation bootstrap")
    parser.add_argument(
        "command",
        # bootstrap-fast is what the fleet wrapper invokes; without it argparse
        # exits 2 and the wrapper falls back to re-ingesting sample/.
        choices=["bootstrap", "bootstrap-fast", "test-api"],
        help="Command to run",
    )
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    parser.add_argument("--full", action="store_true", help="Fetch all records")
    args = parser.parse_args()

    scraper = LALegislationScraper()

    if args.command == "test-api":
        success = scraper.test_api()
        sys.exit(0 if success else 1)

    if args.sample:
        # Samples stay one-file-per-record so they can be committed and reviewed.
        sample_dir = Path(__file__).parent / "sample"
        sample_dir.mkdir(exist_ok=True)
        count = 0
        for record in scraper.fetch_sample():
            out_path = sample_dir / f"{record['_id']}.json"
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(record, f, ensure_ascii=False, indent=2)
            count += 1
            logger.info(f"Saved: {record['_id']} ({len(record['text'])} chars)")
        logger.info(f"Bootstrap complete: {count} records saved to {sample_dir}")
        return

    # Full corpus streams to data/records.jsonl. It used to write one JSON file
    # per record into sample/, which both buried the committed samples under
    # ~52,000 files and left the pipeline nothing to ingest.
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)
    out_path = data_dir / "records.jsonl"
    count = 0
    with open(out_path, "w", encoding="utf-8") as f:
        for record in scraper.fetch_all():
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            count += 1
    logger.info(f"Bootstrap complete: {count} records written to {out_path}")
    if count == 0:
        logger.error("No records saved!")
        sys.exit(1)


if __name__ == "__main__":
    main()
