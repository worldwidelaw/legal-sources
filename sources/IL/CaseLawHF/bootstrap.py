#!/usr/bin/env python3
"""
IL/CaseLawHF -- Israeli case law from HuggingFace dataset guychuk/case-law-israel

10,558 Israeli court judgments with full Hebrew text.
Streams from HuggingFace Parquet files — no large downloads needed.

Usage:
  python bootstrap.py bootstrap            # Full initial pull
  python bootstrap.py bootstrap --sample   # Fetch 15 sample records
  python bootstrap.py bootstrap-fast       # Alias for bootstrap
  python bootstrap.py test                 # Connectivity + ClassLabel schema check
  python bootstrap.py update               # Incremental fetch since last run
"""

import sys
import json
import logging
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Dict, Any, Optional, Tuple
from urllib.parse import quote

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper, as_date_str

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IL.CaseLawHF")

DATASET_ID = "guychuk/case-law-israel"
SPLIT = "judgments"

# The dataset declares court_type_code / district_code / publication_subject_code as
# ClassLabel columns, so the authoritative code→name lists are published in the
# dataset's own feature schema. The lists below are copied verbatim from it; the
# `test` command re-reads the live schema and fails loud if it has drifted.
#
# They are NOT interchangeable with the maps this scraper carried before #1533,
# which were invented rather than read from the dataset and were wrong from code 5
# onward — code 7 ("Military Court") is really SMALL_CLAIMS_COURT, code 8
# ("Military Appeals Court") is JUVENILE_COURT, code 10 ("National Labor Court")
# is PAROLE_BOARD, and every district was off by one or reordered (district 4 was
# emitted as "Central" for judgments whose own titles say בתל אביב).
COURT_TYPE_LABELS = [
    "UNKNOWN",
    "SUPREME_COURT",
    "DISTRICT_COURT",
    "MAGISTRATE_COURT",
    "FAMILY_COURT",
    "LOCAL_AFFAIRS_COURT",
    "TRAFFIC_COURT",
    "SMALL_CLAIMS_COURT",
    "JUVENILE_COURT",
    "LABOR_COURT",
    "PAROLE_BOARD",
]

DISTRICT_LABELS = [
    "UNKNOWN",
    "Supreme",
    "Northern",
    "Haifa",
    "TelAviv",
    "Central",
    "Jerusalem",
    "Southern",
]

SUBJECT_LABELS = [
    "UNKNOWN",
    "Justice",
    "Criminal",
    "Civil",
    "Family",
    "Financial",
    "Administrative",
    "Labor",
    "Traffic",
    "Parole",
    "Miscellaneous",
    "Juvenile",
]

# court_tier follows the repo-wide convention (see FR/Judilibre): 1 = apex,
# 2 = appellate, 3 = first instance. A tier is emitted only where the court type
# fixes it. Labour courts are split between regional (first instance) and national
# (appellate) benches that share a single code, and the parole board is an
# administrative tribunal outside the court hierarchy — both get a null tier here
# rather than a guess, and the labour split is resolved from the title below.
COURT_TYPES = {
    "SUPREME_COURT": ("Supreme Court of Israel", 1),
    "DISTRICT_COURT": ("District Court", 2),
    "MAGISTRATE_COURT": ("Magistrate Court", 3),
    "FAMILY_COURT": ("Family Court", 3),
    "LOCAL_AFFAIRS_COURT": ("Court for Local Affairs", 3),
    "TRAFFIC_COURT": ("Traffic Court", 3),
    "SMALL_CLAIMS_COURT": ("Small Claims Court", 3),
    "JUVENILE_COURT": ("Juvenile Court", 3),
    "LABOR_COURT": ("Labour Court", None),
    "PAROLE_BOARD": ("Parole Board", None),
}

DISTRICT_NAMES = {
    "Supreme": "National",
    "Northern": "Northern",
    "Haifa": "Haifa",
    "TelAviv": "Tel Aviv",
    "Central": "Central",
    "Jerusalem": "Jerusalem",
    "Southern": "Southern",
}

# Israeli labour judgments name their own bench: בית הדין הארצי לעבודה is the
# National Labour Court (hears appeals), בית הדין האזורי לעבודה is a regional
# bench (first instance). The district code does not separate them — national
# rulings appear under both Jerusalem and UNKNOWN — so the title is the only
# in-data signal.
LABOR_NATIONAL_RE = re.compile(r"הארצי")
LABOR_REGIONAL_RE = re.compile(r"אזורי")
# בימ"ש מחוזי לנוער is the juvenile bench of a District Court, sitting on appeal.
JUVENILE_DISTRICT_RE = re.compile(r"מחוזי")

# The dataset's url_name (e.g. "decision78429-01-23") is an internal slug. Before
# #1533 it was expanded into https://www.nevo.co.il/psika_word/{url_name} and
# emitted as `url`, i.e. presented to API consumers as the publisher's own link.
# It is not: Nevo is a commercial paid database, it never published that path
# (it answers 404 to a browser and 403 to everything else), and the slug is not a
# Nevo identifier. Records now carry a deep link into the dataset they actually
# came from, tagged with its provenance, and an explicitly null publisher URL.
DATASET_VIEWER = f"https://huggingface.co/datasets/{DATASET_ID}/viewer/default/{SPLIT}"


class ILCaseLawHFScraper(BaseScraper):
    """Scraper for IL/CaseLawHF — Israeli case law from HuggingFace."""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

    def _clean_text(self, text: str) -> str:
        """Clean judgment text: remove excessive whitespace."""
        if not text:
            return ""
        text = re.sub(r'\n{3,}', '\n\n', text)
        text = re.sub(r' {2,}', ' ', text)
        return text.strip()

    def _parse_judges(self, judges_str: str) -> list:
        """Parse judges from JSON string field."""
        if not judges_str:
            return []
        try:
            return json.loads(judges_str)
        except (json.JSONDecodeError, TypeError):
            return [judges_str] if judges_str else []

    @staticmethod
    def _label(labels: list, code: Any) -> Optional[str]:
        """Resolve a ClassLabel index to its name, or None if it is not one.

        UNKNOWN (index 0) and out-of-range/missing codes both collapse to None:
        an unrecognised court must stay an explicit unknown rather than becoming
        the string "Unknown (None)" or silently inheriting a neighbour's label.
        """
        if not isinstance(code, int) or isinstance(code, bool):
            return None
        if not 0 <= code < len(labels):
            return None
        name = labels[code]
        return None if name == "UNKNOWN" else name

    def _court(
        self, court_code: Any, district_code: Any, title: str
    ) -> Tuple[Optional[str], Optional[int], Optional[str]]:
        """Derive (court name, tier, court_id) from the dataset's court fields."""
        court_key = self._label(COURT_TYPE_LABELS, court_code)
        if court_key is None:
            return None, None, None

        court, tier = COURT_TYPES[court_key]
        title = title or ""

        if court_key == "LABOR_COURT":
            if LABOR_NATIONAL_RE.search(title):
                court, tier = "National Labour Court", 2
            elif LABOR_REGIONAL_RE.search(title):
                court, tier = "Regional Labour Court", 3
        elif court_key == "JUVENILE_COURT" and JUVENILE_DISTRICT_RE.search(title):
            court, tier = "District Juvenile Court", 2

        district_key = self._label(DISTRICT_LABELS, district_code)
        # The Supreme Court is national; naming a district for it would be noise.
        if district_key and district_key != "Supreme" and tier != 1:
            court = f"{court} ({DISTRICT_NAMES[district_key]})"

        parts = ["il", court_key.lower().replace("_", "-")]
        if district_key:
            parts.append(district_key.lower())
        return court, tier, "-".join(parts)

    def normalize(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a HuggingFace record into standard schema."""
        judgment_id = raw.get("judgment_id", "")
        title = raw.get("title") or ""
        case_number = raw.get("name_number", "")
        doc_date = raw.get("doc_create_date", "")
        court_code = raw.get("court_type_code")
        district_code = raw.get("district_code")
        subject_code = raw.get("publication_subject_code")
        text = self._clean_text(raw.get("document_text", ""))
        url_name = raw.get("url_name") or None
        judges = self._parse_judges(raw.get("judges_str", ""))

        if not text:
            return None

        court, court_tier, court_id = self._court(court_code, district_code, title)
        court_type = self._label(COURT_TYPE_LABELS, court_code)
        district_key = self._label(DISTRICT_LABELS, district_code)
        subject_key = self._label(SUBJECT_LABELS, subject_code)

        if not title:
            title = " - ".join(p for p in (court, case_number) if p) or judgment_id

        return {
            "_id": judgment_id,
            "_source": "IL/CaseLawHF",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": doc_date if doc_date else None,
            # Deep link into the dataset this record was ingested from. It is not
            # a publisher URL and is labelled as such — see DATASET_VIEWER (#1533).
            "url": f"{DATASET_VIEWER}?q={quote(judgment_id)}" if judgment_id else DATASET_VIEWER,
            "url_provenance": "dataset_viewer",
            # This corpus carries no verified per-judgment link on any official
            # Israeli court site, so the publisher URL fails closed rather than
            # being constructed.
            "publisher_url": None,
            "publisher_ref": url_name,
            "case_number": case_number,
            # The corpus spans the whole Israeli hierarchy, so court and tier are
            # per record. A static tier tagged every district, family and juvenile
            # judgment as an apex-court ruling (#1533).
            "court": court,
            "court_tier": court_tier,
            "court_id": court_id,
            "court_type": court_type,
            "court_type_code": court_code,
            "district": DISTRICT_NAMES.get(district_key) if district_key else None,
            "district_code": district_code,
            "subject": subject_key,
            "judges": judges,
            "language": "he",
        }

    def fetch_all(self) -> Generator[Dict[str, Any], None, None]:
        """Stream all RAW records from HuggingFace dataset.

        Per the BaseScraper contract, fetch_all yields RAW documents and the
        framework calls normalize(). Yielding already-normalized records here
        caused the VPS bootstrap to double-normalize (normalize reads raw keys
        like 'document_text' which are absent from a normalized dict) → empty
        text for every record. See issue #1202.
        """
        from datasets import load_dataset

        logger.info("Loading dataset %s (split=%s) via streaming...", DATASET_ID, SPLIT)
        ds = load_dataset(DATASET_ID, split=SPLIT, streaming=True)

        count = 0
        for item in ds:
            yield dict(item)
            count += 1
            if count % 500 == 0:
                logger.info("Yielded %d raw records so far...", count)

        logger.info("Finished: yielded %d raw records total", count)

    def fetch_updates(self, since: str) -> Generator[Dict[str, Any], None, None]:
        """Fetch records newer than `since` date."""
        # `update()` passes a datetime, but the comparison below is against a
        # record's ISO date string, which raises TypeError (#1512).
        since = as_date_str(since)
        for raw in self.fetch_all():
            # fetch_all yields RAW dataset rows, which carry doc_create_date —
            # filtering on the normalized key "date" matched nothing at all.
            date = raw.get("doc_create_date")
            if date and date >= since:
                yield raw

    def check_schema(self) -> bool:
        """Verify the dataset's ClassLabel names still match the maps above.

        The pre-#1533 court and district maps were hand-written and wrong. Reading
        the live schema here means a future rename or reordering upstream fails
        loud instead of silently relabelling every judgment.
        """
        import requests

        url = "https://datasets-server.huggingface.co/info"
        resp = requests.get(url, params={"dataset": DATASET_ID}, timeout=60)
        resp.raise_for_status()
        features = resp.json()["dataset_info"]["default"]["features"]

        expected = {
            "court_type_code": ("court_type_label", COURT_TYPE_LABELS),
            "district_code": ("district_label", DISTRICT_LABELS),
            "publication_subject_code": ("publication_subject_label", SUBJECT_LABELS),
        }
        ok = True
        for code_col, (label_col, names) in expected.items():
            live = features.get(label_col, {}).get("names")
            if live is None:
                logger.error("%s: ClassLabel names missing from dataset schema", label_col)
                ok = False
            elif live != names:
                logger.error("%s drifted — dataset now says %s, %s maps %s",
                             label_col, live, code_col, names)
                ok = False
            else:
                logger.info("%s: %d labels match", label_col, len(names))
        return ok

    def test(self) -> bool:
        """Quick connectivity test."""
        try:
            if not self.check_schema():
                return False
            from datasets import load_dataset
            ds = load_dataset(DATASET_ID, split=SPLIT, streaming=True)
            item = next(iter(ds))
            text = item.get("document_text", "")
            logger.info("Test OK: got record '%s' with %d chars text",
                        (item.get("title") or "")[:60], len(text))
            return bool(text)
        except Exception as e:
            logger.error("Test failed: %s", e)
            return False


def main():
    scraper = ILCaseLawHFScraper()
    args = sys.argv[1:]

    if not args:
        print("Usage: python bootstrap.py [bootstrap|bootstrap-fast|test] [--sample] [--full]")
        sys.exit(1)

    command = args[0]
    sample_mode = "--sample" in args

    if command == "test":
        ok = scraper.test()
        sys.exit(0 if ok else 1)

    elif command in ("bootstrap", "bootstrap-fast"):
        # Route through BaseScraper so a full run streams to data/records.jsonl.
        # Printing normalized records to stdout left the fleet wrapper nothing to
        # ingest but the committed sample/ files.
        if sample_mode:
            stats = scraper.bootstrap(sample_mode=True, sample_size=15)
        else:
            stats = scraper.bootstrap_fast()
        logger.info("Done: %s", json.dumps(stats, ensure_ascii=False, default=str))
        sys.exit(0 if stats.get("records_fetched") else 1)

    elif command == "update":
        stats = scraper.update()
        logger.info("Done: %s", json.dumps(stats, ensure_ascii=False, default=str))
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
