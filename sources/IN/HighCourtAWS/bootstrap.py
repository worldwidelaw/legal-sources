#!/usr/bin/env python3
"""
IN/HighCourtAWS -- Indian High Court Judgments (AWS Open Data)

Primary path uses the bucket's pre-extracted text layer:
  derived/landlit-v2/texts/*.jsonl.gz

Each JSONL record contains full judgment text, avoiding the old hot path that
fetched PDFs from data/pdf/ and ran pdfplumber. PDF extraction remains available
only as an explicit fallback for metadata judgments missing from landlit-v2.
"""

from __future__ import annotations

import gzip
import json
import logging
import re
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any, Generator, Iterable, Optional
from urllib.parse import quote

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.pdf_extract import extract_pdf_markdown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.IN.HighCourtAWS")

S3_BASE = "https://s3.ap-south-1.amazonaws.com/indian-high-court-judgments"
METADATA_PREFIX = "metadata/json/"
PDF_PREFIX = "data/pdf/"
TEXT_PREFIX = "derived/landlit-v2/texts/"
SOURCE_ID = "IN/HighCourtAWS"

_TEXT_KEY_RE = re.compile(
    r"data__tar__year=(?P<year>\d{4})__court=(?P<court>[^_]+_[^_]+)__bench=(?P<bench>[^_]+)__data\.tar\.jsonl\.gz$"
)
_SOURCE_RE = re.compile(
    r"year=(?P<year>\d{4})/court=(?P<court>[^/]+)/bench=(?P<bench>[^/]+)/data\.tar:\./(?P<filename>[^/]+\.pdf)$"
)
_FILENAME_RE = re.compile(r"^(?P<cnr>[A-Z0-9]+)_(?P<seq>\d+)_(?P<date>\d{4}-\d{2}-\d{2})\.(?:json|pdf)$")
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class IndianHighCourtAWSScraper(BaseScraper):
    """Scraper for Indian High Court judgments from AWS Open Data."""

    def __init__(
        self,
        year_range: tuple[int, int] | None = None,
        court: str | None = None,
        include_pdf_fallback: bool = False,
        max_text_shards: int | None = None,
    ):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "LegalDataHunter/1.0"})
        self.year_range = year_range
        self.court = court
        self.include_pdf_fallback = include_pdf_fallback
        self.max_text_shards = max_text_shards
        self._metadata_cache: dict[str, dict[str, Any]] = {}
        self._seen_cnr: set[str] = set()
        self.pdf_fallback_attempts = 0

    def _s3_get(self, url: str, timeout: int = 120, retries: int = 3, stream: bool = False) -> requests.Response:
        """GET from S3 with retry logic for intermittent connectivity."""
        for attempt in range(retries):
            try:
                r = self.session.get(url, timeout=timeout, stream=stream)
                r.raise_for_status()
                return r
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                if attempt < retries - 1:
                    wait = 5 * (attempt + 1)
                    logger.warning(
                        "S3 request failed (attempt %s/%s), retrying in %ss: %s",
                        attempt + 1,
                        retries,
                        wait,
                        e,
                    )
                    time.sleep(wait)
                else:
                    raise

    def _list_s3_objects(self, prefix: str, max_keys: int = 1000, delimiter: str = "") -> list:
        """List objects or common prefixes in the public S3 bucket."""
        results = []
        continuation_token = None

        while True:
            url = f"{S3_BASE}/?list-type=2&prefix={quote(prefix, safe='/')}&max-keys={max_keys}"
            if delimiter:
                url += f"&delimiter={delimiter}"
            if continuation_token:
                url += f"&continuation-token={quote(continuation_token, safe='')}"

            r = self._s3_get(url)
            root = ET.fromstring(r.text)
            ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

            if delimiter:
                for cp in root.findall("s3:CommonPrefixes/s3:Prefix", ns):
                    results.append(cp.text)
            else:
                for content in root.findall("s3:Contents", ns):
                    key = content.find("s3:Key", ns).text
                    size = int(content.find("s3:Size", ns).text)
                    results.append({"key": key, "size": size})

            is_truncated = root.find("s3:IsTruncated", ns)
            if is_truncated is not None and is_truncated.text == "true":
                token_el = root.find("s3:NextContinuationToken", ns)
                if token_el is not None:
                    continuation_token = token_el.text
                    continue
            break

        return results

    def _list_years(self) -> list[int]:
        prefixes = self._list_s3_objects(METADATA_PREFIX, delimiter="/")
        years = []
        for p in prefixes:
            m = re.search(r"year=(\d{4})/", p)
            if m:
                years.append(int(m.group(1)))
        return sorted(years)

    def _list_courts(self, year: int) -> list[str]:
        return self._list_s3_objects(f"{METADATA_PREFIX}year={year}/", delimiter="/")

    def _list_benches(self, court_path: str) -> list[str]:
        return self._list_s3_objects(court_path, delimiter="/")

    def _years_to_scan(self) -> list[int] | None:
        if not self.year_range:
            return None
        start, end = self.year_range
        return list(range(start, end + 1))

    def _text_prefixes(self) -> Iterable[str]:
        years = self._years_to_scan()
        if years is None:
            yield TEXT_PREFIX
            return
        for year in years:
            if self.court:
                yield f"{TEXT_PREFIX}data__tar__year={year}__court={self.court}"
            else:
                yield f"{TEXT_PREFIX}data__tar__year={year}__"

    def _iter_text_keys(self) -> Generator[dict[str, Any], None, None]:
        emitted = 0
        seen = set()
        for prefix in self._text_prefixes():
            for obj in self._list_s3_objects(prefix):
                key = obj["key"]
                if key in seen or not key.endswith(".jsonl.gz"):
                    continue
                seen.add(key)
                info = self._parse_text_key(key)
                if not info:
                    logger.warning("Skipping unexpected text-layer key: %s", key)
                    continue
                if self.year_range:
                    start, end = self.year_range
                    if not (start <= info["year"] <= end):
                        continue
                if self.court and info["court"] != self.court:
                    continue
                yield {**info, "key": key, "size": obj.get("size")}
                emitted += 1
                if self.max_text_shards and emitted >= self.max_text_shards:
                    return

    def _parse_text_key(self, key: str) -> dict[str, Any] | None:
        m = _TEXT_KEY_RE.search(key)
        if not m:
            return None
        return {
            "year": int(m.group("year")),
            "court": m.group("court"),
            "bench": m.group("bench"),
        }

    def _parse_text_source(self, source: str, shard: dict[str, Any]) -> dict[str, str]:
        m = _SOURCE_RE.search(source or "")
        if m:
            filename = m.group("filename")
            year = m.group("year")
            court = m.group("court")
            bench = m.group("bench")
        else:
            filename = Path(source or "").name
            year = str(shard["year"])
            court = shard["court"]
            bench = shard["bench"]

        name_match = _FILENAME_RE.match(filename)
        stem = filename[:-4] if filename.endswith(".pdf") else filename
        cnr = name_match.group("cnr") if name_match else stem.split("_")[0]
        date = name_match.group("date") if name_match else self._date_from_filename(filename)
        json_key = f"{METADATA_PREFIX}year={year}/court={court}/bench={bench}/{stem}.json"
        pdf_key = f"{PDF_PREFIX}year={year}/court={court}/bench={bench}/{stem}.pdf"
        return {
            "filename": filename,
            "cnr_number": cnr,
            "date": date,
            "json_key": json_key,
            "pdf_key": pdf_key,
            "year": year,
            "court": court,
            "bench": bench,
        }

    def _date_from_filename(self, filename: str) -> str:
        m = re.search(r"(\d{4}-\d{2}-\d{2})(?:\.(?:json|pdf))?$", filename or "")
        return m.group(1) if m else ""

    def _fetch_metadata(self, json_key: str) -> dict[str, Any]:
        if json_key in self._metadata_cache:
            return self._metadata_cache[json_key]
        try:
            r = self._s3_get(f"{S3_BASE}/{json_key}", timeout=60)
            metadata = r.json()
        except Exception as e:
            logger.warning("Metadata fetch failed for %s: %s", json_key, e)
            metadata = {}
        self._metadata_cache[json_key] = metadata
        # Keep cache bounded for long shards.
        if len(self._metadata_cache) > 5000:
            self._metadata_cache.clear()
        return metadata

    def _iter_text_records(self, shard: dict[str, Any]) -> Generator[dict[str, Any], None, None]:
        key = shard["key"]
        url = f"{S3_BASE}/{key}"
        logger.info("Streaming text shard %s", key)
        with self._s3_get(url, timeout=300, stream=True) as r:
            with gzip.GzipFile(fileobj=r.raw) as gz:
                for line_no, raw_line in enumerate(gz, start=1):
                    if not raw_line.strip():
                        continue
                    try:
                        text_rec = json.loads(raw_line)
                    except json.JSONDecodeError as e:
                        logger.warning("Bad JSONL in %s line %s: %s", key, line_no, e)
                        continue
                    path_info = self._parse_text_source(text_rec.get("source", ""), shard)
                    cnr = path_info.get("cnr_number", "")
                    if cnr:
                        self._seen_cnr.add(cnr)
                    metadata = self._fetch_metadata(path_info["json_key"])
                    yield {
                        "mode": "landlit_text",
                        "text_key": key,
                        "text_record": text_rec,
                        "metadata": metadata,
                        **path_info,
                    }

    def _iter_missing_metadata_records(self) -> Generator[dict[str, Any], None, None]:
        if not self.include_pdf_fallback:
            return
        years = self._years_to_scan() or self._list_years()
        for year in years:
            for court_prefix in self._list_courts(year):
                court_match = re.search(r"court=([^/]+)/", court_prefix)
                if self.court and (not court_match or court_match.group(1) != self.court):
                    continue
                for bench_prefix in self._list_benches(court_prefix):
                    for obj in self._list_s3_objects(bench_prefix):
                        key = obj["key"] if isinstance(obj, dict) else obj
                        if not key.endswith(".json"):
                            continue
                        filename = key.rsplit("/", 1)[-1]
                        name_match = _FILENAME_RE.match(filename)
                        cnr = name_match.group("cnr") if name_match else filename.split("_")[0]
                        if cnr in self._seen_cnr:
                            continue
                        result = self._fetch_judgment(key)
                        if result:
                            self.pdf_fallback_attempts += 1
                            yield result

    def _extract_pdf_text(self, pdf_content: bytes, source_id: str = "") -> str:
        return extract_pdf_markdown(
            source=SOURCE_ID,
            source_id=source_id,
            pdf_bytes=pdf_content,
            table="case_law",
        ) or ""

    def _parse_raw_html(self, raw_html: str) -> dict[str, str]:
        info = {
            "case_number": "",
            "parties": "",
            "judges": "",
            "cnr_number": "",
            "registration_date": "",
            "decision_date": "",
            "disposal_nature": "",
        }

        if not raw_html:
            return info

        html = unescape(raw_html)

        btn_match = re.search(r"aria-label=\"([^\"]+)\"", html)
        if btn_match:
            label = btn_match.group(1)
            label = re.sub(r"\s+pdf$", "", label)
            label = re.sub(r"\.Array\[\d+\]\.?\s*", " Vs ", label)
            parts = re.split(r"\s+of\s+", label, maxsplit=1)
            if len(parts) == 2:
                info["case_number"] = parts[0].strip()
                info["parties"] = parts[1].strip()
            else:
                info["parties"] = label.strip()
        else:
            btn_text = re.search(r"'>([^<]+)</button>", html)
            if btn_text:
                text = re.sub(r"\.Array\[\d+\]\.?\s*", " Vs ", btn_text.group(1).strip())
                parts = re.split(r"\s+of\s+", text, maxsplit=1)
                if len(parts) == 2:
                    info["case_number"] = parts[0].strip()
                    info["parties"] = parts[1].strip()
                else:
                    info["parties"] = text

        patterns = {
            "judges": r"Judge\s*:\s*([^<]+)",
            "cnr_number": r"CNR\s*:\s*</span><font[^>]*>\s*(\w+)",
            "registration_date": r"Date of registration\s*:\s*</span><font[^>]*>\s*([\d-]+)",
            "decision_date": r"Decision Date\s*:\s*</span><font[^>]*>\s*([\d-]+)",
            "disposal_nature": r"Disposal Nature\s*:\s*</span><font[^>]*>\s*([^<]+)",
        }
        for field, pattern in patterns.items():
            match = re.search(pattern, html)
            if match:
                info[field] = match.group(1).strip()

        return info

    def _json_key_to_pdf_key(self, json_key: str) -> str:
        return json_key.replace(METADATA_PREFIX, PDF_PREFIX).replace(".json", ".pdf")

    def _fetch_judgment(self, json_key: str) -> Optional[dict[str, Any]]:
        """PDF fallback for metadata judgments missing from derived text layer."""
        try:
            metadata = self._fetch_metadata(json_key)
            filename = json_key.rsplit("/", 1)[-1]
            cnr = filename.split("_", 1)[0]
            pdf_key = self._json_key_to_pdf_key(json_key)
            pdf_url = f"{S3_BASE}/{pdf_key}"
            try:
                pdf_r = self._s3_get(pdf_url, timeout=180)
                if pdf_r.content[:5] == b"%PDF-":
                    text = self._extract_pdf_text(pdf_r.content, source_id=cnr)
                else:
                    logger.warning("Not a PDF: %s", pdf_key)
                    text = ""
            except Exception as e:
                logger.warning("PDF fallback failed for %s: %s", pdf_key, e)
                text = ""

            return {
                "mode": "pdf_fallback",
                "json_key": json_key,
                "pdf_key": pdf_key,
                "metadata": metadata,
                "text": text,
                "cnr_number": cnr,
                "date": self._date_from_filename(filename),
            }
        except Exception as e:
            logger.error("Failed to fetch judgment %s: %s", json_key, e)
            return None

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield raw records: text-layer judgments first, optional PDF gaps second."""
        text_shards = list(self._iter_text_keys())
        logger.info(
            "Found %d landlit-v2 text shards%s%s",
            len(text_shards),
            f" for years {self.year_range[0]}-{self.year_range[1]}" if self.year_range else "",
            f" court={self.court}" if self.court else "",
        )
        for shard in text_shards:
            yield from self._iter_text_records(shard)
        if self.include_pdf_fallback:
            logger.info("Text layer complete for selected shard(s); scanning metadata gaps for PDF fallback")
            yield from self._iter_missing_metadata_records()

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """Dataset updates quarterly; recent years are the practical update window."""
        current_year = datetime.now().year
        previous_filter = self.year_range
        self.year_range = (current_year - 1, current_year)
        try:
            yield from self.fetch_all()
        finally:
            self.year_range = previous_filter

    def _normalize_date(self, value: str) -> str:
        if not value:
            return ""
        if _ISO_DATE_RE.match(value):
            return value
        if re.match(r"\d{2}-\d{2}-\d{4}$", value):
            dd, mm, yyyy = value.split("-")
            return f"{yyyy}-{mm}-{dd}"
        return ""

    def normalize(self, raw: dict) -> Optional[dict]:
        """Transform a raw text-layer or fallback record into standard schema."""
        metadata = raw.get("metadata", {}) or {}
        parsed = self._parse_raw_html(metadata.get("raw_html", ""))

        text = raw.get("text") or (raw.get("text_record") or {}).get("text") or ""
        if not text or len(text.strip()) < 50:
            return None

        cnr = raw.get("cnr_number") or parsed.get("cnr_number") or ""
        if not cnr:
            filename = (raw.get("json_key") or raw.get("filename") or "").rsplit("/", 1)[-1]
            cnr = filename.split("_", 1)[0]
        if not cnr:
            return None

        decision_date = self._normalize_date(raw.get("date") or parsed.get("decision_date") or "")
        if not decision_date:
            decision_date = self._normalize_date(self._date_from_filename(raw.get("json_key") or raw.get("filename") or ""))
        if not decision_date:
            return None

        court_name = metadata.get("court_name", "")
        court_code = metadata.get("court_code", "")
        title = parsed.get("parties") or parsed.get("case_number") or cnr
        if parsed.get("case_number") and parsed.get("parties"):
            title = f"{parsed['case_number']} - {parsed['parties']}"

        pdf_key = raw.get("pdf_key", "")
        pdf_url = f"{S3_BASE}/{pdf_key}" if pdf_key else ""
        text_record = raw.get("text_record") or {}
        pages = text_record.get("pages") or []

        return {
            "_id": cnr,
            "_source": SOURCE_ID,
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": text,
            "date": decision_date,
            "url": pdf_url,
            "cnr_number": cnr,
            "court_name": court_name,
            "court_code": court_code,
            "case_number": parsed.get("case_number", ""),
            "parties": parsed.get("parties", ""),
            "judges": parsed.get("judges", ""),
            "registration_date": self._normalize_date(parsed.get("registration_date", "")) or None,
            "disposal_nature": parsed.get("disposal_nature", ""),
            "landlit_text_key": raw.get("text_key", ""),
            "text_tier": text_record.get("tier", ""),
            "page_count": text_record.get("page_count") or len(pages) or None,
        }

    def coverage_report(self, count_text_records: bool = False) -> dict[str, Any]:
        """Report text-layer shard coverage against metadata bench shards."""
        text_shards = list(self._iter_text_keys())
        text_shard_ids = {(s["year"], s["court"], s["bench"]) for s in text_shards}

        years = self._years_to_scan() or self._list_years()
        metadata_shard_ids: set[tuple[int, str, str]] = set()
        for year in years:
            for court_prefix in self._list_courts(year):
                court_match = re.search(r"court=([^/]+)/", court_prefix)
                if not court_match:
                    continue
                court = court_match.group(1)
                if self.court and court != self.court:
                    continue
                for bench_prefix in self._list_benches(court_prefix):
                    bench_match = re.search(r"bench=([^/]+)/", bench_prefix)
                    if bench_match:
                        metadata_shard_ids.add((year, court, bench_match.group(1)))

        report = {
            "year_range": self.year_range,
            "court": self.court,
            "text_shards": len(text_shard_ids),
            "metadata_shards": len(metadata_shard_ids),
            "text_shard_coverage_pct": round((len(text_shard_ids) / len(metadata_shard_ids) * 100), 2)
            if metadata_shard_ids else None,
            "metadata_shards_missing_text_layer": len(metadata_shard_ids - text_shard_ids),
            "issue_reported_full_corpus_records": 16_700_000,
        }
        if count_text_records:
            total = 0
            for shard in text_shards:
                url = f"{S3_BASE}/{shard['key']}"
                with self._s3_get(url, timeout=300, stream=True) as r:
                    with gzip.GzipFile(fileobj=r.raw) as gz:
                        total += sum(1 for line in gz if line.strip())
            report["text_records_counted"] = total
        return report


def parse_year_range(value: str | None) -> tuple[int, int] | None:
    if not value:
        return None
    if "-" in value:
        left, right = value.split("-", 1)
        start, end = int(left), int(right)
    else:
        start = end = int(value)
    if start > end:
        raise ValueError("--year-range start must be <= end")
    return start, end


def main():
    import argparse

    parser = argparse.ArgumentParser(description="IN/HighCourtAWS data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    def add_shard_args(p):
        p.add_argument("--year-range", help="Limit to one year or inclusive range, e.g. 2023 or 2020-2023")
        p.add_argument("--court", help="Limit to S3 court shard, e.g. 36_29")
        p.add_argument("--max-text-shards", type=int, help="Debug/test limit on text-layer shard files")

    bp = subparsers.add_parser("bootstrap", help="Full initial fetch")
    bp.add_argument("--sample", action="store_true", help="Fetch sample records only")
    bp.add_argument("--sample-size", type=int, default=15, help="Number of sample records")
    bp.add_argument("--full", action="store_true", help="Fetch all records")
    bp.add_argument("--include-pdf-fallback", action="store_true", help="After text shards, PDF-extract metadata judgments missing from landlit-v2")
    add_shard_args(bp)

    up = subparsers.add_parser("update", help="Incremental update")
    add_shard_args(up)

    cov = subparsers.add_parser("coverage", help="Report landlit-v2 text shard coverage vs metadata shards")
    cov.add_argument("--count-text-records", action="store_true", help="Also stream selected text shards and count JSONL records")
    add_shard_args(cov)

    subparsers.add_parser("test", help="Quick connectivity test")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    year_range = parse_year_range(getattr(args, "year_range", None))
    scraper = IndianHighCourtAWSScraper(
        year_range=year_range,
        court=getattr(args, "court", None),
        include_pdf_fallback=getattr(args, "include_pdf_fallback", False),
        max_text_shards=getattr(args, "max_text_shards", None),
    )

    if args.command == "test":
        logger.info("Testing S3 connectivity...")
        years = scraper._list_years()
        text_keys = list(scraper._iter_text_keys())[:3]
        logger.info("OK: metadata years=%s-%s (%d years)", years[0], years[-1], len(years))
        logger.info("OK: first text shards=%s", [k["key"] for k in text_keys])
        return

    if args.command == "coverage":
        print(json.dumps(scraper.coverage_report(count_text_records=args.count_text_records), indent=2, default=str))
        return

    if args.command == "bootstrap":
        stats = scraper.bootstrap(sample_mode=args.sample, sample_size=args.sample_size)
        stats["pdf_fallback_attempts"] = scraper.pdf_fallback_attempts
        logger.info("Bootstrap complete: %s", json.dumps(stats, indent=2, default=str))
        return

    if args.command == "update":
        stats = scraper.update()
        stats["pdf_fallback_attempts"] = scraper.pdf_fallback_attempts
        logger.info("Update complete: %s", json.dumps(stats, indent=2, default=str))


if __name__ == "__main__":
    main()
