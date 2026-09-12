#!/usr/bin/env python3
"""
Bangladesh Legal Acts Dataset Fetcher (HuggingFace)

Downloads 1,484 Bangladesh legal acts from the HuggingFace dataset
sakhadib/Bangladesh-Legal-Acts-Dataset. Each act includes full text
via structured sections, footnotes, government context, and legal
system metadata. Original data sourced from bdlaws.minlaw.gov.bd.

CC BY 4.0 licensed.
"""

import json
import logging
import os
import re
import sys
import tempfile
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

SOURCE_ID = "BD/BangladeshActs-HuggingFace"
DATASET_URL = "https://huggingface.co/datasets/sakhadib/Bangladesh-Legal-Acts-Dataset/resolve/main/Contextualized_Bangladesh_Legal_Acts.json"
BDLAWS_BASE = "http://bdlaws.minlaw.gov.bd"


def download_dataset(url: str = DATASET_URL) -> dict:
    """Download the consolidated JSON dataset from HuggingFace."""
    logger.info(f"Downloading dataset from {url} ...")
    req = urllib.request.Request(url, headers={
        "User-Agent": "LegalDataHunter/1.0 (legal-data-collection)"
    })
    with urllib.request.urlopen(req, timeout=120) as resp:
        raw = resp.read()
    logger.info(f"Downloaded {len(raw):,} bytes")
    return json.loads(raw)


def build_full_text(act: dict) -> str:
    """Assemble full text from sections and footnotes."""
    parts = []
    sections = act.get("sections") or []
    for sec in sections:
        title = sec.get("section_title", "").strip()
        content = sec.get("section_content", "").strip()
        if title and content:
            parts.append(f"{title}\n{content}")
        elif content:
            parts.append(content)
        elif title:
            parts.append(title)

    footnotes = act.get("footnotes") or []
    if footnotes:
        fn_texts = []
        for fn in footnotes:
            ft = fn.get("footnote_text", "").strip()
            if ft:
                fn_texts.append(ft)
        if fn_texts:
            parts.append("\n--- Footnotes ---\n" + "\n".join(fn_texts))

    return "\n\n".join(parts)


def build_source_url(act: dict) -> str:
    """Build URL to original bdlaws.minlaw.gov.bd page."""
    url = act.get("source_url", "").strip()
    if url:
        return url
    return BDLAWS_BASE


def parse_year(act: dict) -> Optional[str]:
    """Extract year from act data, return ISO date string."""
    year = act.get("act_year", "")
    if year and re.match(r"^\d{4}$", str(year)):
        return f"{year}-01-01"
    return None


def normalize(act: dict) -> Dict[str, Any]:
    """Transform a raw act record into the standard schema."""
    title = act.get("act_title", "Unknown Act").strip()
    act_no = act.get("act_no", "").strip()
    act_year = str(act.get("act_year", "")).strip()

    # Build unique ID from act number and year
    _id = f"BD-act-{act_no}-{act_year}" if act_no and act_year else f"BD-act-{title[:80]}"
    _id = re.sub(r"[^a-zA-Z0-9_-]", "_", _id)

    full_text = build_full_text(act)

    gov_ctx = act.get("government_context") or {}
    legal_ctx = act.get("legal_system_context") or {}

    return {
        "_id": _id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": full_text,
        "date": parse_year(act),
        "url": build_source_url(act),
        "act_number": act_no,
        "act_year": act_year,
        "language": act.get("language", "english"),
        "is_repealed": act.get("is_repealed", False),
        "token_count": act.get("token_count"),
        "government_context": {
            "government_name": gov_ctx.get("government_name", ""),
            "govt_system": gov_ctx.get("govt_system", ""),
            "period_years": gov_ctx.get("period_years", ""),
        } if gov_ctx else None,
        "legal_system_context": {
            "legal_framework": legal_ctx.get("legal_framework", ""),
            "period": legal_ctx.get("period", ""),
            "description": legal_ctx.get("description", ""),
        } if legal_ctx else None,
    }


def fetch_all() -> Iterator[Dict[str, Any]]:
    """Yield all normalized act records."""
    data = download_dataset()
    acts = data.get("acts", [])
    logger.info(f"Found {len(acts)} acts in dataset")
    for act in acts:
        yield normalize(act)


def fetch_updates(since: str) -> Iterator[Dict[str, Any]]:
    """Yield records updated since a given date. For a static dataset, returns all."""
    yield from fetch_all()


def bootstrap_sample(max_records: int = 15) -> None:
    """Download dataset and save sample records."""
    sample_dir = Path(__file__).parent / "sample"
    sample_dir.mkdir(exist_ok=True)

    data = download_dataset()
    acts = data.get("acts", [])
    logger.info(f"Total acts in dataset: {len(acts)}")

    # Pick a spread of records: first 5, middle 5, last 5
    total = len(acts)
    if total <= max_records:
        selected_indices = list(range(total))
    else:
        step = total // max_records
        selected_indices = [i * step for i in range(max_records)]

    saved = 0
    empty_text = 0
    for idx in selected_indices:
        if idx >= total:
            break
        act = acts[idx]
        record = normalize(act)

        if not record["text"] or len(record["text"]) < 50:
            empty_text += 1
            logger.warning(f"Short/empty text for: {record['title'][:60]}")

        filename = f"{record['_id'][:80]}.json"
        filename = re.sub(r"[^a-zA-Z0-9_.-]", "_", filename)
        filepath = sample_dir / filename
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, indent=2, default=str)
        saved += 1
        logger.info(f"  [{saved}/{max_records}] {record['title'][:60]} — {len(record['text']):,} chars")

    logger.info(f"Saved {saved} samples to {sample_dir}")
    if empty_text:
        logger.warning(f"{empty_text} records had short/empty text")

    # Print summary stats
    all_texts = [build_full_text(a) for a in acts[:100]]
    avg_len = sum(len(t) for t in all_texts) / max(len(all_texts), 1)
    logger.info(f"Average text length (first 100 acts): {avg_len:,.0f} chars")


if __name__ == "__main__":
    # `bootstrap-fast` is the fleet runner's entry point; this CLI
    # dispatches on the literal command name, so alias it onto the full
    # bootstrap rather than exiting 1 (VPS CLI mismatch, issue #602).
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap-fast":
        sys.argv[1] = "bootstrap"
    if len(sys.argv) > 1 and sys.argv[1] == "bootstrap":
        sample_flag = "--sample" in sys.argv
        bootstrap_sample(max_records=15)
    else:
        count = 0
        for record in fetch_all():
            print(json.dumps(record, ensure_ascii=False, default=str))
            count += 1
        logger.info(f"Emitted {count} records")
