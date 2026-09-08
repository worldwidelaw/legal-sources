#!/usr/bin/env python3
"""Validate and mirror the already-public canonical licence inventory verbatim."""

import argparse
from collections import Counter
from datetime import datetime
from http.client import HTTPException
import json
import os
from pathlib import Path
import sys
import tempfile
from urllib.request import urlopen

DEFAULT_URL = "https://zachlaik.github.io/LegalDataHunter/licenses.json"
DEFAULT_OUTPUT = Path(__file__).resolve().parents[1] / "docs/licenses.json"
MAX_PAYLOAD_BYTES = 10 * 1024 * 1024
FETCH_TIMEOUT_SECONDS = 30


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def _timestamp(value):
    _require(isinstance(value, str), "generated_at must be an ISO timezone datetime")
    try:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("generated_at must be an ISO timezone datetime") from None
    _require(timestamp.tzinfo is not None, "generated_at must include a timezone")
    return timestamp


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        _require(key not in result, "Duplicate JSON object key")
        result[key] = value
    return result


def _invalid_constant(_value):
    raise ValueError("Non-standard JSON constant")


def _strings(record, fields):
    _require(isinstance(record, dict), "Inventory entries must be objects")
    for field in fields:
        _require(isinstance(record.get(field), str), f"{field} must be a string")


def _permission(record):
    _require("commercial_use" in record, "commercial_use is required (null means unknown)")
    value = record["commercial_use"]
    _require(value is None or type(value) is bool, "commercial_use must be boolean or null")


def _url(record, field):
    _require(field in record and (record[field] is None or isinstance(record[field], str)),
             f"{field} must be a string or null")


def validate_inventory(payload):
    """Check schema v2 and its aggregates without inferring licence permissions."""
    try:
        data = json.loads(payload, object_pairs_hook=_unique_object, parse_constant=_invalid_constant)
    except (UnicodeError, json.JSONDecodeError, RecursionError):
        raise ValueError("Invalid licence inventory JSON") from None
    _require(isinstance(data, dict), "Inventory must be an object")
    _require(type(data.get("schema_version")) is int and data["schema_version"] == 2,
             "Canonical licence inventory must use schema_version 2; publish the upstream v2 feed first")
    generated_at = _timestamp(data.get("generated_at"))
    sources = data.get("sources")
    summary = data.get("summary")
    by_license = data.get("by_license")
    _require(isinstance(sources, list), "sources must be an array")
    _require(isinstance(summary, dict), "summary must be an object")
    _require(isinstance(by_license, dict), "by_license must be an object")

    ids = set()
    counts = Counter()
    permissions = Counter()
    for source in sources:
        _strings(source, ("id", "country", "name", "license_id", "license_name"))
        _url(source, "license_url")
        _require(bool(source["id"]) and source["id"] not in ids, "Source IDs must be nonempty and unique")
        ids.add(source["id"])
        _permission(source)
        counts[source["license_id"]] += 1
        permissions[source["commercial_use"]] += 1

    expected = {
        "total_complete": len(sources),
        "commercial_ok": permissions[True],
        "non_commercial": permissions[False],
        "commercial_unknown": permissions[None],
        "unverified": counts["unverified"],
        "unique_license_ids": len(counts),
    }
    for field, count in expected.items():
        value = summary.get(field)
        _require(type(value) is int and value >= 0 and value == count,
                 f"summary.{field} must be a nonnegative integer matching sources")

    _require(set(by_license) == set(counts), "by_license IDs must match source grouping")
    for license_id, entry in by_license.items():
        _strings(entry, ("display_name",))
        _url(entry, "url")
        _permission(entry)
        count = entry.get("count")
        _require(type(count) is int and count >= 0 and count == counts[license_id],
                 "by_license count must be a nonnegative integer matching sources")
    return generated_at


def sync_licenses(url=DEFAULT_URL, output=DEFAULT_OUTPUT):
    """Copy validated bytes atomically; return False for an identical inventory.

    Failures propagate and leave the existing file unchanged. Only a parseable,
    timezone-aware existing generated_at participates in the rollback check;
    this also protects an existing legacy inventory during the v2 rollout.
    """
    with urlopen(url, timeout=FETCH_TIMEOUT_SECONDS) as response:
        _require(response.status == 200, "Expected a complete HTTP 200 inventory response")
        length = response.headers.get("Content-Length")
        if length is not None:
            _require(length.isdigit(), "Invalid HTTP Content-Length")
            length = int(length)
            _require(length <= MAX_PAYLOAD_BYTES, "Inventory exceeds the 10 MiB limit")
        payload = response.read(MAX_PAYLOAD_BYTES + 1)
        _require(len(payload) <= MAX_PAYLOAD_BYTES, "Inventory exceeds the 10 MiB limit")
        _require(length is None or len(payload) == length, "Truncated inventory HTTP response")
    generated_at = validate_inventory(payload)
    output = Path(output)
    previous = output.read_bytes() if output.exists() else None
    if previous is not None:
        try:
            old_data = json.loads(previous)
            old_timestamp = _timestamp(old_data.get("generated_at")) if isinstance(old_data, dict) else None
        except (ValueError, UnicodeError, RecursionError):
            old_timestamp = None
        _require(old_timestamp is None or generated_at >= old_timestamp,
                 "Refusing inventory timestamp rollback: received an older generated_at")
        if payload == previous:
            return False

    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=output.parent, prefix=f".{output.name}.", delete=False) as stream:
            temporary = Path(stream.name)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, output)
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=DEFAULT_URL, help="Canonical inventory URL")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT, help="Destination JSON file")
    args = parser.parse_args()
    try:
        changed = sync_licenses(url=args.url, output=args.output)
    except (ValueError, OSError, HTTPException) as error:
        # Exception text from remote services can contain untrusted content.
        # The helper propagates errors; the CLI deliberately logs no payload.
        print(f"Licence sync failed ({type(error).__name__}); existing inventory preserved. "
              "Check upstream availability and schema v2.", file=sys.stderr)
        return 1
    print("Licence inventory synced." if changed else "Licence inventory unchanged.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
