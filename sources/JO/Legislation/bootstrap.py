#!/usr/bin/env python3
"""
JO/Legislation - Jordan Legislation and Opinion Bureau (LOB) Fetcher

Fetches laws, bylaws, regulations, instructions, and agreements from
the Jordanian Legislation and Opinion Bureau (ديوان التشريع والرأي).

Data source: https://www.lob.gov.jo/
Method: ASMX web service with RSA/AES encrypted responses
License: Free access (public government data)

Usage:
  python bootstrap.py bootstrap --sample   # Fetch sample records
  python bootstrap.py bootstrap             # Full bootstrap
  python bootstrap.py test                  # Test connectivity
"""

import argparse
import base64
import hashlib
import json
import re
import sys
import time
import urllib.parse
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Generator

import requests
from bs4 import BeautifulSoup
from Crypto.Cipher import AES, PKCS1_v1_5
from Crypto.PublicKey import RSA

ASMX_BASE = "https://www.lob.gov.jo/OPSHandler/Customization/LobJo/LobJo.asmx"
AUTH_URL = "https://www.lob.gov.jo/OPSHandler/OPS_Auth.asmx/GetPortalVersion"
SOURCE_ID = "JO/Legislation"
SAMPLE_DIR = Path(__file__).parent / "sample"

HEADERS = {
    "User-Agent": "LegalDataHunter/1.0 (Open Data Research)",
    "Content-Type": "application/x-www-form-urlencoded",
}

RATE_LIMIT_DELAY = 1.5
PAGE_SIZE = 10  # LOB API returns 10 per page

# Legislation types on LOB
LEGISLATION_TYPES = {
    1: "constitution",
    2: "law",
    3: "bylaw",
    4: "agreement",
    5: "instructions",
}


# ── Crypto helpers ──────────────────────────────────────────────────────


def evp_bytes_to_key(password: bytes, salt: bytes, key_len: int = 32, iv_len: int = 16) -> tuple:
    """CryptoJS-compatible EVP_BytesToKey with MD5."""
    d = b""
    d_list = []
    while len(b"".join(d_list)) < key_len + iv_len:
        data = d + password + salt
        d = hashlib.md5(data).digest()
        d_list.append(d)
    derived = b"".join(d_list)
    return derived[:key_len], derived[key_len:key_len + iv_len]


def decrypt_aes(ciphertext_b64: str, passphrase: str) -> str:
    """Decrypt CryptoJS-compatible AES-256-CBC (Salted__ prefix)."""
    raw = base64.b64decode(ciphertext_b64)
    # CryptoJS format: "Salted__" (8 bytes) + salt (8 bytes) + ciphertext
    assert raw[:8] == b"Salted__", f"Expected Salted__ prefix, got {raw[:8]!r}"
    salt = raw[8:16]
    ciphertext = raw[16:]
    key, iv = evp_bytes_to_key(passphrase.encode("utf-8"), salt)
    cipher = AES.new(key, AES.MODE_CBC, iv)
    plaintext = cipher.decrypt(ciphertext)
    # Remove PKCS7 padding
    pad_len = plaintext[-1]
    if pad_len < 1 or pad_len > 16:
        return plaintext.decode("utf-8", errors="replace")
    return plaintext[:-pad_len].decode("utf-8", errors="replace")


def decrypt_rsa(ciphertext_b64: str, private_key_pem: str) -> str:
    """Decrypt RSA-encrypted data with PKCS1 v1.5."""
    key = RSA.import_key(private_key_pem)
    cipher = PKCS1_v1_5.new(key)
    raw = base64.b64decode(ciphertext_b64)
    # RSA with 4096-bit key can encrypt up to 512 bytes per block
    block_size = key.size_in_bytes()
    blocks = [raw[i:i + block_size] for i in range(0, len(raw), block_size)]
    plaintext = b""
    for block in blocks:
        decrypted = cipher.decrypt(block, sentinel=None)
        if decrypted is None:
            raise ValueError("RSA decryption failed")
        plaintext += decrypted
    return plaintext.decode("utf-8", errors="replace")


def decrypt_response(data: dict, rsa_key: str, aes_key: str) -> dict:
    """Decrypt an API response based on its algorithm field."""
    enc = data.get("enc", "")
    alg = data.get("alg", "")
    if alg == "aes":
        plaintext = decrypt_aes(enc, aes_key)
    elif alg == "rsa":
        plaintext = decrypt_rsa(enc, rsa_key)
    else:
        raise ValueError(f"Unknown algorithm: {alg}")
    return json.loads(plaintext)


# ── API Session ─────────────────────────────────────────────────────────


class LOBSession:
    """Manages encrypted communication with the LOB API."""

    def __init__(self):
        self.session = requests.Session()
        self.rsa_private_key = None
        self.aes_key = None
        self.request_rsa_key = None
        self.request_aes_key = None

    def handshake(self):
        """Call GetPortalVersion to get encryption keys."""
        resp = self.session.post(AUTH_URL, data="id=-1", headers=HEADERS, timeout=30)
        resp.raise_for_status()
        self.rsa_private_key = urllib.parse.unquote(resp.headers["ResponseRSAKey"])
        self.aes_key = resp.headers["ResponseAESKey"]
        self.request_rsa_key = urllib.parse.unquote(resp.headers.get("RequestRSAKey", ""))
        self.request_aes_key = resp.headers.get("RequestAESKey", "")
        print("  Handshake OK — got encryption keys")

    def call(self, method: str, params: dict) -> dict:
        """Call an ASMX method and decrypt the response."""
        url = f"{ASMX_BASE}/{method}"
        body = urllib.parse.urlencode(params)
        resp = self.session.post(url, data=body, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        return decrypt_response(data, self.rsa_private_key, self.aes_key)


# ── Data fetching ───────────────────────────────────────────────────────


def clean_html(html_str: str) -> str:
    """Strip HTML tags and decode entities."""
    if not html_str:
        return ""
    soup = BeautifulSoup(html_str, "html.parser")
    return unescape(soup.get_text(separator="\n", strip=True))


def extract_articles_text(articles: list) -> str:
    """Extract full text from article list."""
    parts = []
    for art in articles:
        num = art.get("Article_Number", "")
        text = clean_html(art.get("Article", ""))
        if text:
            if num:
                parts.append(f"المادة {num}\n{text}")
            else:
                parts.append(text)
    return "\n\n".join(parts)


def fetch_legislation_list(api: LOBSession, leg_type: int, max_items: int = 0) -> list:
    """Fetch all legislation IDs of a given type."""
    all_items = []
    page_index = 1  # LOB API is 1-indexed

    search_data = json.dumps({
        "LegislationTitle": "",
        "LegislationType": leg_type,
        "LegislationYear": -1,
        "LegislationNumber": -1,
        "LegislationYearFrom": -1,
        "LegislationYearTo": -1,
        "LegislationStatus": -1,
        "ArticleText": "",
        "ArticleSearch": 0,
        "MatchingSearch": 0,
        "CourtType": -1,
        "Issuer": -1,
        "FromHome": 0,
    })

    while True:
        data = api.call("GetLegislationSearch", {
            "LangID": 0,
            "PageIndex": page_index,
            "SearchData": search_data,
        })

        value = data.get("Value", data)
        items = value.get("List", []) if isinstance(value, dict) else []
        total = value.get("TotalResult", 0) if isinstance(value, dict) else 0
        if not items:
            break

        all_items.extend(items)
        type_name = LEGISLATION_TYPES.get(leg_type, str(leg_type))
        print(f"  Type {type_name}: page {page_index}, got {len(items)} items (total: {len(all_items)}/{total})")

        if len(items) < PAGE_SIZE:
            break
        if max_items and len(all_items) >= max_items:
            break

        page_index += 1
        time.sleep(RATE_LIMIT_DELAY)

    return all_items[:max_items] if max_items else all_items


def fetch_legislation_detail(api: LOBSession, leg_id: int, leg_type: int) -> dict:
    """Fetch full legislation details including articles."""
    data = api.call("GetLegislationDetails", {
        "LangID": 0,
        "LegislationID": leg_id,
        "LegislationType": leg_type,
        "isMod": "false",
    })
    return data.get("Value", data)


def normalize(raw: dict, detail: dict, leg_type: int) -> dict:
    """Transform raw legislation data into standard schema."""
    # Extract legislation metadata from detail (may be list or dict)
    leg_raw = detail.get("Legislation", detail)
    legislation_data = leg_raw[0] if isinstance(leg_raw, list) and leg_raw else leg_raw if isinstance(leg_raw, dict) else {}

    leg_id = raw.get("pmk_ID") or raw.get("LegislationID") or raw.get("ID", 0)
    name = raw.get("Name", "") or legislation_data.get("Name", "")
    number = raw.get("Number", "") or legislation_data.get("Nunmber", "")  # API typo
    year = raw.get("Year", "") or legislation_data.get("Year", "")
    type_name = LEGISLATION_TYPES.get(leg_type, "legislation")
    articles = detail.get("Articles", [])
    text = extract_articles_text(articles)

    # If no articles, try Introduction (used by agreements) or other body fields
    if not text:
        for field in ["Introduction", "LegislationBody", "Body"]:
            body = legislation_data.get(field, "")
            if body:
                text = clean_html(body)
                if text:
                    break

    # Date handling — LOB returns ISO strings like "2025-01-22T00:00:00"
    date_str = None
    for date_field in ["Magazine_Date", "Active_Date"]:
        raw_date = legislation_data.get(date_field)
        if raw_date and isinstance(raw_date, str) and len(raw_date) >= 10:
            date_str = raw_date[:10]
            break

    doc_id = f"JO-{type_name}-{year}-{number}" if number else f"JO-{type_name}-{leg_id}"

    return {
        "_id": doc_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": name,
        "text": text,
        "date": date_str,
        "url": f"https://www.lob.gov.jo/?lang=ar#!LegislationDetails&LID={leg_id}&LTY={leg_type}",
        "legislation_type": type_name,
        "legislation_number": str(number) if number else None,
        "legislation_year": str(year) if year else None,
        "language": "ar",
        "status": raw.get("Status_AR", ""),
    }


def fetch_all(api: LOBSession, sample: bool = False) -> Generator[dict, None, None]:
    """Yield all normalized legislation records."""
    types_to_fetch = [2, 3, 1, 5, 4]  # law, bylaw, constitution, instructions, agreement

    for leg_type in types_to_fetch:
        type_name = LEGISLATION_TYPES.get(leg_type, str(leg_type))
        print(f"\nFetching {type_name} listings...")
        items = fetch_legislation_list(api, leg_type, max_items=3 if sample else 0)
        print(f"  Found {len(items)} {type_name} items")

        for i, item in enumerate(items):
            leg_id = item.get("pmk_ID") or item.get("LegislationID") or item.get("ID")
            if not leg_id:
                continue

            try:
                detail = fetch_legislation_detail(api, leg_id, leg_type)
                record = normalize(item, detail, leg_type)
                if record["text"]:
                    yield record
                    if (i + 1) % 10 == 0:
                        print(f"  Fetched {i + 1}/{len(items)} {type_name} details...")
                else:
                    print(f"  Warning: no text for {type_name} ID {leg_id}")
            except Exception as e:
                print(f"  Error fetching {type_name} ID {leg_id}: {e}")

            time.sleep(RATE_LIMIT_DELAY)


def test_connection():
    """Test API connectivity and decryption."""
    print("Testing Jordan LOB API...")
    api = LOBSession()
    api.handshake()

    # Test search form data
    print("\nTesting GetSearchFormData...")
    form_data = api.call("GetSearchFormData", {"LangID": 0})
    print(f"  Form data keys: {list(form_data.keys()) if isinstance(form_data, dict) else type(form_data)}")

    # Test search for laws (type 2)
    print("\nTesting legislation search (laws)...")
    search_data = json.dumps({
        "LegislationTitle": "",
        "LegislationType": 2,
        "LegislationYear": -1,
        "LegislationNumber": -1,
        "LegislationYearFrom": -1,
        "LegislationYearTo": -1,
        "LegislationStatus": -1,
        "ArticleText": "",
        "ArticleSearch": 0,
        "MatchingSearch": 0,
        "CourtType": -1,
        "Issuer": -1,
        "FromHome": 0,
    })
    results = api.call("GetLegislationSearch", {
        "LangID": 0,
        "PageIndex": 0,
        "SearchData": search_data,
    })
    if isinstance(results, list):
        print(f"  Got {len(results)} items")
        if results:
            print(f"  First item keys: {list(results[0].keys())}")
            print(f"  First item: {results[0].get('Name', 'N/A')}")
    elif isinstance(results, dict):
        print(f"  Response keys: {list(results.keys())}")

    # Test fetching detail for first item
    if isinstance(results, list) and results:
        first = results[0]
        leg_id = first.get("pmk_ID") or first.get("LegislationID")
        if leg_id:
            print(f"\nTesting legislation detail (ID: {leg_id})...")
            detail = api.call("GetLegislationDetails", {
                "LangID": 0,
                "LegislationID": leg_id,
                "LegislationType": 2,
                "isMod": "false",
            })
            if isinstance(detail, dict):
                print(f"  Detail keys: {list(detail.keys())}")
                articles = detail.get("Articles", [])
                print(f"  Articles: {len(articles)}")
                if articles:
                    first_art = articles[0]
                    text = clean_html(first_art.get("Article", ""))
                    print(f"  First article ({len(text)} chars): {text[:200]}...")

    print("\nTest complete!")
    return True


def bootstrap(sample: bool = False):
    """Run the bootstrap process."""
    api = LOBSession()
    api.handshake()
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    records = []
    for rec in fetch_all(api, sample=sample):
        records.append(rec)

    print(f"\nTotal records with text: {len(records)}")

    if sample:
        to_save = sorted(records, key=lambda r: len(r.get("text", "")), reverse=True)[:15]
    else:
        to_save = records

    saved = 0
    for rec in to_save:
        safe_id = re.sub(r'[^\w\-]', '_', rec["_id"])
        path = SAMPLE_DIR / f"{safe_id}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rec, f, ensure_ascii=False, indent=2)
        saved += 1

    print(f"Saved {saved} records to {SAMPLE_DIR}")

    has_text = sum(1 for r in to_save if r.get("text") and len(r["text"]) > 100)
    print(f"Records with substantial text: {has_text}/{saved}")

    if to_save:
        avg_len = sum(len(r.get("text", "")) for r in to_save) // len(to_save)
        print(f"Average text length: {avg_len} chars")

    return saved


def main():
    parser = argparse.ArgumentParser(description="JO/Legislation bootstrap")
    parser.add_argument("command", choices=["bootstrap", "test"])
    parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    args = parser.parse_args()

    if args.command == "test":
        test_connection()
    elif args.command == "bootstrap":
        count = bootstrap(sample=args.sample)
        if count == 0:
            print("ERROR: No records fetched", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
