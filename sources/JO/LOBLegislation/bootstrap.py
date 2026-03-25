#!/usr/bin/env python3
"""
JO/LOBLegislation -- Jordan Legislation & Opinion Bureau Data Fetcher

Fetches Jordanian legislation from the LOB encrypted ASMX API.

Strategy:
  - Bootstrap: Iterates through all legislation types via GetLegislationSearch,
    then fetches full article text via GetLegislationDetails.
  - Update: Re-fetches all (no date filter in API).
  - Sample: Fetches 10+ records with full text for validation.

API: https://www.lob.gov.jo/OPSHandler/Customization/LobJo/LobJo.asmx/
Note: Responses are AES-encrypted (CryptoJS OpenSSL format).
      Key obtained via GetPortalVersion handshake (ResponseAESKey header).

Usage:
  python bootstrap.py bootstrap            # Full initial pull (~6,500 laws)
  python bootstrap.py bootstrap --sample   # Fetch sample records for validation
  python bootstrap.py update               # Same as bootstrap (no date filter)
  python bootstrap.py test-api             # Quick API connectivity test
"""

import sys
import json
import logging
import time
import re
import hashlib
import base64
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator, Optional

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter.JO.LOBLegislation")

API_BASE = "https://www.lob.gov.jo/OPSHandler/Customization/LobJo/LobJo.asmx"
AUTH_URL = "https://www.lob.gov.jo/OPSHandler/OPS_Auth.asmx/GetPortalVersion"

# Legislation types: 1=Constitution, 2=Law, 3=Bylaw, 4=Agreement, 5=Instructions
LEGISLATION_TYPES = [1, 2, 3, 4, 5]

TYPE_NAMES = {
    1: "Constitution",
    2: "Law",
    3: "Bylaw",
    4: "Agreement",
    5: "Instructions",
}


def _evp_bytes_to_key(password: bytes, salt: bytes, key_len: int = 32, iv_len: int = 16):
    """Derive key and IV using OpenSSL EVP_BytesToKey with MD5."""
    d = b""
    d_i = b""
    while len(d) < key_len + iv_len:
        d_i = hashlib.md5(d_i + password + salt).digest()
        d += d_i
    return d[:key_len], d[key_len:key_len + iv_len]


def _decrypt_aes(encrypted_b64: str, passphrase: str) -> str:
    """Decrypt CryptoJS AES-encrypted data (OpenSSL Salted__ format)."""
    try:
        from Crypto.Cipher import AES
        from Crypto.Util.Padding import unpad
    except ImportError:
        try:
            from Cryptodome.Cipher import AES
            from Cryptodome.Util.Padding import unpad
        except ImportError:
            raise ImportError(
                "PyCryptodome is required. Install with: pip install pycryptodome"
            )

    raw = base64.b64decode(encrypted_b64)

    if raw[:8] == b"Salted__":
        salt = raw[8:16]
        ciphertext = raw[16:]
    else:
        salt = b"\x00" * 8
        ciphertext = raw

    key, iv = _evp_bytes_to_key(passphrase.encode("utf-8"), salt)
    cipher = AES.new(key, AES.MODE_CBC, iv)
    decrypted = unpad(cipher.decrypt(ciphertext), AES.block_size)
    return decrypted.decode("utf-8")


def _strip_html(html_text: str) -> str:
    """Strip HTML tags and clean up text."""
    if not html_text:
        return ""
    text = re.sub(r"<[^>]+>", " ", html_text)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"&quot;", '"', text)
    text = re.sub(r"&#\d+;", "", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


class LOBLegislationScraper(BaseScraper):
    """
    Scraper for JO/LOBLegislation -- Jordan Legislation & Opinion Bureau.
    Country: JO
    URL: https://www.lob.gov.jo/

    Data types: legislation
    Auth: none (encrypted API but no login required)
    """

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)

        self.client = HttpClient(
            base_url="",
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; LegalDataHunter/1.0)",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            timeout=60,
        )
        self.aes_key = None
        self._session = None

    def _ensure_session(self):
        """Initialize HTTP session and get AES key."""
        if self.aes_key:
            return

        logger.info("Initializing session via GetPortalVersion...")
        resp = self.client.post(AUTH_URL, data={"id": "-1"})
        resp.raise_for_status()

        self.aes_key = resp.headers.get("ResponseAESKey")
        if not self.aes_key:
            for header in resp.headers:
                if "aes" in header.lower():
                    self.aes_key = resp.headers[header]
                    break

        if not self.aes_key:
            self.aes_key = "d82b824f-bf6b-41c0-8345-3d937e0c4da7"
            logger.warning("AES key not in headers, using known static key")

        logger.info(f"AES key: {self.aes_key[:8]}...")

    def _api_call(self, method: str, data: dict) -> dict:
        """Make an encrypted API call and return the decrypted Value."""
        self._ensure_session()
        url = f"{API_BASE}/{method}"
        self.rate_limiter.wait()

        resp = self.client.post(url, data=data)
        resp.raise_for_status()

        result = resp.json()

        # Decrypt AES-encrypted response
        if isinstance(result, dict) and result.get("alg") == "aes":
            decrypted = _decrypt_aes(result["enc"], self.aes_key)
            result = json.loads(decrypted)

        # Unwrap ASMX "d" wrapper
        if isinstance(result, dict) and "d" in result:
            d_val = result["d"]
            if isinstance(d_val, str):
                try:
                    result = json.loads(d_val)
                except (json.JSONDecodeError, ValueError):
                    return {"text": d_val}
            else:
                result = d_val

        # Extract Value from standard response envelope
        if isinstance(result, dict) and "Value" in result:
            value = result["Value"]
            if isinstance(value, str):
                try:
                    return json.loads(value)
                except (json.JSONDecodeError, ValueError):
                    return {"text": value}
            return value

        return result

    def _search_legislation(self, leg_type: int, page: int = 1) -> dict:
        """Search for legislation of a given type, returning one page."""
        search_data = json.dumps({
            "LegislationType": leg_type,
            "LegislationTitle": "",
            "LegislationNumber": -1,
            "LegislationYear": -1,
            "LegislationYearTo": -1,
            "LegislationYearFrom": -1,
            "LegislationStatus": -1,
            "MatchingSearch": 0,
            "ArticleSearch": 0,
            "ArticleText": "",
            "Issuer": 0,
            "CourtType": 0,
            "FromHome": 0,
        })

        return self._api_call("GetLegislationSearch", {
            "LangID": "0",
            "PageIndex": str(page),
            "SearchData": search_data,
        })

    def _get_legislation_details(self, leg_id: int, leg_type: int) -> dict:
        """Get full legislation details including article text."""
        return self._api_call("GetLegislationDetails", {
            "LangID": "0",
            "LegislationID": str(leg_id),
            "LegislationType": str(leg_type),
            "isMod": "false",
        })

    def fetch_all(self) -> Generator[dict, None, None]:
        """Yield all legislation with full text."""
        for leg_type in LEGISLATION_TYPES:
            type_name = TYPE_NAMES.get(leg_type, f"Type{leg_type}")
            logger.info(f"Fetching legislation type: {type_name} (type={leg_type})")

            page = 1
            total_pages = None

            while True:
                try:
                    result = self._search_legislation(leg_type, page)
                except Exception as e:
                    logger.error(f"Search error {type_name} page {page}: {e}")
                    time.sleep(5)
                    try:
                        result = self._search_legislation(leg_type, page)
                    except Exception as e2:
                        logger.error(f"Retry failed: {e2}")
                        break

                if not isinstance(result, dict):
                    logger.warning(f"Unexpected result type: {type(result)}")
                    break

                items = result.get("List", [])
                if total_pages is None:
                    total_result = result.get("TotalResult", 0)
                    total_pages = result.get("TotalPages", 0)
                    logger.info(f"  {type_name}: {total_result} records, {total_pages} pages")

                if not items:
                    break

                for item in items:
                    if not isinstance(item, dict):
                        continue

                    leg_id = item.get("pmk_ID")
                    if not leg_id:
                        continue

                    # Fetch full details with articles
                    try:
                        details = self._get_legislation_details(leg_id, leg_type)
                    except Exception as e:
                        logger.warning(f"  Details failed for ID {leg_id}: {e}")
                        item["_leg_type"] = leg_type
                        item["_type_name"] = type_name
                        yield item
                        continue

                    # Merge details into item
                    if isinstance(details, dict):
                        # Extract legislation metadata
                        leg_list = details.get("Legislation", [])
                        if leg_list and isinstance(leg_list, list):
                            item.update(leg_list[0])
                        # Attach articles
                        item["_articles"] = details.get("Articles", [])

                    item["_leg_type"] = leg_type
                    item["_type_name"] = type_name
                    yield item

                if total_pages and page >= total_pages:
                    logger.info(f"  Done: all {total_pages} pages for {type_name}")
                    break

                page += 1
                if page % 10 == 0:
                    logger.info(f"  Page {page}/{total_pages} for {type_name}")

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        """No date filter available — re-fetches all."""
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        """Transform raw LOB data into standard schema."""
        leg_type = raw.get("_leg_type", 2)
        type_name = raw.get("_type_name", TYPE_NAMES.get(leg_type, "Unknown"))

        doc_id = raw.get("pmk_ID", "")
        title = _strip_html(raw.get("Name", ""))

        # Note: API has typo "Nunmber" for Number
        leg_number = raw.get("Number") or raw.get("Nunmber", "")
        leg_year = raw.get("Year", "")

        status = raw.get("Status_AR") or raw.get("Status") or ""

        # Dates: prefer Active_Date, then Magazine_Date
        date_str = raw.get("Active_Date") or raw.get("Magazine_Date") or ""
        date_iso = self._parse_date(date_str)

        # Build full text from Introduction + Articles
        text_parts = []

        intro = raw.get("Introduction") or ""
        if intro:
            clean_intro = _strip_html(intro)
            if clean_intro:
                text_parts.append(clean_intro)

        articles = raw.get("_articles", [])
        if isinstance(articles, list):
            for art in sorted(articles, key=lambda a: a.get("Article_Number", 0) if isinstance(a, dict) else 0):
                if not isinstance(art, dict):
                    continue
                art_num = art.get("Article_Number", "")
                art_text = art.get("Article", "")
                if art_text:
                    clean_text = _strip_html(art_text)
                    if clean_text:
                        if art_num:
                            text_parts.append(f"المادة {art_num}: {clean_text}")
                        else:
                            text_parts.append(clean_text)

        full_text = "\n\n".join(text_parts)

        return {
            "_id": f"JO-LOB-{leg_type}-{doc_id}",
            "_source": "JO/LOBLegislation",
            "_type": "legislation",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "title": title,
            "text": full_text,
            "date": date_iso,
            "url": f"https://www.lob.gov.jo/#!/LegislationDetails?LegislationID={doc_id}&LegislationType={leg_type}&isMod=false",
            "legislation_type": type_name,
            "legislation_number": str(leg_number) if leg_number else None,
            "legislation_year": str(leg_year) if leg_year else None,
            "status": _strip_html(str(status)) if status else None,
        }

    def _parse_date(self, date_str) -> Optional[str]:
        """Parse various date formats to ISO 8601."""
        if not date_str:
            return None
        date_str = str(date_str).strip()

        # .NET JSON date: /Date(1234567890000)/
        net_match = re.search(r"/Date\((\d+)\)/", date_str)
        if net_match:
            ts = int(net_match.group(1)) / 1000
            return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")

        for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%d/%m/%Y"]:
            try:
                return datetime.strptime(date_str, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
        return date_str if date_str else None


# ── CLI entrypoint ────────────────────────────────────────────────────

if __name__ == "__main__":
    scraper = LOBLegislationScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update|test-api] [--sample]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv

    if command == "bootstrap":
        result = scraper.bootstrap(sample_mode=sample_mode, sample_size=12)
        print(json.dumps(result, indent=2, default=str))

    elif command == "update":
        result = scraper.bootstrap(sample_mode=False)
        print(json.dumps(result, indent=2, default=str))

    elif command == "test-api":
        print("Testing LOB API connectivity...")
        try:
            scraper._ensure_session()
            print(f"AES key: {scraper.aes_key[:8]}...")

            result = scraper._search_legislation(2, 1)
            print(f"Search result keys: {list(result.keys())}")
            print(f"Total laws: {result.get('TotalResult')}")
            print(f"Total pages: {result.get('TotalPages')}")
            items = result.get("List", [])
            print(f"Items on page 1: {len(items)}")
            if items:
                first = items[0]
                print(f"First item: {first.get('Name')} ({first.get('Year')})")

                # Test details
                details = scraper._get_legislation_details(first["pmk_ID"], 2)
                print(f"Details keys: {list(details.keys())}")
                articles = details.get("Articles", [])
                print(f"Articles: {len(articles)}")
                if articles:
                    print(f"First article: {_strip_html(articles[0].get('Article', ''))[:200]}")

            print("API test passed!")
        except Exception as e:
            print(f"API test failed: {e}")
            import traceback
            traceback.print_exc()

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)
