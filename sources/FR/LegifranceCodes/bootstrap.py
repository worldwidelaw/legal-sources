#!/usr/bin/env python3
"""
FR/LegifranceCodes -- French Consolidated Legal Codes (PISTE API)

Fetches all French consolidated codes via the official PISTE/Légifrance API.
Requires OAuth2 credentials from https://piste.gouv.fr/

Usage:
    python bootstrap.py bootstrap --sample   # Fetch sample records
    python bootstrap.py bootstrap --full     # Full fetch (all codes)
    python bootstrap.py list-codes           # List available codes
"""

import argparse
import html
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Generator, Optional

import requests
import yaml
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configuration
SOURCE_ID = "FR/LegifranceCodes"
API_BASE_URL = "https://api.piste.gouv.fr/dila/legifrance/lf-engine-app"
TOKEN_URL = "https://oauth.piste.gouv.fr/api/oauth/token"
USER_AGENT = "LegalDataHunter/1.0 (Open Data Research)"
REQUEST_DELAY = 0.5  # seconds between requests

# Paths
SCRIPT_DIR = Path(__file__).parent
SAMPLE_DIR = SCRIPT_DIR / "sample"
DATA_DIR = SCRIPT_DIR / "data"
STATUS_FILE = SCRIPT_DIR / "status.yaml"

# French code list with LEGITEXT IDs
CODE_LIST = [
    {"name": "Code de l'action sociale et des familles", "id": "LEGITEXT000006074069"},
    {"name": "Code de l'artisanat", "id": "LEGITEXT000006075116"},
    {"name": "Code des assurances", "id": "LEGITEXT000006073984"},
    {"name": "Code de l'aviation civile", "id": "LEGITEXT000006074234"},
    {"name": "Code du cinéma et de l'image animée", "id": "LEGITEXT000020908868"},
    {"name": "Code civil", "id": "LEGITEXT000006070721"},
    {"name": "Code de la commande publique", "id": "LEGITEXT000037701019"},
    {"name": "Code de commerce", "id": "LEGITEXT000005634379"},
    {"name": "Code des communes", "id": "LEGITEXT000006070162"},
    {"name": "Code des communes de la Nouvelle-Calédonie", "id": "LEGITEXT000006070300"},
    {"name": "Code de la consommation", "id": "LEGITEXT000006069565"},
    {"name": "Code de la construction et de l'habitation", "id": "LEGITEXT000006074096"},
    {"name": "Code de la défense", "id": "LEGITEXT000006071307"},
    {"name": "Code de déontologie des architectes", "id": "LEGITEXT000006074232"},
    {"name": "Code disciplinaire et pénal de la marine marchande", "id": "LEGITEXT000006071188"},
    {"name": "Code du domaine de l'État", "id": "LEGITEXT000006070208"},
    {"name": "Code du domaine de l'État et des collectivités publiques applicable à la collectivité territoriale de Mayotte", "id": "LEGITEXT000006074235"},
    {"name": "Code du domaine public fluvial et de la navigation intérieure", "id": "LEGITEXT000006074237"},
    {"name": "Code des douanes", "id": "LEGITEXT000006071570"},
    {"name": "Code des douanes de Mayotte", "id": "LEGITEXT000006071645"},
    {"name": "Code de l'éducation", "id": "LEGITEXT000006071191"},
    {"name": "Code électoral", "id": "LEGITEXT000006070239"},
    {"name": "Code de l'énergie", "id": "LEGITEXT000023983208"},
    {"name": "Code de l'entrée et du séjour des étrangers et du droit d'asile", "id": "LEGITEXT000006070158"},
    {"name": "Code de l'environnement", "id": "LEGITEXT000006074220"},
    {"name": "Code de l'expropriation pour cause d'utilité publique", "id": "LEGITEXT000006074224"},
    {"name": "Code de la famille et de l'aide sociale", "id": "LEGITEXT000006072637"},
    {"name": "Code forestier (nouveau)", "id": "LEGITEXT000025244092"},
    {"name": "Code général de la fonction publique", "id": "LEGITEXT000044416551"},
    {"name": "Code général de la propriété des personnes publiques", "id": "LEGITEXT000006070299"},
    {"name": "Code général des collectivités territoriales", "id": "LEGITEXT000006070633"},
    {"name": "Code général des impôts", "id": "LEGITEXT000006069577"},
    {"name": "Code général des impôts, annexe I", "id": "LEGITEXT000006069568"},
    {"name": "Code général des impôts, annexe II", "id": "LEGITEXT000006069569"},
    {"name": "Code général des impôts, annexe III", "id": "LEGITEXT000006069574"},
    {"name": "Code général des impôts, annexe IV", "id": "LEGITEXT000006069576"},
    {"name": "Code des impositions sur les biens et services", "id": "LEGITEXT000044595989"},
    {"name": "Code des instruments monétaires et des médailles", "id": "LEGITEXT000006070666"},
    {"name": "Code des juridictions financières", "id": "LEGITEXT000006070249"},
    {"name": "Code de justice administrative", "id": "LEGITEXT000006070933"},
    {"name": "Code de justice militaire (nouveau)", "id": "LEGITEXT000006071360"},
    {"name": "Code de la justice pénale des mineurs", "id": "LEGITEXT000039086952"},
    {"name": "Code de la Légion d'honneur, de la Médaille militaire et de l'ordre national du Mérite", "id": "LEGITEXT000006071007"},
    {"name": "Livre des procédures fiscales", "id": "LEGITEXT000006069583"},
    {"name": "Code minier", "id": "LEGITEXT000006071785"},
    {"name": "Code minier (nouveau)", "id": "LEGITEXT000023501962"},
    {"name": "Code monétaire et financier", "id": "LEGITEXT000006072026"},
    {"name": "Code de la mutualité", "id": "LEGITEXT000006074067"},
    {"name": "Code de l'organisation judiciaire", "id": "LEGITEXT000006071164"},
    {"name": "Code du patrimoine", "id": "LEGITEXT000006074236"},
    {"name": "Code pénal", "id": "LEGITEXT000006070719"},
    {"name": "Code pénitentiaire", "id": "LEGITEXT000045476241"},
    {"name": "Code des pensions civiles et militaires de retraite", "id": "LEGITEXT000006070302"},
    {"name": "Code des pensions de retraite des marins français du commerce, de pêche ou de plaisance", "id": "LEGITEXT000006074066"},
    {"name": "Code des pensions militaires d'invalidité et des victimes de guerre", "id": "LEGITEXT000006074068"},
    {"name": "Code des ports maritimes", "id": "LEGITEXT000006074233"},
    {"name": "Code des postes et des communications électroniques", "id": "LEGITEXT000006070987"},
    {"name": "Code de procédure civile", "id": "LEGITEXT000006070716"},
    {"name": "Code de procédure pénale", "id": "LEGITEXT000006071154"},
    {"name": "Code des procédures civiles d'exécution", "id": "LEGITEXT000025024948"},
    {"name": "Code de la propriété intellectuelle", "id": "LEGITEXT000006069414"},
    {"name": "Code de la recherche", "id": "LEGITEXT000006071190"},
    {"name": "Code des relations entre le public et l'administration", "id": "LEGITEXT000031366350"},
    {"name": "Code de la route", "id": "LEGITEXT000006074228"},
    {"name": "Code rural (ancien)", "id": "LEGITEXT000006071366"},
    {"name": "Code rural et de la pêche maritime", "id": "LEGITEXT000006071367"},
    {"name": "Code de la santé publique", "id": "LEGITEXT000006072665"},
    {"name": "Code de la sécurité intérieure", "id": "LEGITEXT000025503132"},
    {"name": "Code de la sécurité sociale", "id": "LEGITEXT000006073189"},
    {"name": "Code du service national", "id": "LEGITEXT000006071335"},
    {"name": "Code du sport", "id": "LEGITEXT000006071318"},
    {"name": "Code du tourisme", "id": "LEGITEXT000006074073"},
    {"name": "Code des transports", "id": "LEGITEXT000023086525"},
    {"name": "Code du travail", "id": "LEGITEXT000006072050"},
    {"name": "Code du travail maritime", "id": "LEGITEXT000006072051"},
    {"name": "Code de l'urbanisme", "id": "LEGITEXT000006074075"},
    {"name": "Code de la voirie routière", "id": "LEGITEXT000006070667"},
]

# Priority codes for sampling (most commonly used)
PRIORITY_CODES = [
    "LEGITEXT000006070721",  # Code civil
    "LEGITEXT000006070719",  # Code pénal
    "LEGITEXT000006072050",  # Code du travail
    "LEGITEXT000005634379",  # Code de commerce
    "LEGITEXT000006069565",  # Code de la consommation
    "LEGITEXT000006070716",  # Code de procédure civile
    "LEGITEXT000006071154",  # Code de procédure pénale
    "LEGITEXT000006069577",  # Code général des impôts
]


class PISTEApiClient:
    """Client for the PISTE Légifrance API with OAuth2 authentication."""

    def __init__(self, client_id: str, client_secret: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token: Optional[str] = None
        self.token_expires: float = 0
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
            "Content-Type": "application/json",
        })

    def _get_token(self) -> str:
        """Get OAuth2 access token (refreshes if expired)."""
        if self.access_token and time.time() < self.token_expires - 60:
            return self.access_token

        response = self.session.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": "openid",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        self.access_token = data["access_token"]
        self.token_expires = time.time() + data.get("expires_in", 3600)
        return self.access_token

    def _force_refresh_token(self) -> str:
        """Invalidate cached token and fetch a new one."""
        self.access_token = None
        self.token_expires = 0
        return self._get_token()

    def _request(self, endpoint: str, payload: dict) -> dict:
        """Make an authenticated POST request to the API.

        Automatically refreshes the OAuth2 token and retries once on 401.
        """
        token = self._get_token()
        url = f"{API_BASE_URL}/{endpoint}"

        response = self.session.post(
            url,
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
            timeout=60,
        )

        # On 401, force token refresh and retry once
        if response.status_code == 401:
            print("  Token expired, refreshing...", file=sys.stderr)
            token = self._force_refresh_token()
            response = self.session.post(
                url,
                json=payload,
                headers={"Authorization": f"Bearer {token}"},
                timeout=60,
            )

        response.raise_for_status()
        return response.json()

    def get_code_toc(self, text_id: str, date: Optional[datetime] = None) -> dict:
        """Get the table of contents for a code."""
        if date is None:
            date = datetime.now(timezone.utc)

        payload = {
            "textId": text_id,
            "sctId": "",
            "date": int(date.timestamp() * 1000),
        }
        return self._request("consult/code/tableMatieres", payload)

    def get_article(self, article_id: str) -> dict:
        """Get a specific article by ID."""
        payload = {"id": article_id}
        return self._request("consult/getArticle", payload)

    def list_codes(self) -> list[dict]:
        """List all available codes from API."""
        try:
            result = self._request("list/code", {})
            return result.get("results", [])
        except Exception:
            # Fallback to our hardcoded list
            return CODE_LIST


def extract_text_from_html(html_content: str) -> str:
    """Extract clean text from HTML content."""
    if not html_content:
        return ""

    # Decode HTML entities
    text = html.unescape(html_content)

    # Replace common block elements with newlines
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<p[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</p>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<div[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</div>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<li[^>]*>', '\n- ', text, flags=re.IGNORECASE)
    text = re.sub(r'</li>', '', text, flags=re.IGNORECASE)

    # Remove all remaining HTML tags
    text = re.sub(r'<[^>]+>', '', text)

    # Clean up whitespace
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = '\n'.join(line.strip() for line in text.split('\n'))
    text = text.strip()

    return text


def extract_articles_from_toc(toc_data: dict, path: str = "") -> Generator[dict, None, None]:
    """Recursively extract article references from table of contents."""
    # Handle different TOC structures
    sections = toc_data.get("sections", []) or []
    articles = toc_data.get("articles", []) or []

    # Get current section title for path building
    current_title = toc_data.get("title", "") or toc_data.get("titleTa", "")
    new_path = f"{path} > {current_title}" if path and current_title else (path or current_title)

    # Yield articles at this level
    for article in articles:
        article_id = article.get("id") or article.get("cid")
        if article_id:
            yield {
                "article_id": article_id,
                "article_num": article.get("num", ""),
                "section_path": new_path,
                "etat": article.get("etat", "VIGUEUR"),
            }

    # Recurse into sections
    for section in sections:
        yield from extract_articles_from_toc(section, new_path)


def normalize(article_data: dict, code_info: dict, section_path: str = "") -> dict:
    """Transform raw API article data into normalized schema."""
    article = article_data.get("article", {})

    # Extract article details
    article_id = article.get("id", "")
    article_num = article.get("num", "")
    date_debut = article.get("dateDebut")
    date_fin = article.get("dateFin")
    etat = article.get("etat", "VIGUEUR")

    # Extract and clean text content
    text_html = article.get("texte") or article.get("texteHtml") or ""
    text = extract_text_from_html(text_html)

    # Get nota (notes)
    nota_html = article.get("nota") or ""
    nota = extract_text_from_html(nota_html)

    # Parse dates
    date = None
    if date_debut:
        try:
            if isinstance(date_debut, int):
                date = datetime.fromtimestamp(date_debut / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            else:
                date = date_debut
        except Exception:
            pass

    date_fin_str = None
    if date_fin:
        try:
            if isinstance(date_fin, int):
                date_fin_str = datetime.fromtimestamp(date_fin / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            else:
                date_fin_str = date_fin
        except Exception:
            pass

    # Build Légifrance URL
    url = f"https://www.legifrance.gouv.fr/codes/article_lc/{article_id}"

    # Build title
    title = f"{code_info['name']} - Article {article_num}" if article_num else f"{code_info['name']} - {article_id}"

    return {
        "_id": article_id,
        "_source": SOURCE_ID,
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": title,
        "text": text,
        "date": date,
        "url": url,
        "code_id": code_info["id"],
        "code_name": code_info["name"],
        "article_num": article_num,
        "section_path": section_path,
        "etat": etat,
        "date_debut": date,
        "date_fin": date_fin_str,
        "nota": nota,
    }


def fetch_code_articles(client: PISTEApiClient, code_info: dict, max_articles: int = 0) -> Generator[dict, None, None]:
    """Fetch all articles from a specific code."""
    print(f"  Fetching TOC for {code_info['name']}...")

    try:
        toc = client.get_code_toc(code_info["id"])
    except Exception as e:
        print(f"    Error getting TOC: {e}", file=sys.stderr)
        return

    # Extract article references from TOC
    article_refs = list(extract_articles_from_toc(toc))
    total = len(article_refs)
    print(f"    Found {total} articles in TOC")

    # Filter out repealed articles (ABROGE status) BEFORE limiting
    active_refs = [ref for ref in article_refs if ref.get("etat") != "ABROGE"]
    skipped_abroge = len(article_refs) - len(active_refs)
    if skipped_abroge > 0:
        print(f"    Skipping {skipped_abroge} ABROGE (repealed) articles")
    article_refs = active_refs
    print(f"    {len(article_refs)} active (VIGUEUR) articles")

    if max_articles > 0:
        article_refs = article_refs[:max_articles]
        print(f"    Limiting to {len(article_refs)} articles")

    count = 0
    consecutive_errors = 0
    MAX_CONSECUTIVE_ERRORS = 10
    for ref in article_refs:
        try:
            article_data = client.get_article(ref["article_id"])
            record = normalize(article_data, code_info, ref.get("section_path", ""))

            # Skip if no meaningful text
            if len(record.get("text", "")) < 50:
                continue

            # Secondary filter: skip articles with ABROGE* status from article API
            # (TOC may show VIGUEUR but article API returns more specific status)
            article_etat = record.get("etat", "")
            if article_etat.startswith("ABROGE"):
                continue

            yield record
            count += 1
            consecutive_errors = 0

            if count % 10 == 0:
                print(f"    Processed {count} articles...")

            time.sleep(REQUEST_DELAY)

        except Exception as e:
            consecutive_errors += 1
            print(f"    Error fetching article {ref['article_id']}: {e}", file=sys.stderr)
            if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
                print(f"    ABORTING code: {MAX_CONSECUTIVE_ERRORS} consecutive errors", file=sys.stderr)
                break
            continue

    print(f"    Completed: {count} articles with text")


def _make_client(client: Optional[PISTEApiClient] = None) -> PISTEApiClient:
    """Return the given client or create one from environment variables."""
    if client is not None:
        return client
    load_dotenv(SCRIPT_DIR / ".env")
    client_id = os.environ.get("PISTE_CLIENT_ID")
    client_secret = os.environ.get("PISTE_CLIENT_SECRET")
    if not client_id or not client_secret:
        raise RuntimeError("PISTE_CLIENT_ID and PISTE_CLIENT_SECRET must be set")
    return PISTEApiClient(client_id, client_secret)


def fetch_sample(client: Optional[PISTEApiClient] = None, count: int = 15) -> list[dict]:
    """Fetch a sample of articles from priority codes."""
    client = _make_client(client)
    records = []
    articles_per_code = max(1, count // len(PRIORITY_CODES))

    for code_id in PRIORITY_CODES:
        code_info = next((c for c in CODE_LIST if c["id"] == code_id), None)
        if not code_info:
            continue

        print(f"Sampling {code_info['name']}...")

        for record in fetch_code_articles(client, code_info, max_articles=articles_per_code * 2):
            records.append(record)
            if len(records) >= count:
                return records
            if len([r for r in records if r["code_id"] == code_id]) >= articles_per_code:
                break

        time.sleep(REQUEST_DELAY)

    return records


def fetch_all(client: Optional[PISTEApiClient] = None) -> Generator[dict, None, None]:
    """Fetch all articles from all codes."""
    client = _make_client(client)
    for code_info in CODE_LIST:
        print(f"Processing {code_info['name']}...")
        yield from fetch_code_articles(client, code_info)
        time.sleep(REQUEST_DELAY * 2)


def save_samples(records: list[dict]) -> None:
    """Save sample records to the sample directory."""
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)

    for i, record in enumerate(records):
        filepath = SAMPLE_DIR / f"record_{i:04d}.json"
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=2)

    # Also save all samples in one file
    all_samples = SAMPLE_DIR / "all_samples.json"
    with open(all_samples, 'w', encoding='utf-8') as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(records)} samples to {SAMPLE_DIR}")


def update_status(records_fetched: int, errors: int, sample_count: int = 0) -> None:
    """Update the status.yaml file."""
    now = datetime.now(timezone.utc).isoformat()

    status = {
        "last_run": now,
        "last_bootstrap": now if sample_count > 0 else None,
        "last_error": None,
        "total_records": 0,
        "run_history": [{
            "started_at": now,
            "finished_at": now,
            "records_fetched": records_fetched,
            "records_new": 0,
            "records_updated": 0,
            "records_skipped": 0,
            "sample_records_saved": sample_count,
            "errors": errors,
        }]
    }

    # Load existing status if present
    if STATUS_FILE.exists():
        try:
            with open(STATUS_FILE) as f:
                existing = yaml.safe_load(f) or {}
            if "run_history" in existing:
                status["run_history"] = existing["run_history"][-9:] + status["run_history"]
        except Exception:
            pass

    with open(STATUS_FILE, 'w') as f:
        yaml.dump(status, f, default_flow_style=False)


def main():
    parser = argparse.ArgumentParser(description="FR/LegifranceCodes data fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Bootstrap command
    bootstrap_parser = subparsers.add_parser("bootstrap", help="Initial data fetch")
    bootstrap_parser.add_argument("--sample", action="store_true", help="Fetch sample only")
    bootstrap_parser.add_argument("--full", action="store_true", help="Full fetch")
    bootstrap_parser.add_argument("--count", type=int, default=15, help="Number of samples")

    # List codes command
    subparsers.add_parser("list-codes", help="List available codes")

    args = parser.parse_args()

    # Get credentials
    client_id = os.environ.get("PISTE_CLIENT_ID")
    client_secret = os.environ.get("PISTE_CLIENT_SECRET")

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "list-codes":
        print("Available French Codes:")
        print("-" * 60)
        for code in CODE_LIST:
            print(f"  {code['id']}: {code['name']}")
        print(f"\nTotal: {len(CODE_LIST)} codes")
        sys.exit(0)

    if not client_id or not client_secret:
        print("Error: PISTE_CLIENT_ID and PISTE_CLIENT_SECRET must be set", file=sys.stderr)
        print("Copy .env.template to .env and add your credentials", file=sys.stderr)
        sys.exit(1)

    client = PISTEApiClient(client_id, client_secret)

    if args.command == "bootstrap":
        if args.sample:
            print(f"Fetching {args.count} sample records...")
            records = fetch_sample(client, args.count)
            if records:
                save_samples(records)
                update_status(len(records), 0, len(records))

                # Print summary
                text_lengths = [len(r.get('text', '')) for r in records]
                avg_len = sum(text_lengths) / len(text_lengths) if text_lengths else 0
                codes_sampled = len(set(r['code_name'] for r in records))
                print(f"\nSummary:")
                print(f"  Records: {len(records)}")
                print(f"  Codes sampled: {codes_sampled}")
                print(f"  Avg text length: {avg_len:.0f} chars")
                print(f"  Min text length: {min(text_lengths)} chars")
                print(f"  Max text length: {max(text_lengths)} chars")
            else:
                print("No records fetched!", file=sys.stderr)
                update_status(0, 1)
                sys.exit(1)

        elif args.full:
            print("Starting full fetch...")
            DATA_DIR.mkdir(parents=True, exist_ok=True)
            records_file = DATA_DIR / "records.jsonl"
            count = 0
            with open(records_file, "a", encoding="utf-8") as f:
                for record in fetch_all(client):
                    line = json.dumps(record, ensure_ascii=False, default=str)
                    f.write(line + "\n")
                    f.flush()
                    count += 1
                    if count % 100 == 0:
                        print(f"  {count} records written...")
            print(f"Fetched {count} records → {records_file}")
            update_status(count, 0)


if __name__ == "__main__":
    main()
