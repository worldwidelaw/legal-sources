#!/usr/bin/env python3
"""
Monaco Official Journal (Journal de Monaco) data fetcher.

Fetches legislation from the official gazette of Monaco including:
- Lois (Laws)
- Ordonnances Souveraines (Sovereign Ordinances)
- Ordonnances-Lois (Law-Ordinances)
- Arrêtés Ministériels (Ministerial Decrees)
- Décisions Souveraines (Sovereign Decisions)
- Décisions Ministérielles (Ministerial Decisions)

Source: https://journaldemonaco.gouv.mc
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional
from html import unescape

import requests
from bs4 import BeautifulSoup

# Configuration
BASE_URL = "https://journaldemonaco.gouv.mc"
SAMPLE_DIR = Path(__file__).parent / "sample"
RATE_LIMIT_DELAY = 1.5  # seconds between requests

# Categories to fetch (legal documents only, not announcements)
LEGAL_CATEGORIES = {
    "Lois",
    "Ordonnances Souveraines", 
    "Ordonnances-Lois",
    "Arrêtés Ministériels",
    "Décisions Souveraines",
    "Décisions Ministérielles",
    "Arrêtés Municipaux",
    "Arrêtés de la Direction des Services Judiciaires",
}

# Categories to skip (not legislation)
SKIP_CATEGORIES = {
    "Avis et Communiqués",
    "Informations",
    "Insertions légales et Annonces",
    "Débats du Conseil National",
    "Maison Souveraine",
    "Décisions Archiépiscopales",
    "Décrets archiépiscopaux",
    "Tribunal Suprême",
    "Annexe",
}

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) LegalDataHunter/1.0 (research)"
})


def fetch_with_retry(url: str, max_retries: int = 3) -> Optional[requests.Response]:
    """Fetch URL with retry logic."""
    for attempt in range(max_retries):
        try:
            response = session.get(url, timeout=30)
            if response.status_code == 200:
                return response
            elif response.status_code == 404:
                return None
            else:
                print(f"  HTTP {response.status_code} for {url}", file=sys.stderr)
        except requests.RequestException as e:
            print(f"  Request error (attempt {attempt + 1}): {e}", file=sys.stderr)
        
        if attempt < max_retries - 1:
            time.sleep(RATE_LIMIT_DELAY * (attempt + 1))
    
    return None


def get_journal_issues(year: int) -> list[tuple[str, str]]:
    """Get all journal issues for a given year."""
    url = f"{BASE_URL}/Journaux/{year}"
    response = fetch_with_retry(url)
    
    if not response:
        return []
    
    issues = []
    soup = BeautifulSoup(response.text, "html.parser")
    
    # Find journal issue links
    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        match = re.match(rf"/Journaux/{year}/Journal-(\d+)", href)
        if match:
            issue_num = match.group(1)
            # Extract date from link text
            date_text = link.get_text(strip=True)
            issues.append((issue_num, date_text))
    
    # Deduplicate
    seen = set()
    unique_issues = []
    for issue_num, date_text in issues:
        if issue_num not in seen:
            seen.add(issue_num)
            unique_issues.append((issue_num, date_text))
    
    return sorted(unique_issues, key=lambda x: int(x[0]))


def get_articles_from_issue(year: int, issue_num: str) -> list[dict]:
    """Get all legal articles from a journal issue."""
    url = f"{BASE_URL}/Journaux/{year}/Journal-{issue_num}"
    response = fetch_with_retry(url)

    if not response:
        return []

    articles = []
    soup = BeautifulSoup(response.text, "html.parser")
    current_category = None

    # Parse the issue page structure
    # Find all h2 and li elements in document order
    for element in soup.find_all(["h2", "li"]):
        if element.name == "h2":
            # Get category name (handle whitespace/newlines)
            cat_text = element.get_text(strip=True)
            if cat_text:
                current_category = cat_text
        elif element.name == "li" and current_category:
            link = element.find("a", href=True)
            if link:
                href = link.get("href", "")
                # Match articles in this issue (handle both year patterns)
                if (f"/Journaux/{year}/Journal-{issue_num}/" in href or
                    f"/Journaux/{year - 1}/Journal-{issue_num}/" in href):
                    # Check if this is a legal category
                    if current_category in LEGAL_CATEGORIES:
                        title = link.get_text(strip=True)
                        slug = href.split("/")[-1]
                        articles.append({
                            "url": f"{BASE_URL}{href}",
                            "title": title,
                            "slug": slug,
                            "category": current_category,
                            "issue_num": issue_num,
                            "year": year,
                        })

    return articles


def clean_html_text(html_content: str) -> str:
    """Clean HTML content and extract plain text."""
    soup = BeautifulSoup(html_content, "html.parser")
    
    # Remove script and style elements
    for element in soup(["script", "style", "nav", "header", "footer"]):
        element.decompose()
    
    # Get text
    text = soup.get_text(separator="\n")
    
    # Clean up whitespace
    lines = []
    for line in text.split("\n"):
        line = line.strip()
        if line:
            lines.append(line)
    
    text = "\n".join(lines)
    
    # Unescape HTML entities
    text = unescape(text)
    
    return text


def clean_footer_text(text: str) -> str:
    """Remove common footer patterns from text."""
    # Common footer patterns to remove
    footer_patterns = [
        r"Veuillez compléter le champ ci-dessous.*$",
        r"Acheter le\s*Journal de Monaco.*$",
        r"Tous droits reservés Monaco.*$",
        r"Recevoir le sommaire par e-mail.*$",
        r"Publications.*Acheter.*$",
    ]

    for pattern in footer_patterns:
        text = re.sub(pattern, "", text, flags=re.IGNORECASE | re.DOTALL)

    return text.strip()


def fetch_article_full_text(article: dict) -> Optional[dict]:
    """Fetch full text for an article."""
    response = fetch_with_retry(article["url"])

    if not response:
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    # Find the main content area
    # The article text is typically in <p> tags after the <h1> title
    content_parts = []

    # Get the h1 title
    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else article["title"]

    # Find content paragraphs
    # They are in the main article body after navigation
    main_content = soup.find("div", class_="article__content")
    if not main_content:
        # Fallback: find all paragraphs in the body
        body = soup.find("body")
        if body:
            main_content = body

    if main_content:
        for p in main_content.find_all("p"):
            text = p.get_text(strip=True)
            if text and len(text) > 10:  # Skip very short fragments
                content_parts.append(text)

    # Join content
    full_text = "\n\n".join(content_parts)

    # If we couldn't get structured content, try raw text extraction
    if len(full_text) < 100:
        # More aggressive extraction
        for p in soup.find_all("p"):
            style = p.get("style", "")
            text = p.get_text(strip=True)
            # Skip navigation and footer elements
            if text and len(text) > 20 and "justify" in style.lower():
                content_parts.append(text)
        full_text = "\n\n".join(content_parts)

    # Clean footer text
    full_text = clean_footer_text(full_text)

    if not full_text or len(full_text) < 50:
        return None

    return {
        "title": title,
        "text": full_text,
        "url": article["url"],
        "category": article["category"],
        "issue_num": article["issue_num"],
        "year": article["year"],
        "slug": article["slug"],
    }


def normalize(raw: dict) -> dict:
    """Transform raw article data into standard schema."""
    # Generate unique ID
    doc_id = f"MC-JDM-{raw['issue_num']}-{raw['slug'][:50]}"
    
    # Extract date from issue if possible
    pub_date = None
    # Try to extract date from title (often contains "du DD month YYYY")
    date_match = re.search(
        r"du\s+(\d{1,2})\s*(janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre)\s*(\d{4})",
        raw.get("title", ""),
        re.IGNORECASE
    )
    if date_match:
        day, month_fr, year = date_match.groups()
        month_map = {
            "janvier": "01", "février": "02", "mars": "03", "avril": "04",
            "mai": "05", "juin": "06", "juillet": "07", "août": "08",
            "septembre": "09", "octobre": "10", "novembre": "11", "décembre": "12"
        }
        month = month_map.get(month_fr.lower(), "01")
        pub_date = f"{year}-{month}-{int(day):02d}"
    
    return {
        "_id": doc_id,
        "_source": "MC/JournalMonaco",
        "_type": "legislation",
        "_fetched_at": datetime.now(timezone.utc).isoformat(),
        "title": raw["title"],
        "text": raw["text"],
        "date": pub_date,
        "url": raw["url"],
        "category": raw["category"],
        "journal_issue": raw["issue_num"],
        "year": raw["year"],
    }


def fetch_all(start_year: int = 2020, end_year: int = None) -> Iterator[dict]:
    """Yield all legal documents from the Journal de Monaco."""
    if end_year is None:
        end_year = datetime.now().year
    
    for year in range(start_year, end_year + 1):
        print(f"Fetching year {year}...", file=sys.stderr)
        issues = get_journal_issues(year)
        print(f"  Found {len(issues)} issues", file=sys.stderr)
        
        for issue_num, date_text in issues:
            print(f"  Processing Journal-{issue_num} ({date_text})...", file=sys.stderr)
            time.sleep(RATE_LIMIT_DELAY)
            
            articles = get_articles_from_issue(year, issue_num)
            print(f"    Found {len(articles)} legal articles", file=sys.stderr)
            
            for article in articles:
                time.sleep(RATE_LIMIT_DELAY)
                full_article = fetch_article_full_text(article)
                
                if full_article and len(full_article.get("text", "")) > 50:
                    yield normalize(full_article)


def fetch_updates(since: str) -> Iterator[dict]:
    """Fetch documents updated since a given date."""
    since_date = datetime.fromisoformat(since.replace("Z", "+00:00"))
    current_year = datetime.now().year
    
    # Only fetch from the year of the since date onwards
    for doc in fetch_all(start_year=since_date.year, end_year=current_year):
        yield doc


def bootstrap_sample(count: int = 15) -> list[dict]:
    """Fetch a sample of documents for testing."""
    samples = []
    seen_categories = set()
    seen_ids = set()  # Track seen article slugs to avoid duplicates

    # Fetch from recent years to get diverse samples
    current_year = datetime.now().year

    # First pass: find categories we want (especially Lois)
    priority_order = ["Lois", "Ordonnances-Lois", "Arrêtés Ministériels",
                     "Décisions Souveraines", "Ordonnances Souveraines"]

    for year in range(current_year, current_year - 4, -1):
        if len(samples) >= count:
            break

        print(f"Sampling year {year}...", file=sys.stderr)
        issues = get_journal_issues(year)

        # Take issues from end of year (where laws are typically published)
        sample_issues = issues[-15:] if len(issues) > 15 else issues

        for issue_num, date_text in reversed(sample_issues):
            if len(samples) >= count:
                break

            print(f"  Sampling Journal-{issue_num}...", file=sys.stderr)
            time.sleep(RATE_LIMIT_DELAY)

            articles = get_articles_from_issue(year, issue_num)

            # Group by category for diversity
            by_category = {}
            for art in articles:
                cat = art["category"]
                if cat not in by_category:
                    by_category[cat] = []
                by_category[cat].append(art)

            # Prioritize unseen categories, especially Lois
            for category in priority_order:
                if len(samples) >= count:
                    break
                if category in by_category and category not in seen_categories:
                    for article in by_category[category][:2]:
                        if len(samples) >= count:
                            break
                        if article["slug"] in seen_ids:
                            continue

                        time.sleep(RATE_LIMIT_DELAY)
                        full_article = fetch_article_full_text(article)

                        if full_article and len(full_article.get("text", "")) > 100:
                            samples.append(normalize(full_article))
                            seen_categories.add(category)
                            seen_ids.add(article["slug"])
                            print(f"    Sampled [{category}]: {article['title'][:50]}...", file=sys.stderr)

            # Fill remaining with any category
            for article in articles[:5]:
                if len(samples) >= count:
                    break
                if article["slug"] in seen_ids:
                    continue

                time.sleep(RATE_LIMIT_DELAY)
                full_article = fetch_article_full_text(article)

                if full_article and len(full_article.get("text", "")) > 100:
                    samples.append(normalize(full_article))
                    seen_ids.add(article["slug"])
                    print(f"    Sampled: {article['title'][:60]}...", file=sys.stderr)

    return samples


def main():
    parser = argparse.ArgumentParser(description="Monaco Official Journal fetcher")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Bootstrap command
    bootstrap_parser = subparsers.add_parser("bootstrap", help="Bootstrap sample data")
    bootstrap_parser.add_argument(
        "--sample", action="store_true", 
        help="Fetch sample data only (15 records)"
    )
    bootstrap_parser.add_argument(
        "--start-year", type=int, default=2020,
        help="Start year for full bootstrap"
    )
    bootstrap_parser.add_argument(
        "--end-year", type=int, default=None,
        help="End year for full bootstrap"
    )
    
    # Updates command
    updates_parser = subparsers.add_parser("updates", help="Fetch updates since date")
    updates_parser.add_argument("since", help="ISO date to fetch updates from")
    updates_parser.add_argument("--full", action="store_true", help="Fetch all records")
    
    args = parser.parse_args()
    
    if args.command == "bootstrap":
        SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
        
        if args.sample:
            samples = bootstrap_sample()
            print(f"\nFetched {len(samples)} sample records", file=sys.stderr)
            
            for i, doc in enumerate(samples):
                filepath = SAMPLE_DIR / f"sample_{i:03d}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(doc, f, ensure_ascii=False, indent=2)
                print(f"  Saved: {filepath.name} - {doc['title'][:50]}...")
            
            # Print stats
            if samples:
                avg_len = sum(len(d.get("text", "")) for d in samples) / len(samples)
                print(f"\nStatistics:")
                print(f"  Total samples: {len(samples)}")
                print(f"  Average text length: {avg_len:.0f} characters")
                categories = set(d.get("category") for d in samples)
                print(f"  Categories: {', '.join(sorted(categories))}")
        else:
            # Full bootstrap
            count = 0
            for doc in fetch_all(args.start_year, args.end_year):
                filepath = SAMPLE_DIR / f"doc_{count:05d}.json"
                with open(filepath, "w", encoding="utf-8") as f:
                    json.dump(doc, f, ensure_ascii=False, indent=2)
                count += 1
                if count % 100 == 0:
                    print(f"  Saved {count} documents...", file=sys.stderr)
            
            print(f"\nTotal documents: {count}")
    
    elif args.command == "updates":
        count = 0
        for doc in fetch_updates(args.since):
            print(json.dumps(doc, ensure_ascii=False))
            count += 1
        print(f"Total updates: {count}", file=sys.stderr)
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
