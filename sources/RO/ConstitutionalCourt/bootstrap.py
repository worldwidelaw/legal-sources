import sys
import json
import hashlib
import logging
import re
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
logger = logging.getLogger("legal-data-hunter")


def solve_waf_challenge(html: str) -> Optional[str]:
    """Solve the ccr.ro SHA1 proof-of-work WAF challenge.
    Returns the cookie value 'res=<token><nonce>' or None if not a challenge page."""
    m = re.search(r"const a0_0x2a54=\['([^']+)','([^']+)','([^']+)'\]", html)
    if not m:
        return None
    arr = [m.group(1), m.group(2), m.group(3)]
    # Rotation: ++0x178 = 377, then while(--n) runs 376 times = 376 mod 3 shifts
    rot = 376 % len(arr)
    for _ in range(rot):
        arr.append(arr.pop(0))
    # arr[2] is the challenge token, arr[1] is 'array', arr[0] is 'res='
    challenge = arr[2]
    n1 = int(challenge[0], 16)
    for i in range(500000):
        h = hashlib.sha1((challenge + str(i)).encode()).digest()
        if h[n1] == 0xb0 and h[n1 + 1] == 0x0b:
            return f"res={challenge}{i}"
    return None


class SourceScraper(BaseScraper):
    """Scraper for Romanian Constitutional Court (ccr.ro)"""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        import requests
        self.session = requests.Session()
        self.session.verify = False
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ro-RO,ro;q=0.9,en;q=0.8",
        })
        self._waf_solved = False

    def _ensure_waf(self):
        """Solve WAF challenge if not already done."""
        if self._waf_solved:
            return
        import time
        resp = self.session.get("https://www.ccr.ro/jurisprudenta/jurisprudenta-decizii-de-admitere/", timeout=30)
        if resp.status_code == 503:
            cookie_val = solve_waf_challenge(resp.text)
            if cookie_val:
                name, val = cookie_val.split("=", 1)
                self.session.cookies.set(name, val, domain="www.ccr.ro", path="/")
                logger.info("WAF challenge solved")
                time.sleep(1)
            else:
                raise RuntimeError("Could not solve WAF challenge")
        self._waf_solved = True

    def _get(self, url: str):
        """GET with WAF handling."""
        import time
        self._ensure_waf()
        resp = self.session.get(url, timeout=30)
        if resp.status_code == 503:
            self._waf_solved = False
            self._ensure_waf()
            resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        time.sleep(1)
        return resp

    def fetch_all(self) -> Generator[dict, None, None]:
        from bs4 import BeautifulSoup
        from common.pdf_extract import extract_pdf_markdown

        sections = [
            "/jurisprudenta/jurisprudenta-decizii-de-admitere/",
            "/jurisprudenta/decizii-relevante/",
            "/jurisprudenta/hotarari-de-admitere/",
            "/jurisprudenta/hotarari-relevante/",
        ]

        seen_urls = set()

        for section in sections:
            logger.info(f"Scraping section: {section}")
            page = 1
            while True:
                url = f"https://www.ccr.ro{section}?page={page}"
                try:
                    resp = self._get(url)
                except Exception as e:
                    logger.error(f"Failed to fetch {url}: {e}")
                    break

                soup = BeautifulSoup(resp.text, "html.parser")
                links = soup.select("a[href*='wp-content/uploads'][href$='.pdf']")

                if not links:
                    logger.info(f"No more PDFs in {section} after page {page - 1}")
                    break

                page_had_new = False
                for link in links:
                    pdf_url = link["href"].replace("#new_tab", "")
                    if not pdf_url.startswith("http"):
                        pdf_url = "https://www.ccr.ro" + pdf_url

                    if pdf_url in seen_urls:
                        continue
                    seen_urls.add(pdf_url)
                    page_had_new = True

                    title = link.get_text(strip=True)
                    if title == "Citește mai mult":
                        title = ""

                    try:
                        pdf_resp = self._get(pdf_url)
                        pdf_filename = pdf_url.split("/")[-1].replace(".pdf", "").replace("#new_tab", "")
                        text = extract_pdf_markdown(
                            "RO/ConstitutionalCourt",
                            pdf_filename,
                            pdf_bytes=pdf_resp.content,
                        )
                    except Exception as e:
                        logger.warning(f"Failed to extract PDF {pdf_url}: {e}")
                        continue

                    yield {
                        "pdf_url": pdf_url,
                        "title": title,
                        "text": text,
                    }

                page += 1

    def fetch_updates(self, since: datetime) -> Generator[dict, None, None]:
        yield from self.fetch_all()

    def normalize(self, raw: dict) -> dict:
        filename = raw["pdf_url"].split("/")[-1].replace(".pdf", "").replace("#new_tab", "")
        date_match = re.search(r'/(\d{4})/(\d{2})/', raw["pdf_url"])
        date = f"{date_match.group(1)}-{date_match.group(2)}-01" if date_match else ""

        return {
            "_id": filename,
            "_source": "RO/ConstitutionalCourt",
            "_type": "case_law",
            "_fetched_at": datetime.now(timezone.utc).isoformat(),
            "id": filename,
            "title": raw.get("title", ""),
            "text": raw.get("text", ""),
            "date": date,
            "url": raw.get("pdf_url", ""),
        }


def main():
    scraper = SourceScraper()

    if len(sys.argv) < 2:
        print("Usage: python bootstrap.py [bootstrap|update] [--sample] [--sample-size N]")
        sys.exit(1)

    command = sys.argv[1]
    sample_mode = "--sample" in sys.argv
    sample_size = 10
    if "--sample-size" in sys.argv:
        idx = sys.argv.index("--sample-size")
        sample_size = int(sys.argv[idx + 1])

    if command == "bootstrap":
        if sample_mode:
            stats = scraper.run_sample(n=sample_size)
            print(f"\nSample complete: {stats.get('sample_records_saved', 0)} records saved to sample/")
        else:
            stats = scraper.bootstrap()
            print(f"\nBootstrap complete: {stats['records_new']} new, {stats['records_updated']} updated, {stats['records_skipped']} skipped")
    elif command == "update":
        stats = scraper.update()
        print(f"\nUpdate complete: {stats['records_new']} new, {stats['records_updated']} updated")
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
