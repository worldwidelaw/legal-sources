import sys
import json
import logging
import re
from pathlib import Path
from datetime import datetime, timezone
from typing import Generator

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from common.base_scraper import BaseScraper
from common.http_client import HttpClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("legal-data-hunter")


class SourceScraper(BaseScraper):
    """Scraper for Romanian Constitutional Court (ccr.ro)"""

    def __init__(self):
        source_dir = Path(__file__).parent
        super().__init__(source_dir)
        self.client = HttpClient(
            base_url="https://www.ccr.ro",
            headers={
                **self._auth_headers,
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "ro-RO,ro;q=0.9,en;q=0.8",
            },
            verify=False,
        )

    def fetch_all(self) -> Generator[dict, None, None]:
        from bs4 import BeautifulSoup
        from common.pdf_extract import extract_pdf_markdown

        sections = [
            "/jurisprudenta/jurisprudenta-decizii-de-admitere/",
            "/jurisprudenta/decizii-relevante/",
            "/jurisprudenta/hotarari-de-admitere/",
            "/jurisprudenta/hotarari-relevante/",
        ]

        for section in sections:
            page = 1
            while True:
                resp = self.client.get(f"{section}?page={page}")
                soup = BeautifulSoup(resp.text, "html.parser")
                links = soup.select("a[href*='wp-content/uploads'][href$='.pdf']")

                if not links:
                    break

                for link in links:
                    pdf_url = link["href"].replace("#new_tab", "")
                    if not pdf_url.startswith("http"):
                        pdf_url = "https://www.ccr.ro" + pdf_url

                    pdf_resp = self.client.get(pdf_url)
                    text = extract_pdf_markdown(pdf_resp.content)

                    yield {
                        "pdf_url": pdf_url,
                        "title": link.get_text(strip=True),
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