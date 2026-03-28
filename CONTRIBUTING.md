# Contributing to World Wide Law

Thank you for helping make the world's open legal data more accessible. This guide covers how to add a new data source, improve an existing collection script, or report issues.

All sources in this repository must be **open data** -- publicly available legal information from official government portals. We always prefer API and bulk download access over web extraction.

## Who Is This Guide For?

Jump to the section that fits you:

| I am... | Go to... |
|---|---|
| **Developer** | [Build a new script](#2-build-a-new-collection-script) · [Fix existing script](#3-fix-or-improve-an-existing-script) · [Add retrieve script](#4-add-a-retrieve-script) |
| **Government official / jurisdiction lead** | [Submit a data source](#1-submit-a-data-source-no-coding-required) (no coding required) |
| **Lawyer / legal researcher** | [Validate data quality](#5-validate-data-quality-for-lawyers-and-legal-researchers) · [Submit a data source](#1-submit-a-data-source-no-coding-required) |

New to open source? Look for [`label:good-first-issue`](https://github.com/worldwidelaw/legal-sources/issues?q=label%3Agood-first-issue) issues.

## Ways to Contribute

### 1. Submit a Data Source (No Coding Required)

Know about a legal data portal that isn't covered? [Open a "New Source" issue](https://github.com/worldwidelaw/legal-sources/issues/new?template=new-source.yml) with:
- Country and source name
- URL to the portal
- What kind of data it has (legislation, case law, gazette)
- Whether it has an API, bulk download, or only a website
- Whether access is open or requires registration

**Government officials and jurisdiction leads:** you don't need to write any code. Your knowledge of your country's official portals, APIs, and bulk download endpoints is the most valuable contribution you can make. Even a rough description (e.g. "Italy publishes gazette data at X, requires Y registration") is enough to get a developer started. Just open an issue.

### 2. Build a New Collection Script

#### Prerequisites

```bash
git clone https://github.com/worldwidelaw/legal-sources.git
cd legal-sources
pip install -r requirements.txt
```

#### Steps

1. **Find a source to build**: Run `python runner.py next` or pick from the [issues](https://github.com/worldwidelaw/legal-sources/issues?q=label%3Anew-source).

2. **Create the source directory**:
   ```bash
   mkdir -p sources/{CC}/{SourceName}
   ```

3. **Copy the templates**:
   ```bash
   cp templates/scraper_template.py sources/{CC}/{SourceName}/bootstrap.py
   cp templates/config_template.yaml sources/{CC}/{SourceName}/config.yaml
   ```

4. **Fill in `config.yaml`** with:
   - Source name, URL, country code
   - Data types (legislation, case_law)
   - Auth requirements (none, api_key, oauth2)
   - Rate limit settings

5. **Implement `bootstrap.py`** with three methods:
   - `fetch_all()` — Yields all documents (for initial bootstrap)
   - `fetch_updates(since)` — Yields documents modified since a date
   - `normalize(raw)` — Transforms raw API/HTML data into the standard schema

6. **Generate sample data**:
   ```bash
   python runner.py sample {CC}/{SourceName}
   ```
   This should save 10+ sample JSON documents in `sources/{CC}/{SourceName}/sample/`.

7. **Write a README.md** for the source explaining:
   - What the data source is
   - How the API/portal works
   - Any quirks or limitations

8. **Submit a PR** with your changes.

#### Script Interface

Every collection script inherits from `common.base_scraper.BaseScraper` and must implement:

```python
class MyScraper(BaseScraper):
    def fetch_all(self):
        """Yield all documents from the source."""
        ...

    def fetch_updates(self, since: datetime):
        """Yield documents modified since `since`."""
        ...

    def normalize(self, raw: dict) -> dict:
        """Transform raw data into the standard schema."""
        return {
            "_id": raw["unique_id"],
            "_source": self.source_id,
            "_type": "legislation",  # or "case_law"
            "title": raw["title"],
            "text": raw["full_text"],
            "date": raw["publication_date"],
            "url": raw["source_url"],
        }
```

### 3. Fix or Improve an Existing Script

1. Check the [issues](https://github.com/worldwidelaw/legal-sources/issues) for bug reports or data quality issues.
2. Test locally: `python runner.py sample {CC}/{SourceName}`
3. Make your fix.
4. Regenerate samples to verify: `python runner.py sample {CC}/{SourceName}`
5. Submit a PR.

### 4. Add a Retrieve Script

Retrieve scripts resolve human-readable legal references (like "article 1240 code civil") to specific documents in the dataset.

```bash
cp templates/retrieve_template.py sources/{CC}/{SourceName}/retrieve.py
```

Run `python runner.py retrieve-next` to find sources that need retrieve scripts.

Test with: `python runner.py retrieve-test {CC}/{SourceName}`

### 5. Validate Data Quality (For Lawyers and Legal Researchers)

You don't need to write code to make a meaningful contribution. Legal professionals can help by:

- **Reviewing sampled documents**: Check that `sources/{CC}/{SourceName}/sample/` files have correct titles, dates, and text — and flag errors in an issue.
- **Improving legal reference resolution**: If you know how citations work in your jurisdiction (e.g. "§ 242 BGB" in Germany, or "C. civ., art. 1240" in France), open an issue or comment on the relevant source to help us improve `retrieve.py`.
- **Identifying coverage gaps**: Know of important case law databases, official gazettes, or consolidated legislation sources that aren't listed? [Open a "New Source" issue](https://github.com/worldwidelaw/legal-sources/issues/new?template=new-source.yml).
- **Flagging data quality issues**: Missing decisions, truncated texts, wrong classification (legislation vs. case law)? [Open a data quality issue](https://github.com/worldwidelaw/legal-sources/issues/new?template=data-quality.yml).

No GitHub account? Email [zacharie@goodlegal.fr](mailto:zacharie@goodlegal.fr) and we'll file the issue for you.

## PR Checklist

Before submitting a pull request:

- [ ] `config.yaml` has correct metadata (country, data types, auth, URL)
- [ ] `bootstrap.py` implements all three methods
- [ ] Sample directory has 10+ documents
- [ ] `README.md` documents the data source
- [ ] No secrets or API keys in the code (use `.env.template` for required credentials)
- [ ] Rate limiting is configured appropriately (be respectful of government servers)

## Contributor License Agreement (CLA)

All contributors must sign our [Contributor License Agreement (CLA)](CLA.md) before their pull requests can be merged. The CLA bot will automatically prompt you on your first PR.

**Why?** This project is dual-licensed: open source under AGPL-3.0, and separately available under a commercial license. The CLA ensures all contributions can be included in both licensing models.

Signing is simple -- just comment on your PR with:
> I have read the CLA Document and I hereby sign the CLA

## License

All contributions to this project are licensed under the [GNU Affero General Public License v3.0 (AGPL-3.0)](LICENSE). By submitting a contribution, you agree that your work will be licensed under this license.

## Code of Conduct

- Be respectful of rate limits — these are government servers, not your stress test target
- Always use appropriate User-Agent strings
- If a source requires authentication, document it in `.env.template` but never commit actual credentials
- Always prefer official APIs and bulk downloads over web extraction
- Only include open data sources -- publicly available legal information
