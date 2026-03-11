# DE/Niedersachsen - Niedersachsen State Law (VORIS)

## Source Information

- **Name**: Niedersächsisches Vorschrifteninformationssystem (NI-VORIS)
- **URL**: https://voris.wolterskluwer-online.de
- **Country**: Germany
- **Jurisdiction**: Niedersachsen (Lower Saxony)
- **Data Types**: Legislation, Regulations, Administrative Rules
- **Language**: German

## Description

VORIS is the official legal information system of Niedersachsen, operated by
Wolters Kluwer under contract with the state government. It provides access to:

- **Gesetze** - Laws passed by the Niedersachsen state parliament
- **Verordnungen** - Regulations issued by the executive branch
- **Verwaltungsvorschriften** - Administrative regulations and directives

All content is public domain under German law (§ 5 UrhG - amtliche Werke).

## Technical Implementation

### Access Method

The website uses a Drupal 11 CMS with JavaScript-driven navigation, requiring
browser-based scraping with Playwright.

### Approach

1. **Discovery**: Use search API to find parent documents
2. **TOC Extraction**: For each parent, extract section IDs from the TOC
3. **Section Collection**: Fetch each section's full text individually
4. **Combination**: Combine all sections into complete document text

### Requirements

- Python 3.8+
- Playwright (`pip install playwright && playwright install chromium`)
- BrowserScraper from `common/browser_scraper.py`

## Usage

```bash
# Test with 2 documents
python bootstrap.py

# Generate 12 sample documents
python bootstrap.py bootstrap --sample

# Generate 50 sample documents
python bootstrap.py bootstrap
```

## Schema

| Field | Type | Description |
|-------|------|-------------|
| _id | string | Unique ID (NI-{doc_id[:12]}) |
| _source | string | "DE/Niedersachsen" |
| _type | string | "legislation", "regulation", or "administrative_regulation" |
| title | string | Document title |
| text | string | Full document text (all sections combined) |
| date | string | Publication date (ISO 8601) |
| url | string | Link to original document |
| normtyp | string | Document type in German |
| abbreviation | string | Official abbreviation |
| gliederungs_nr | string | Classification number |
| section_count | int | Number of sections |

## Rate Limiting

- 1.5 seconds between requests
- 60-second timeout per request
- Headless browser mode by default

## Notes

- Documents are organized hierarchically with parent documents containing TOCs
- Individual sections (Abschnitt) have their own document IDs
- Print functionality uses JavaScript, so we collect sections individually
- Some documents may have dozens of sections
