# CA/SupremeCourt - Supreme Court of Canada Decisions

## Source Information

- **Name**: Supreme Court of Canada Decisions
- **Country**: Canada (CA)
- **URL**: https://decisions.scc-csc.ca
- **Data Type**: Case Law
- **Authentication**: None (Open Access)

## Coverage

- **Time Range**: 1970 to present (full text); older cases may have limited content
- **Volume**: ~15,500+ decisions
- **Languages**: Bilingual (English and French versions available)

## Data Access Strategy

### Bootstrap (Full Fetch)
1. Iterate through years from current year back to 1970
2. For each year, fetch the navigation page to get all case item IDs
3. For each case, fetch the full HTML content from the iframe endpoint
4. Parse HTML to extract metadata and full judgment text

### Updates (Incremental)
1. Use the RSS feed at `/scc-csc/scc-csc/en/rss.do`
2. The feed contains recently published and updated decisions
3. Parse the feed to get item IDs and fetch full content for new cases

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `/scc-csc/scc-csc/en/{year}/nav_date.do?iframe=true` | Year navigation page with case links |
| `/scc-csc/scc-csc/en/item/{id}/index.do?iframe=true` | Full case content (iframe version) |
| `/scc-csc/scc-csc/en/rss.do` | RSS feed for recent decisions |

## Fields Extracted

| Field | Description |
|-------|-------------|
| `item_id` | Unique identifier for the case on SCC website |
| `title` | Case name (e.g., "R. v. Smith") |
| `neutral_citation` | Neutral citation (e.g., "2024 SCC 1") |
| `case_number` | Court docket number |
| `decision_date` | Date of the decision (YYYY-MM-DD) |
| `judges` | Panel of judges |
| `appealed_from` | Province/court appealed from |
| `subjects` | Subject areas of law |
| `text` | Full text of the judgment |

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records for validation
python bootstrap.py bootstrap --sample

# Full bootstrap (all decisions since 1970)
python bootstrap.py bootstrap

# Incremental update (last 30 days)
python bootstrap.py update
```

## Rate Limiting

- 1 request per second with burst of 3
- Be respectful of the court's server resources

## Notes

- The full text is extracted from HTML pages served via iframes
- Some older cases may have less detailed metadata
- PDF versions are also available but HTML is used for text extraction
- The RSS feed provides both new decisions and updates to existing decisions (translations, corrections)
