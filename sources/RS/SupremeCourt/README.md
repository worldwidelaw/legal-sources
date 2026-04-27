# RS/SupremeCourt — Serbian Supreme Court

## Source Information

| Field | Value |
|-------|-------|
| Country | Serbia (RS) |
| Court | Supreme Court (Vrhovni sud) |
| Language | Serbian (Latin script) |
| Data Type | Case Law |
| Authentication | None |
| Platform | Drupal 7 + Apache SOLR |

## Description

This scraper fetches case law decisions from the Serbian Supreme Court (Vrhovni sud),
formerly known as the Supreme Court of Cassation (Vrhovni kasacioni sud).

The court is the highest court in the Republic of Serbia and handles:
- Criminal appeals and requests for protection of legality (Kzz)
- Civil revision appeals (Rev, Rev2, Prev)
- General requests for legality protection (Gzz)
- Unified legal positions (Uzp)
- Administrative matters

## Technical Details

### Data Access Method

1. **Search**: SOLR-based search at `/sr-lat/solr-search-page/results`
   - Filter: `court_type=sc` for Supreme Court decisions
   - Pagination: `page=0,1,2...` with configurable `results` per page
   - Sorting: `by_date_down` for newest first

2. **Detail Pages**: Drupal 7 nodes at `/sr-lat/{slug}`
   - Full text in `div.field-name-body` inside `article.node-court-practice`
   - Metadata in `<meta property="og:title">` and `<meta property="article:published_time">`
   - Node ID from `<link rel="shortlink">` or body class

### Case Types

| Prefix | Serbian | English |
|--------|---------|---------|
| Kzz | Zahtev za zaštitu zakonitosti | Request for protection of legality (criminal) |
| Kž | Krivična žalba | Criminal appeal |
| Rev | Revizija | Revision appeal (civil) |
| Rev2 | Revizija drugostepena | Second-instance revision |
| Prev | Predrevizija | Pre-revision appeal |
| Gzz | Građanski zahtev za zakonitost | Civil legality request |
| Uzp | Ujednačavanje prakse | Unified position |

## Usage

```bash
# Sample run (12 records)
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Update since last run
python bootstrap.py update
```

## Coverage

- **Volume**: ~13,000+ decisions
- **Time Range**: Various years, primarily recent decisions
- **Update Frequency**: Regular updates as new decisions are published

## Data Schema

| Field | Type | Description |
|-------|------|-------------|
| `_id` | string | Unique ID (RS/SupremeCourt/{node_id}) |
| `node_id` | string | Drupal node ID |
| `case_reference` | string | Case number (e.g., Kzz 293/2023) |
| `title` | string | Decision title |
| `text` | string | **Full text of the decision** |
| `date` | date | Decision date (YYYY-MM-DD) |
| `url` | string | Source URL |
| `case_type` | string | Type classification |
| `matter` | string | Legal matter (criminal, civil, etc.) |
| `published_at` | datetime | Publication timestamp |

## License

Open government data — publicly accessible court decisions.

## Notes

- Full text is extracted from the decision body, removing PDF download prompts
- Decisions are anonymized (parties referred to as AA, BB, etc.)
- Some decisions reference lower court cases (Apelacioni sud, Viši sud, etc.)
- The court database was previously at vk.sud.rs and is now at vrh.sud.rs
