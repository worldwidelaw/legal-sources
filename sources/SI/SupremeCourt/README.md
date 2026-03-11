# SI/SupremeCourt - Slovenian Case Law Database

## Overview

This source fetches court decisions from Slovenia's official public case law database at [sodnapraksa.si](https://www.sodnapraksa.si).

The database is operated by the Supreme Court of the Republic of Slovenia and contains **283,450+** court decisions from multiple Slovenian courts.

## Data Sources

### Databases Available

| Code | Court | Description |
|------|-------|-------------|
| SOVS | Vrhovno sodišče | Supreme Court |
| IESP | Višja sodišča | Higher Courts (Courts of Appeal) |
| VDSS | Višje delovno in socialno sodišče | Higher Labor and Social Court |
| UPRS | Upravno sodišče | Administrative Court |
| SEU | Sodišče Evropske unije | Court of Justice of the EU |
| NEGM | Odmera nepremoženjske škode | Non-pecuniary Damage Assessment |
| SOSC | Strokovni članki | Expert Articles |
| SOPM | Pravna mnenja in stališča | Legal Opinions |

### Data Structure

Each court decision includes:

- **Jedro** (Core/Summary): Key points of the decision
- **Izrek** (Disposition): The ruling/order
- **Obrazložitev** (Reasoning): Full legal reasoning
- **ECLI**: European Case Law Identifier
- **Metadata**: Court, date, legal area, keywords

## Usage

```bash
# Fetch 15 sample records (recommended for testing)
python bootstrap.py bootstrap --sample

# Fetch all records (NOT recommended - 283K+ documents)
python bootstrap.py bootstrap --full
```

## Technical Details

- **Search Engine**: SOLR-based full-text search
- **Access Method**: HTML parsing (no API key required)
- **Rate Limiting**: 2 seconds between requests
- **Format**: HTML pages with structured content

## License

Open Government Data - freely reusable for commercial and non-commercial purposes.

**Required Attribution**:
> Javne informacije Slovenije, Vrhovno sodišče Republike Slovenije
> https://www.sodnapraksa.si

## Notes

- Constitutional Court decisions are NOT included (separate database at us-rs.si, currently Cloudflare protected)
- The database is updated regularly with new decisions
- Full text is available directly in HTML, no PDF extraction needed
