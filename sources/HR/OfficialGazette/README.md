# HR/OfficialGazette - Croatian Official Gazette (Narodne novine)

## Overview

This source fetches Croatian legislation from **Narodne novine** (Official Gazette of the Republic of Croatia), the official publication for all Croatian laws, regulations, and official acts.

- **Website**: https://narodne-novine.nn.hr
- **Data types**: Legislation (laws, decrees, regulations)
- **Authentication**: None required
- **Coverage**: 1990 to present
- **Language**: Croatian (HRV)
- **License**: Open Government Data

## Data Access

Croatia has fully implemented the **European Legislation Identifier (ELI)** standard, providing structured access to legislation.

### Endpoints

| Endpoint | Description |
|----------|-------------|
| `/sitemap.xml` | Master sitemap index |
| `/sitemap_1_{year}_{issue}.xml` | Per-issue document sitemaps |
| `/eli/sluzbeni/{year}/{issue}/{doc}` | Document ELI URI |
| `/eli/sluzbeni/{year}/{issue}/{doc}/json-ld` | JSON-LD metadata |
| `/eli/sluzbeni/{year}/{issue}/{doc}/hrv/html` | Full text HTML |
| `/eli/sluzbeni/{year}/{issue}/{doc}/hrv/pdf` | PDF version |

### Rate Limits

- Maximum 3 requests per second (per official documentation)
- This scraper uses 2 requests/second to be conservative

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample records (10+)
python bootstrap.py bootstrap --sample

# Fetch all records (full bootstrap)
python bootstrap.py bootstrap

# Incremental update since last run
python bootstrap.py update
```

## Output Schema

Each record includes:

| Field | Description |
|-------|-------------|
| `_id` | Document ID (format: year/issue/doc_num) |
| `_source` | "HR/OfficialGazette" |
| `_type` | "legislation" |
| `title` | Document title in Croatian |
| `text` | **Full text of the legislation** |
| `date` | Publication date (ISO 8601) |
| `url` | ELI URI to original document |
| `year` | Publication year |
| `issue` | Gazette issue number |
| `doc_num` | Document number within issue |

## References

- [Data Access Documentation](https://narodne-novine.nn.hr/data_access.aspx)
- [ELI Register - Croatia](https://eur-lex.europa.eu/eli-register/croatia.html)
- [N-Lex Croatia](https://n-lex.europa.eu/n-lex/info/info-hr/index)
