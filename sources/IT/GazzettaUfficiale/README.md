# IT/GazzettaUfficiale - Italian Official Gazette

Data source for Italian legislation from the **Gazzetta Ufficiale della Repubblica Italiana** (Official Gazette of the Italian Republic).

## Source Information

- **Country**: Italy (IT)
- **URL**: https://www.gazzettaufficiale.it
- **Data Type**: Legislation
- **Language**: Italian
- **Authentication**: None required (Open Government Data)
- **License**: IODL 2.0 (Italian Open Data License)

## Data Access Method

Uses the Gazzetta Ufficiale's ELI (European Legislation Identifier) endpoints:

1. **Gazette Issues**: `/eli/gu/{yyyy}/{mm}/{dd}/{issue}/sg/html` - Lists all acts in a gazette issue
2. **Act Details**: `/atto/vediMenuHTML` - Shows act structure with article links
3. **Article Text**: `/atto/serie_generale/caricaArticolo` - Fetches individual article full text

Italy implemented ELI in 2014, making legislation accessible via standardized URIs.

## Document Types

The gazette contains various act types:
- **LEGGE** - Laws
- **DECRETO LEGISLATIVO** - Legislative Decrees
- **DECRETO-LEGGE** - Decree-Laws
- **DECRETO DEL PRESIDENTE DELLA REPUBBLICA** - Presidential Decrees
- **DECRETO MINISTERIALE** - Ministerial Decrees

## Data Model

Each record contains:
- `doc_id`: Unique identifier (codiceRedazionale, e.g., "24G00164")
- `title`: Act title in Italian
- `text`: **Full text of the legislation** (mandatory)
- `date`: Document date (ISO 8601)
- `url`: URL to the official source
- `act_type`: Type of legislative act
- `gazette_date`: Date of gazette publication
- `gazette_number`: Gazette issue number

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample records (for validation)
python bootstrap.py bootstrap --sample

# Full bootstrap (all available records)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Rate Limiting

Conservative rate limiting at 1 request/second to respect server resources.

## License

[Italian Open Data License v2.0 (IODL 2.0)](https://www.dati.gov.it/content/italian-open-data-license-v20) — commercial use permitted with attribution.

## Notes

- Serie Generale available from 1986 to present
- Full text extracted from HTML `<pre>` tags in article pages
- ELI metadata embedded in HTML pages provides structured information
- Related source: Normattiva (www.normattiva.it) provides consolidated legislation texts
