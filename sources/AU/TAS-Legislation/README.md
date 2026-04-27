# AU/TAS-Legislation — Tasmania Legislation

Fetches Tasmanian Acts and Statutory Rules from legislation.tas.gov.au.

## Data Source

- **Publisher**: Office of Parliamentary Counsel, Tasmania
- **Portal**: https://www.legislation.tas.gov.au/
- **Format**: JSON index API + HTML full text
- **License**: CC BY 4.0
- **Auth**: None required

## How It Works

1. Queries the projectdata JSON API year-by-year (1839–present) for Acts and Statutory Rules
2. Filters out repealed legislation
3. Fetches full text HTML from `/view/whole/html/inforce/current/{id}`
4. Extracts clean text from HTML content div, strips navigation/footer

## Usage

```bash
python bootstrap.py test               # Connectivity test
python bootstrap.py bootstrap --sample # 15 sample records
python bootstrap.py bootstrap          # Full pull (all years)
python bootstrap.py update             # Latest from Atom feed
```

## Notes

- API returns single object (not array) when only 1 result for a year
- ID format: `act-YYYY-NNN` for Acts, `sr-YYYY-NNN` for Statutory Rules
- Atom feed at `/feed?id=crawler` provides recent updates

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — published by the Office of Parliamentary Counsel, Tasmania.
