# UK/CMA - Competition and Markets Authority

## Data Source

The UK Competition and Markets Authority (CMA) publishes case information on GOV.UK:
- **URL**: https://www.gov.uk/cma-cases
- **API**: GOV.UK Content API (no authentication required)
- **License**: Open Government Licence v3.0
- **Rate Limit**: 10 requests/second

## Coverage

The CMA handles:
- **Mergers**: Reviews of company mergers for competition concerns
- **Markets**: Market studies and investigations
- **Consumer Enforcement**: Actions against unfair trading practices
- **CA98/Civil Cartels**: Anti-competitive agreements and abuse of dominance
- **Criminal Cartels**: Criminal prosecution of cartel conduct
- **Digital Markets Unit**: Oversight of major digital platforms
- **Regulatory References**: Appeals from sector regulators

## Data Format

Each record includes:
- `_id`: Case slug (e.g., "cloud-services-market-investigation")
- `title`: Case title
- `text`: Full text content (HTML stripped)
- `date`: Last update timestamp
- `case_type`: Type of case (mergers, markets, etc.)
- `case_state`: open or closed
- `market_sector`: Industry sectors involved
- `opened_date`/`closed_date`: Case lifecycle dates
- `attachment_count`: Number of PDF attachments

## Usage

```bash
# Test API connectivity
python bootstrap.py test

# Fetch sample records (15 cases)
python bootstrap.py bootstrap --sample

# Fetch more samples
python bootstrap.py bootstrap --sample --count 30
```

## License

[Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) — free reuse with attribution.

## API Details

1. **Search API** (`/api/search.json`): Lists cases with filtering
2. **Content API** (`/api/content/cma-cases/{slug}`): Full case content

The Content API returns HTML body content which is converted to plain text.
PDF attachments are linked but not downloaded (full decisions available in HTML body).
