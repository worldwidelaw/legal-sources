# UK/Legislation — legislation.gov.uk

United Kingdom primary and secondary legislation from The National Archives.

## Data Source

- **URL:** https://www.legislation.gov.uk
- **API Documentation:** https://legislation.github.io/data-documentation/
- **License:** Open Government Licence v3.0

## Coverage

The UK legislation database contains 350,000+ documents including:

- **Primary Legislation:**
  - UK Public General Acts (ukpga): 12,000+ acts from 1801 to present
  - Acts of Scottish Parliament (asp): 400+
  - Welsh Parliament Acts (asc, anaw): 70+
  - Northern Ireland Acts (nia): 230+

- **Secondary Legislation:**
  - UK Statutory Instruments (uksi): 108,000+
  - Scottish Statutory Instruments (ssi): 11,000+
  - Welsh Statutory Instruments (wsi): 6,700+
  - Northern Ireland Statutory Rules (nisr): 18,000+

## API

The API is RESTful with content negotiation. Key endpoints:

- **Atom Feed:** `/{type}/data.feed?page={n}` - lists documents with metadata
- **XML Full Text:** `/{type}/{year}/{number}/data.xml` - CLML structured content
- **SPARQL:** For querying legislative metadata

## Data Format

Legislation is provided in Crown Legislation Markup Language (CLML) XML format,
which includes:
- Dublin Core metadata
- Structured document body (Parts, Sections, Schedules)
- Cross-references and amendments
- XHTML tables and MathML formulae

## License

[Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) — free reuse with attribution.

## Notes

- No authentication required
- Rate limits: Be reasonable, use delays between requests
- Use `/enacted` or `/made` version URLs for original text
- Use bare URLs for current consolidated version
