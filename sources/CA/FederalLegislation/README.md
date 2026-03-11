# CA/FederalLegislation

Canadian Federal Laws and Regulations from the Justice Laws Website.

## Source

- **Website**: https://laws-lois.justice.gc.ca
- **Data Format**: XML (structured legislative text)
- **License**: Open Government Licence - Canada
- **Update Frequency**: Bi-weekly

## Coverage

- **Acts**: ~956 consolidated federal statutes (English and French)
- **Regulations**: ~4,834 consolidated federal regulations (English and French)
- **Languages**: Bilingual (English / French)
- **Timeframe**: Current consolidated versions

## API

The Justice Laws Website provides XML endpoints for all federal legislation:

- **Catalog**: `https://laws-lois.justice.gc.ca/eng/XML/Legis.xml`
  - Lists all Acts and Regulations with metadata
  - Includes links to full XML documents

- **Individual Documents**: `https://laws-lois.justice.gc.ca/eng/XML/{UniqueId}.xml`
  - Full structured XML with sections, paragraphs, etc.
  - Contains complete legislative text

## Data Model

Each record contains:
- `unique_id`: Document identifier (e.g., "A-1", "SOR-2024-123")
- `title`: Full document title
- `text`: Complete legislative text
- `language`: "eng" or "fra"
- `doc_type`: "act" or "regulation"
- `current_to_date`: Date the consolidation is current to
- `official_number`: Official chapter/SOR number
- `short_title`: Short title if applicable
- `in_force`: Whether the legislation is in force

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records
python bootstrap.py bootstrap --sample

# Full bootstrap (all Acts and Regulations)
python bootstrap.py bootstrap

# Incremental update (last 30 days)
python bootstrap.py update
```

## Notes

- All documents are publicly available under Open Government Licence
- XML contains detailed structural markup (sections, subsections, schedules)
- GitHub mirror available at: https://github.com/justicecanada/laws-lois-xml
