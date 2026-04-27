# EU/EPO - European Patent Office Boards of Appeal

## Source Information

- **Name**: European Patent Office - Boards of Appeal Decisions
- **Country**: EU (European Union)
- **Type**: Case Law (Patent Appeals)
- **URL**: https://www.epo.org/en/law-practice/case-law-appeals

## Data Access

This source uses the **EPO Bulk Data Download Service (BDDS)**:
- **API**: https://publication-bdds.apps.epo.org/bdds/bdds-bff-service/prod/api
- **Product ID**: 21 (14.6 EPO Boards of Appeal decisions)
- **Format**: XML bulk download
- **Auth**: None required (free public access since January 2025)

## Coverage

- **Time range**: 1979 to present
- **Records**: ~51,000 decisions
- **Update frequency**: Twice yearly (March and September)
- **Languages**: DE, EN, FR (procedure language)

## Decision Types

The database includes decisions from various EPO Boards:
- **T** - Technical Boards of Appeal (patent validity, examination)
- **G** - Enlarged Board of Appeal (points of law)
- **J** - Legal Board of Appeal (procedural matters)
- **D** - Disciplinary Board of Appeal (professional representatives)
- **W** - Ex parte appeals

## Data Structure

Each decision contains:
- **ECLI**: European Case Law Identifier
- **Case number**: e.g., T0001/2020
- **Decision date**: Date of the decision
- **Board code**: Identifying the deciding board
- **Full text sections**:
  - Headnote (legal summary)
  - Catchwords (key terms)
  - Summary of Facts
  - Reasons for Decision
  - Order

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample records
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Check for updates
python bootstrap.py update
```

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — EPO Boards of Appeal decisions are available as open data under Creative Commons Attribution 4.0.

## Notes

- The bulk file is ~300MB compressed, ~1.1GB uncompressed
- Uses streaming XML parsing to handle the large file efficiently
- Full text is extracted from structured XML elements
