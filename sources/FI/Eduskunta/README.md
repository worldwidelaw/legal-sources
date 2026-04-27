# FI/Eduskunta - Finnish Parliament Open Data

## Overview

This source fetches parliamentary documents from the Finnish Parliament (Eduskunta) Open Data service at https://avoindata.eduskunta.fi.

## Data Coverage

- **Government Proposals (HE)**: Hallituksen esitys - legislative proposals from the government
- **Committee Reports**: Reports from parliamentary committees (e.g., MmVM, TaVM)
- **Parliamentary Proceedings**: Minutes, statements, and other legislative process documents

This source complements **FI/Finlex**, which covers enacted legislation. Eduskunta provides the legislative *process* documents that lead to enacted laws.

## API

The Eduskunta Open Data API provides access to several database tables:

- `VaskiData` - Main document repository (323K+ documents)
- `SaliDBPuheenvuoro` - Plenary session speeches (138K+ records)
- `SaliDBAanestys` - Voting records (42K+ records)
- `MemberOfParliament` - MP information

### Key Endpoints

- `GET /api/v1/tables/` - List available tables
- `GET /api/v1/tables/{tableName}/rows` - Query table with filtering
- `GET /api/v1/tables/{tableName}/batch` - Batch read with pagination

### Document Format

Documents are stored as XML in the `XmlData` column using Finnish Parliament's custom schema. Full text is contained within `<sis:KappaleKooste>` elements.

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records for validation
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## License

Open Data — Public Domain (no restrictions on reuse). See [Eduskunta Open Data](https://avoindata.eduskunta.fi).
