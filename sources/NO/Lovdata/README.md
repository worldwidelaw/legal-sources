# NO/Lovdata - Norwegian Legislation

Norwegian laws and regulations from the official Lovdata database.

## Data Source

- **Provider**: Lovdata (lovdata.no)
- **API**: https://api.lovdata.no
- **License**: Norwegian Licence for Open Government Data (NLOD) 2.0
- **Authentication**: None required for bulk downloads

## Available Data

1. **gjeldende-lover.tar.bz2** (~5.8 MB) - Current consolidated laws
2. **gjeldende-sentrale-forskrifter.tar.bz2** (~20 MB) - Current central regulations
3. Historical archives available (lovtidend-avd1-*)

## API Endpoints

### Free (No Auth)
- `GET /v1/publicData/list` - List available bulk datasets
- `GET /v1/publicData/get/{filename}` - Download bulk dataset

### Authenticated (API Key Required)
- Real-time document access via `/v1/structuredRules/`
- Full-text search via `/v1/search`
- AI-powered features via `/v1/ai/`

## ELI Implementation

Norway implemented ELI (European Legislation Identifier) in September 2016.
URI template: `/eli/{jurisdiction}/{type}/{year}/{month}/{day}/{natural_identifier}/...`

## Usage

```bash
# List available datasets
python3 bootstrap.py list

# Fetch sample records
python3 bootstrap.py bootstrap --sample

# Fetch all records (outputs JSONL)
python3 bootstrap.py fetch
```

## Data Format

Documents are provided in XML-compatible HTML with structured elements
(chapters, paragraphs, sections) enabling machine-readable access.

## Contact

- ELI information: eli@Lovdata.no
- General: ltavd1@lovdata.no
