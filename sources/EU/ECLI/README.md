# EU/ECLI - European Case Law (ECLI Search Engine)

EU-wide case law from the Court of Justice of the European Union (CJEU) and General Court,
accessed via the CELLAR SPARQL endpoint and REST API using ECLI identifiers.

## Data Source

- **SPARQL endpoint**: https://publications.europa.eu/webapi/rdf/sparql
- **Full text**: CELLAR REST API with content negotiation (`Accept: text/html`)
- **Authentication**: None required
- **Coverage**: ~66,700 case law documents with ECLI identifiers
- **Courts**: Court of Justice (C), General Court (T), Civil Service Tribunal (F)
- **Complements**: EU/EUR-Lex (which focuses on legislation)

## How It Works

1. SPARQL query discovers case law documents with ECLI identifiers
2. Full text retrieved via `http://publications.europa.eu/resource/celex/{CELEX}` with HTML content negotiation
3. HTML converted to clean text using html2text

## Usage

```bash
# Sample mode (15 recent documents)
python3 bootstrap.py bootstrap --sample

# Full fetch
python3 bootstrap.py bootstrap --full
```
