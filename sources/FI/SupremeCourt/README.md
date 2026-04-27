# FI/SupremeCourt — Finnish Supreme Court (Korkein oikeus)

## Overview

Case law from the Finnish Supreme Court (Korkein oikeus / KKO).

## Data Source

- **Endpoint**: LawSampo SPARQL endpoint at `http://ldf.fi/lawsampo/sparql`
- **Coverage**: ~6,000 KKO judgments with full text
- **Date Range**: Historical through 2021 (based on LawSampo dataset updates)
- **License**: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)

## Data Format

Judgments are fetched via SPARQL queries from the LawSampo Linked Open Data service.
Full text is extracted from HTML stored in the `lss:html` property.

### Fields

| Field | Description |
|-------|-------------|
| `_id` | ECLI identifier with colons replaced by underscores |
| `ecli` | European Case Law Identifier (e.g., `ECLI:FI:KKO:2021:55`) |
| `title` | Case name/number |
| `text` | Full text of the judgment (extracted from HTML) |
| `date` | Decision date (YYYY-MM-DD) |
| `url` | Link to Finlex or KKO website |
| `court` | "Korkein oikeus" |
| `court_en` | "Supreme Court" |
| `language` | "fi" (Finnish) |

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch 12 sample records
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) (LawSampo / Semantic Finlex)

## Related

- [FI/SupremeAdministrativeCourt](../SupremeAdministrativeCourt/) — KHO judgments
- [FI/Finlex](../Finlex/) — Finnish legislation
- [LawSampo Project](https://seco.cs.aalto.fi/projects/lakisampo/en/)
