# EU/EUIPO - European Union Intellectual Property Office

This data source fetches case law decisions from the EUIPO eSearch Case Law database.

## Data Coverage

- **Trademark decisions**: Opposition, cancellation, examination
- **Design decisions**: Invalidity, examination
- **Board of Appeal decisions**: Appeals from first-instance decisions
- **Total records**: ~342,000+ decisions

## API Access

The eSearch Case Law API provides JSON access to decision metadata:
- Endpoint: `https://euipo.europa.eu/caselaw/officesearch/json/{lang}`
- Method: POST with JSON query
- No authentication required

Full text documents are available as PDF or DOC files linked from the metadata.

## Dependencies

```bash
pip install pdfplumber python-docx
```

## Usage

```bash
# Test API connectivity
python bootstrap.py

# Fetch sample data (15 documents)
python bootstrap.py bootstrap --sample

# Full bootstrap (100 documents)
python bootstrap.py bootstrap
```

## Schema

| Field | Description |
|-------|-------------|
| `_id` | Unique decision identifier (uniqueSolrKey) |
| `case_number` | Official case number |
| `decision_type` | OPPOSITION, CANCELLATION, EXAMINATION, APPEAL |
| `ip_right` | EUTM (trademark) or RCD (design) |
| `title` | Decision title |
| `text` | Full text of the decision |
| `date` | Decision date (ISO 8601) |
| `outcome` | Decision outcome |
| `legal_norms` | Applicable legal articles |

## License

[EUR-Lex legal notice](https://eur-lex.europa.eu/content/legal-notice/legal-notice.html) — EUIPO decisions are public documents, reuse authorised with attribution.
