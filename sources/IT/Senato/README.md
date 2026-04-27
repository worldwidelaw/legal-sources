# IT/Senato — Senato della Repubblica

Italian Senate legislative documents (Disegni di Legge).

## Data Source

- **Portal:** https://dati.senato.it
- **SPARQL Endpoint:** https://dati.senato.it/sparql
- **GitHub Repository:** https://github.com/SenatoDellaRepubblica/AkomaNtosoBulkData

## Coverage

- **Legislatures:** 13-19 (1996-present)
- **Document Types:** Bills (DDL), committee reports, amendments
- **Total Records:** ~58,000 bills
- **Full Text:** Yes, via Akoma Ntoso XML

## License

[CC BY 3.0](https://creativecommons.org/licenses/by/3.0/) — attribution required.

## Technical Details

### Data Access Strategy

1. **Metadata:** SPARQL queries to dati.senato.it for DDL metadata (title, date, sponsors, status)
2. **Full Text:** GitHub raw content from AkomaNtosoBulkData repository
3. **Format:** Akoma Ntoso XML (international legal document standard)

### Key Fields

| Field | Description |
|-------|-------------|
| `idFase` | Unique phase ID (primary key) |
| `idDdl` | Bill ID |
| `titolo` | Bill title |
| `dataPresentazione` | Presentation date |
| `legislatura` | Legislature number |
| `statoDdl` | Current status |
| `descrIniziativa` | Sponsor names |

### GitHub Repository Structure

```
AkomaNtosoBulkData/
├── Leg13/
│   ├── Atto00012345/
│   │   ├── ddlpres/           # Presented bill text
│   │   │   └── 12345-ft.akn.xml
│   │   ├── ddlcomm/           # Committee version
│   │   └── ddlmess/           # Approved version
│   └── ...
├── Leg14/
...
└── Leg19/
```

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records (12 DDL with full text)
python bootstrap.py bootstrap --sample

# Full bootstrap (58K+ records - use with caution)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Notes

- Akoma Ntoso is an OASIS standard for legal documents in XML
- GitHub repository updated daily with new DDL
- SPARQL endpoint may be slow for large queries; use pagination
