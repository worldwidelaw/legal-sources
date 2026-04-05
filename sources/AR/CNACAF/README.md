# AR/CNACAF — Cámara Nacional de Apelaciones en lo Contencioso Administrativo Federal

Argentine Federal Administrative Appeals Court decisions via the SAIJ public API.

## Data Source

- **API**: SAIJ (saij.gob.ar) public JSON search API
- **Filter**: Tribunal = "CAMARA NACIONAL DE APELACIONES EN LO CONTENCIOSO ADMINISTRATIVO FEDERAL"
- **Auth**: None required
- **Coverage**: ~2,200+ administrative law decisions
- **Format**: JSON with inline full text for summaries

## Usage

```bash
python bootstrap.py test-api             # Test API connectivity
python bootstrap.py bootstrap --sample   # Fetch 15 sample records
python bootstrap.py bootstrap            # Full bootstrap
```

## Notes

- Uses the same SAIJ API as AR/SAIJ but filtered to CNACAF tribunal only
- Summaries (sumario) contain inline full text
- Full judgments (fallo) reference PDF files
- Rate limited to 2 requests/second
