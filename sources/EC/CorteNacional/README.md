# EC/CorteNacional — Ecuador National Court of Justice

Case law from Ecuador's Corte Nacional de Justicia (National Court of Justice).

## Data

- **Type:** Case law (cassation and revision sentences)
- **Records:** ~14,000 sentences
- **Coverage:** All 8 specialized chambers (Civil, Criminal, Labor, Administrative, Tax, Family, Juvenile, Military/Police/Transit)
- **Language:** Spanish
- **Auth:** None required

## API

Uses the Función Judicial REST API at:
- Search: `POST https://api.funcionjudicial.gob.ec/BUSCADOR-SENTENCIAS-SERVICES/api/buscador-sentencias/query/sentencia/busqueda/busquedaPorFiltros`
- Documents: `GET https://api.funcionjudicial.gob.ec/CJ-DOCUMENTO-SERVICE/api/document/query/hba?code=...`

Full text is extracted from PDF documents using pdfminer.six.

## Usage

```bash
python bootstrap.py test-api             # Quick API test
python bootstrap.py bootstrap --sample   # Fetch 12 sample records
python bootstrap.py bootstrap            # Full pull (~14K records)
python bootstrap.py update 2026-01-01    # Incremental update
```

## Notes

- API intermittently returns 500 errors (reCAPTCHA validation); retries handle this
- `salaId` filter parameter is ignored by API; pagination covers all records across all salas
- PDF extraction requires pdfminer.six (`pip install pdfminer.six`)
