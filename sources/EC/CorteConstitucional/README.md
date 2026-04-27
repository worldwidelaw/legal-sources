# EC/CorteConstitucional — Ecuador Constitutional Court Decisions

## Overview
Fetches case law from Ecuador's Corte Constitucional via the SACC
(Sistema Automatizado de la Corte Constitucional) REST API.

- **~9,100+ decisions** with full text summaries
- **18 legal subject areas** (materias)
- **Multiple constitutional action types**: Extraordinaria de Protección,
  Incumplimiento, Inconstitucionalidad, etc.

## API Details
The API is an Angular SPA backend at:
`https://buscador.corteconstitucional.gob.ec/buscador-externo/rest/api/`

Parameters are encoded as: `{dato: base64(urlencode(json(params)))}`

### Key Endpoints
| Endpoint | Description |
|----------|-------------|
| `sentencia/100_BUSCR_SNTNCIA` | Search decisions |
| `sentencia/100_OBT_FCHA_SNTNCA` | Get decision detail |
| `catalogoSentencia/100_OBT_RSMN_CTLG` | Get filter catalogs |
| `expedienteDocumento/100_EXPEDNTE_DCMTO` | Get case documents |

### Document Storage
PDFs are served from HDFS storage at:
`https://esacc.corteconstitucional.gob.ec/storage/api/v1/10_DWL_FL/{base64_params}`

## Full Text
The `motivo` field contains the decision reasoning/summary (typically 400-3000 chars).
Full PDF documents are also available for download.

## Usage
```bash
python bootstrap.py test-api             # Test connectivity
python bootstrap.py bootstrap --sample   # Fetch sample data
python bootstrap.py bootstrap            # Full data pull
python bootstrap.py update 2026-01-01    # Incremental update
```

## License

[Open Government Data](https://www.corteconstitucional.gob.ec) — official decisions published by the Constitutional Court of Ecuador.
