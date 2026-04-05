# AR/CNAT — Cámara Nacional de Apelaciones del Trabajo

Argentine National Labor Appeals Court case law, sourced from the SAIJ (Sistema Argentino de Información Jurídica) public API.

## Data source

- **API**: SAIJ REST JSON API at `https://www.saij.gob.ar/busqueda`
- **Coverage**: ~17,300+ labor law decisions
- **Tribunal filter**: `CAMARA NACIONAL DE APELACIONES DEL TRABAJO`
- **Full text**: Inline text (sumarios) + PDF extraction via pdfplumber (fallos)
- **Auth**: None required (open public data)

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch 15 sample records
python bootstrap.py bootstrap --sample

# Full bootstrap (all records)
python bootstrap.py bootstrap --full
```

## Schema

| Field | Description |
|-------|-------------|
| `_id` | SAIJ UUID |
| `title` | Case title (actor c/ demandado) |
| `text` | Full decision text |
| `date` | Decision date (ISO 8601) |
| `tribunal` | Court name |
| `materia` | Subject matter |
| `actor` | Plaintiff |
| `demandado` | Defendant |
| `magistrados` | Judges |
