# PY/CSJJurisprudencia — Paraguay Supreme Court Jurisprudence

**Source:** Corte Suprema de Justicia del Paraguay — Sistema de Información Jurisprudencial
**URL:** https://www.csj.gov.py/jurisprudencia/
**Data type:** Case law
**Coverage:** Supreme Court and appellate court decisions since 1995
**Records:** ~70,000 decisions
**License:** Open Government Data (Paraguay)

## How it works

The CSJ jurisprudencia portal is an ASP.NET MVC application with DataTables server-side pagination.

1. Establishes a session with the portal
2. Submits a search form with year criteria (iterates year by year)
3. Paginates results via the DataTables API (`/Jurisprudencias/GetData`)
4. Downloads PDF for each decision (`/home/DocumentoJurisprudencia?codigo=N`)
5. Extracts full text from PDF using PyPDF2

## Requirements

- Python 3.6+
- `requests`
- `PyPDF2` (for PDF text extraction)

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records
python bootstrap.py bootstrap --sample

# Full bootstrap (all years)
python bootstrap.py bootstrap
```
