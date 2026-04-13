# GT/CC — Guatemala Corte de Constitucionalidad

Guatemala Constitutional Court decisions.

## Data Source

- **Portal**: https://jurisprudencia.cc.gob.gt/
- **API**: Elasticsearch-backed REST API at `coredataretriever/api/jurisprudencia/V1`
- **Documents**: ~65,000 constitutional court decisions (1986–present)
- **Format**: PDF documents with extractable text

## How It Works

1. Queries the search API with broad term to enumerate documents
2. Downloads individual PDFs from `jurisprudencia.cc.gob.gt/Sentencias/`
3. Extracts text using PyMuPDF (with PyPDF2/pdfminer fallbacks)

## Document Types

- Amparo (constitutional protection)
- Apelación de Sentencia de Amparo
- Inconstitucionalidad
- Dictamen
- Opinión Consultiva
- Exhibición Personal

## Requirements

- `requests`
- PDF extraction: `PyMuPDF` (fitz), `PyPDF2`, or `pdfminer.six`
