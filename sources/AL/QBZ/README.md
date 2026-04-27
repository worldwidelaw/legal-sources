# AL/QBZ - Albanian Official Gazette

Fetches Albanian legislation from **Qendra e Botimeve Zyrtare** (Official Publishing Center).

## Data Source

- **Website**: https://qbz.gov.al
- **Official Name**: Qendra e Botimeve Zyrtare (Official Publishing Center)
- **Ministry**: Ministry of Justice of Albania
- **Publication**: Fletorja Zyrtare (Official Gazette)

## Access Method

The scraper uses the WebDAV/Alfresco Content Management System to browse and download legislation:

1. **Directory Browsing**: `/alfresco/webdav/Aktet/ligj/kuvendi-i-shqiperise/{year}/{month}/{day}/{number}/`
2. **PDF Download**: Documents are stored as PDF files in the `base/` subdirectory
3. **Text Extraction**: Full text extracted from PDFs using pdfplumber

## ELI Implementation

Albania implemented the European Legislation Identifier (ELI) in January 2023:

- **ELI URI Pattern**: `https://qbz.gov.al/eli/ligj/{year}/{month}/{day}/{number}`
- **Example**: `https://qbz.gov.al/eli/ligj/2024/01/25/1`

## Document Types

| Albanian Term | Translation | Description |
|---------------|-------------|-------------|
| ligj | Law | Primary legislation from Parliament |
| vendim | Decision | Governmental decisions |
| dekret | Decree | Presidential decrees |
| urdher | Order | Administrative orders |
| rregullore | Regulation | Regulatory acts |

## Coverage

- **Time Range**: 1990 to present
- **Language**: Albanian (SQI)
- **Update Frequency**: As published
- **Issuing Bodies**: Kuvendi i Shqiperise (Parliament), Presidenti (President)

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch 10 sample records for validation
python bootstrap.py bootstrap --sample

# Full bootstrap (all documents)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Requirements

- Python 3.8+
- pdfplumber (for PDF text extraction)
- requests

## License

Open government data — no authentication required, freely accessible.

## Notes

- Albania is an EU candidate country
- No authentication required (open government data)
- Rate limited to 2 requests/second to avoid overloading the server
- PDFs may contain scanned documents which require OCR (not implemented)
