# EU/EIOPA Data Source

European Insurance and Occupational Pensions Authority (EIOPA) regulatory documents.

## Data Types

- **Guidelines**: Regulatory guidance for insurance and pensions sectors
- **Opinions**: EIOPA positions on regulatory matters
- **Decisions**: Administrative and regulatory decisions
- **Technical Standards**: Regulatory and implementing technical standards
- **Supervisory Statements**: Supervisory guidance and expectations
- **Reports**: Analysis and research publications

## Data Access

EIOPA provides access to documents via:
1. RSS feed for recent documents: `/node/4770/rss_en`
2. Document library with pagination: `/document-library_en`
3. Individual publication pages with PDF downloads

All documents are available as PDFs without authentication.

## Usage

```bash
# Test mode (3 documents)
python3 bootstrap.py

# Sample mode (15 documents)
python3 bootstrap.py bootstrap --sample

# Full bootstrap (50 documents)
python3 bootstrap.py bootstrap
```

## Output Schema

Each normalized document contains:
- `_id`: Unique document identifier (e.g., "EIOPA-abc123")
- `_source`: "EU/EIOPA"
- `_type`: "doctrine"
- `document_type`: Type of document (guideline, opinion, etc.)
- `title`: Document title
- `text`: Full text content extracted from PDF
- `date`: Publication date (ISO 8601)
- `url`: Link to publication page

## Rate Limiting

The fetcher respects a 2-second delay between PDF downloads to avoid overloading the server.

## Dependencies

Requires `pdfplumber` or `PyPDF2` for PDF text extraction.
