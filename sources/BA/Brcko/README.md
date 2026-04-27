# BA/BrckoDistrikt - Brčko District Legislation

## Overview

Brčko District is an autonomous administrative unit in northeastern Bosnia and Herzegovina, established in 1999 following the Dayton Agreement. It has its own legal system, separate from the Federation of BiH and Republika Srpska, and is under the sovereignty of both entities.

This source fetches legislation from the Brčko District Assembly (Skupština Brčko distrikta).

## Data Source

- **Website**: https://skupstinabd.ba
- **Laws Directory**: https://skupstinabd.ba/3-zakon/ba/
- **Format**: PDF files in directory listing
- **Coverage**: ~200 laws covering all aspects of district governance
- **Languages**: Bosnian, Croatian, Serbian (trilingual)

## Access Method

The laws are organized in a directory listing structure:
- Each law has its own subdirectory
- Directories contain original law PDFs plus amendments
- Consolidated texts (prečišćeni tekst) are available for frequently amended laws

The fetcher prefers consolidated texts when available, otherwise uses the most recent PDF.

## Document Structure

PDF filenames follow the pattern: `NNBXX-YY Description.pdf`
- `NN`: Official Gazette issue number
- `B`: Section indicator (always B for laws)
- `XX`: Document number within issue
- `YY`: Year (2-digit)

Example: `007B34-19 Zakon o radu.pdf` = Gazette issue 7/2019, document 34, Labor Law

## Usage

```bash
# List available laws
python3 bootstrap.py list

# Fetch sample documents
python3 bootstrap.py bootstrap --sample

# Fetch specific count
python3 bootstrap.py bootstrap --sample --count 20
```

## Dependencies

- PyPDF2 (for PDF text extraction)
- requests

## License

Open government data — publicly accessible legislation.

## Notes

- Text extraction from PDFs preserves structure reasonably well
- Some older laws may have scanned PDFs with lower quality OCR
- Rate limiting: 1 request per second to be respectful to the server
