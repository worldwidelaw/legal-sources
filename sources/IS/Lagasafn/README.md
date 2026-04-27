# IS/Lagasafn - Icelandic Consolidated Legislation

## Source Information

- **Name**: Lagasafn (Law Collection)
- **Country**: Iceland (IS)
- **URL**: https://www.althingi.is/lagasafn/
- **Data Types**: Legislation (consolidated statutes)
- **Language**: Icelandic
- **License**: Public Domain (official government publications)

## Description

Lagasafn is the official consolidated collection of all Icelandic statutes in force,
maintained by the Althingi (Parliament of Iceland). The collection is updated regularly
(several times per year) and numbered by legislative session/version (e.g., "156b" =
version b of the 156th legislative session, September 2025).

## Data Access

The Althingi provides multiple access methods:

1. **ZIP Archive** (used by this source):
   - URL: `https://www.althingi.is/lagasafn/zip/{version}/allt.zip`
   - Contains all laws as HTML files (~8.7 MB compressed)
   - Files named by year and law number: `YYYYNNN.html`

2. **Individual Law Pages**:
   - URL: `https://www.althingi.is/lagas/{version}/{YYYYNNN}.html`
   - Example: Constitution = `https://www.althingi.is/lagas/156b/1944033.html`

3. **PDF Format**: Available at `/lagasafn/pdf/{version}/allt_pdf.zip`

4. **SGML Format**: Available at `/lagasafn/zip/{version}/allt_sgml.zip`

## Community Projects

- **althingi-net/lagasafn-xml**: Converts HTML to structured XML
  - GitHub: https://github.com/althingi-net/lagasafn-xml
  - Provides pre-built XML files in `data/xml/{version}/` directory

- **althingi/lagasafn**: Unofficial mirror with Markdown conversion
  - GitHub: https://github.com/althingi/lagasafn

## Notable Laws

- **Constitution (Stjórnarskrá)**: 1944/33 - `1944033.html`
- **General Penal Code**: 1940/19 - `1940019.html`
- **Civil Procedure Act**: 1991/91 - `1991091.html`

## Usage

```bash
# Fetch sample records
python3 bootstrap.py bootstrap --sample

# Show source info
python3 bootstrap.py info

# Fetch all records (outputs JSON to stdout)
python3 bootstrap.py fetch
```

## License

Open government data, public domain — official government publications under Icelandic law.

## Technical Notes

- HTML files use ISO-8859-1 encoding
- Law numbers in filenames have leading zeros (3 digits)
- Version numbers follow format: {session_number}{letter}
- Current version: 156b (September 2025)
