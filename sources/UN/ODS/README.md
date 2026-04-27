# UN/ODS — UN Official Document System

Fetches UN official documents (GA resolutions, SC resolutions) with full text
from the UN Official Document System.

## Strategy

1. **Symbol enumeration**: Generate document symbols systematically
   - GA resolutions: `A/RES/{session}/{num}` (sessions 31-79+)
   - SC resolutions: `S/RES/{num}({year})` (1-2700+)
2. **PDF resolution**: Use ODS API (`/api/symbol/access`) to resolve symbols to PDF URLs (302 redirect)
3. **Text extraction**: Download PDFs and extract full text via pdfplumber
4. **Metadata parsing**: Extract title and date from PDF content

## Coverage

- **GA resolutions**: Sessions 31-79 (~300 per session, ~15,000 modern resolutions)
- **SC resolutions**: 1-2700+ (~2,700 resolutions since 1946)
- Language: English (6 UN languages available)
- Documents are public domain

## Usage

```bash
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py bootstrap            # Full pull (~18K+ documents)
python bootstrap.py test-api             # API connectivity test
```

## License

[UN Terms of Use](https://www.un.org/en/about-us/terms-of-use) — UN documents are generally public domain. Verify UN terms before bulk commercial redistribution.

## Notes

- No authentication required
- ODS search API requires auth, but symbol resolution is open
- UN Digital Library search is WAF-protected (bot challenge)
- Early GA sessions (1-30) used different symbol format: `A/RES/{num}({roman_session})`
- Rate limit: 1 request/second (self-imposed)
