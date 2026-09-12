# UN/DigitalLibrary — UN Digital Library

**Source:** [https://documents.un.org](https://documents.un.org)
**Data types:** legislation

UN General Assembly resolutions (`A/RES/...`) with full text.

## Access strategy

Discovery enumerates the deterministic symbol space `A/RES/{session}/{number}`
newest-first, walking a session until 15 consecutive numbers come back absent.
Gaps within a session are normal — sub-lettered resolutions such as
`A/RES/80/236/A-B` leave the plain number empty — hence the generous tolerance.

Full text comes from the ODS endpoint
`documents.un.org/api/symbol/access?s={symbol}&l=en&t=doc`, extracted with the
shared Word extractor (handles both DOCX and legacy OLE2 `.doc`), falling back
to `t=pdf` when the Word rendition is unparseable. ODS answers **HTTP 200 even
for symbols that do not exist**, returning a ~1.3 KB HTML stub, so absence is
detected from the payload shape rather than the status code.

`digitallibrary.un.org` is deliberately **not** used. That host now sits behind
an AWS WAF JS challenge which answers the search API, the OAI-PMH endpoint and
the homepage alike with `HTTP 202` / `x-amzn-waf-action: challenge` and an empty
body. The 202 is a challenge, not an async-search accept, so polling never
resolves it; it reproduces from residential IPs, so it is not a datacenter
block. See issue #1598.

## Coverage

Sessions 54 and later carry extractable full text (~7,000 resolutions).
Earlier sessions are skipped rather than stored as empty records: pre-1993
resolutions exist only as scanned PDFs with no text layer, and sessions ~48–53
ship WordPerfect 5.1 files that neither extractor can read.

## License

> ⚠️ **Commercial use unclear.** Verify terms before commercial redistribution.

[UN Terms of Use](https://www.un.org/en/about-us/terms-of-use)
