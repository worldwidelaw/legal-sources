# US/AR-TaxDecisions — Arkansas DFA Act 896 Administrative Decisions & Legal Opinions

Full text of the Arkansas Department of Finance & Administration's (DFA) tax
adjudicative and interpretive corpus, published online under **Act 896 of 2015**:

- **Administrative Decisions** (`case_law`) — written, ALJ-signed decisions of
  the DFA **Office of Hearings & Appeals** resolving a taxpayer's protest of an
  assessment, refund denial, or other determination (~1,600 documents).
- **Legal Opinions** (`doctrine`) — **Revenue Legal Counsel** opinions
  interpreting Arkansas tax law — the Department's official written position
  (~600 documents).

Both families are public, taxpayer-identifier-redacted, **born-digital PDFs**
served from the Tyler Technologies "Act 896" portal at
`app.ar.tylertech.com/dfa/act896`.

## Access

No JavaScript, no CAPTCHA, no auth. Each family exposes a server-rendered
search endpoint:

- `/index.php/search/decision` — Administrative Decisions (case_law)
- `/index.php/search/opinion` — Legal Opinions (doctrine)

A GET search (`?query=<term>&search=Search&page=N`) renders a table whose rows
carry, per document, the **Docket Number**, **Release Date** (MM/DD/YYYY), and
one or more `.../download/<hash>.pdf` links. Results paginate via `?page=N`.

An empty query returns nothing and common stop-words are filtered, so the
scraper unions a set of tax-domain query terms (`tax`, `taxpayer`,
`assessment`, `protest`, `refund`, `sales`, `income`), pages through each until
a page yields no rows, and dedups globally by the PDF's hashed URL to enumerate
the full corpus. Text is extracted from each PDF via the shared OOM-hardened
`common.pdf_extract` helper.

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 samples
python bootstrap.py bootstrap            # Full pull
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
```

## Notes

- Multi-part documents ("… Part1 / Part2") appear as separate rows and become
  separate records keyed by their own PDF hash.
- `date` is the results table's Release Date cell (reliable temporal key).
- The `dfa.arkansas.gov` landing page requires browser-like request headers
  (returns 403 to a bare UA); the Act 896 portal itself serves normally. Verify
  neither host is datacenter-IP-gated on the VPS at launch.

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — Administrative Decisions of the DFA Office of Hearings & Appeals and Revenue Legal Counsel Legal Opinions are official Arkansas state-government works published under Act 896 of 2015, in the public domain under the government-edicts doctrine. Commercial use permitted; no attribution required.
