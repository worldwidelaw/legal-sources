# US/MN-CFBOpinions — Minnesota Campaign Finance & Public Disclosure Board — Advisory Opinions

Full-text advisory opinions of the **Minnesota Campaign Finance and Public
Disclosure Board (CFB)** and its predecessors (the State Ethics Commission /
Ethical Practices Board). Each opinion is the Board's written interpretation of
**Minn. Stat. ch. 10A** (Campaign Finance & Public Disclosure) and the related
lobbying, gift-ban, conflict-of-interest and economic-interest statutes,
requested by an official, candidate, committee, lobbyist or principal =
**doctrine**.

- **Publisher:** Minnesota Campaign Finance and Public Disclosure Board
- **Coverage:** Advisory Opinions AO6–AO471+ (1974–present), ~308 opinions
- **Type:** doctrine
- **Jurisdiction:** US-MN

## Access

No JavaScript, CAPTCHA, or authentication. A single listing page —
`https://cfb.mn.gov/citizen-resources/the-board/board-decisions/advisory-opinions/`
— is a table carrying each opinion's number, program (Campaign Finance /
Lobbying / Gift Ban / Economic Interest / …), subject summary, date issued and
requestor, plus a direct href to the opinion PDF at
`https://cfb.mn.gov/pdf/advisory_opinions/AO{N}.pdf`.

Modern opinions are born-digital (clean text layer); the oldest 1974–1990s
opinions are scanned images and fall back to OCR. Text extraction uses the
shared `common.pdf_extract._extract` backend chain
(opendataloader → pdfplumber → pypdf → OCR).

## Usage

```bash
python bootstrap.py bootstrap            # Full pull (all opinions)
python bootstrap.py bootstrap --sample   # ~12 samples (newest first)
python bootstrap.py test-api             # Connectivity + extraction test
```

## License

[Public Domain — Minnesota state government edict / public record](https://www.law.cornell.edu/uscode/text/17/105) — CFB advisory opinions are official public records of a Minnesota state agency interpreting statute (government-edict works); no attribution required, commercial use permitted.
