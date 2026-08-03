# US/VA-EthicsOpinions — Virginia Conflict of Interest and Ethics Advisory Council (Formal Advisory Opinions)

Full text of the **formal advisory opinions** issued by the **Virginia Conflict
of Interest and Ethics Advisory Council**.

Under Va. Code title 30, chapter 56 (§ 30-355 et seq.), the Council issues
formal advisory opinions interpreting:

- the **State and Local Government Conflict of Interests Act** (Va. Code § 2.2-3100 et seq.),
- the **General Assembly Conflicts of Interests Act** (§ 30-100 et seq.), and
- the **lobbyist-disclosure** provisions (§ 2.2-418 et seq.).

Formal advisory opinions are public record and are the Council's official written
interpretation of the ethics statutes → classified as **doctrine**. (The Council's
*informal* advisory opinions are confidential and not published.)

## Source

- **Listing:** https://ethics.dls.virginia.gov/advisory-opinions.asp
- Each opinion is a direct, **born-digital PDF** with a real text layer (no OCR,
  no CAPTCHA, no authentication).
- Opinion numbers follow the `YYYY-F-NNN` form (e.g. `2016-F-001`), sometimes with
  a `.N` revision or trailing letter (`2015-F-004A`). A few older memos live under
  a `Formal Advisory Opinions/` subfolder.

## How it works

1. `GET /advisory-opinions.asp` and parse every `<a href="...pdf">`.
2. Parse the opinion number from the filename/caption.
3. Download each PDF and extract its text layer via the shared
   `common.pdf_extract` backend.
4. Parse the issue date from the first `Month DD, YYYY` in the opinion body,
   falling back to the opinion-number year.

## Coverage

The full set of formal advisory opinions the Council has published since its 2014
creation (~25 opinions, `2015-F-001` through `2023-F-002`). The Council issues only
a handful of formal opinions per year, so the corpus is small but authoritative.

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull
```

## License

[Public Domain (Commonwealth of Virginia Government Work)](https://www.law.cornell.edu/uscode/text/17/105) — formal advisory opinions of the Virginia Conflict of Interest and Ethics Advisory Council are official public records of the Commonwealth of Virginia, published for public use with no copyright restriction. Commercial use permitted; no attribution required.
