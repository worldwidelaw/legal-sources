# US/NE-EthicsOpinions — Nebraska NADC Advisory Opinions

Full text of the advisory opinions of the **Nebraska Accountability and
Disclosure Commission (NADC)**, interpreting the Nebraska Political
Accountability and Disclosure Act (NPADA — Neb. Rev. Stat. ch. 49): the state
**conflict-of-interest, campaign-finance and lobbying** provisions. Each
advisory opinion is the Commission's authoritative written interpretation,
issued on request and published as a public record → `doctrine`.

## Source & access

`nadc.nebraska.gov` is a server-rendered Drupal site (no CAPTCHA, no auth, no
JavaScript engine required).

1. **Enumeration:** the index `https://nadc.nebraska.gov/advisory-opinions`
   lists every opinion as `/advisory-opinion-{NNN}`. Numbers are 3-digit
   zero-padded sequential integers, contiguous **001–206** (no gaps).
2. **Full text:** each opinion page is a Drupal node whose labelled fields
   carry the opinion number, Date Adopted, Subject (Conflict of Interest /
   Campaign Finance / Lobbying), "Requested by" and Summary, and whose
   `<div class="field--name-body">` holds the complete opinion text as clean
   born-digital HTML. When the HTML body is thin (a few of the newest opinions,
   ~203–206), the full text is an attached PDF at
   `/sites/default/files/doc/Advisory Opinion-{NNN}.pdf`, read via
   `common.pdf_extract` (text layer; OCR fallback for scans).

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (206 opinions)
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) —
advisory opinions of the Nebraska Accountability and Disclosure Commission are
official public records of the State of Nebraska, published for public use with
no copyright restriction. Commercial use permitted.
