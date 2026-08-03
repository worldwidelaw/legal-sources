# US/CA-FPPC — California Fair Political Practices Commission (Advice Letters & Commission Opinions)

Full text of the two published classes of the California Fair Political
Practices Commission's written interpretations of the **Political Reform Act**
(Cal. Gov. Code § 81000 et seq.):

- **Advice Letters** — the FPPC's written responses to specific requests for
  advice under Gov. Code § 83114. Formal (`A-`) letters confer immunity;
  informal (`I-`) letters provide guidance. ~13,000+ letters, 1970s–present.
- **Commission Opinions** — formal opinions adopted by the Commission under
  Gov. Code § 83114(a) construing a provision of the Act. ~150 opinions.

Both are the Commission's authoritative construction of the Act = **doctrine**.

## Source

- Landing page: https://www.fppc.ca.gov/the-law/advice-letters-and-commission-opinions/
- The site (Optimizely/EPiServer) exposes a server-rendered faceted search.
  Each publication type is enumerated by paginating
  `/search/?FacetPublicationType={Advice+Letters|Commission+Opinions}&FacetPageType=Document&page={N}&sortBy=relevance`
  (10 results/page). Each result card links a born-digital PDF under
  `/siteassets/documents/…` with a title `"{Requester} - {File No} - {Month DD, YYYY}"`.
- Full text comes from the PDF text layer via `common.pdf_extract` (OCR
  fallback for the oldest scans). No JavaScript, CAPTCHA or auth required.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (streams to data/records.jsonl)
```

## License

[Public Domain (California state government work / public record)](https://www.law.cornell.edu/uscode/text/17/105) — Advice letters and opinions of the California Fair Political Practices Commission are official public records of a California state agency interpreting state law (government-edict works, published by the FPPC under the Political Reform Act and the California Public Records Act). Commercial use permitted.
