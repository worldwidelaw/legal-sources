# US/LA-Legislation — Louisiana Revised Statutes and Codes (legis.la.gov)

**Source:** [https://legis.la.gov/legis/LawSearch.aspx](https://legis.la.gov/legis/LawSearch.aspx)
**Data types:** legislation

## Coverage

Louisiana is the only civil-law state in the United States, and its statutory law
is split between the Revised Statutes and several separately-numbered codes. Both
halves are crawled:

| Folder(s) | Citation prefix | Body of law | Approx. sections |
|-----------|-----------------|-------------|-----------------:|
| 77–130 | `RS` | Revised Statutes (54 titles) | 46,300 |
| 67 | `CC` | Civil Code | 2,517 |
| 68 | `CCP` | Code of Civil Procedure | 1,260 |
| 69 | `CCRP` | Code of Criminal Procedure | 795 |
| 71 | `CHC` | Children's Code | 1,202 |
| 70 | `CE` | Code of Evidence | 106 |
| 66 | `CA` | Constitution Ancillaries | 54 |
| 73 / 74 | `HRULE` / `JRULE` | House Rules / Joint Rules | 244 |

The codes were listed in `config.yaml` from the beginning but were not actually
crawled until issue #1200 — only folders 77–130 were in the folder table, so the
Civil Code and the procedural codes were absent from the corpus entirely.

## How it works

1. `Laws_Toc.aspx?folder=N` lists every section as a `Law.aspx?d=NNNNN` link.
2. `LawPrint.aspx?d=NNNNN` returns a clean print view; the citation comes from
   `<span id="LabelName">` and the body from `<span id="LabelDocument">`.
3. `date` is the most recent year in the section's own trailing source note
   (e.g. `Acts 1976, No. 307, §1` → `1976-01-01`), falling back to the crawl date
   for headings and preambles that carry no legislative history.

Rate limit: 1 request/second. No authentication.

## Usage

```bash
python bootstrap.py test-api             # connectivity check
python bootstrap.py bootstrap --sample   # 18 samples across RS and the codes
python bootstrap.py bootstrap            # full corpus -> data/records.jsonl
python bootstrap.py bootstrap-fast       # alias the fleet wrapper invokes
```

## License

[Public domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105)
