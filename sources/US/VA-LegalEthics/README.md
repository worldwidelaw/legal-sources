# US/VA-LegalEthics — Virginia State Bar: Legal Ethics Opinions (LEOs)

Advisory **Legal Ethics Opinions ("LEOs")** issued by the **Standing Committee
on Legal Ethics of the Virginia State Bar**. A LEO applies the Virginia Rules of
Professional Conduct to a hypothetical set of facts and states whether the
described conduct complies with or violates the ethics rules. Many LEOs are
formally approved by the Supreme Court of Virginia.

- **Type:** `doctrine` (official interpretation of the attorney-conduct rules)
- **Jurisdiction:** US-VA (Virginia)
- **Corpus:** ~350+ opinions, LEO Nos. ~183–1900, 1980–present
- **Full text:** yes — born-digital PDF text layer (PyMuPDF, no OCR)

## Source & access

The Virginia State Bar publishes its LEOs through a Telerik RadGrid
"LEO – RPC Index" at
`https://vsb.org/Site/Site/about/rules-regulations/leo-opinions.aspx`.

The grid is a **rule-cross-reference index** (one row per opinion × cited Rule of
Professional Conduct), so each opinion appears on several rows. The scraper
collects the **unique PDF hrefs** (`LEOs/{filename}.pdf`) across every page.
Pagination is driven by ASP.NET `__doPostBack` against the pager (page-number
anchors in the visible 10-page window, plus the "Next Pages" window-advance
control), carrying `__VIEWSTATE` / `__EVENTVALIDATION` forward on each POST.

Each opinion PDF is downloaded from
`https://vsb.org/common/Uploaded files/LEOs/{filename}.pdf` and its text layer
extracted with PyMuPDF. Filenames are inconsistently zero-padded
(`0847.pdf` vs `872.pdf`) and some carry letter suffixes (`0186A.pdf`), so the
exact filename is always taken from the index href, never constructed.

No JavaScript execution, no CAPTCHA, no auth.

## Distinct from other Virginia sources

- **US/VA-EthicsOpinions** — Virginia Conflict of Interest & Ethics Advisory
  Council (advises *public officials*; executive branch). This source is the
  *State Bar* advising *lawyers* on the Rules of Professional Conduct.
- Also distinct from Virginia Attorney General opinions.

Fifth source in the state-bar attorney-ethics vein after US/NC-, US/AZ-, US/TX-,
US/UT-LegalEthics.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample records (page 1 discovery)
python bootstrap.py bootstrap           # full pull (all pages)
```

## License

[Public Domain — U.S. Government / State Official Record](https://www.law.cornell.edu/uscode/text/17/105) — Legal Ethics Opinions of the Virginia State Bar (the agency of the Supreme Court of Virginia that regulates the practice of law in Virginia) are official public records, published on the State Bar website for public use with no copyright restriction. Commercial use permitted.
