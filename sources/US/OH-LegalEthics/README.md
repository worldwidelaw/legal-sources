# US/OH-LegalEthics — Ohio Board of Professional Conduct Advisory Opinions

Full text of the **Advisory Opinions** of the **Ohio Board of Professional
Conduct** (formerly the Board of Commissioners on Grievances & Discipline),
an arm of the Supreme Court of Ohio.

Each opinion is the Board's nonbinding written interpretation, issued in
response to a prospective or hypothetical question, of the ethics rules
applicable to Ohio **judges and lawyers** — the Ohio Rules of Professional
Conduct, the Ohio Code of Judicial Conduct, the Rules for the Government of
the Bar, and (for pre-2007 opinions) the former Code of Professional
Responsibility. This is **doctrine**.

- **Corpus:** one continuous per-year numbered series (`YYYY-NNN`),
  1986–present, ~458 opinions.
- **Coverage:** Ohio issues both lawyer-ethics and judge-ethics opinions from
  a single Board, so this source combines what other states split into
  `US/{ST}-LegalEthics` and `US/{ST}-JudicialEthics`.
- **Jurisdiction:** US-OH.

## Access / recipe

No JavaScript, CAPTCHA or auth required.

1. Opinions are published on the Board's dedicated public WordPress site
   [ohioadvop.org](https://ohioadvop.org/). Its REST API enumerates one page
   per year:
   `/wp-json/wp/v2/pages?per_page=100&_fields=id,title,content`
2. Each year-titled page's `content.rendered` lists every opinion issued that
   year as an `<a href="...pdf">Op. YY-NNN</a>` link.
3. Each opinion PDF is **born-digital** (text layer) — extracted with PyMuPDF,
   **no OCR** needed, back to 1986.

## Usage

```bash
python bootstrap.py bootstrap            # Full pull (all opinions)
python bootstrap.py bootstrap --sample   # ~12 samples
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
python bootstrap.py test-api             # Connectivity + extraction test
```

## Distinct from

- **Ohio Ethics Commission** (executive branch — public officials, not
  attorneys/judges).
- **Ohio Attorney General opinions.**

## License

[Public Domain (U.S. state government edict)](https://www.law.cornell.edu/uscode/text/17/105) —
Advisory Opinions of the Ohio Board of Professional Conduct are official
written interpretations issued by an arm of the Supreme Court of Ohio and
published free to the public on ohioadvop.org (and bpc.ohio.gov) with no
login, paywall or terms prohibiting reuse. As edicts of a U.S. state
government body they carry no copyright (17 U.S.C. § 105 government-edicts
doctrine). Commercial use permitted.
