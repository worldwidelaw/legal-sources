# US/MI-MERC — Michigan Employment Relations Commission (Decisions & Orders)

Full text of published decisions and orders of the **Michigan Employment
Relations Commission (MERC)** — the state's quasi-judicial agency that
administers Michigan's labor-relations statutes: the Public Employment
Relations Act (PERA, 1965 PA 379), the Labor Mediation Act, and Act 312
compulsory arbitration for police and fire. MERC adjudicates
unfair-labor-practice (ULP) charges, representation / election petitions, and
unit-clarification petitions. Each decision resolves a specific contested case
= **case_law**.

## Source

- **Decision store:** https://gsaindexed.apps.lara.state.mi.us/MERC/
- **Agency:** https://www.michigan.gov/leo/bureaus-agencies/ber/michigan-employment-relations-commission
- **Coverage:** ~1,460 decisions, roughly 1994/1998–2015 (born-digital PDFs).

## How it works

The MERC historical corpus is published on an IIS **directory-browse** file
store. The tree layout is inconsistent across years (flat `/MERC/{YEAR}/`,
month-nested `/MERC/{YEAR}/{MM}/`, and extra-nested variants), so the scraper
**recursively walks** the `/MERC/` tree and collects every `*.pdf`. Each PDF is
one decision.

Full text is extracted with the shared `common.pdf_extract` extractor (the PDFs
are born-digital with a real text layer). Case number, parties, and (where
present) the decision date are parsed from the decision body. No auth, no
CAPTCHA.

> Note: the michigan.gov LEO index page that lists these decisions is
> Akamai/WAF-gated (HTTP 403), but the `gsaindexed` file store itself serves
> 200 to any browser UA, so the 1994–2015 corpus is fully retrievable.
> Post-2015 decisions live behind the WAF-gated LEO index and are not covered
> here.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull
```

## License

[Public Domain — U.S. state government edict](https://www.law.cornell.edu/uscode/text/17/105) — decisions of the Michigan Employment Relations Commission are official works of Michigan state government (edicts of a quasi-judicial government body) and are not subject to copyright under the government-edicts doctrine. Free to use, including commercially.
