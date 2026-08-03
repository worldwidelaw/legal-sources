# US/ME-LegalEthics — Maine Board of Overseers of the Bar Ethics Opinions

Full text of the advisory **Ethics Opinions** of the **Professional Ethics
Commission** of the **Maine Board of Overseers of the Bar** (an agency of the
Maine Supreme Judicial Court).

Each opinion is the Commission's formal written interpretation and application
of the Maine Rules of Professional Conduct (formerly the Maine Bar Rules /
Code of Professional Responsibility), rendered on request to advise **lawyers**
whether described conduct is proper. This is **doctrine**.

- **Corpus:** one continuous numbered series (`Opinion #N`), 1979–present,
  ~228 opinions.
- **Jurisdiction:** US-ME.

## Access / recipe

No JavaScript, CAPTCHA or auth required.

1. The public index
   [`/attorney_services/ethics_opinions.html`](https://www.mebaroverseers.org/attorney_services/ethics_opinions.html)
   lists every opinion as an `opinion.html?id={id}` link with anchor text
   `#N. {Title}`.
2. Each opinion page is **born-digital HTML**; the opinion body is
   `div#maincontent2`, carrying `Opinion #N. {Title}`, `Date Issued: {Month
   DD, YYYY}`, the Question and the Commission's full Opinion.

## Usage

```bash
python bootstrap.py bootstrap            # Full pull (all opinions)
python bootstrap.py bootstrap --sample   # ~12 samples
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
python bootstrap.py test-api             # Connectivity + extraction test
```

## Notes

- **Data quality:** the source CMS renders some smart quotes/apostrophes as
  literal `?` (e.g. `Prosecutor?s`); this is upstream, not a decoding error,
  and is left as served. The full text is otherwise clean (no HTML, no
  U+FFFD).

## Distinct from

- **Maine Attorney General opinions** (`US/ME-AGOpinions`).

## License

[Public Domain (U.S. state government edict)](https://www.law.cornell.edu/uscode/text/17/105) —
Ethics Opinions of the Maine Board of Overseers of the Bar are official written
interpretations of the Maine Rules of Professional Conduct, published free to
the public on mebaroverseers.org with no login, paywall or terms prohibiting
reuse. As edicts of a U.S. state government body they carry no copyright
(17 U.S.C. § 105 government-edicts doctrine). Commercial use permitted.
