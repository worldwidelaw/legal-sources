# US/AZ-AGOpinions — Arizona Attorney General Formal Opinions

Full text of formal legal opinions issued by the Arizona Office of the
Attorney General, published openly at
[azag.gov/opinions](https://www.azag.gov/opinions).

Each opinion answers a legal question posed by a public official (the
Legislature, a state agency, a county attorney, etc.) and constitutes an
authoritative advisory interpretation of Arizona law — i.e. **doctrine**.

## Access

- **Index:** the opinions view is a Drupal exposed-form filtered by year
  via `?field_date_posted_value=N`, where `N` is an index (1 = 2025,
  2 = 2024, … 15 = 2011). Arizona issues only ~5–14 formal opinions per
  year, so each year fits on a single page.
- **Detail:** `/opinions/iYY-NNN-rYY-NNN`. Each page exposes the title
  (`<h1 class="field--name-title">`), the issued date
  (`<time datetime>`), and the full opinion body in the Drupal
  `field--name-body` region. Text is extracted directly from HTML
  (a per-opinion PDF mirror also exists but is not needed).
- Online coverage is 2011–present (~150 opinions). No authentication,
  no CAPTCHA.

## Usage

```bash
python bootstrap.py test-api            # connectivity / parse check
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — Arizona
Attorney General opinions are official state government works in the
public domain. Commercial use permitted; no attribution required.
