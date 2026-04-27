# BE/CourTravail - Belgian Labour Courts

Belgian Labour Courts case law from JUPORTAL (juportal.be).

## Coverage

### Labour Courts of Appeal (Cour du Travail / Arbeidshof)
- CTANT: Antwerp (Antwerpen / Anvers)
- CTBRL: Brussels (Brussel / Bruxelles)
- CTGND: Ghent (Gent / Gand)
- CTLIE: Liège (Luik)
- CTMNS: Mons (Bergen)

### Labour Tribunals - First Instance (Tribunal du Travail / Arbeidsrechtbank)
- TTANT, TTBRL, TTGND, TTLIE, TTMNS

## Data Source

- **URL**: https://juportal.be
- **Discovery**: ECLI sitemaps via robots.txt
- **Content**: Full text via `/content/ECLI:BE:<COURT>:YYYY:...`
- **Format**: HTML with structured ECLI metadata
- **Languages**: French, Dutch, German
- **Period**: 2017 onwards
- **License**: Open Government Data

## Usage

```bash
# Run connectivity test
python bootstrap.py test

# Fetch sample data (10+ records)
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Subject Matter

Belgian Labour Courts handle:
- Employment contracts and disputes
- Social security matters
- Collective labor agreements
- Work accidents and occupational diseases
- Discrimination in employment
- Dismissal and severance
- Trade union rights

## Notes

Labour court decisions are indexed via the JUPORTAL ECLI sitemap system.
The court codes starting with "CT" are appellate courts (Cour du Travail),
while codes starting with "TT" are first instance tribunals (Tribunal du Travail).

## License

[Belgian Open Government Data](https://data.gov.be/en/licence-conditions) — free reuse of Belgian public sector information.
