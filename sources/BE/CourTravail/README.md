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

### Crawl shape

`robots.txt` lists ~14,650 daily sitemap indexes (1958 onwards), and every
sub-sitemap under them holds exactly **one** decision for the whole of Belgium.
A full walk is therefore one request per Belgian decision (~366K) before the
CT/TT filter is applied. juportal answers in ~0.1s, so the crawl is bound by our
own pacing:

- sub-sitemaps are fetched through a 4-thread pool, throttled to an aggregate
  20 req/s (`SITEMAP_RATE_PER_SEC`);
- content pages keep the serial rate limiter;
- the day cursor is checkpointed to `data/checkpoint.json` every 20 days, so a
  killed or timed-out run resumes where it stopped instead of re-walking from
  2026 (issue #1422 — the previous serial 1 req/s walk could not finish inside
  the fleet's 100h cap).

Use `python bootstrap.py status` to see how many days have been walked, and
`clear-checkpoint` to force a full re-walk.

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
