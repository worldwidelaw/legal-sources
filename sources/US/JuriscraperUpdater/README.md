# US/JuriscraperUpdater

Daily US court opinion fetcher using Free Law Project's [Juriscraper](https://github.com/freelawproject/juriscraper) library (BSD-2 licensed).

## Coverage

- **198 state court scrapers** (Supreme Courts, Appellate Courts across all 50 states + DC)
- **22 federal appellate scrapers** (Circuit Courts 1-11, DC, Federal Circuit)
- ~50% of courts accessible from any given IP (others return 403/SSL errors)

## How it works

1. Calls Juriscraper scrapers to get recent opinion metadata + PDF URLs
2. Downloads opinion PDFs from court websites
3. Extracts full text via `common.pdf_extract`
4. Normalizes into LDH standard schema

## Usage

```bash
python bootstrap.py bootstrap --sample   # 15 samples from known-good courts
python bootstrap.py bootstrap --full     # All accessible courts
python bootstrap.py test                 # Test which scrapers work
python bootstrap.py update --since 2026-04-01  # Same as --full (Juriscraper returns latest)
```

## Dependencies

- `juriscraper>=3.0` (BSD-2 licensed)
- `httpx` (installed with juriscraper)

## Licensing

- Juriscraper: BSD 2-Clause (commercial use permitted)
- Court opinions: US government works, not copyrightable

## Relation to other US sources

This source complements (does not replace) the CourtListener-based sources:
- `US/FederalCourts`, `US/FederalDistrictCourts`, `US/FederalSpecialtyCourts` — bootstrap via CL search API
- `US/{ST}-Courts` — per-state bootstrap via CL search API
- `US/CaselawAccessProject` — 6.7M historical opinions from Harvard
- **This source** — daily-fresh opinions scraped directly from court websites

## License

[Public domain](https://www.law.cornell.edu/uscode/text/17/105) — US government works under 17 U.S.C. § 105.
