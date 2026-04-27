# DK/Skattestyrelsen-Vejledning

Danish tax legal guidance (Den Juridiske Vejledning) from Skattestyrelsen.

## Data Source

- **Portal**: https://info.skat.dk/data.aspx?oid=124
- **Format**: HTML pages, no API — recursive crawl via `data.aspx?oid=` links
- **Auth**: None (public)
- **Content**: Full text HTML in `<div class="MPtext">` on leaf pages

## Coverage

~9,200 leaf sections across 24 chapters covering all Danish tax law areas.
Updated biannually (January/July). Language: Danish.

## Usage

```bash
python bootstrap.py test                # Connectivity test
python bootstrap.py bootstrap --sample  # Fetch 15 sample records
python bootstrap.py bootstrap           # Full crawl (~9,200 sections)
```

## License

[Open Data](https://info.skat.dk) — Danish tax guidance is publicly available from Skattestyrelsen.
