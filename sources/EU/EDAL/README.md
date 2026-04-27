# EU/EDAL - European Database of Asylum Law

Asylum case law summaries from 22 EU member states, maintained by ECRE.

## Data Source

- **URL**: https://www.asylumlawdatabase.eu/en
- **Method**: HTML scraping (Drupal 7 site)
- **Authentication**: None
- **Coverage**: ~900 cases, frozen at March 2021
- **Crawl delay**: 10 seconds (per robots.txt)
- **Content**: Structured summaries (headnote, facts, decision/reasoning, outcome, observations)

## Usage

```bash
# Sample mode (15 documents, first 5 pages)
python3 bootstrap.py bootstrap --sample

# Full fetch (~900 cases, ~3 hours due to crawl delay)
python3 bootstrap.py bootstrap
```

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — ECRE/EDAL content licensed under Creative Commons Attribution 4.0.
