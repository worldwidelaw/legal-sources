# CH/OpenCaseLaw

Swiss court decisions from OpenCaseLaw.ch / Entscheidsuche.

## Source

HuggingFace dataset: [voilaj/swiss-caselaw](https://huggingface.co/datasets/voilaj/swiss-caselaw)

## Data

- 963K+ court decisions from all Swiss cantons and federal courts
- Full text in German, French, Italian, Romansh
- Structured metadata: court, canton, docket number, legal area, judges
- Citation references between decisions

## Access

No authentication required. Uses HuggingFace datasets-server rows API.

## License

Data sourced from HuggingFace; original court decisions are public domain under Swiss law.

## Usage

```bash
python bootstrap.py test               # Test connectivity
python bootstrap.py bootstrap --sample # Fetch 15 samples
python bootstrap.py bootstrap          # Full fetch (963K rows - very large)
```
