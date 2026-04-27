# BR/BrazilianCourtDecisionsHF

Brazilian Court Decisions from TJAL (Tribunal de Justiça de Alagoas).

## Source

HuggingFace dataset: [joelniklaus/brazilian_court_decisions](https://huggingface.co/datasets/joelniklaus/brazilian_court_decisions)

## Data

- ~3,234 court decisions with ementas (headnotes) and decision descriptions
- Labeled for case outcome prediction (partial/yes/no, unanimity)
- Fields: process_number, orgao_julgador, publish_date, judge_relator, ementa_text, decision_description, judgment_text/label, unanimity_text/label

## Access

No authentication required. Uses HuggingFace datasets-server rows API.

## Usage

```bash
python bootstrap.py test               # Test connectivity
python bootstrap.py bootstrap --sample # Fetch 15 samples
python bootstrap.py bootstrap          # Full fetch
```

## License

[Open Government Data](https://huggingface.co/datasets/joelniklaus/brazilian_court_decisions) — HuggingFace dataset based on official court decisions from TJAL (Tribunal de Justica de Alagoas).
