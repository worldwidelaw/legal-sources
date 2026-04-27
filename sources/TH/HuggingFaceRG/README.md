# TH/HuggingFaceRG — Thailand Royal Gazette OCR Dataset

## Source
- **Dataset**: [obbzung/soc-ratchakitcha](https://huggingface.co/datasets/obbzung/soc-ratchakitcha) on HuggingFace
- **Coverage**: Royal Gazette (ราชกิจจานุเบกษา) pages from 1884 to present
- **OCR text**: Available for 2018–2025 (96 monthly JSONL files)
- **Metadata**: Available for all years (1884–2025)
- **License**: CC-BY 4.0

## Data Access
Direct HTTP download of JSONL files from HuggingFace (no `datasets` library needed):
- `meta/{year}/{year-month}.jsonl` — metadata (title, date, category, etc.)
- `ocr/iapp/{year}/{year-month}.jsonl` — OCR full text

Two OCR formats exist:
- 2018–2024: `data.ocr_results[].markdown_output`
- 2025+: `data.formatted_result.formatted_output`

## Usage
```bash
python bootstrap.py test              # Test connectivity
python bootstrap.py bootstrap --sample # Fetch 15 sample records
python bootstrap.py bootstrap          # Full bootstrap (all OCR files)
```

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — HuggingFace dataset of Royal Gazette pages. Underlying government publications are open data.
