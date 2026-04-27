# EG/LegalCorpus - Egyptian Legal Corpus

## Overview

This fetcher retrieves Egyptian legislation from the [dataflare/egypt-legal-corpus](https://huggingface.co/datasets/dataflare/egypt-legal-corpus) dataset on Hugging Face.

## Data Source

- **URL**: https://huggingface.co/datasets/dataflare/egypt-legal-corpus
- **License**: MIT
- **Language**: Arabic
- **Records**: 2,434 laws
- **Tokens**: 25M+ (GPT-4 cl100k_base encoding)

## Data Structure

Each record contains:

| Field | Description |
|-------|-------------|
| `text` | Full Arabic legal text (168-341K characters) |
| `law_name` | Official law/document identifier |
| `categories` | Hierarchical legal taxonomy (1-3 categories) |
| `tokens` | Token count for the document |

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records (15 documents)
python bootstrap.py bootstrap --sample

# Full bootstrap (all 2,434 records)
python bootstrap.py bootstrap
```

## Dependencies

```bash
pip install datasets
```

## Notes

- Dataset is pre-processed and clean
- All documents contain full text (not just metadata)
- Categories provide hierarchical legal classification
- No authentication required

## License

[MIT License](https://huggingface.co/datasets/dataflare/egypt-legal-corpus) — HuggingFace dataset of Egyptian legislation.
