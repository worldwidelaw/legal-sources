# TR/MevzuatHF - Turkish Legislation (Hugging Face Dataset)

## Overview

This source fetches Turkish legislation from the Hugging Face dataset
`muhammetakkurt/mevzuat-gov-dataset`, which contains laws scraped from
the official Turkish government legislation database (mevzuat.gov.tr).

This is an alternative to TR/Mevzuat which is VPS-blocked.

## Dataset Details

- **Source**: https://huggingface.co/datasets/muhammetakkurt/mevzuat-gov-dataset
- **License**: MIT
- **Language**: Turkish
- **Records**: 907 laws
- **Size**: ~37 MB (parquet: ~14 MB)

## Data Structure

Each record contains:
- `url`: Link to law on mevzuat.gov.tr
- `Kanun Adı`: Law name
- `kanun_numarasi`: Law number
- `kabul_tarihi`: Acceptance date
- `resmi_gazete`: Official gazette info (issue number, date)
- `dustur`: Legal compendium info (volume, page)
- `maddeler`: Array of articles with full text

## Full Text Extraction

The full text is extracted from the `maddeler` (articles) array.
Each article contains:
- `madde_numarasi`: Article number
- `text`: Article content

All articles are concatenated to form the complete law text.

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records for validation
python bootstrap.py bootstrap --sample

# Full bootstrap (all 907 records)
python bootstrap.py bootstrap
```

## Requirements

```bash
pip install datasets
```
