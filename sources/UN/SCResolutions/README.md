# UN/SCResolutions — Corpus of UN Security Council Resolutions

All 2,798 UNSC resolutions (1946-2025) from the Fobbe academic corpus on Zenodo.

## Strategy

1. Download metadata CSV (1.3 MB) with 74 fields per resolution
2. Download pre-extracted English TXT ZIP (8.1 MB)
3. Join metadata + text by doc_id
4. Total download: ~10 MB

## Coverage

- 2,798 resolutions (S/RES/1 through S/RES/2798)
- 1946 to 2025
- Pre-extracted clean text (no PDF processing needed)
- 74 metadata variables including coded topics and voting information
- CC-BY-4.0 license

## Usage

```bash
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py bootstrap            # All 2,798 resolutions
python bootstrap.py test-api             # API connectivity test
```

## Source

Zenodo record: https://zenodo.org/records/15154519 (concept: 7319780)
