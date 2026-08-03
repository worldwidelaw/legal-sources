# IN/HighCourtAWS — Indian High Court Judgments (AWS Open Data)

## Overview

Fetches Indian High Court judgments from the AWS Open Data Registry.
The dataset contains ~16.7 million judgments from 25 High Courts across India,
dating back to 1950.

## Data Source

- **Bucket**: `s3://indian-high-court-judgments` (ap-south-1)
- **Registry**: https://registry.opendata.aws/indian-high-court-judgments/
- **License**: CC-BY-4.0
- **Updates**: Quarterly

## Strategy

1. Lists pre-extracted full-text shards from `derived/landlit-v2/texts/*.jsonl.gz`.
2. Streams and gunzips each JSONL shard, yielding one full-text judgment per line.
3. Joins each text record to `metadata/json/year=YYYY/court=X_Y/bench=ZZZ/*.json` for title, court, CNR, date, and PDF URL metadata.
4. Derives `cnr_number` and ISO `date` from the judgment filename to avoid validator drops when HTML metadata is sparse.
5. Uses PDF extraction only when explicitly launched with `--include-pdf-fallback`, after the text layer has been exhausted for the selected shard.

## S3 Structure

```text
derived/landlit-v2/texts/data__tar__year=YYYY__court=X_Y__bench=ZZZ__data.tar.jsonl.gz
metadata/json/year=YYYY/court=X_Y/bench=ZZZ/CNRXXX_N_YYYY-MM-DD.json
data/pdf/year=YYYY/court=X_Y/bench=ZZZ/CNRXXX_N_YYYY-MM-DD.pdf  # fallback only
```

## Usage

```bash
python bootstrap.py test
python bootstrap.py bootstrap --sample --sample-size 15
python bootstrap.py bootstrap --year-range 2023 --court 36_29
python bootstrap.py bootstrap --year-range 2020-2023 --court 36_29
python bootstrap.py bootstrap --year-range 2023 --court 36_29 --include-pdf-fallback
python bootstrap.py coverage --year-range 2023 --court 36_29
python bootstrap.py coverage --year-range 2023 --court 36_29 --count-text-records
```

## Sharded launch pattern

Fan out workers by `(year, court)` because both the derived text layer and metadata tree are partitioned on those dimensions:

```bash
for year in 2020 2021 2022 2023 2024; do
  for court in 36_29 1_1 2_2; do
    python sources/IN/HighCourtAWS/bootstrap.py bootstrap \
      --year-range "$year" \
      --court "$court"
  done
done
```

Use `--include-pdf-fallback` only on shards where the `coverage` command reports metadata bench shards missing from `derived/landlit-v2/texts/`. The hot path does not download PDFs.

## Notes

- S3 bucket is in ap-south-1 (Mumbai); connections may be slow from other regions.
- The full dataset is ~1.11 TB in PDFs, so the text layer is required for practical full-corpus ingest.
- The scraper yields raw dicts from `fetch_all()` and lets `BaseScraper` call `normalize()`.
- Deduplication remains keyed on `cnr_number`.

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — published via the [AWS Open Data Registry](https://registry.opendata.aws/indian-high-court-judgments/).
