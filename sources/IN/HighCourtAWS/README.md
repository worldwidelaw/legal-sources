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

1. Requires one exact `(year, court, bench)` shard per job.
2. Streams the shard's raw PDF tar parts under fixed memory caps.
3. Joins each PDF to the matching bundled JSON metadata.
4. Writes to hidden staging, then atomically publishes `data/` only after exact inventory and every judgment pass; failures retry from zero in a new job directory.

## S3 Structure

```text
data/tar/year=YYYY/court=X_Y/bench=ZZZ/data.index.json
data/tar/year=YYYY/court=X_Y/bench=ZZZ/data.tar
metadata/tar/year=YYYY/court=X_Y/bench=ZZZ/metadata.index.json
metadata/tar/year=YYYY/court=X_Y/bench=ZZZ/metadata.tar.gz
```

## Usage

```bash
python bootstrap.py test
python bootstrap.py bootstrap --sample --sample-size 15 --output-dir /tmp/highcourtaws-sample
python bootstrap.py bootstrap --year-range 2024 --court 11_24 --bench sikkimhc_pg \
  --workers 4 --output-dir /srv/highcourtaws/jobs/2024-11_24-sikkimhc_pg
```

Each worker must use a new or empty non-symlink output directory and one scraper instance per attempt. Preserve failed-job evidence and rerun the same queue item from zero in a fresh isolated directory.

## Notes

- S3 bucket is in ap-south-1 (Mumbai); connections may be slow from other regions.
- The full dataset is ~1.11 TB in PDFs; workers stream archive members without retaining tar files.
- The scraper yields raw dicts from `fetch_all()` and lets `BaseScraper` call `normalize()`.
- Deduplication remains keyed on `cnr_number`.

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — published via the [AWS Open Data Registry](https://registry.opendata.aws/indian-high-court-judgments/).
