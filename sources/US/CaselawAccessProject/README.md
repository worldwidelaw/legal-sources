# US/CaselawAccessProject

Harvard Law Library Caselaw Access Project via HuggingFace.

## Source Information

- **Dataset**: common-pile/caselaw_access_project
- **URL**: https://huggingface.co/datasets/common-pile/caselaw_access_project
- **Original**: https://case.law/
- **Coverage**: 6.7M+ U.S. federal and state court decisions (360 years)
- **License**: CC0 1.0 (Public Domain)

## Usage

```bash
# Fetch sample records
python3 bootstrap.py bootstrap --sample

# Validate samples
python3 bootstrap.py validate

# Fetch N records
python3 bootstrap.py fetch --count 100
```

## Data Format

Records are normalized to the standard schema:

```json
{
  "_id": "f2d_474/html/0001-01.html",
  "_source": "US/CaselawAccessProject",
  "_type": "case_law",
  "_fetched_at": "2026-02-28T...",
  "title": "UNITED STATES of America, Appellee, v. Daniel Dee VEON, Appellant.",
  "text": "Full opinion text...",
  "date": "1973-02-12",
  "url": "https://case.law/search/?q=...",
  "court": "United States Court of Appeals, Ninth Circuit",
  "case_number": "72-1889",
  "author": "PER CURIAM"
}
```

## Technical Notes

- Uses HuggingFace Datasets library with streaming mode
- No full download required (78GB dataset)
- Rate limiting not required (HuggingFace manages bandwidth)
- Text is pre-cleaned (OCR corrections applied by Teraflop AI)
