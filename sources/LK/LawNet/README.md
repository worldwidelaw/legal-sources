# LK/LawNet — LawNet Sri Lanka (Ministry of Justice)

**Source:** [https://www.lawnet.gov.lk/](https://www.lawnet.gov.lk/)
**Data types:** legislation, case_law

## Coverage

Full text is read from the open `nuuuwan` GitHub datasets, which extract the
official government PDFs from parliament.lk, supremecourt.lk and courtofappeal.lk:

| Collection | Index entries | Source |
|---|---|---|
| Acts of Parliament (1947–2026) | ~2,435 | `lk_acts_data` |
| Supreme Court judgments (2009–2026) | ~2,729 | `lk_supreme_court_judgements` |
| Appeal Court judgments (2012–2026) | ~14,507 | `lk_appeal_court_judgements` |

### Act text filenames

Acts store their text under three different filenames depending on how the
upstream extractor fared, and `bootstrap.py` tries each in order:

1. `en.txt` — born-digital acts (roughly 2005 onward)
2. `blocks.txt` — acts where only the block dump was written
3. `en.ocr.txt` — pre-2005 scanned gazettes, where `en.txt.fail` marks a failed
   text-layer extraction and the publisher's OCR output is the only full text

Records carry a `text_quality` field of `extracted` or `ocr` so downstream
consumers can tell the noisy OCR text apart. Trying only `en.txt` yields ~606
acts; the full chain yields ~2,435.

## Usage

```bash
python bootstrap.py test                       # connectivity + first-doc check
python bootstrap.py bootstrap --sample         # 15 samples (5 per collection)
python bootstrap.py bootstrap-fast             # full corpus -> data/records.jsonl
python bootstrap.py bootstrap --updates --since 2026-01-01
```

The full run streams to `data/records.jsonl` and checkpoints its index position
per collection in `data/checkpoint.json`, so a relaunched fleet worker resumes
instead of re-walking every index from the top and re-appending duplicates.

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
