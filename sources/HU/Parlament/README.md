# HU/Parlament — Hungarian Parliament Documents

## Overview

This source fetches parliamentary documents (irományok) from the Hungarian Parliament (Országgyűlés) website. These documents represent the legislative process before laws are enacted.

**Website:** https://www.parlament.hu

**Data type:** Parliamentary documents (draft legislation, amendments, committee reports)

**Coverage:** Parliamentary cycles 38-42 (2006-present)

**License:** [Open Government Data](https://njt.hu)

## What This Source Provides

Unlike HU/NJT which provides enacted legislation in force, HU/Parlament provides:

- **Draft legislation** (törvényjavaslatok) - bills before they become law
- **Amendments** (módosító javaslatok) - proposed changes to bills
- **Committee reports** (bizottsági jelentések)
- **Parliamentary resolutions** (határozatok)
- **Other parliamentary documents**

This is valuable for:
- Tracking the legislative process
- Analyzing bill amendments and evolution
- Understanding parliamentary debates and proposals
- Historical research on legislation that was proposed but not enacted

## Data Access

Documents are accessed via publicly available PDF files at predictable URLs:

```
https://www.parlament.hu/irom{cycle}/{docnum}/{docnum}.pdf
```

Where:
- `cycle` = Parliamentary cycle number (38, 39, 40, 41, 42)
- `docnum` = 5-digit document number with leading zeros

Example: `https://www.parlament.hu/irom42/13608/13608.pdf`

### Parliamentary Cycles

| Cycle | Years | Approx. Documents |
|-------|-------|-------------------|
| 42 | 2022-present | ~14,000+ |
| 41 | 2018-2022 | ~19,000 |
| 40 | 2014-2018 | ~23,000 |
| 39 | 2010-2014 | ~15,000 |
| 38 | 2006-2010 | ~18,000 |

## Document Types

Documents are prefixed with a type identifier:

- **T** - Törvényjavaslat (Bill/Draft legislation)
- **H** - Határozat (Resolution)
- **K** - Kérdés (Question)
- **I** - Interpelláció (Interpellation)
- **J** - Jelentés (Report)

## Technical Implementation

The scraper:
1. Downloads PDF documents from known URL patterns
2. Extracts text using `pdfplumber` library
3. Parses metadata (title, date, submitter) from the text content
4. Normalizes data into the standard schema

### Dependencies

- `pdfplumber` - For PDF text extraction

### Rate Limiting

Requests are rate-limited to 1.5 seconds between requests to respect server resources.

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample records (12 documents)
python bootstrap.py bootstrap --sample

# Full bootstrap (all cycles - thousands of documents)
python bootstrap.py bootstrap

# Update (check for new documents in current cycle)
python bootstrap.py update
```

## Sample Output

```json
{
  "_id": "42-13608",
  "_source": "HU/Parlament",
  "_type": "legislation",
  "title": "Az akkumulátorgyártó és feldolgozó vegyi üzemek...",
  "text": "Iromány száma: T/13608...[full bill text]...",
  "date": "2026-02-11",
  "url": "https://www.parlament.hu/irom42/13608/13608.pdf",
  "cycle": 42,
  "doc_number": 13608,
  "doc_type": "T",
  "official_number": "T/13608",
  "submitter": "Sebián-Petrovszki László (DK)...",
  "pdf_pages": 16,
  "language": "hu"
}
```

## Comparison with HU/NJT

| Aspect | HU/Parlament | HU/NJT |
|--------|--------------|--------|
| Content | Draft legislation, proposals | Enacted laws in force |
| Source | Parliament website | National Legislation Database |
| Format | PDF extraction | HTML parsing |
| Updates | As documents are submitted | As laws are enacted/amended |
| Historical | Yes (includes failed bills) | Current law only |

Both sources are complementary:
- Use **HU/NJT** for current law and legal research
- Use **HU/Parlament** for legislative process tracking and historical analysis

## License

Open Government Data — Hungarian parliamentary documents are freely reusable.

## Notes

- The official Parliament Web API requires registration and is not used
- Some document numbers may be missing (reserved, removed, or never assigned)
- PDF quality varies; some older documents may have OCR artifacts
