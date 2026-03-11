# LU/Parliament - Luxembourg Chamber of Deputies Parliamentary Documents

## Overview

This source fetches parliamentary questions and answers from the Luxembourg
Chamber of Deputies (Chambre des Députés du Grand-Duché de Luxembourg).

**Data type:** Parliamentary questions and government responses
**Full text:** Yes - both questions and answers include complete text
**License:** CC0 (Creative Commons Zero - Public Domain)
**Languages:** French, German, Luxembourgish

## Data Source

Data is published on the Luxembourg Open Data Portal:
- **Portal:** https://data.public.lu
- **Organization:** Chambre des Députés du Grand-Duché de Luxembourg
- **Dataset:** [Ensemble des Questions et Réponses Parlementaires](https://data.public.lu/en/datasets/ensemble-des-questions-et-reponses-parlementaires-incluant-le-texte-complet/)

## Coverage

| Legislature | Period | Approximate Count |
|-------------|--------|-------------------|
| 2013-2018 | 2013-2018 | ~2,500 questions |
| 2018-2023 | 2018-2023 | ~3,500 questions |
| 2023-2028 | 2023-present | Growing |

## Data Fields

| Field | Description |
|-------|-------------|
| `_id` | Unique identifier (legislature_questionnumber) |
| `title` | Question title |
| `text` | Combined question and answer full text |
| `date` | Question deposit date |
| `url` | Link to question on chd.lu |
| `author` | Deputy who submitted the question |
| `party` | Political party |
| `respondent` | Minister(s) who responded |
| `question_text` | Full text of question |
| `response_text` | Full text of response |

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample records (12)
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Notes

- Data files are updated approximately every two months
- Questions are in mixed languages (FR, DE, LB)
- Each record contains both the parliamentary question AND the government response
- For enacted legislation, see LU/LegalDatabase (Legilux)
