# CN/HuggingFace-CAIL2018 — CAIL2018 Chinese Court Judgments Dataset

**Source:** [https://huggingface.co/datasets/china-ai-law-challenge/cail2018](https://huggingface.co/datasets/china-ai-law-challenge/cail2018)
**Data types:** case_law
**Records:** ~2.17M criminal case records across 6 splits

## Overview

Large-scale Chinese legal dataset from the China AI Law Challenge 2018. Contains
criminal case records with fact descriptions (full text), relevant law articles,
accusations, defendant names, and sentencing information.

## Fields

| Field | Description |
|-------|-------------|
| `fact` | Case fact description (full narrative text) |
| `relevant_articles` | IDs of applicable Chinese criminal law articles |
| `accusation` | Criminal charges |
| `criminals` | Defendant names |
| `imprisonment` | Prison sentence in months |
| `punish_of_money` | Monetary fine in CNY |
| `death_penalty` | Death penalty flag |
| `life_imprisonment` | Life imprisonment flag |

## License

[Dataset page](https://huggingface.co/datasets/china-ai-law-challenge/cail2018) — Academic dataset, no explicit license specified. Attribution to CAIL 2018 organizers recommended.
