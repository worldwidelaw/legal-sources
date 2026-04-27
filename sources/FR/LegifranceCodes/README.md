# FR/LegifranceCodes — French Consolidated Legal Codes

This source provides access to all 78+ French consolidated legal codes via the
official PISTE/Légifrance API.

## Coverage

- **Code civil** — Civil law (property, contracts, obligations, family)
- **Code pénal** — Criminal law
- **Code du travail** — Labor law
- **Code de commerce** — Commercial law
- **Code de la consommation** — Consumer protection
- **Code général des impôts** — Tax code
- **Code de procédure civile** — Civil procedure
- **Code de procédure pénale** — Criminal procedure
- And 70+ more specialized codes

## Authentication

This source requires OAuth2 credentials from the PISTE platform.

### Getting Credentials

1. Create an account at https://piste.gouv.fr/
2. Create an application in the portal
3. Subscribe to the "Légifrance" API
4. Get your OAuth identifiers from: APPLICATIONS > API subscribed > OAuth Identifiers

### Configuration

Copy `.env.template` to `.env` and add your credentials:

```bash
cp .env.template .env
# Edit .env with your credentials
```

## Usage

```bash
# List available codes
python bootstrap.py list-codes

# Fetch sample records (15 articles from priority codes)
python bootstrap.py bootstrap --sample

# Fetch more samples
python bootstrap.py bootstrap --sample --count 50

# Full fetch (all codes - takes many hours)
python bootstrap.py bootstrap --full
```

## Data Model

Each record represents a single article from a code:

- `_id`: Article LEGIARTI ID (e.g., LEGIARTI000006420098)
- `code_id`: Parent code LEGITEXT ID
- `code_name`: Code name (e.g., "Code civil")
- `article_num`: Article number (e.g., "1134", "L. 121-1")
- `text`: Full article text content
- `section_path`: Hierarchical location (e.g., "Livre I > Titre II > Chapitre I")
- `date`: Article version date
- `etat`: Status (VIGUEUR=in force, ABROGE=repealed)

## API Endpoints Used

- `list/code` — List all available codes
- `consult/code/tableMatieres` — Get code table of contents
- `consult/getArticle` — Get article content by ID

## Rate Limits

PISTE manages quotas per access token. The default delay between requests is 0.5
seconds to stay within limits.

## License

[Licence Ouverte 2.0 / Open Licence (Etalab)](https://www.etalab.gouv.fr/licence-ouverte-open-licence/) — free reuse with attribution.

## See Also

- **FR/JournalOfficiel** — Daily legislation updates via LEGI bulk archives
- **FR/Judilibre** — French judicial case law
- **FR/CouncilState** — Administrative court decisions
