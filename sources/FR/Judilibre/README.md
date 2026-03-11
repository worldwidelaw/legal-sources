# FR/Judilibre - French Judicial Case Law

Fetches case law from the Cour de Cassation's Judilibre platform via the PISTE API.

## Coverage

Judilibre provides access to French judicial case law including:
- **Cour de cassation** - Supreme Court for private and criminal law matters
- **Cours d'appel** - Appellate courts
- **Tribunaux judiciaires** - First-instance courts

This source is more comprehensive than FR/Legifrance which only includes Court of Cassation decisions from the DILA archives.

## Authentication Required

This source requires a PISTE API key. To obtain one:

1. Create an account at https://developer.aife.economie.gouv.fr (PISTE portal)
2. Activate your account via the email link
3. Log in and accept the terms of service
4. Search for "Judilibre" and subscribe to the API (sandbox and/or production)
5. Your API key (KeyId) is shown in your subscription details

## Setup

1. Copy `.env.template` to `.env`:
   ```bash
   cp .env.template .env
   ```

2. Fill in your API key in `.env`:
   ```
   JUDILIBRE_API_KEY=your_api_key_here
   JUDILIBRE_ENVIRONMENT=sandbox
   ```

3. Install dependencies:
   ```bash
   pip install requests pyyaml python-dotenv
   ```

## Usage

Fetch sample records:
```bash
python bootstrap.py bootstrap --sample --count 15
```

Full archive fetch (all jurisdictions, 2010-present):
```bash
python bootstrap.py bootstrap --full
```

Recent decisions (last 30 days):
```bash
python bootstrap.py bootstrap --recent --days 30
```

Incremental updates:
```bash
python bootstrap.py updates --since 2026-01-01
```

## API Documentation

- [API Judilibre - api.gouv.fr](https://api.gouv.fr/les-api/api-judilibre)
- [PISTE Portal](https://piste.gouv.fr)
- [GitHub - judilibre-search](https://github.com/Cour-de-cassation/judilibre-search)
- [OpenAPI Spec](https://github.com/Cour-de-cassation/judilibre-search/blob/dev/public/JUDILIBRE-public.json)

## License

Licence Ouverte / Open Licence - https://www.etalab.gouv.fr/licence-ouverte-open-licence
