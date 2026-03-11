# BA/RS - Republika Srpska Legislation

This source fetches legislation from **Republika Srpska**, one of the two constituent entities of Bosnia and Herzegovina.

## Data Source

**Paragraf BA** provides free public access to key Republika Srpska legislation:
- URL: https://www.paragraf.ba/besplatni-propisi-republike-srpske.html
- Coverage: ~160+ major laws, regulations, and decisions
- Format: HTML with full text

The legislation is originally published in **Službeni glasnik Republike Srpske** (Official Gazette of Republika Srpska).

## Coverage

Document types included:
- **Zakon/Zakonik** - Laws and Codes (e.g., Criminal Code, Family Law)
- **Uredba** - Government Decrees
- **Pravilnik** - Rulebooks and Administrative Regulations
- **Odluka** - Decisions
- **Ugovor** - Collective Agreements

## Usage

```bash
# List available legislation
python3 bootstrap.py list --limit 30

# Fetch sample documents
python3 bootstrap.py bootstrap --sample --count 12
```

## Output Schema

Each document includes:
- `_id`: Unique identifier (e.g., `BA-RS-64-2017-krivicni-zakon`)
- `title`: Document title
- `text`: Full text of the legislation (consolidated version)
- `gazette_number`: Official Gazette issue (e.g., "64/2017")
- `doc_type`: Type (zakon, uredba, pravilnik, odluka)
- `url`: Link to source document
- `language`: "sr" (Serbian, Cyrillic/Latin)

## Notes

- Paragraf BA provides consolidated (prečišćeni) versions with all amendments incorporated
- The official gazette (slglasnik.org) requires subscription for full historical access
- This source complements BA/SluzbenGlasnik (state-level) and BA/FBiH (Federation of BiH)
- Language is primarily Serbian (sr), but documents may also be read as Bosnian (bs) or Croatian (hr) due to mutual intelligibility
