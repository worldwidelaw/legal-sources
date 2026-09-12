# UK/FSA — UK Food Standards Agency (FSA)

**Source:** [https://www.gov.uk/government/organisations/food-standards-agency](https://www.gov.uk/government/organisations/food-standards-agency)
**Data types:** doctrine

## Access

The FSA website (`www.food.gov.uk`) was migrated onto GOV.UK during 2026 — the old
Drupal `/search-api` endpoint now returns 404 and every food.gov.uk content path
301s to `www.gov.uk`. The corpus is therefore read from GOV.UK's two official
public APIs:

| Step | Endpoint |
|------|----------|
| Enumerate | `GET https://www.gov.uk/api/search.json?filter_organisations=food-standards-agency` |
| Full text | `GET https://www.gov.uk/api/content{base_path}` |

Enumeration is partitioned by `content_store_document_type` (37 substantive types,
~900 documents) so no single query approaches the Search API's `start` ceiling.

Full text is assembled from `details.body` (HTML) and `details.parts` (multi-part
guides). Where the landing page is only a stub in front of attachments, the
document's HTML attachments (separate GOV.UK content items) and PDF attachments on
`assets.publishing.service.gov.uk` are read as well.

## Usage

```bash
python bootstrap.py test              # connectivity check
python bootstrap.py bootstrap --sample # 12 validation samples
python bootstrap.py bootstrap          # full pull
python bootstrap.py bootstrap-fast     # concurrent full pull (fleet)
```

## License

[OGL v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) — attribution required.
