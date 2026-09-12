# CO/CorteSuprema — Colombian Supreme Court Jurisprudence

**Source:** [https://consultaprovidencias.cortesuprema.gov.co/](https://consultaprovidencias.cortesuprema.gov.co/)
**Data types:** case_law

Decisions of the Corte Suprema de Justicia (Civil, Laboral, Penal and Tutelas
chambers), read through the site's public GraphQL API at
`consultaprovidenciasbk.cortesuprema.gov.co`.

## How it works

| Step | Endpoint | Notes |
|------|----------|-------|
| Years per chamber | `GET /filters` | drives the (room, year) crawl units |
| Listing | `POST /api` — `getSearchResult` | page size fixed at 10, deep paging works to the last hit |
| Document | `POST /downloadFile` `{"path": ...}` | the listing `id` is a filesystem path in the store |

The listing `id` extension tells you which renditions exist, so the scraper
makes **at most two** download attempts per document:

| Listed as | Available rendition | Extraction |
|-----------|--------------------|------------|
| `.doc` | rendered `.pdf`, **image-only** | OCR (deferred to phase 2) |
| `.docx` / `.pdf` | `.docx` *and* `.pdf`, both with a text layer | `.docx` XML, else PyMuPDF |

### Two-phase crawl

`bootstrap` / `bootstrap-fast` runs phase `text` first — everything with a real
text layer — and parks image-only PDFs in `data/deferred_ocr.jsonl`. Phase `ocr`
then drains that backlog through the shared extractor. A run that dies on the
fleet's 100-hour cap therefore lands the cheap documents first instead of
spending the whole budget on OCR. Progress (unit, paging offset, phase) is
checkpointed to `data/checkpoint.json`, so the next run resumes rather than
re-walking ~96,000 listing pages.

## Coverage caveat — the index is broader than the file store

The Solr index returns roughly 961,000 documents, but a large share of them
have **no downloadable rendition at all**: every extension 404s and the site's
own `/fileExists` reports `false`, so the official download button fails the
same way. Verified 2026-08-15 (issue #1431):

- the entire **Penal** chamber (~90,000 indexed documents) is unreachable,
  across every year sampled from 2000 to 2025;
- assorted room/year subtrees are likewise unmounted — e.g. Civil 2025,
  Laboral 2023–2025, Tutelas ≤2018 and 2024–2025.

The scraper probes the first 40 non-junk documents of each unit; if none of
them yields a file, it logs a loud `UNIT UNAVAILABLE` error, records the unit
in the checkpoint and skips the rest rather than spending hours on guaranteed
404s. Nothing is silently dropped — every skipped unit is named in the log.

## Usage

```bash
python bootstrap.py test-api            # connectivity + per-room availability probe
python bootstrap.py bootstrap --sample  # ~13 records into sample/
python bootstrap.py bootstrap-fast      # full corpus -> data/records.jsonl
```

`PyMuPDF` is required for the text-layer fast path; `tesseract` is required for
phase 2 (OCR of the `.doc`-era scans).

## License

Government open access — Colombian Supreme Court decisions are publicly accessible. Court judgments are public records under Colombian law ([Ley 1712 de 2014](https://www.funcionpublica.gov.co/eva/gestornormativo/norma.php?i=56882)).
