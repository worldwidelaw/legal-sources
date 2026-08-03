# NL/Tractatenblad — Dutch Treaty Series

Full-text corpus of the **Tractatenblad van het Koninkrijk der Nederlanden**, the
official Dutch treaty gazette. Every treaty, convention and international agreement
to which the Kingdom of the Netherlands is a party — together with its authentic
Dutch text, ratification notices and entry-into-force announcements — is published
here. ~15,970 publications since 1951.

This complements the existing Dutch sources without overlap:
- **NL/Staatsblad** — Bulletin of Acts & Decrees (national laws, royal decrees)
- **NL/wetten.overheid.nl** — consolidated national legislation
- **NL/CVDR** — decentralized (municipal/provincial/water-board) regulations

Treaties are binding legal instruments that none of the above cover.

## Data source

- **API:** KOOP / overheid.nl SRU 2.0 — `https://repository.overheid.nl/sru`
- **Query:** `(w.publicatienaam==Tractatenblad)`
- **Full text:** born-digital XML manifestation
  (`.../frbr/officielepublicaties/trb/{year}/{id}/1/xml/{id}.xml`), with a
  born-digital **PDF** fallback (PyMuPDF) for older publications whose XML is only
  a metadata stub. The rare scanned-only publication with no text layer is skipped.
- **Auth:** none (open government data)

## Fields

Each normalized record contains `_id`, `_source`, `_type` (`legislation`),
`_fetched_at`, `title`, `text` (full text), `date` (publication date), `url`,
plus `doc_id`, `doc_type` (e.g. Verdrag, Notawisseling), `creator`, `subject`,
`language` and `date_concluded` (date the treaty was concluded).

## Usage

```bash
python bootstrap.py test                # connectivity check
python bootstrap.py bootstrap --sample  # 10+ sample records
python bootstrap.py bootstrap-fast      # full corpus (runner entrypoint)
```

## License

[CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/) — Dutch legislation,
treaties and official government publications carry no copyright (art. 11
Auteurswet). The KOOP official-publications data (repository.overheid.nl) is
provided as open government data. Commercial use permitted; no attribution required.
