# CR/SCIJ — SCIJ - Sistema Costarricense de Información Jurídica

**Source:** [https://sinalevi.go.cr/](https://sinalevi.go.cr/)
**Data types:** legislation
**Language:** Spanish
**Coverage:** ~100,000 norms, 1821 to present — Constitution, laws, executive
decrees, international treaties, regulations, municipal agreements,
resolutions, directives and circulars.

Costa Rica's official legal information system, operated by the Procuraduría
General de la República.

## Access

The platform migrated off the old ASP.NET WebForms site at `pgrweb.go.cr/scij`;
those `nrm_norma.aspx` / `nrm_texto_completo.aspx` URLs now 302 to
`sinalevi.go.cr` (issue #1455). The numeric norm ID space carried over
unchanged, so the crawler still walks IDs, but against JSON XHR endpoints:

| Step | Endpoint | Notes |
|------|----------|-------|
| Existence probe | `POST /ResultadosNormativa/_CargarTextoCompleto` with `version=-1` | Returns a redirect whose `param2` is the norm's live version id, or `0` when no norm sits behind the ID |
| Metadata | `POST /ResultadosNormativa/_CargarFicha` | Type, number, date, issuing body, Gaceta reference |
| Full text | `POST /ResultadosNormativa/_CargarTextoCompleto` with that version id | Word-exported HTML, cleaned to plain text |

Requests need `X-Requested-With: XMLHttpRequest` and a browser User-Agent.

## Operational notes

- **Resumable.** IDs walked are checkpointed to `data/scij_checkpoint.json`, so
  a torn-down or timed-out fleet slot resumes where it stopped rather than
  restarting at ID 1. The resume point rewinds 200 IDs to cover documents still
  in flight when the process died; the loader dedups on `_id`.
- **The ID ceiling is probed at runtime.** A hardcoded ceiling goes stale and
  silently truncates the corpus — the old one (107,000) was already short of
  the live maximum (~107,600).
- **Fails loud.** 500 consecutive dead probes with nothing found raises
  `SCIJUnreachable` instead of exiting 0 with an empty corpus, which is how the
  platform migration went unnoticed for a full fleet cycle.
- Roughly three requests per document (~1 doc/s), so a full walk is ~28h.

## Commands

```bash
python bootstrap.py test               # connectivity check
python bootstrap.py bootstrap --sample # 15 validation samples
python bootstrap.py bootstrap-fast     # full corpus (fleet entry point)
python bootstrap.py update --since 2026-01-01
```

## License

Open government data — Costa Rican legislation published by the Procuraduría
General de la República is public official information, freely accessible for
consultation and reuse. See [https://sinalevi.go.cr/](https://sinalevi.go.cr/).
