# GH/ParliamentRepository — Ghana Parliament Institutional Repository

Official Parliament of Ghana DSpace 9 institutional repository containing 3,900+ parliamentary documents: bills, official reports (Hansard), committee reports, constitutional/executive/legislative instruments, decrees, budget estimates, agreements, conventions, and more.

- **URL**: https://repository.parliament.gh/
- **API**: DSpace 9 REST API (`/server/api/`)
- **Language**: English
- **Coverage**: 1957–present

## Crawl behaviour

Two requests per item: `core/items/{uuid}?embed=bundles/bitstreams` (metadata + the
TEXT bundle's `sizeBytes` and content link) then the pre-extracted `.txt` bitstream.
The full ~4,035-item corpus takes roughly 4.5 hours.

Each request carries a connect, whole-transfer and stall deadline, and the curl
process group is hard-killed if it ever outlives them, so no single fetch can freeze
the crawl. Settled item UUIDs are checkpointed to `data/checkpoint.json` (gitignored)
and skipped with no network calls on restart, so an interrupted run resumes rather
than starting over. The run also stops cleanly at a wall-clock budget
(`GH_PARLREPO_DEADLINE_HOURS`, default 20) with its checkpoint written.

```bash
python bootstrap.py bootstrap            # full crawl, resumes from checkpoint
python bootstrap.py bootstrap --restart  # discard the checkpoint and re-crawl
```

## License

[Open Government Data](https://repository.parliament.gh/) — Official Parliament of Ghana institutional repository, open access. Attribution required.
