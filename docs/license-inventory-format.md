# Licence inventory (schema v2)

`docs/licenses.json` is a **byte-for-byte mirror** of the already-public
[canonical Hunter inventory](https://zachlaik.github.io/LegalDataHunter/licenses.json).
The private Hunter manifest is canonical; its generator publishes this feed.
This repository does not import private generator code or a private registry and
must not regenerate licence decisions from its independently synced manifest.

## Contract

- `schema_version`: integer `2`.
- `generated_at`: ISO datetime with a timezone, set by the canonical generator.
- `sources`: an array of objects with `id`, `country`, `name`, `license_id`,
  `license_name` (strings), `license_url` (string or `null`) and `commercial_use`.
  Source IDs must be nonempty and unique. Blank names and licence IDs are valid
  unresolved audit metadata; the mirror preserves them.
- `commercial_use` is **only `true`, `false`, or `null`** in both source and
  registry entries. `null` means **UNKNOWN**, not permission to use commercially
  and not a confirmed prohibition. Missing permission in the upstream manifest
  must become `null` in the generated feed. A missing field in the feed itself
  is a schema error; strings and integer substitutes are rejected.
- `summary` contains nonnegative integer `total_complete`, `commercial_ok`,
  `non_commercial`, `commercial_unknown`, `unverified`, and `unique_license_ids`.
  The first equals the source count. The three permission buckets count
  `true`/`false`/`null` and partition the sources. `unverified` counts only the
  literal licence ID `unverified`, not blank IDs or all unknown permissions.
- `by_license` maps each source licence ID (including a blank one, if present)
  to an object with string `display_name`, string-or-`null` `url`, `commercial_use`, and
  nonnegative integer `count`. Its IDs and counts must match source grouping;
  `unique_license_ids` counts these groups. Registry metadata and permissions
  may differ from explicit source values: the mirror must not overwrite either.

## Synchronization and rollout

**Deploy private-first:** publish and verify the canonical schema-v2 feed before
rolling out this public sync. A legacy feed is intentionally rejected, even if
it is valid JSON; public refresh will fail until upstream v2 is available.
Do not hand-edit the mirror or manufacture a successful initial inventory.

Run only the licence sync with:

```sh
python3 scripts/sync_licenses_json.py
```

The stdlib-only copier uses a 30-second network timeout and a 10 MiB payload cap.
It validates the complete response before atomic replacement, preserving the
received bytes and metadata. Identical bytes are a no-op. An older timestamp
than an existing parseable, timezone-aware `generated_at` is rejected (including
when the existing file is legacy). Invalid JSON, HTTP failures, schema/count
errors and duplicate IDs leave the existing inventory unchanged.

The refresh workflow runs licence-only sync every two hours. Manual runs and
relevant main-branch pushes generate `status.json` and sync licences; sync errors
propagate and prevent publication. Publication is serialized, stages only
`docs/status.json` and `docs/licenses.json`, and makes at most three non-force
push attempts, fetching latest main and regenerating after a race. It does not
copy or overwrite community source files. Source-code/manifest synchronization
remains an independent process, so inventory and public manifest counts can
differ legitimately. Preserve this public sync integration during that process.

PR and relevant push tests never publish and use ephemeral HTTP servers bound
to `127.0.0.1`, not the live feed. To run locally:

```sh
uv run --with pyyaml --with pytest python -m pytest tests/test_license_sync.py -q
```
