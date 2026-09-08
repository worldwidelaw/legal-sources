# License inventory contract and refresh

`commercial_use` is tri-state: `true` = explicitly permitted, `false` = explicitly
restricted, `null` (including an absent manifest field) = unknown. Only literal
booleans or null are accepted; strings, numbers, lists, and mappings fail generation.
Existing booleans and source IDs are preserved. The generator does not fill in
license metadata, infer source permissions from taxonomy, or deduplicate sources.
Only `status: complete` sources appear in the inventory.

`summary.commercial_ok`, `summary.non_commercial`, and the new
`summary.commercial_unknown` are disjoint and sum to `summary.total_complete`.
`summary.unverified` remains the count of `license_id: unverified`, an independent
axis that can overlap any commercial-use state. Consumers must use explicit
boolean comparisons, not truthiness or `total_complete - non_commercial`.
This inventory is not a blanket commercial-use whitelist or a legal clearance.

The validator accepts absent/null commercial use with a research warning and
rejects non-boolean values. Existing license ID/name requirements and explicit
registry-consistency checks remain. `--strict` still fails on unverified license
IDs; it does not make unknown commercial permission into a false determination.

Run from the repository root, with Python and PyYAML installed:

```sh
python scripts/validate_licenses.py
python generate_dashboard.py
```

License generation is a required first step in dashboard generation. Missing
scripts, missing taxonomy, parse failures, and invalid commercial-use types exit
nonzero before status output is rewritten. Workflow publication runs only after
successful generation and must stage both `docs/status.json` and `docs/licenses.json`.
The two writes are not a filesystem transaction; a later dashboard failure still
stops workflow publication.

Offline regression tests (pytest + PyYAML):

```sh
python -m pytest tests/test_license_inventory.py tests/test_license_refresh.py -q
```

Tests use synthetic manifests and temporary checkouts, not production data or a
database. PR CI runs only these fixtures, not a refresh or publication.

## Public mirror prerequisites

The public repository must carry these self-contained, non-secret files:
`generate_dashboard.py`, `scripts/generate_licenses_json.py`,
`license_registry.yaml`, and its refresh workflow. The registry contains generic
license taxonomy only, not source determinations. Keep the generator, taxonomy,
and fixture tests aligned across repositories. Public refresh uses its own
checked-in manifest; it does not read private sources, a private database, or
private modules. Schedule refresh as well as input-file push triggers, because
GitHub-token bot pushes do not trigger new push workflows.

No private-to-public export/sync implementation was found in the checked-in
scripts or workflows during this fix. Any external exporter must preserve this
allowlist and synchronize reviewed manifest changes through its existing approval
process; that external integration is not changed or verified here. Do not copy
private manifests, source metadata, credentials, or generated snapshots to fix
missing public code dependencies.
