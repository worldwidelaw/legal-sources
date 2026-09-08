"""Offline regressions for the public licence mirror (never use the live feed)."""

import importlib
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import types
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import Mock

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_URL = "https://zachlaik.github.io/LegalDataHunter/licenses.json"
MAX_BYTES = 10 * 1024 * 1024


def inventory():
    # Deliberately inconsistent registry/source metadata is valid: the registry
    # is not authority to overwrite an explicit source value. Blank IDs/names
    # represent unresolved audit metadata, not the literal 'unverified' ID.
    sources = [
        {"id": "XX/Allowed", "country": "XX", "name": "Allowed", "license_id": "open",
         "license_name": "Source-specific name", "license_url": "https://example.org/source", "commercial_use": True},
        {"id": "XX/Denied", "country": "XX", "name": "Denied", "license_id": "open",
         "license_name": "", "license_url": "", "commercial_use": False},
        {"id": "XX/Unknown", "country": "XX", "name": "", "license_id": "",
         "license_name": "", "license_url": "", "commercial_use": None},
        {"id": "XX/Unverified", "country": "XX", "name": "Unverified", "license_id": "unverified",
         "license_name": "Unverified", "license_url": "", "commercial_use": None},
    ]
    return {
        "schema_version": 2,
        "generated_at": "2026-09-08T10:00:00+00:00",
        "summary": {"total_complete": 4, "commercial_ok": 1, "non_commercial": 1,
                    "commercial_unknown": 2, "unverified": 1, "unique_license_ids": 3},
        "by_license": {
            "open": {"display_name": "Registry name", "url": "https://example.org/registry", "commercial_use": None, "count": 2},
            "": {"display_name": "", "url": "", "commercial_use": None, "count": 1},
            "unverified": {"display_name": "Unverified", "url": "", "commercial_use": None, "count": 1},
        },
        "sources": sources,
    }


def encode(data):
    return (json.dumps(data, ensure_ascii=False, indent=3) + "\n\n").encode("utf-8")


@pytest.fixture
def http_feed():
    """A real bounded local HTTP server, shut down and joined after each test."""
    state = {"body": encode(inventory()), "status": 200, "length": "auto", "requests": []}

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            state["requests"].append(self.path)
            self.send_response(state["status"])
            self.send_header("Content-Type", "application/json")
            length = state["length"]
            if length is not None:
                self.send_header("Content-Length", str(len(state["body"]) if length == "auto" else length))
            self.end_headers()
            try:
                self.wfile.write(state["body"])
            except (BrokenPipeError, ConnectionResetError):
                pass  # Expected when an over-limit response is rejected early.

        def log_message(self, *_args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01))
    thread.start()
    state["url"] = f"http://127.0.0.1:{server.server_port}/licenses.json"
    try:
        yield state
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        assert not thread.is_alive()


@pytest.fixture
def sync_module():
    assert (ROOT / "scripts/sync_licenses_json.py").is_file(), "The public mirror needs a licence sync implementation"
    return importlib.import_module("scripts.sync_licenses_json")


@pytest.fixture
def dashboard(monkeypatch, tmp_path):
    module = importlib.import_module("generate_dashboard")
    monkeypatch.setattr(module, "DOCS_DIR", tmp_path / "docs")
    for name, value in {
        "load_manifest": {"sources": []}, "_load_neon_database_url": None,
        "load_pipeline_index": {}, "load_contributors": {},
        "parse_blocked_md": [], "get_session_logs": ([], ""),
    }.items():
        monkeypatch.setattr(module, name, Mock(return_value=value))
    return module


def stub_license_modules(monkeypatch, sync):
    # Stubbing both old/new integration targets lets RED exercise generate()
    # itself, not fail during collection due to an absent scripts package.
    package = types.ModuleType("scripts")
    package.__path__ = []
    old_module = types.ModuleType("scripts.generate_licenses_json")
    old_module.main = Mock(side_effect=RuntimeError("private generator is unavailable in the public repo"))
    new_module = types.ModuleType("scripts.sync_licenses_json")
    new_module.sync_licenses = sync
    monkeypatch.setitem(sys.modules, "scripts", package)
    monkeypatch.setitem(sys.modules, old_module.__name__, old_module)
    monkeypatch.setitem(sys.modules, new_module.__name__, new_module)
    return old_module.main


def test_dashboard_copies_canonical_inventory_instead_of_regenerating(dashboard, monkeypatch):
    sync = Mock()
    old_generator = stub_license_modules(monkeypatch, sync)
    dashboard.generate()
    sync.assert_called_once_with(output=dashboard.DOCS_DIR / "licenses.json")
    old_generator.assert_not_called()


def test_dashboard_propagates_sync_failure(dashboard, monkeypatch):
    sync = Mock(side_effect=RuntimeError("canonical feed unavailable"))
    stub_license_modules(monkeypatch, sync)
    with pytest.raises(RuntimeError, match="canonical feed unavailable"):
        dashboard.generate()


def test_copies_exact_bytes_and_preserves_unknown_and_source_metadata(sync_module, http_feed, tmp_path):
    data = inventory()
    data["sources"][0]["name"] = "Législation ⚖"
    data["audit_note"] = "Additional public metadata is preserved, not regenerated."
    http_feed["body"] = encode(data)
    output = tmp_path / "docs/licenses.json"
    assert sync_module.sync_licenses(url=http_feed["url"], output=output) is True
    assert output.read_bytes() == http_feed["body"]
    assert http_feed["requests"] == ["/licenses.json"]


def test_identical_bytes_are_a_noop(sync_module, http_feed, tmp_path):
    output = tmp_path / "licenses.json"
    output.write_bytes(http_feed["body"])
    before = output.stat()
    assert sync_module.sync_licenses(url=http_feed["url"], output=output) is False
    after = output.stat()
    assert (after.st_ino, after.st_mtime_ns) == (before.st_ino, before.st_mtime_ns)


def set_field(data, path, value):
    target = data
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value


INVALID_FIELDS = [
    (("schema_version",), 1), (("schema_version",), 2.0), (("schema_version",), True),
    (("generated_at",), "2026-09-08"), (("generated_at",), "2026-09-08T10:00:00"),
    (("generated_at",), "yesterday"), (("generated_at",), None),
    (("sources",), {}), (("sources", 0), []), (("sources", 0, "id"), ""),
    (("sources", 0, "id"), 42), (("sources", 0, "country"), None),
    (("sources", 0, "name"), None), (("sources", 0, "license_id"), None),
    (("sources", 0, "license_name"), []), (("sources", 0, "license_url"), False),
    (("sources", 1, "id"), "XX/Allowed"),
    (("by_license",), []), (("by_license", "open"), None),
    (("by_license", "open", "display_name"), None), (("by_license", "open", "url"), 5),
    (("by_license", "open", "count"), 1), (("by_license", "open", "count"), True),
    (("by_license", "open", "count"), -1), (("by_license", "open", "count"), 2.0),
    (("summary",), []),
] + [
    (("summary", key), value)
    for key in inventory()["summary"] for value in [-1, True, "1", 1.0, 99]
] + [
    (("sources", 0, "commercial_use"), value) for value in [0, 1, "true", "false", "unknown", [], {}]
] + [
    (("by_license", "open", "commercial_use"), value) for value in [0, 1, "true", "false", "unknown", [], {}]
]


@pytest.mark.parametrize("path,value", INVALID_FIELDS)
def test_invalid_field_never_alters_existing_output(sync_module, http_feed, tmp_path, path, value):
    data = inventory()
    set_field(data, path, value)
    http_feed["body"] = encode(data)
    output = tmp_path / "licenses.json"
    output.write_bytes(b"existing inventory must survive")
    with pytest.raises(ValueError):
        sync_module.sync_licenses(url=http_feed["url"], output=output)
    assert output.read_bytes() == b"existing inventory must survive"
    assert list(tmp_path.iterdir()) == [output]


@pytest.mark.parametrize("path", [("sources", 0, "license_url"), ("by_license", "open", "url")])
def test_nullable_urls_are_preserved_as_canonical_metadata(sync_module, http_feed, tmp_path, path):
    data = inventory()
    set_field(data, path, None)
    http_feed["body"] = encode(data)
    output = tmp_path / "licenses.json"
    assert sync_module.sync_licenses(url=http_feed["url"], output=output) is True
    assert output.read_bytes() == http_feed["body"]


@pytest.mark.parametrize("path", [
    ("schema_version",), ("generated_at",), ("summary",), ("sources",), ("by_license",),
    ("summary", "commercial_unknown"), ("sources", 0, "commercial_use"),
    ("sources", 0, "license_url"), ("by_license", "open", "url"),
    ("sources", 0, "license_name"), ("by_license", "open", "commercial_use"),
    ("by_license", "open", "count"), ("by_license", "open"),
])
def test_missing_required_fields_fail_closed(sync_module, http_feed, tmp_path, path):
    data = inventory()
    target = data
    for key in path[:-1]:
        target = target[key]
    del target[path[-1]]
    http_feed["body"] = encode(data)
    output = tmp_path / "licenses.json"
    output.write_bytes(b"previous")
    with pytest.raises(ValueError):
        sync_module.sync_licenses(url=http_feed["url"], output=output)
    assert output.read_bytes() == b"previous"


def test_extra_registry_id_is_rejected_even_with_zero_count(sync_module, http_feed, tmp_path):
    data = inventory()
    data["by_license"]["extra"] = dict(data["by_license"]["open"], count=0)
    data["summary"]["unique_license_ids"] = 4
    http_feed["body"] = encode(data)
    output = tmp_path / "licenses.json"
    output.write_bytes(b"previous")
    with pytest.raises(ValueError):
        sync_module.sync_licenses(url=http_feed["url"], output=output)
    assert output.read_bytes() == b"previous"


@pytest.mark.parametrize("body", [b"{", b"not json", b"[]", b"null", b"\xff", b"{}",
    b'{"schema_version": 2, "schema_version": 2}',
    encode(inventory()).replace(b'"schema_version": 2,', b'"schema_version": 2, "extra": NaN,'),
])
def test_bad_or_legacy_json_is_not_written(sync_module, http_feed, tmp_path, body):
    http_feed["body"] = body
    output = tmp_path / "licenses.json"
    output.write_bytes(b"previous")
    with pytest.raises(ValueError):
        sync_module.sync_licenses(url=http_feed["url"], output=output)
    assert output.read_bytes() == b"previous"


@pytest.mark.parametrize("status", [404, 500])
def test_http_errors_preserve_existing_file(sync_module, http_feed, tmp_path, status):
    from urllib.error import HTTPError
    http_feed["status"] = status
    output = tmp_path / "licenses.json"
    output.write_bytes(b"previous")
    with pytest.raises(HTTPError):
        sync_module.sync_licenses(url=http_feed["url"], output=output)
    assert output.read_bytes() == b"previous"


@pytest.mark.parametrize("length", ["auto", None])
def test_oversized_response_is_rejected_with_or_without_length(sync_module, http_feed, tmp_path, length):
    http_feed.update(body=b" " * (MAX_BYTES + 1), length=length)
    output = tmp_path / "licenses.json"
    output.write_bytes(b"previous")
    with pytest.raises(ValueError):
        sync_module.sync_licenses(url=http_feed["url"], output=output)
    assert output.read_bytes() == b"previous"


def test_valid_json_in_truncated_http_body_is_rejected(sync_module, http_feed, tmp_path):
    http_feed["length"] = len(http_feed["body"]) + 100
    output = tmp_path / "licenses.json"
    output.write_bytes(b"previous")
    with pytest.raises((ValueError, OSError)):
        sync_module.sync_licenses(url=http_feed["url"], output=output)
    assert output.read_bytes() == b"previous"


@pytest.mark.parametrize("existing_timestamp,incoming_timestamp,allowed", [
    ("2026-09-08T10:00:01+00:00", "2026-09-08T10:00:00Z", False),
    ("2026-09-08T12:00:00+02:00", "2026-09-08T10:00:00Z", True),
    ("2026-09-08T12:00:00+02:00", "2026-09-08T10:00:01Z", True),
    ("bad", "2026-09-08T10:00:00Z", True),
])
def test_timestamp_rollback_uses_existing_timestamp_even_if_legacy(sync_module, http_feed, tmp_path,
        existing_timestamp, incoming_timestamp, allowed):
    output = tmp_path / "licenses.json"
    before = encode({"generated_at": existing_timestamp})
    output.write_bytes(before)
    data = inventory()
    data["generated_at"] = incoming_timestamp
    http_feed["body"] = encode(data)
    if allowed:
        assert sync_module.sync_licenses(url=http_feed["url"], output=output) is True
        assert output.read_bytes() == http_feed["body"]
    else:
        with pytest.raises(ValueError, match="(?i)older|rollback"):
            sync_module.sync_licenses(url=http_feed["url"], output=output)
        assert output.read_bytes() == before


def test_atomic_replace_failure_preserves_previous_and_cleans_temp(sync_module, http_feed, tmp_path, monkeypatch):
    output = tmp_path / "licenses.json"
    output.write_bytes(b"previous")

    def fail_replace(source, destination):
        assert Path(source).parent == output.parent
        assert Path(source).read_bytes() == http_feed["body"]
        assert Path(destination) == output
        assert output.read_bytes() == b"previous"
        raise OSError("replacement unavailable")

    monkeypatch.setattr(sync_module.os, "replace", fail_replace)
    with pytest.raises(OSError, match="replacement unavailable"):
        sync_module.sync_licenses(url=http_feed["url"], output=output)
    assert output.read_bytes() == b"previous"
    assert list(tmp_path.iterdir()) == [output]


def test_default_url_and_network_read_are_bounded(sync_module, http_feed, tmp_path, monkeypatch):
    from urllib.request import urlopen
    calls = []

    def local_urlopen(url, *, timeout):
        calls.append((url, timeout))
        return urlopen(http_feed["url"], timeout=timeout)

    monkeypatch.setattr(sync_module, "urlopen", local_urlopen)
    sync_module.sync_licenses(output=tmp_path / "licenses.json")
    assert calls[0][0] == DEFAULT_URL
    assert 0 < calls[0][1] <= 30


def test_empty_inventory_is_valid(sync_module, http_feed, tmp_path):
    data = inventory()
    data.update(sources=[], by_license={})
    data["summary"] = {key: 0 for key in data["summary"]}
    http_feed["body"] = encode(data)
    output = tmp_path / "licenses.json"
    assert sync_module.sync_licenses(url=http_feed["url"], output=output) is True
    assert output.read_bytes() == http_feed["body"]


def test_cli_local_http_success_noop_and_failure(sync_module, http_feed, tmp_path):
    output = tmp_path / "licenses.json"
    command = [sys.executable, str(ROOT / "scripts/sync_licenses_json.py"),
               "--url", http_feed["url"], "--output", str(output)]
    first = subprocess.run(command, text=True, capture_output=True, timeout=10)
    assert first.returncode == 0, first.stderr
    assert output.read_bytes() == http_feed["body"]
    before = output.stat().st_mtime_ns
    second = subprocess.run(command, text=True, capture_output=True, timeout=10)
    assert second.returncode == 0, second.stderr
    assert output.stat().st_mtime_ns == before
    http_feed["body"] = b"DO_NOT_LOG_DOWNLOADED_PAYLOAD"
    failed = subprocess.run(command, text=True, capture_output=True, timeout=10)
    assert failed.returncode != 0
    assert "DO_NOT_LOG_DOWNLOADED_PAYLOAD" not in failed.stdout + failed.stderr
    assert output.stat().st_mtime_ns == before


def load_workflow(name):
    path = ROOT / ".github/workflows" / name
    assert path.is_file(), f"Missing focused workflow: {name}"
    return yaml.load(path.read_text(), Loader=yaml.BaseLoader)


def test_refresh_workflow_has_safe_triggers_and_serializes_publication():
    workflow = load_workflow("refresh-dashboard.yml")
    triggers = workflow["on"]
    assert set(triggers) == {"push", "workflow_dispatch", "schedule"}
    assert triggers["push"]["branches"] == ["main"]
    assert {"sources/**", "manifest.yaml", "generate_dashboard.py", "scripts/sync_licenses_json.py",
            ".github/workflows/refresh-dashboard.yml"} <= set(triggers["push"]["paths"])
    cron = triggers["schedule"][0]["cron"].split()
    assert cron[1:] == ["*/2", "*", "*", "*"]
    assert workflow["concurrency"]["group"]
    assert workflow["concurrency"]["cancel-in-progress"] == "false"
    refresh = workflow["jobs"]["refresh"]
    assert "github.ref == 'refs/heads/main'" in refresh["if"]
    checkout = next(step for step in refresh["steps"] if step.get("uses", "").startswith("actions/checkout@"))
    assert checkout["with"]["ref"] == "main"
    scripts = "\n".join(step.get("run", "") for step in refresh["steps"])
    assert "git add docs/status.json docs/licenses.json" in scripts
    assert "set -euo pipefail" in scripts
    assert "--force" not in scripts
    assert "git add -A" not in scripts
    assert "git pull" not in scripts


def test_test_workflow_runs_on_pr_and_relevant_push_without_publication():
    workflow = load_workflow("test-license-sync.yml")
    assert set(workflow["on"]) == {"pull_request", "push"}
    for event in ["pull_request", "push"]:
        assert {"scripts/sync_licenses_json.py", "generate_dashboard.py", "tests/test_license_sync.py",
                ".github/workflows/refresh-dashboard.yml", ".github/workflows/test-license-sync.yml"} <= set(workflow["on"][event]["paths"])
    assert workflow["permissions"] == {"contents": "read"}
    scripts = "\n".join(step.get("run", "") for job in workflow["jobs"].values() for step in job["steps"])
    assert "python -m pytest tests/test_license_sync.py" in scripts
    assert "git push" not in scripts
    assert "git commit" not in scripts
    assert "python3 generate_dashboard.py" not in scripts


@pytest.mark.parametrize("event,push_failures,generation_failure,no_changes,expected_attempts,exit_code", [
    ("schedule", 0, False, False, 1, 0),
    ("push", 1, False, False, 2, 0),
    ("workflow_dispatch", 3, False, False, 3, 1),
    ("schedule", 0, True, False, 1, 1),
    ("push", 0, False, True, 1, 0),
])
def test_publication_shell_modes_retries_and_fail_closed(tmp_path, event, push_failures,
        generation_failure, no_changes, expected_attempts, exit_code):
    # Execute the workflow's actual bash, but use local test doubles for git and
    # generation. No git commits, pushes, credentials or external calls occur.
    workflow = load_workflow("refresh-dashboard.yml")
    steps = workflow["jobs"]["refresh"]["steps"]
    publication = [step for step in steps if "git push" in step.get("run", "")]
    assert len(publication) == 1
    step = publication[0]
    assert step.get("env", {}).get("EVENT_NAME") == "${{ github.event_name }}"
    script = step["run"]
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "calls.jsonl"
    stub = '''import json, os, pathlib, sys
name = pathlib.Path(sys.argv[0]).name
args = sys.argv[1:]
log = pathlib.Path(os.environ["CALL_LOG"])
previous = [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []
with log.open("a") as stream:
    stream.write(json.dumps([name, *args]) + "\\n")
if name == "python3":
    sys.exit(1 if os.environ["GENERATION_FAILURE"] == "1" else 0)
if args[:1] == ["push"]:
    count = sum(call[:2] == ["git", "push"] for call in previous)
    sys.exit(1 if count < int(os.environ["PUSH_FAILURES"]) else 0)
if args[:3] == ["diff", "--cached", "--quiet"]:
    sys.exit(0 if os.environ["NO_CHANGES"] == "1" else 1)
'''
    for name in ["git", "python3"]:
        executable = bin_dir / name
        executable.write_text(f"#!{sys.executable}\n" + stub)
        executable.chmod(0o755)
    env = dict(os.environ, PATH=f"{bin_dir}:{os.environ['PATH']}", EVENT_NAME=event,
               CALL_LOG=str(log), PUSH_FAILURES=str(push_failures),
               GENERATION_FAILURE=str(int(generation_failure)), NO_CHANGES=str(int(no_changes)))
    result = subprocess.run(["bash", "-c", script], cwd=tmp_path, env=env,
                            text=True, capture_output=True, timeout=20)
    assert result.returncode == exit_code, result.stdout + result.stderr
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    generations = [call for call in calls if call[0] == "python3"]
    command = "scripts/sync_licenses_json.py" if event == "schedule" else "generate_dashboard.py"
    assert generations == [["python3", command]] * expected_attempts
    fetches = [i for i, call in enumerate(calls) if call == ["git", "fetch", "origin", "main"]]
    resets = [i for i, call in enumerate(calls) if call == ["git", "reset", "--hard", "origin/main"]]
    assert len(fetches) == len(resets) == expected_attempts
    generated = [i for i, call in enumerate(calls) if call[0] == "python3"]
    assert all(fetch < reset < generate for fetch, reset, generate in zip(fetches, resets, generated))
    if generation_failure:
        assert not any(call[:2] in [["git", "add"], ["git", "commit"], ["git", "push"]] for call in calls)
    else:
        stages = [call for call in calls if call[:2] == ["git", "add"]]
        assert stages == [["git", "add", "docs/status.json", "docs/licenses.json"]] * expected_attempts
        pushes = [call for call in calls if call[:2] == ["git", "push"]]
        assert pushes == ([] if no_changes else [["git", "push", "origin", "HEAD:main"]] * expected_attempts)
