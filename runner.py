#!/usr/bin/env python3
"""
World Wide Law — Source Runner

CLI for running and testing legal data scrapers.

Usage:
  python runner.py status                              # Show project status summary
  python runner.py next                                # Show the next source to build
  python runner.py test <source>                       # Test a source's scraper (sample mode)
  python runner.py sample <source>                     # Run sample mode for a source
  python runner.py fast <source>                       # Run bootstrap_fast on a source
  python runner.py batch [--max-parallel 3]            # Run multiple sources in parallel
  python runner.py retrieve-test <source>              # Run retrieve.py tests for a source
  python runner.py retrieve-next                       # Find next source needing a retrieve script
"""

import sys
import yaml
import json
import subprocess
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional
from concurrent.futures import ProcessPoolExecutor, as_completed

PROJECT_ROOT = Path(__file__).parent


def load_manifest() -> dict:
    """Load the project manifest."""
    manifest_path = PROJECT_ROOT / "manifest.yaml"
    with open(manifest_path, "r") as f:
        return yaml.safe_load(f)


def save_manifest(manifest: dict):
    """Save the project manifest."""
    manifest_path = PROJECT_ROOT / "manifest.yaml"
    with open(manifest_path, "w") as f:
        yaml.dump(manifest, f, default_flow_style=False, allow_unicode=True)


def get_next_source(manifest: dict) -> Optional[dict]:
    """Find the highest-priority source with status 'planned' or 'needs_maintenance'."""
    sources = manifest.get("sources", [])
    planned = [s for s in sources if s.get("status") == "planned"]
    if planned:
        planned.sort(key=lambda s: (s.get("priority", 99), s.get("id", "")))
        return planned[0]
    maintenance = [s for s in sources if s.get("status") == "needs_maintenance"]
    if maintenance:
        maintenance.sort(key=lambda s: (s.get("priority", 99), s.get("id", "")))
        return maintenance[0]
    return None


def get_status_summary(manifest: dict) -> dict:
    """Generate a status summary of the project."""
    sources = manifest.get("sources", [])
    by_status = {}
    by_country = {}

    for s in sources:
        status = s.get("status", "unknown")
        country = s.get("country", "??")

        by_status[status] = by_status.get(status, 0) + 1
        if country not in by_country:
            by_country[country] = {"total": 0, "complete": 0, "in_progress": 0, "planned": 0, "blocked": 0, "needs_maintenance": 0}
        by_country[country]["total"] += 1
        by_country[country][status] = by_country[country].get(status, 0) + 1

    return {
        "total_sources": len(sources),
        "by_status": by_status,
        "by_country": by_country,
        "last_updated": datetime.now(timezone.utc).isoformat(),
    }


def _run_source_fast(source_id: str):
    """Run bootstrap_fast for a single source (for use in parallel batch)."""
    source_dir = PROJECT_ROOT / "sources" / source_id
    bootstrap_path = source_dir / "bootstrap.py"
    if not bootstrap_path.exists():
        return source_id, "error", "No bootstrap.py"

    result = subprocess.run(
        [sys.executable, str(bootstrap_path), "bootstrap-fast"],
        capture_output=True, text=True, timeout=7200,
    )
    if result.returncode == 0:
        return source_id, "ok", result.stdout[-200:] if result.stdout else ""
    else:
        return source_id, "error", result.stderr[-200:] if result.stderr else ""


def run_batch(manifest: dict, max_parallel: int = 3):
    """Run multiple sources in parallel."""
    sources = manifest.get("sources", [])

    runnable = []
    for s in sources:
        if s.get("status") in ("complete", "planned"):
            source_dir = PROJECT_ROOT / "sources" / s["id"]
            if (source_dir / "bootstrap.py").exists():
                runnable.append(s)

    runnable.sort(key=lambda s: (s.get("priority", 99), s.get("id", "")))

    seen_countries = set()
    batch = []
    for s in runnable:
        country = s.get("country", "")
        if country not in seen_countries and len(batch) < max_parallel:
            batch.append(s)
            seen_countries.add(country)

    if not batch:
        print("No runnable sources found for batch execution.")
        return

    print(f"Running {len(batch)} sources in parallel (max {max_parallel}):")
    for s in batch:
        print(f"  - {s['id']} (priority {s.get('priority', '?')})")
    print()

    with ProcessPoolExecutor(max_workers=max_parallel) as executor:
        futures = {executor.submit(_run_source_fast, s["id"]): s["id"] for s in batch}
        for future in as_completed(futures):
            source_id, status, detail = future.result()
            mark = "+" if status == "ok" else "x"
            print(f"  [{mark}] {source_id}: {status}")
            if detail:
                print(f"    {detail.strip()[:200]}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python runner.py [status|next|test|sample|fast|batch|retrieve-test|retrieve-next] [source_id]")
        sys.exit(1)

    command = sys.argv[1]
    manifest = load_manifest()

    if command == "status":
        summary = get_status_summary(manifest)
        print(json.dumps(summary, indent=2))
        print(f"\nTotal: {summary['total_sources']} sources")
        for status, count in sorted(summary["by_status"].items()):
            print(f"  {status}: {count}")

    elif command == "next":
        next_source = get_next_source(manifest)
        if next_source:
            status = next_source.get("status", "planned")
            if status == "needs_maintenance":
                print(f"Next source to maintain: {next_source['id']}")
            else:
                print(f"Next source to build: {next_source['id']}")
            print(f"  Status: {status}")
            print(f"  Name: {next_source['name']}")
            print(f"  Priority: {next_source.get('priority', '?')}")
            print(f"  Data types: {next_source.get('data_types', [])}")
            print(f"  Auth: {next_source.get('auth', 'none')}")
            print(f"  URL: {next_source.get('url', 'N/A')}")
            print(f"  Notes: {next_source.get('notes', '')}")
        else:
            print("All sources are complete or blocked! Check GitHub issues for work.")

    elif command == "test":
        if len(sys.argv) < 3:
            print("Usage: python runner.py test <source_id>")
            sys.exit(1)
        source_id = sys.argv[2]
        source_dir = PROJECT_ROOT / "sources" / source_id
        bootstrap_path = source_dir / "bootstrap.py"
        if bootstrap_path.exists():
            print(f"Testing {source_id}...")
            result = subprocess.run(
                [sys.executable, str(bootstrap_path), "bootstrap", "--sample"],
                capture_output=True, text=True
            )
            print(result.stdout)
            if result.stderr:
                print(f"STDERR: {result.stderr}")
        else:
            print(f"No bootstrap.py found for {source_id}")

    elif command == "sample":
        if len(sys.argv) < 3:
            print("Usage: python runner.py sample <source_id>")
            sys.exit(1)
        source_id = sys.argv[2]
        source_dir = PROJECT_ROOT / "sources" / source_id
        bootstrap_path = source_dir / "bootstrap.py"
        if bootstrap_path.exists():
            print(f"Running sample for {source_id}...")
            result = subprocess.run(
                [sys.executable, str(bootstrap_path), "bootstrap", "--sample", "--sample-size", "10"],
                capture_output=True, text=True
            )
            print(result.stdout)
            if result.stderr:
                print(f"STDERR: {result.stderr}")
        else:
            print(f"No bootstrap.py found for {source_id}")

    elif command == "fast":
        if len(sys.argv) < 3:
            print("Usage: python runner.py fast <source_id> [--workers N] [--batch-size N]")
            sys.exit(1)
        source_id = sys.argv[2]
        source_dir = PROJECT_ROOT / "sources" / source_id
        bootstrap_path = source_dir / "bootstrap.py"
        if bootstrap_path.exists():
            workers = 5
            batch_size = 100
            for i, arg in enumerate(sys.argv[3:], 3):
                if arg == "--workers" and i + 1 < len(sys.argv):
                    workers = int(sys.argv[i + 1])
                if arg == "--batch-size" and i + 1 < len(sys.argv):
                    batch_size = int(sys.argv[i + 1])
            print(f"Running fast bootstrap for {source_id} (workers={workers}, batch={batch_size})...")
            subprocess.run(
                [sys.executable, str(bootstrap_path), "bootstrap-fast",
                 "--workers", str(workers), "--batch-size", str(batch_size)],
                text=True
            )
        else:
            print(f"No bootstrap.py found for {source_id}")

    elif command == "batch":
        max_parallel = 3
        for i, arg in enumerate(sys.argv[2:], 2):
            if arg == "--max-parallel" and i + 1 < len(sys.argv):
                max_parallel = int(sys.argv[i + 1])
        run_batch(manifest, max_parallel)

    elif command == "retrieve-test":
        if len(sys.argv) < 3:
            print("Usage: python runner.py retrieve-test <source_id>")
            sys.exit(1)
        source_id = sys.argv[2]
        retrieve_path = PROJECT_ROOT / "sources" / source_id / "retrieve.py"
        if retrieve_path.exists():
            print(f"Running retrieve tests for {source_id}...")
            result = subprocess.run(
                [sys.executable, str(retrieve_path), "--test"],
                capture_output=True, text=True
            )
            print(result.stdout)
            if result.stderr:
                print(f"STDERR: {result.stderr}")
            sys.exit(result.returncode)
        else:
            print(f"No retrieve.py found for {source_id}")
            sys.exit(1)

    elif command == "retrieve-next":
        sources = manifest.get("sources", [])
        for s in sorted(sources, key=lambda s: (s.get("priority", 99), s.get("id", ""))):
            if s.get("status") != "complete":
                continue
            sid = s["id"]
            source_dir = PROJECT_ROOT / "sources" / sid
            sample_dir = source_dir / "sample"
            if not sample_dir.exists():
                continue
            sample_files = list(sample_dir.glob("record_*.json"))
            if not sample_files:
                continue
            if (source_dir / "retrieve.py").exists():
                continue
            print(f"Next source needing retrieve script: {sid}")
            print(f"  Name: {s.get('name', '')}")
            print(f"  Data types: {s.get('data_types', [])}")
            print(f"  Samples: {len(sample_files)}")
            sys.exit(0)
        print("All complete sources with samples already have retrieve scripts.")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
