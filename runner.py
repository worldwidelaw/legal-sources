#!/usr/bin/env python3
"""
Legal Data Hunter — Session Runner

This is the entry point that the Cowork shortcut calls.
It reads the manifest, finds the next task, and executes it.

Usage:
  python runner.py next               # Build the next planned scraper
  python runner.py status             # Show project status summary
  python runner.py test <source>      # Test a specific source's scraper
  python runner.py sample <source>    # Run sample mode for a source
  python runner.py fast <source>      # Run bootstrap_fast on a source
  python runner.py stress-test <source> [--duration 60] # Discover API rate limits
  python runner.py batch [--max-parallel 3]             # Run multiple sources in parallel
  python runner.py retrieve-test <source>  # Test a source's retrieve.py
  python runner.py retrieve-next           # Find next source needing retrieve script
"""

import sys
import yaml
import json
import time
import subprocess
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, List
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


def read_inbox() -> str:
    """Read INBOX.md contents."""
    inbox_path = PROJECT_ROOT / "INBOX.md"
    if inbox_path.exists():
        return inbox_path.read_text()
    return ""


def read_blocked() -> List[dict]:
    """Parse BLOCKED.md and return list of blocked items."""
    blocked_path = PROJECT_ROOT / "BLOCKED.md"
    if not blocked_path.exists():
        return []
    # Simple parsing — look for ### headers with status
    content = blocked_path.read_text()
    # Return raw content for the AI to parse
    return content


def get_next_source(manifest: dict) -> Optional[dict]:
    """Find the highest-priority source with status 'planned' or 'needs_maintenance'.

    Priority order: all 'planned' sources first, then 'needs_maintenance'.
    Within each group, sort by priority field (lower = higher priority).
    """
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


def get_next_retrieve_source(manifest: dict) -> Optional[dict]:
    """Find the next complete source that needs a retrieve script.

    Returns the highest-priority complete source that:
    - Has status 'complete'
    - Does NOT have retrieve: true in manifest
    - Does NOT already have a retrieve.py file
    """
    sources = manifest.get("sources", [])
    candidates = []
    for s in sources:
        if s.get("status") != "complete":
            continue
        if s.get("retrieve"):
            continue
        source_dir = PROJECT_ROOT / "sources" / s["id"].replace("/", "/")
        retrieve_path = source_dir / "retrieve.py"
        if retrieve_path.exists():
            continue
        candidates.append(s)
    if candidates:
        candidates.sort(key=lambda s: (s.get("priority", 99), s.get("id", "")))
        return candidates[0]
    return None


def run_retrieve_test(source_id: str) -> bool:
    """Run retrieve tests for a source.

    Returns True if all tests pass, False otherwise.
    """
    source_dir = PROJECT_ROOT / "sources" / source_id.replace("/", "/")
    retrieve_path = source_dir / "retrieve.py"

    if not retrieve_path.exists():
        print(f"No retrieve.py found for {source_id}")
        return False

    result = subprocess.run(
        [sys.executable, str(retrieve_path), "--test"],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.stderr:
        print(f"STDERR: {result.stderr}")

    return result.returncode == 0


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


def run_stress_test(source_id: str, duration: int = 60):
    """
    Empirically discover the rate limit of a source's API.

    Sends lightweight requests at increasing rates, observing when
    429s or errors appear. Reports the sustainable rate.
    """
    source_dir = PROJECT_ROOT / "sources" / source_id.replace("/", "/")
    config_path = source_dir / "config.yaml"

    if not config_path.exists():
        print(f"No config.yaml found for {source_id}")
        sys.exit(1)

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # Try multiple config patterns to find the base URL
    base_url = (
        config.get("api", {}).get("base_url")
        or config.get("source", {}).get("url")
        or config.get("endpoints", {}).get("portal")
        or config.get("endpoints", {}).get("toc", "").rsplit("/", 1)[0]
    )
    if not base_url:
        print(f"No base_url found in config for {source_id}")
        sys.exit(1)

    try:
        import requests
    except ImportError:
        print("Install requests: pip install requests")
        sys.exit(1)

    print(f"Stress-testing {source_id} at {base_url}")
    print(f"Duration: {duration}s\n")

    # Test rates: 1, 2, 5, 10, 20, 50 req/sec
    test_rates = [1, 2, 5, 10, 20, 50]
    results = {}
    session = requests.Session()
    session.headers["User-Agent"] = "LegalDataHunter/1.0 (Stress Test)"

    for rate in test_rates:
        delay = 1.0 / rate
        successes = 0
        errors_429 = 0
        errors_other = 0
        phase_duration = min(duration // len(test_rates), 15)
        start = time.monotonic()

        print(f"Testing {rate} req/s for {phase_duration}s...", end=" ", flush=True)

        while time.monotonic() - start < phase_duration:
            try:
                resp = session.head(base_url, timeout=10, allow_redirects=True)
                if resp.status_code == 429:
                    errors_429 += 1
                    retry_after = int(resp.headers.get("Retry-After", 5))
                    time.sleep(retry_after)
                elif resp.status_code < 400:
                    successes += 1
                else:
                    errors_other += 1
            except Exception:
                errors_other += 1
            time.sleep(delay)

        total = successes + errors_429 + errors_other
        results[rate] = {
            "success": successes,
            "429s": errors_429,
            "errors": errors_other,
            "total": total,
            "success_pct": round(100 * successes / total, 1) if total > 0 else 0,
        }
        print(f"✓ {successes}/{total} ok, {errors_429} throttled, {errors_other} errors")

        # Stop escalating if we're getting throttled
        if errors_429 > total * 0.1:
            print(f"  → Throttling detected at {rate} req/s, stopping escalation")
            break

    # Determine safe rate
    safe_rate = 1
    for rate, res in sorted(results.items()):
        if res["429s"] == 0 and res["errors"] <= res["total"] * 0.05:
            safe_rate = rate

    print(f"\n{'='*50}")
    print(f"RESULT: Safe sustainable rate for {source_id}: {safe_rate} req/s")
    print(f"{'='*50}")

    # Save to discovered_limits.yaml
    limits_path = PROJECT_ROOT / "discovered_limits.yaml"
    limits = {}
    if limits_path.exists():
        with open(limits_path, "r") as f:
            limits = yaml.safe_load(f) or {}

    limits[source_id] = {
        "safe_rate": safe_rate,
        "tested_at": datetime.now(timezone.utc).isoformat(),
        "details": results,
    }
    with open(limits_path, "w") as f:
        yaml.dump(limits, f, default_flow_style=False)
    print(f"Saved to {limits_path}")


def _run_source_fast(source_id: str):
    """Run bootstrap_fast for a single source (for use in parallel batch)."""
    source_dir = PROJECT_ROOT / "sources" / source_id.replace("/", "/")
    bootstrap_path = source_dir / "bootstrap.py"
    if not bootstrap_path.exists():
        return source_id, "error", "No bootstrap.py"

    result = subprocess.run(
        [sys.executable, str(bootstrap_path), "bootstrap-fast"],
        capture_output=True, text=True, timeout=7200,  # 2 hour timeout
    )
    if result.returncode == 0:
        return source_id, "ok", result.stdout[-200:] if result.stdout else ""
    else:
        return source_id, "error", result.stderr[-200:] if result.stderr else ""


def run_batch(manifest: dict, max_parallel: int = 3):
    """
    Run multiple sources in parallel.

    Picks the top N highest-priority planned or complete sources
    (targeting different servers) and runs bootstrap_fast concurrently.
    """
    sources = manifest.get("sources", [])

    # Get sources that have bootstrap.py and are either planned or complete
    runnable = []
    for s in sources:
        if s.get("status") in ("complete", "planned"):
            source_dir = PROJECT_ROOT / "sources" / s["id"].replace("/", "/")
            if (source_dir / "bootstrap.py").exists():
                runnable.append(s)

    # Sort by priority
    runnable.sort(key=lambda s: (s.get("priority", 99), s.get("id", "")))

    # Deduplicate by country (different servers)
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
        futures = {
            executor.submit(_run_source_fast, s["id"]): s["id"]
            for s in batch
        }
        for future in as_completed(futures):
            source_id, status, detail = future.result()
            emoji = "✓" if status == "ok" else "✗"
            print(f"  {emoji} {source_id}: {status}")
            if detail:
                print(f"    {detail.strip()[:200]}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python runner.py [next|status|test|sample|fast|stress-test|batch|retrieve-test|retrieve-next] [source_id]")
        sys.exit(1)

    command = sys.argv[1]
    manifest = load_manifest()

    if command == "status":
        summary = get_status_summary(manifest)
        parts = [f"Total: {summary['total_sources']}"]
        parts.extend(f"{s}: {c}" for s, c in sorted(summary["by_status"].items()))
        print(" | ".join(parts))

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
        source_dir = PROJECT_ROOT / "sources" / source_id.replace("/", "/")
        bootstrap_path = source_dir / "bootstrap.py"
        if bootstrap_path.exists():
            print(f"Testing {source_id}...")
            import subprocess
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
        source_dir = PROJECT_ROOT / "sources" / source_id.replace("/", "/")
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
        source_dir = PROJECT_ROOT / "sources" / source_id.replace("/", "/")
        bootstrap_path = source_dir / "bootstrap.py"
        if bootstrap_path.exists():
            # Parse optional args
            workers = 5
            batch = 100
            for i, arg in enumerate(sys.argv[3:], 3):
                if arg == "--workers" and i + 1 < len(sys.argv):
                    workers = int(sys.argv[i + 1])
                if arg == "--batch-size" and i + 1 < len(sys.argv):
                    batch = int(sys.argv[i + 1])
            print(f"Running fast bootstrap for {source_id} (workers={workers}, batch={batch})...")
            result = subprocess.run(
                [sys.executable, str(bootstrap_path), "bootstrap-fast",
                 "--workers", str(workers), "--batch-size", str(batch)],
                capture_output=False, text=True
            )
        else:
            print(f"No bootstrap.py found for {source_id}")

    elif command == "stress-test":
        if len(sys.argv) < 3:
            print("Usage: python runner.py stress-test <source_id> [--duration 60]")
            sys.exit(1)
        source_id = sys.argv[2]
        duration = 60
        for i, arg in enumerate(sys.argv[3:], 3):
            if arg == "--duration" and i + 1 < len(sys.argv):
                duration = int(sys.argv[i + 1])
        run_stress_test(source_id, duration)

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
        print(f"Running retrieve tests for {source_id}...")
        success = run_retrieve_test(source_id)
        sys.exit(0 if success else 1)

    elif command == "retrieve-next":
        next_source = get_next_retrieve_source(manifest)
        if next_source:
            print(f"Next source needing retrieve script: {next_source['id']}")
            print(f"  Name: {next_source['name']}")
            print(f"  Priority: {next_source.get('priority', '?')}")
            print(f"  Data types: {next_source.get('data_types', [])}")
            print(f"  URL: {next_source.get('url', 'N/A')}")
        else:
            print("All complete sources have retrieve scripts or are not eligible.")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
