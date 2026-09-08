"""Exercise the refresh CLI in a synthetic checkout, without data or credentials."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("failure", [None, "missing-generator", "missing-registry", "invalid-type"])
def test_dashboard_refresh_requires_license_generation(tmp_path, failure):
    shutil.copyfile(ROOT / "generate_dashboard.py", tmp_path / "generate_dashboard.py")
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    if failure != "missing-generator" and (ROOT / "scripts/generate_licenses_json.py").exists():
        shutil.copyfile(ROOT / "scripts/generate_licenses_json.py", scripts / "generate_licenses_json.py")
    source = {"id": "ZZ/Test", "country": "ZZ", "status": "complete"}
    if failure == "invalid-type":
        source["commercial_use"] = "false"
    (tmp_path / "manifest.yaml").write_text(yaml.safe_dump({"sources": [source]}))
    if failure != "missing-registry":
        (tmp_path / "license_registry.yaml").write_text("licenses: {}\n")
    docs = tmp_path / "docs"
    docs.mkdir()
    (docs / "status.json").write_text("previous status")
    (docs / "licenses.json").write_text("previous licenses")
    env = {k: v for k, v in os.environ.items()
           if k not in {"NEON_DATABASE_URL", "DATABASE_URL", "PYTHONPATH"}}
    env["HOME"] = str(tmp_path)
    result = subprocess.run([sys.executable, "generate_dashboard.py"], cwd=tmp_path,
                            env=env, capture_output=True, text=True, timeout=15)
    if failure:
        assert result.returncode != 0, result.stdout + result.stderr
        assert (docs / "status.json").read_text() == "previous status"
        assert (docs / "licenses.json").read_text() == "previous licenses"
    else:
        assert result.returncode == 0, result.stdout + result.stderr
        assert json.loads((docs / "status.json").read_text())["summary"]["complete"] == 1
        inventory = json.loads((docs / "licenses.json").read_text())
        assert inventory["sources"][0]["commercial_use"] is None
        assert inventory["summary"]["commercial_unknown"] == 1


def test_refresh_stages_both_outputs_and_has_generator_dependencies():
    assert (ROOT / "scripts/generate_licenses_json.py").is_file()
    registry = yaml.safe_load((ROOT / "license_registry.yaml").read_text())
    assert isinstance(registry["licenses"], dict)
    workflow = yaml.safe_load((ROOT / ".github/workflows/refresh-dashboard.yml").read_text())
    runs = "\n".join(step.get("run", "") for step in workflow["jobs"]["refresh"]["steps"])
    assert "pyyaml" in runs.lower()
    assert "git add docs/status.json docs/licenses.json" in runs
    # PyYAML's YAML 1.1 loader treats the GitHub 'on' key as True.
    triggers = workflow.get("on", workflow.get(True))
    if "push" in triggers:
        paths = triggers["push"]["paths"]
        for required in ("manifest.yaml", "license_registry.yaml", "generate_dashboard.py",
                         "scripts/generate_licenses_json.py", ".github/workflows/refresh-dashboard.yml"):
            assert required in paths
        assert "schedule" in triggers  # bot-token pushes do not dispatch push workflows
