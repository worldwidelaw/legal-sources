"""Small, offline fixtures for the published license inventory contract."""
import json

import pytest
import yaml

from scripts import generate_licenses_json as generator


def configure(tmp_path, monkeypatch, sources, registry=None):
    manifest = tmp_path / "manifest.yaml"
    taxonomy = tmp_path / "license_registry.yaml"
    output = tmp_path / "docs" / "licenses.json"
    manifest.write_text(yaml.safe_dump({"sources": sources}))
    taxonomy.write_text(yaml.safe_dump({"licenses": registry or {}}))
    monkeypatch.setattr(generator, "MANIFEST", str(manifest))
    monkeypatch.setattr(generator, "REGISTRY", str(taxonomy))
    monkeypatch.setattr(generator, "OUTPUT", str(output))
    return output


def test_commercial_use_is_tristate_without_guessing_or_dedup(tmp_path, monkeypatch):
    sources = [
        {"id": "ZZ/Missing", "status": "complete", "license_id": "open"},
        {"id": "ZZ/Null", "status": "complete", "commercial_use": None},
        {"id": "ZZ/True", "status": "complete", "commercial_use": True},
        {"id": "ZZ/False", "status": "complete", "commercial_use": False},
        {"id": "ZZ/True", "status": "complete", "commercial_use": True},
        {"id": "ZZ/Planned", "status": "planned", "commercial_use": True},
    ]
    output = configure(tmp_path, monkeypatch, sources, {"open": {"commercial_use": True}})
    generator.main()
    data = json.loads(output.read_text())
    assert [(s["id"], s["commercial_use"]) for s in data["sources"]] == [
        ("ZZ/False", False), ("ZZ/Missing", None), ("ZZ/Null", None),
        ("ZZ/True", True), ("ZZ/True", True),
    ]
    assert data["summary"] == {
        "total_complete": 5, "commercial_ok": 2, "non_commercial": 1,
        "commercial_unknown": 2, "unverified": 0, "unique_license_ids": 2,
    }
    assert data["sources"][1]["license_name"] == ""
    assert data["sources"][1]["license_url"] is None
    assert data["by_license"][""]["commercial_use"] is None


@pytest.mark.parametrize("value", ["true", "false", "", 0, 1, [], {}])
@pytest.mark.parametrize("location", ["source", "registry"])
def test_invalid_commercial_use_fails_without_overwriting(tmp_path, monkeypatch, value, location):
    source = {"id": "ZZ/Test", "status": "complete", "license_id": "custom"}
    registry = {"custom": {}}
    (source if location == "source" else registry["custom"])["commercial_use"] = value
    output = configure(tmp_path, monkeypatch, [source], registry)
    output.parent.mkdir()
    output.write_text("previous valid inventory")
    with pytest.raises(ValueError, match="commercial_use"):
        generator.main()
    assert output.read_text() == "previous valid inventory"


@pytest.mark.parametrize("value", ["true", "false", "", 0, 1, [], {}, None, True, False, "missing"])
def test_validator_accepts_only_boolean_or_unknown(tmp_path, monkeypatch, capsys, value):
    from scripts import validate_licenses as validator

    source = {"id": "ZZ/Test", "status": "complete", "license_id": "custom",
              "license_name": "Custom"}
    if value != "missing":
        source["commercial_use"] = value
    configure(tmp_path, monkeypatch, [source], {"custom": {"commercial_use": None}})
    monkeypatch.setattr(validator, "MANIFEST", generator.MANIFEST)
    monkeypatch.setattr(validator, "REGISTRY", generator.REGISTRY)
    monkeypatch.setattr("sys.argv", ["validate_licenses.py"])
    with pytest.raises(SystemExit) as result:
        validator.main()
    valid = value is None or type(value) is bool or value == "missing"
    assert result.value.code == (0 if valid else 1)
    report = capsys.readouterr().out
    if value is None or value == "missing":
        assert "Commercial use OK:  0" in report
        assert "Commercial unknown: 1" in report
