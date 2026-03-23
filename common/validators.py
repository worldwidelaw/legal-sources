"""
Schema validation for Legal Data Hunter.

Validates normalized records against expected schemas defined in config.yaml.
Ensures data quality before storage.
"""

import logging
from typing import Tuple

logger = logging.getLogger("legal-data-hunter")


# Fields that every normalized record must have, regardless of source
REQUIRED_BASE_FIELDS = {
    "_id": str,
    "_source": str,
    "_type": str,        # "legislation" or "case_law"
    "_fetched_at": str,  # ISO 8601
}

VALID_TYPES = {"legislation", "case_law", "regulation", "directive", "treaty", "parliamentary_proceedings", "doctrine", "other"}


class SchemaValidator:
    """
    Validates records against a schema defined in the source's config.yaml.

    The schema config looks like:
        schema:
          key_fields:
            - name: "article_id"
              type: "string"
              required: true
            - name: "text"
              type: "string"
              required: true
            - name: "effective_date"
              type: "date"
              required: false
    """

    TYPE_MAP = {
        "string": str,
        "str": str,
        "int": int,
        "integer": int,
        "float": float,
        "number": (int, float),
        "bool": bool,
        "boolean": bool,
        "list": list,
        "dict": dict,
        "date": str,      # dates are stored as ISO strings
        "datetime": str,
        "enum": str,
    }

    def __init__(self, schema_config: dict):
        self.schema_config = schema_config
        self.key_fields = schema_config.get("key_fields", [])
        self.secondary_fields = schema_config.get("secondary_fields", [])

    def validate(self, record: dict) -> Tuple[bool, list[str]]:
        """
        Validate a normalized record.

        Returns:
            (is_valid, list_of_error_messages)
            A record can be valid with warnings (empty error list).
        """
        errors = []

        # Check base required fields
        for field_name, expected_type in REQUIRED_BASE_FIELDS.items():
            if field_name not in record:
                errors.append(f"Missing required base field: {field_name}")
            elif not isinstance(record[field_name], expected_type):
                errors.append(
                    f"Field {field_name} should be {expected_type.__name__}, "
                    f"got {type(record[field_name]).__name__}"
                )

        # Check _type value
        record_type = record.get("_type", "")
        if record_type and record_type not in VALID_TYPES:
            errors.append(f"Invalid _type: {record_type}. Expected one of: {VALID_TYPES}")

        # Check key fields from schema
        for field_spec in self.key_fields:
            name = field_spec.get("name")
            required = field_spec.get("required", True)
            field_type = field_spec.get("type", "string")

            if name not in record:
                if required:
                    errors.append(f"Missing required key field: {name}")
                continue

            value = record[name]
            if value is None:
                if required:
                    errors.append(f"Key field {name} is None but required")
                continue

            expected = self.TYPE_MAP.get(field_type)
            if expected and not isinstance(value, expected):
                errors.append(
                    f"Field {name} should be {field_type}, got {type(value).__name__}"
                )

            # Check enum values if specified
            if field_spec.get("values") and value not in field_spec["values"]:
                errors.append(
                    f"Field {name} value '{value}' not in allowed values: {field_spec['values']}"
                )

        is_valid = len(errors) == 0
        if not is_valid:
            logger.warning(f"Validation failed for record {record.get('_id', '?')}: {errors}")

        return is_valid, errors

    def summarize_record(self, record: dict) -> dict:
        """
        Generate a summary of a record's field coverage.
        Useful for sample data analysis.
        """
        total_fields = len(record)
        non_null = sum(1 for v in record.values() if v is not None and v != "" and v != [])
        key_present = sum(
            1 for f in self.key_fields if f.get("name") in record and record[f["name"]] is not None
        )
        key_total = len(self.key_fields)

        return {
            "total_fields": total_fields,
            "non_null_fields": non_null,
            "key_fields_present": f"{key_present}/{key_total}",
            "completeness": round(non_null / total_fields * 100, 1) if total_fields else 0,
        }
