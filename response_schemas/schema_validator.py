import logging
from typing import Any

logger = logging.getLogger(__name__)

# Maps schema type names to accepted Python types
_TYPE_MAP = {
    "str": (str,),
    "int": (int,),
    "float": (int, float),  # int is acceptable where float is expected
    "bool": (bool,),
    "null": (type(None),),
    "list": (list,),
    "dict": (dict,),
}


def validate_response(response: dict, schema: dict) -> list[str]:
    """Validate an API response against a stored schema.

    Args:
        response: Parsed JSON response from Alpha Vantage.
        schema: Schema dict produced by ``infer_schema`` or loaded from file.

    Returns:
        List of violation description strings.  Empty list means valid.
    """
    violations: list[str] = []
    _validate_node(response, schema, "$", violations)
    return violations


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _validate_node(value: Any, schema: dict, path: str, violations: list[str]) -> None:
    expected_type = schema.get("_type")

    if expected_type in ("unknown", "mixed"):
        return

    # Null handling
    if value is None:
        if expected_type != "null" and not schema.get("_optional"):
            violations.append(f"{path}: expected {expected_type}, got null")
        return

    # Type check
    if expected_type in _TYPE_MAP:
        accepted = _TYPE_MAP[expected_type]
        # bool is a subclass of int — reject bools when int is expected
        if expected_type == "int" and isinstance(value, bool):
            violations.append(f"{path}: expected int, got bool")
            return
        if not isinstance(value, accepted):
            violations.append(f"{path}: expected {expected_type}, got {type(value).__name__}")
            return

    # Recurse into dicts
    if expected_type == "dict" and isinstance(value, dict):
        _validate_dict(value, schema, path, violations)

    # Recurse into lists
    elif expected_type == "list" and isinstance(value, list):
        elem_schema = schema.get("element", {})
        for i, item in enumerate(value):
            _validate_node(item, elem_schema, f"{path}[{i}]", violations)


def _validate_dict(value: dict, schema: dict, path: str, violations: list[str]) -> None:
    children = schema.get("children", {})
    is_dynamic = schema.get("_dynamic_keys", False)

    if is_dynamic and "*" in children:
        wildcard_schema = children["*"]
        for k, v in value.items():
            _validate_node(v, wildcard_schema, f"{path}.{k}", violations)
    else:
        # Missing required keys
        for key, child_schema in children.items():
            if key not in value:
                if not child_schema.get("_optional"):
                    violations.append(f"{path}: missing required key '{key}'")
            else:
                _validate_node(value[key], child_schema, f"{path}.{key}", violations)

        # Unexpected keys
        expected_keys = set(children.keys())
        unexpected = set(value.keys()) - expected_keys
        if unexpected:
            violations.append(f"{path}: unexpected keys {sorted(unexpected)}")
