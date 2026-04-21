import json
import re
import logging
from typing import Any
from pathlib import Path

logger = logging.getLogger(__name__)

# Patterns that indicate dict keys are dynamic (dates, timestamps, numeric IDs)
_DYNAMIC_KEY_PATTERNS = [
    re.compile(r"^\d{4}-\d{2}-\d{2}"),  # 2024-01-15, 2024-01-15 14:30:00
    re.compile(r"^\d{4}-\d{2}-\d{2}T"),  # ISO timestamps
    re.compile(r"^\d+$"),  # Pure numeric keys
]

# A dict needs at least this many children before we consider it dynamic
_DYNAMIC_KEY_THRESHOLD = 3


def infer_schema(response: dict) -> dict:
    """Infer a structural schema from an Alpha Vantage API response.

    Walks the JSON recursively and produces a schema dict that describes
    key names, nesting, value types, and dynamic-key regions.

    Args:
        response: Parsed JSON response dict from Alpha Vantage.

    Returns:
        Schema dict describing the structure.
    """
    return _infer_node(response)


def infer_schema_from_samples(responses: list[dict]) -> dict:
    """Infer a schema from multiple sample responses of the same endpoint.

    Fields present in some but not all samples are marked ``_optional``.
    """
    if not responses:
        raise ValueError("Need at least one response to infer a schema.")
    schemas = [_infer_node(r) for r in responses]
    return _merge_schemas(schemas)


def save_schema(schema: dict, endpoint_name: str, schemas_dir: str = None) -> Path:
    """Save a schema to ``schemas/<endpoint_name>.json``."""
    if schemas_dir is None:
        schemas_dir = Path(__file__).parent / "schemas"
    else:
        schemas_dir = Path(schemas_dir)
    schemas_dir.mkdir(parents=True, exist_ok=True)

    path = schemas_dir / f"{endpoint_name}.json"
    with open(path, "w") as f:
        json.dump(schema, f, indent=2)
    logger.info(f"Schema saved to {path}")
    return path


def load_schema(endpoint_name: str, schemas_dir: str = None) -> dict:
    """Load a previously saved schema by endpoint name."""
    if schemas_dir is None:
        schemas_dir = Path(__file__).parent / "schemas"
    else:
        schemas_dir = Path(schemas_dir)

    path = schemas_dir / f"{endpoint_name}.json"
    with open(path, "r") as f:
        return json.load(f)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _infer_node(value: Any) -> dict:
    """Recursively infer the schema for a single value."""
    if value is None:
        return {"_type": "null"}
    if isinstance(value, bool):  # bool before int (bool is subclass of int)
        return {"_type": "bool"}
    if isinstance(value, int):
        return {"_type": "int"}
    if isinstance(value, float):
        return {"_type": "float"}
    if isinstance(value, str):
        return {"_type": "str"}
    if isinstance(value, list):
        if not value:
            return {"_type": "list", "element": {"_type": "unknown"}}
        element_schemas = [_infer_node(item) for item in value]
        return {"_type": "list", "element": _merge_schemas(element_schemas)}
    if isinstance(value, dict):
        if not value:
            return {"_type": "dict", "children": {}}
        children = {k: _infer_node(v) for k, v in value.items()}
        if _is_dynamic_keys(value):
            merged = _merge_schemas(list(children.values()))
            return {"_type": "dict", "_dynamic_keys": True, "children": {"*": merged}}
        return {"_type": "dict", "children": children}
    return {"_type": type(value).__name__}


def _is_dynamic_keys(d: dict) -> bool:
    """Decide whether a dict's keys are dynamic (dates, IDs, etc.).

    Uses two heuristics:
    1. More than half the keys match a known dynamic pattern (date, number).
    2. All children share the same structural signature (same key sets or types).
    """
    if len(d) < _DYNAMIC_KEY_THRESHOLD:
        return False

    keys = list(d.keys())

    # Heuristic 1: key patterns
    pattern_matches = sum(
        1 for k in keys if any(p.match(k) for p in _DYNAMIC_KEY_PATTERNS)
    )
    if pattern_matches > len(keys) * 0.5:
        return True

    # Heuristic 2: uniform child structure — only for dict/list children.
    # A flat dict of primitives (e.g. {"1. Info": "...", "2. Symbol": "..."})
    # should NOT be treated as dynamic just because all values share a type.
    child_signatures = []
    for v in d.values():
        if isinstance(v, dict):
            child_signatures.append(frozenset(v.keys()))
        elif isinstance(v, list):
            child_signatures.append("list")
        else:
            child_signatures.append(None)  # primitives don't contribute

    # Only trigger if we have non-primitive children that are all the same shape
    non_primitive = [s for s in child_signatures if s is not None]
    if non_primitive and len(non_primitive) == len(child_signatures) and len(set(non_primitive)) == 1:
        return True

    return False


def _merge_schemas(schemas: list[dict]) -> dict:
    """Merge several schemas into one, marking divergences as optional."""
    if not schemas:
        return {"_type": "unknown"}
    if len(schemas) == 1:
        return schemas[0]

    types = {s["_type"] for s in schemas}

    # All same type → merge deeper
    if len(types) == 1:
        t = types.pop()
        if t == "dict":
            return _merge_dict_schemas(schemas)
        if t == "list":
            elements = [s["element"] for s in schemas if "element" in s]
            merged_elem = _merge_schemas(elements) if elements else {"_type": "unknown"}
            return {"_type": "list", "element": merged_elem}
        return schemas[0]

    # One type is null, rest agree → optional
    non_null = [s for s in schemas if s["_type"] != "null"]
    if non_null and len({s["_type"] for s in non_null}) == 1:
        result = _merge_schemas(non_null) if len(non_null) > 1 else non_null[0].copy()
        result["_optional"] = True
        return result

    # Truly mixed types
    return {"_type": "mixed", "_types": sorted(types)}


def _merge_dict_schemas(schemas: list[dict]) -> dict:
    """Merge multiple dict schemas, tracking which keys are optional."""
    all_dynamic = all(s.get("_dynamic_keys") for s in schemas)
    any_dynamic = any(s.get("_dynamic_keys") for s in schemas)

    if all_dynamic:
        wildcards = [s["children"]["*"] for s in schemas if "*" in s.get("children", {})]
        merged = _merge_schemas(wildcards) if wildcards else {"_type": "unknown"}
        return {"_type": "dict", "_dynamic_keys": True, "children": {"*": merged}}

    # Collect every key that appears across all schemas
    all_keys: set[str] = set()
    for s in schemas:
        all_keys.update(s.get("children", {}).keys())

    total = len(schemas)
    children = {}
    for key in sorted(all_keys):
        key_schemas = [s["children"][key] for s in schemas if key in s.get("children", {})]
        merged = _merge_schemas(key_schemas) if len(key_schemas) > 1 else key_schemas[0].copy()
        if len(key_schemas) < total:
            merged["_optional"] = True
        children[key] = merged

    result = {"_type": "dict", "children": children}
    if any_dynamic:
        result["_dynamic_keys"] = True
    return result
