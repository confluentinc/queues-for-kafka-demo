"""Configuration loader: reads settings from a `.properties` file.

Looks for `config.properties` at the project root by default; set
`QFK_CONFIG_FILE` to point at a different file. Lines are `key=value`,
`#`/`!` start a comment, and a commented-out key falls back to the default
passed by the caller.
"""

import os
from pathlib import Path
from typing import Dict, Optional

_DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.properties"

_properties: Optional[Dict[str, str]] = None


def _config_path() -> Path:
    override = os.environ.get("QFK_CONFIG_FILE")
    return Path(override) if override else _DEFAULT_CONFIG_PATH


def _load() -> Dict[str, str]:
    global _properties
    if _properties is not None:
        return _properties

    properties: Dict[str, str] = {}
    path = _config_path()
    if path.exists():
        for raw_line in path.read_text().splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or line.startswith("!") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            properties[key.strip()] = value.strip()
    else:
        print(f"⚠️ Config file not found at {path}, using built-in defaults")

    _properties = properties
    return _properties


def get(name: str, default: str) -> str:
    value = _load().get(name)
    return value if value else default


def get_int(name: str, default: int) -> int:
    value = _load().get(name)
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        print(f"Invalid {name}={value!r}, using default {default}")
        return default


def get_float(name: str, default: float) -> float:
    value = _load().get(name)
    if not value:
        return default
    try:
        return float(value)
    except ValueError:
        print(f"Invalid {name}={value!r}, using default {default}")
        return default
