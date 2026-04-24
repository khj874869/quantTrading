from __future__ import annotations

import json
import os
import re
import tomllib
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ConfigDict = dict[str, Any]


@dataclass(slots=True)
class Config:
    path: Path
    raw: ConfigDict

    @classmethod
    def load(cls, path: str | Path) -> "Config":
        config_path = _expand_path(path).resolve()
        return cls(path=config_path, raw=_load_config_payload(config_path))

    @property
    def paths(self) -> ConfigDict:
        return self.section("paths")

    @property
    def api(self) -> ConfigDict:
        return self.section("api")

    @property
    def strategy(self) -> ConfigDict:
        return self.section("strategy")

    @property
    def wrds(self) -> ConfigDict:
        return self.section("wrds")

    @property
    def cache(self) -> ConfigDict:
        return self.section("cache")

    @property
    def sweep(self) -> ConfigDict:
        return self.section("sweep")

    @property
    def walk_forward(self) -> ConfigDict:
        return self.section("walk_forward")

    def section(self, name: str) -> ConfigDict:
        section = self.raw.get(name, {})
        if isinstance(section, dict):
            return section
        raise TypeError(f"config section {name!r} must be an object")

    def with_section_overrides(self, section: str, **overrides: object) -> "Config":
        normalized = {
            key: value
            for key, value in overrides.items()
            if value is not None
        }
        if not normalized:
            return self
        raw = deepcopy(self.raw)
        section_payload = dict(self.section(section))
        section_payload.update(normalized)
        raw[section] = section_payload
        return Config(path=self.path, raw=raw)

    def with_path_overrides(self, **overrides: str | Path | None) -> "Config":
        normalized = {
            key: str(value)
            for key, value in overrides.items()
            if value not in (None, "")
        }
        return self.with_section_overrides("paths", **normalized)

    def with_strategy_overrides(self, **overrides: object) -> "Config":
        return self.with_section_overrides("strategy", **overrides)

    def resolve(self, key: str) -> Path:
        return self.resolve_path(self.paths[key])

    def resolve_path(self, value: str) -> Path:
        path = _expand_path(value)
        if path.is_absolute():
            return path
        config_relative = (self.path.parent / path).resolve()
        if config_relative.exists() or config_relative.parent.exists():
            return config_relative
        return (Path.cwd() / path).resolve()


def _expand_path(value: str | Path) -> Path:
    expanded = _expand_percent_vars(str(value))
    expanded = os.path.expandvars(expanded)
    return Path(expanded).expanduser()


def _load_config_payload(path: Path) -> ConfigDict:
    suffix = path.suffix.lower()
    if suffix == ".json":
        with path.open("r", encoding="utf-8-sig") as handle:
            payload = json.load(handle)
    elif suffix == ".toml":
        with path.open("rb") as handle:
            payload = tomllib.load(handle)
    else:
        raise ValueError(f"Unsupported config format: {path.suffix or '<no extension>'}")
    if not isinstance(payload, dict):
        raise ValueError(f"Config root must be an object: {path}")
    return payload


def _expand_percent_vars(value: str) -> str:
    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        return os.environ.get(key, match.group(0))

    return re.sub(r"%([^%]+)%", replace, value)
