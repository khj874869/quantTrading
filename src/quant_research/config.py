from __future__ import annotations

import json
import os
import re
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class Config:
    path: Path
    raw: dict

    @classmethod
    def load(cls, path: str | Path) -> "Config":
        config_path = _expand_path(path).resolve()
        with config_path.open("r", encoding="utf-8-sig") as handle:
            return cls(path=config_path, raw=json.load(handle))

    @property
    def paths(self) -> dict:
        return self.raw.get("paths", {})

    @property
    def api(self) -> dict:
        return self.raw.get("api", {})

    @property
    def strategy(self) -> dict:
        return self.raw.get("strategy", {})

    def with_path_overrides(self, **overrides: str | Path | None) -> "Config":
        normalized = {
            key: str(value)
            for key, value in overrides.items()
            if value not in (None, "")
        }
        if not normalized:
            return self
        raw = deepcopy(self.raw)
        paths = dict(raw.get("paths", {}))
        paths.update(normalized)
        raw["paths"] = paths
        return Config(path=self.path, raw=raw)

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


def _expand_percent_vars(value: str) -> str:
    def replace(match: re.Match[str]) -> str:
        key = match.group(1)
        return os.environ.get(key, match.group(0))

    return re.sub(r"%([^%]+)%", replace, value)
