from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class Config:
    path: Path
    raw: dict

    @classmethod
    def load(cls, path: str | Path) -> "Config":
        config_path = Path(path).resolve()
        with config_path.open("r", encoding="utf-8") as handle:
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

    def resolve(self, key: str) -> Path:
        return self.resolve_path(self.paths[key])

    def resolve_path(self, value: str) -> Path:
        path = Path(value)
        if path.is_absolute():
            return path
        config_relative = (self.path.parent / path).resolve()
        if config_relative.exists() or config_relative.parent.exists():
            return config_relative
        return (Path.cwd() / path).resolve()
