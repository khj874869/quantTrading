from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from datetime import datetime
from pathlib import Path

from .config import Config
from .utils import ensure_directory


REDACTED = "***REDACTED***"
SENSITIVE_KEY_TOKENS = ("password", "secret", "token", "apikey", "api_key", "private_key")


class RunManifestWriter:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.output_dir = config.resolve_path(config.paths.get("output_dir", "output"))

    def write(
        self,
        args: argparse.Namespace,
        argv: list[str],
        requested_command: str,
        executed_command: str,
        outputs: list[Path],
        summary: dict[str, object] | None = None,
        profile: dict[str, float] | None = None,
        cache: dict[str, object] | None = None,
        extra: dict[str, object] | None = None,
    ) -> Path:
        ensure_directory(self.output_dir)
        manifest_path = self.output_dir / "run_manifest.json"
        payload = {
            "generated_at": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
            "requested_command": requested_command,
            "executed_command": executed_command,
            "argv": list(argv),
            "command_options": self._command_options(args),
            "working_directory": str(Path.cwd().resolve()),
            "config": {
                "path": str(self.config.path),
                "sha256": self._file_sha256(self.config.path),
                "redacted": self._sanitize_value(self.config.raw),
            },
            "inputs": self._input_manifest(),
            "git": self._git_manifest(),
            "cache": cache or {"used": False},
            "profile": profile or {},
            "outputs": [str(path) for path in outputs],
            "summary": summary or {},
            "extra": extra or {},
        }
        manifest_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return manifest_path

    def _command_options(self, args: argparse.Namespace) -> dict[str, object]:
        payload = self._coerce_jsonable(args)
        payload.pop("_argv", None)
        return payload

    def _coerce_jsonable(self, args: argparse.Namespace) -> dict[str, object]:
        payload: dict[str, object] = {}
        for key, value in vars(args).items():
            if isinstance(value, Path):
                payload[key] = str(value)
            else:
                payload[key] = value
        return payload

    def _input_manifest(self) -> dict[str, object]:
        input_rows = []
        metadata_fingerprints = []
        for key, value in sorted(self.config.paths.items()):
            if key in {"output_dir", "data_dir"}:
                continue
            resolved = self.config.resolve_path(value)
            metadata = self._path_metadata(resolved)
            input_rows.append({"key": key, **metadata})
            metadata_fingerprints.append(metadata["metadata_fingerprint"])
        combined_fingerprint = hashlib.sha256(
            json.dumps(metadata_fingerprints, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        ).hexdigest()
        return {
            "combined_metadata_fingerprint": combined_fingerprint,
            "paths": input_rows,
        }

    def _path_metadata(self, path: Path) -> dict[str, object]:
        if not path.exists():
            fingerprint = hashlib.sha256(f"{path}|missing".encode("utf-8")).hexdigest()
            return {
                "path": str(path),
                "exists": False,
                "metadata_fingerprint": fingerprint,
            }
        stat = path.stat()
        fingerprint = hashlib.sha256(
            f"{path}|{stat.st_size}|{stat.st_mtime_ns}".encode("utf-8")
        ).hexdigest()
        return {
            "path": str(path),
            "exists": True,
            "size": stat.st_size,
            "mtime_ns": stat.st_mtime_ns,
            "metadata_fingerprint": fingerprint,
        }

    def _git_manifest(self) -> dict[str, object]:
        root = self._git_command(["rev-parse", "--show-toplevel"])
        commit = self._git_command(["rev-parse", "HEAD"])
        branch = self._git_command(["rev-parse", "--abbrev-ref", "HEAD"])
        status = self._git_command(["status", "--short"])
        return {
            "root": root,
            "commit": commit,
            "branch": branch,
            "dirty": bool(status) if status is not None else None,
        }

    def _git_command(self, args: list[str]) -> str | None:
        try:
            result = subprocess.run(
                ["git", *args],
                check=False,
                capture_output=True,
                text=True,
                cwd=Path.cwd(),
            )
        except OSError:
            return None
        if result.returncode != 0:
            return None
        return result.stdout.strip() or None

    def _file_sha256(self, path: Path) -> str | None:
        if not path.exists():
            return None
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
        return digest.hexdigest()

    def _sanitize_value(self, value: object, key_hint: str = "") -> object:
        if self._is_sensitive_key(key_hint):
            return REDACTED
        if isinstance(value, dict):
            return {
                key: self._sanitize_value(item, str(key))
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [self._sanitize_value(item, key_hint) for item in value]
        return value

    def _is_sensitive_key(self, key: str) -> bool:
        lowered = key.lower()
        return any(token in lowered for token in SENSITIVE_KEY_TOKENS)
