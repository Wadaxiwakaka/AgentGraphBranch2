from __future__ import annotations

import json
import os
import tempfile
from hashlib import sha256
from pathlib import Path, PurePosixPath
from typing import Any, Literal

from pydantic import ValidationError

from .contract import ArtifactVerificationError
from .state import ArtifactRef


CaptureClass = Literal["full", "hashed", "metadata_only"]


class ArtifactStoreError(ValueError):
    safe_message = "The artifact operation could not be completed."

    def __init__(self) -> None:
        super().__init__(self.safe_message)


class ArtifactIntegrityError(ArtifactVerificationError):
    pass


class ArtifactAccessError(ArtifactStoreError):
    safe_message = "Only full-capture artifacts can be read."


class ArtifactWriteError(ArtifactStoreError):
    safe_message = "The artifact could not be stored atomically."


class ArtifactStore:
    def __init__(self, root: str | os.PathLike[str]) -> None:
        if not isinstance(root, (str, os.PathLike)):
            raise TypeError("artifact root must be a filesystem path")
        try:
            candidate = Path(root).expanduser()
            candidate.mkdir(parents=True, exist_ok=True)
            self._root = candidate.resolve(strict=True)
        except OSError:
            raise ArtifactWriteError() from None
        if not self._root.is_dir():
            raise ArtifactWriteError()

    @property
    def root(self) -> Path:
        return self._root

    def put_bytes(
        self,
        data: bytes,
        *,
        media_type: str,
        capture_class: CaptureClass = "full",
    ) -> ArtifactRef:
        payload = self._validate_bytes(data)
        if capture_class == "hashed":
            return self.hash_bytes(payload, media_type=media_type)
        if capture_class == "metadata_only":
            return self.metadata_only(media_type=media_type, byte_size=len(payload))
        if capture_class != "full":
            raise ValueError("capture_class must be full, hashed, or metadata_only")

        digest = sha256(payload).hexdigest()
        ref = self._full_ref(digest, media_type=media_type, byte_size=len(payload))
        final_path = self._path_for_digest(digest)
        self._prepare_parent(final_path.parent)
        temporary_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                prefix=f".{digest}.",
                suffix=".tmp",
                dir=final_path.parent,
                delete=False,
            ) as temporary:
                temporary_path = Path(temporary.name)
                temporary.write(payload)
                temporary.flush()
                os.fsync(temporary.fileno())

            if final_path.exists():
                self._verify_file(final_path, ref)
                self._remove_temporary(temporary_path)
                return ref

            os.replace(temporary_path, final_path)
            temporary_path = None
            return ref
        except ArtifactVerificationError:
            self._remove_temporary(temporary_path)
            raise
        except OSError:
            self._remove_temporary(temporary_path)
            raise ArtifactWriteError() from None

    def put_json(
        self,
        value: Any,
        *,
        media_type: str = "application/json",
        capture_class: CaptureClass = "full",
    ) -> ArtifactRef:
        try:
            payload = json.dumps(
                value,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        except (TypeError, ValueError, UnicodeError):
            raise ValueError("value must contain canonical JSON data") from None
        return self.put_bytes(
            payload,
            media_type=media_type,
            capture_class=capture_class,
        )

    def hash_bytes(self, data: bytes, *, media_type: str) -> ArtifactRef:
        payload = self._validate_bytes(data)
        return ArtifactRef(
            capture_class="hashed",
            content_hash=sha256(payload).hexdigest(),
            media_type=media_type,
            byte_size=len(payload),
            relative_path=None,
        )

    def metadata_only(self, *, media_type: str, byte_size: int) -> ArtifactRef:
        return ArtifactRef(
            capture_class="metadata_only",
            content_hash=None,
            media_type=media_type,
            byte_size=byte_size,
            relative_path=None,
        )

    def read_bytes(self, ref: ArtifactRef) -> bytes:
        validated = self._validated_ref(ref)
        if validated.capture_class != "full":
            raise ArtifactAccessError()
        path = self._validated_full_path(validated)
        try:
            payload = path.read_bytes()
        except OSError:
            raise ArtifactIntegrityError() from None
        self._verify_payload(payload, validated)
        return payload

    def verify(self, ref: ArtifactRef) -> None:
        validated = self._validated_ref(ref)
        if validated.capture_class != "full":
            return
        self._verify_file(self._validated_full_path(validated), validated)

    @staticmethod
    def _validate_bytes(data: bytes) -> bytes:
        if type(data) is not bytes:
            raise TypeError("artifact content must be bytes")
        return data

    @staticmethod
    def _relative_path(digest: str) -> str:
        return f"sha256/{digest[:2]}/{digest}"

    def _full_ref(self, digest: str, *, media_type: str, byte_size: int) -> ArtifactRef:
        return ArtifactRef(
            capture_class="full",
            content_hash=digest,
            media_type=media_type,
            byte_size=byte_size,
            relative_path=self._relative_path(digest),
        )

    def _path_for_digest(self, digest: str) -> Path:
        relative = PurePosixPath(self._relative_path(digest))
        path = self._root.joinpath(*relative.parts)
        self._require_below_root(path)
        return path

    def _validated_full_path(self, ref: ArtifactRef) -> Path:
        if ref.content_hash is None:
            raise ArtifactIntegrityError()
        expected_relative_path = self._relative_path(ref.content_hash)
        if ref.relative_path != expected_relative_path:
            raise ArtifactIntegrityError()
        return self._path_for_digest(ref.content_hash)

    def _prepare_parent(self, parent: Path) -> None:
        try:
            parent.mkdir(parents=True, exist_ok=True)
            self._require_below_root(parent)
            if not parent.is_dir():
                raise ArtifactWriteError()
        except ArtifactStoreError:
            raise
        except OSError:
            raise ArtifactWriteError() from None

    def _require_below_root(self, path: Path) -> None:
        try:
            resolved = path.resolve(strict=False)
        except OSError:
            raise ArtifactIntegrityError() from None
        if not resolved.is_relative_to(self._root):
            raise ArtifactIntegrityError()

    @staticmethod
    def _validated_ref(ref: ArtifactRef) -> ArtifactRef:
        if not isinstance(ref, ArtifactRef):
            raise ArtifactIntegrityError()
        try:
            return ArtifactRef.model_validate_json(ref.model_dump_json())
        except (ValidationError, ValueError, TypeError):
            raise ArtifactIntegrityError() from None

    def _verify_file(self, path: Path, ref: ArtifactRef) -> None:
        try:
            payload = path.read_bytes()
        except OSError:
            raise ArtifactIntegrityError() from None
        self._verify_payload(payload, ref)

    @staticmethod
    def _verify_payload(payload: bytes, ref: ArtifactRef) -> None:
        if (
            ref.content_hash is None
            or len(payload) != ref.byte_size
            or sha256(payload).hexdigest() != ref.content_hash
        ):
            raise ArtifactIntegrityError()

    @staticmethod
    def _remove_temporary(path: Path | None) -> None:
        if path is None:
            return
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass


__all__ = [
    "ArtifactAccessError",
    "ArtifactIntegrityError",
    "ArtifactStore",
    "ArtifactStoreError",
    "ArtifactWriteError",
]
