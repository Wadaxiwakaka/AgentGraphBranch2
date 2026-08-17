from __future__ import annotations

import logging
import os
from hashlib import sha256
from pathlib import Path

import pytest
from pydantic import ValidationError

from experiment_system import artifacts as artifact_module
from experiment_system.artifacts import (
    ArtifactAccessError,
    ArtifactIntegrityError,
    ArtifactStore,
    ArtifactWriteError,
)
from experiment_system.engine import ArtifactVerificationError
from experiment_system.state import ArtifactRef


def _artifact_files(root: Path) -> tuple[Path, ...]:
    return tuple(path for path in root.rglob("*") if path.is_file())


def test_full_capture_is_sha256_addressed_and_deduplicated(tmp_path: Path) -> None:
    store = ArtifactStore(tmp_path)

    first = store.put_bytes(
        b"payload",
        media_type="text/plain",
        capture_class="full",
    )
    second = store.put_bytes(
        b"payload",
        media_type="text/plain",
        capture_class="full",
    )
    different = store.put_bytes(
        b"different",
        media_type="text/plain",
        capture_class="full",
    )

    digest = sha256(b"payload").hexdigest()
    assert first == second
    assert first == ArtifactRef(
        capture_class="full",
        content_hash=digest,
        media_type="text/plain",
        byte_size=7,
        relative_path=f"sha256/{digest[:2]}/{digest}",
    )
    assert different.content_hash != first.content_hash
    assert store.read_bytes(first) == b"payload"
    assert len(_artifact_files(tmp_path)) == 2
    assert store.verify(first) is None


def test_json_capture_uses_canonical_utf8_and_rejects_nan(tmp_path: Path) -> None:
    store = ArtifactStore(tmp_path)

    ref = store.put_json(
        {"b": 1, "a": "\u00e9"},
        capture_class="full",
    )

    assert ref.media_type == "application/json"
    assert store.read_bytes(ref) == '{"a":"\u00e9","b":1}'.encode("utf-8")
    with pytest.raises(ValueError, match="JSON"):
        store.put_json({"number": float("nan")}, capture_class="full")


def test_hashed_and_metadata_only_capture_never_write_content(tmp_path: Path) -> None:
    store = ArtifactStore(tmp_path)

    hashed = store.put_bytes(
        b"hash-me",
        media_type="application/octet-stream",
        capture_class="hashed",
    )
    direct_hash = store.hash_bytes(
        b"hash-me",
        media_type="application/octet-stream",
    )
    metadata = store.put_bytes(
        b"do-not-store",
        media_type="application/octet-stream",
        capture_class="metadata_only",
    )
    direct_metadata = store.metadata_only(
        media_type="application/octet-stream",
        byte_size=12,
    )

    assert hashed == direct_hash
    assert hashed.capture_class == "hashed"
    assert hashed.content_hash == sha256(b"hash-me").hexdigest()
    assert hashed.byte_size == 7
    assert hashed.relative_path is None
    assert metadata == direct_metadata
    assert metadata.capture_class == "metadata_only"
    assert metadata.content_hash is None
    assert metadata.relative_path is None
    assert _artifact_files(tmp_path) == ()
    assert store.verify(hashed) is None
    assert store.verify(metadata) is None


@pytest.mark.parametrize(
    "relative_path",
    ("../escape", "/absolute/file", "C:/absolute/file", "safe/../escape"),
)
def test_artifact_refs_reject_traversal_and_absolute_paths(
    relative_path: str,
) -> None:
    with pytest.raises(ValidationError, match="safe POSIX path"):
        ArtifactRef(
            capture_class="full",
            content_hash="0" * 64,
            media_type="application/octet-stream",
            byte_size=0,
            relative_path=relative_path,
        )


def test_store_revalidates_refs_and_never_trusts_a_supplied_path(
    tmp_path: Path,
) -> None:
    store = ArtifactStore(tmp_path)
    valid = store.put_bytes(
        b"payload",
        media_type="text/plain",
        capture_class="full",
    )
    forged = valid.model_copy(update={"relative_path": "other/location"})

    with pytest.raises(ArtifactIntegrityError, match="reference"):
        store.read_bytes(forged)


def test_temporary_file_shares_final_directory_and_is_fsynced_before_replace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = ArtifactStore(tmp_path)
    calls: list[tuple[str, object, object | None]] = []
    real_fsync = artifact_module.os.fsync
    real_replace = artifact_module.os.replace

    def recording_fsync(fd: int) -> None:
        calls.append(("fsync", fd, None))
        real_fsync(fd)

    def recording_replace(source: os.PathLike[str], target: os.PathLike[str]) -> None:
        calls.append(("replace", Path(source), Path(target)))
        real_replace(source, target)

    monkeypatch.setattr(artifact_module.os, "fsync", recording_fsync)
    monkeypatch.setattr(artifact_module.os, "replace", recording_replace)

    store.put_bytes(
        b"atomic",
        media_type="application/octet-stream",
        capture_class="full",
    )

    replace_index = next(
        index for index, call in enumerate(calls) if call[0] == "replace"
    )
    assert any(call[0] == "fsync" for call in calls[:replace_index])
    _, source, target = calls[replace_index]
    assert isinstance(source, Path)
    assert isinstance(target, Path)
    assert source.parent == target.parent


def test_replace_failure_cleans_temp_preserves_existing_artifact_and_redacts_content(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    store = ArtifactStore(tmp_path)
    existing = store.put_bytes(
        b"existing",
        media_type="text/plain",
        capture_class="full",
    )
    secret = b"TOP-SECRET-CONTENT-DO-NOT-LEAK"

    def fail_replace(source: os.PathLike[str], target: os.PathLike[str]) -> None:
        raise OSError("simulated replace failure")

    monkeypatch.setattr(artifact_module.os, "replace", fail_replace)
    with caplog.at_level(logging.DEBUG):
        with pytest.raises(ArtifactWriteError) as caught:
            store.put_bytes(
                secret,
                media_type="application/octet-stream",
                capture_class="full",
            )

    assert store.read_bytes(existing) == b"existing"
    assert not tuple(tmp_path.rglob("*.tmp"))
    assert secret.decode("ascii") not in str(caught.value)
    assert secret.decode("ascii") not in caplog.text


def test_tampering_causes_safe_verification_failure(tmp_path: Path) -> None:
    store = ArtifactStore(tmp_path)
    secret = b"private-original-content"
    ref = store.put_bytes(
        secret,
        media_type="application/octet-stream",
        capture_class="full",
    )
    assert ref.relative_path is not None
    (tmp_path / Path(ref.relative_path)).write_bytes(b"tampered")

    with pytest.raises(ArtifactIntegrityError) as caught:
        store.verify(ref)

    assert isinstance(caught.value, ArtifactVerificationError)
    assert secret.decode("ascii") not in str(caught.value)
    assert "tampered" not in str(caught.value)


def test_non_full_artifacts_cannot_be_read(tmp_path: Path) -> None:
    store = ArtifactStore(tmp_path)
    hashed = store.hash_bytes(b"payload", media_type="text/plain")
    metadata = store.metadata_only(media_type="text/plain", byte_size=7)

    with pytest.raises(ArtifactAccessError, match="full"):
        store.read_bytes(hashed)
    with pytest.raises(ArtifactAccessError, match="full"):
        store.read_bytes(metadata)
