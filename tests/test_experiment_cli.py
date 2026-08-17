from __future__ import annotations

import json
import asyncio
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from uuid import UUID

import pytest

import experiment_system.cli as cli_module
from experiment_system.actions import RecoveryPolicy
from experiment_system.cli import CliDependencies, main, run_command
from experiment_system.commands import CreateAttempt, StartAttempt
from experiment_system.contract import CommandResult, SystemClock, Uuid4IdFactory
from experiment_system.engine import AttemptEngine
from experiment_system.recovery import RecoverySummary
from experiment_system.state import (
    ArtifactRef,
    AttemptPhase,
    BudgetState,
    ErrorSummary,
    ExternalRequestKind,
    ResourceBudget,
    StrategyStateEnvelope,
)
from experiment_system.artifacts import ArtifactStore
from experiment_system.store import RevisionConflict
from experiment_system.stores import SQLiteAttemptRepository
from tests.test_experiment_control import _external_decision
from tests.test_experiment_recovery import _persist_action


ATTEMPT_ID = "attempt-1"


def _artifact(name: str) -> ArtifactRef:
    data = name.encode("utf-8")
    return ArtifactRef(
        capture_class="hashed",
        content_hash=sha256(data).hexdigest(),
        media_type="application/json",
        byte_size=len(data),
    )


def _create_command() -> CreateAttempt:
    strategy_value = {"round": 0}
    strategy_payload = json.dumps(
        strategy_value, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return CreateAttempt(
        schema_version=1,
        command_type="CREATE_ATTEMPT",
        command_id=UUID("10000000-0000-0000-0000-000000000001"),
        attempt_id=ATTEMPT_ID,
        expected_revision=0,
        state_schema_version=1,
        experiment_id="experiment-1",
        trial_id="trial-1",
        strategy_id="router",
        manifest_ref=_artifact("manifest"),
        strategy=StrategyStateEnvelope(
            strategy_id="router",
            strategy_schema_version=1,
            value=strategy_value,
            content_hash=sha256(strategy_payload).hexdigest(),
            byte_size=len(strategy_payload),
        ),
        budget=BudgetState(
            resources=(ResourceBudget(resource="MODEL_CALLS", limit=10),),
            deadline_at=datetime(2026, 7, 23, tzinfo=timezone.utc)
            + timedelta(hours=1),
            max_call_depth=4,
            max_concurrent_actions=2,
        ),
    )


def _run(capsys: pytest.CaptureFixture[str], database: Path, *args: str) -> tuple[int, dict[str, object], str]:
    status = main(["--database", str(database), *args])
    captured = capsys.readouterr()
    payload = json.loads(captured.out) if captured.out else {}
    return status, payload, captured.err


def _write_command(path: Path, command: CreateAttempt) -> None:
    path.write_text(command.model_dump_json(), encoding="utf-8")


async def _start_attempt(database: Path) -> None:
    repository = SQLiteAttemptRepository(database)
    clock = SystemClock()
    engine = AttemptEngine(
        repository=repository,
        clock=clock,
        id_factory=Uuid4IdFactory(),
        artifact_verifier=ArtifactStore(database.parent / "artifacts"),
        allowed_edges=frozenset(),
    )
    loaded = await repository.load(ATTEMPT_ID)
    assert loaded is not None
    result = await engine.handle(
        StartAttempt(
            schema_version=1,
            command_type="START_ATTEMPT",
            command_id=UUID("10000000-0000-0000-0000-000000000002"),
            attempt_id=ATTEMPT_ID,
            expected_revision=loaded.state.revision,
        )
    )
    assert result.accepted


async def _request_additional_input(database: Path) -> None:
    repository = SQLiteAttemptRepository(database)
    clock = SystemClock()
    engine = AttemptEngine(
        repository=repository,
        clock=clock,
        id_factory=Uuid4IdFactory(),
        artifact_verifier=ArtifactStore(database.parent / "artifacts"),
        allowed_edges=frozenset(),
    )
    result = await engine.handle(
        _external_decision(
            request_id="request-1",
            request_kind=ExternalRequestKind.ADDITIONAL_INPUT,
            proposal=None,
        )
    )
    assert result.accepted


class _Loaded:
    class _State:
        revision = 7

    state = _State()


class _Repository:
    def __init__(self) -> None:
        self.load_calls = 0

    async def load(self, attempt_id: str) -> _Loaded:
        assert attempt_id == ATTEMPT_ID
        self.load_calls += 1
        return _Loaded()


class _Engine:
    def __init__(self) -> None:
        self.command = None
        self.handle_calls = 0

    async def handle(self, command: object) -> CommandResult:
        self.handle_calls += 1
        self.command = command
        return CommandResult(
            command_id=command.command_id,
            attempt_id=command.attempt_id,
            accepted=True,
            revision=8,
            phase=AttemptPhase.RUNNING,
        )


class _Ids:
    def __init__(self) -> None:
        self.calls = 0

    def new_uuid(self) -> UUID:
        self.calls += 1
        return UUID("20000000-0000-0000-0000-000000000001")


class _Recovery:
    async def recover_startup(self, max_actions: int) -> RecoverySummary:
        assert max_actions == 100
        return RecoverySummary()


def _dependencies(
    engine: object,
    *,
    repository: object | None = None,
    ids: object | None = None,
    recovery: object | None = None,
) -> CliDependencies:
    return CliDependencies(
        repository=repository or _Repository(),
        engine=engine,
        id_factory=ids or _Ids(),
        recovery_coordinator=recovery or _Recovery(),
    )


def test_attempt_create_status_pause_resume_and_cancel_emit_redacted_json(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    database = tmp_path / "attempts.sqlite3"
    command_path = tmp_path / "create.json"
    _write_command(command_path, _create_command())

    created_status, created, created_err = _run(
        capsys, database, "attempt", "create", "--command-json", str(command_path)
    )
    assert created_status == 0
    assert created_err == ""
    assert created["accepted"] is True
    assert created["attempt_id"] == ATTEMPT_ID
    assert created["phase"] == "PLANNED"
    assert created["revision"] == 1

    asyncio.run(_start_attempt(database))

    status_code, view, status_err = _run(capsys, database, "attempt", "status", ATTEMPT_ID)
    assert status_code == 0
    assert status_err == ""
    assert view["phase"] == "RUNNING"
    serialized = json.dumps(view)
    for secret in ("manifest_ref", "strategy", "payload_ref", "credentials", "headers", "http"):
        assert secret not in serialized

    pause_code, paused, _ = _run(capsys, database, "attempt", "pause", ATTEMPT_ID)
    assert pause_code == 0
    assert paused["phase"] in {"PAUSED", "PAUSE_REQUESTED"}

    resume_code, resumed, _ = _run(capsys, database, "attempt", "resume", ATTEMPT_ID)
    assert resume_code == 0
    assert resumed["phase"] == "RUNNING"

    cancel_code, cancelled, _ = _run(capsys, database, "attempt", "cancel", ATTEMPT_ID)
    assert cancel_code == 0
    assert cancelled["phase"] in {"CANCELLED", "CANCEL_REQUESTED"}


@pytest.mark.parametrize(
    ("response_kind", "extra"),
    (
        ("APPROVE", ()),
        ("REJECT", ()),
        ("ABANDON", ()),
        ("PROVIDE_INPUT", ("--response-ref-json", '{"capture_class":"hashed","content_hash":"' + "a" * 64 + '","media_type":"text/plain","byte_size":1}')),
        ("CONFIRM_SUCCEEDED", ("--response-ref-json", '{"capture_class":"hashed","content_hash":"' + "a" * 64 + '","media_type":"text/plain","byte_size":1}')),
        ("CONFIRM_FAILED", ("--error-json", '{"code":"FAILED","retryable":false,"safe_message":"failed"}')),
    ),
)
def test_submit_input_accepts_each_existing_response_shape(
    response_kind: str,
    extra: tuple[str, ...],
) -> None:
    parser = __import__("experiment_system.cli", fromlist=["build_parser"]).build_parser()
    args = parser.parse_args(
        [
            "--database",
            "ignored.sqlite3",
            "attempt",
            "submit-input",
            ATTEMPT_ID,
            "--request-id",
            "request-1",
            "--response-kind",
            response_kind,
            *extra,
        ]
    )
    engine = _Engine()
    result = asyncio.run(run_command(args, _dependencies(engine)))
    assert result.accepted is True
    assert engine.command.response_kind.value == response_kind
    assert engine.command.request_id == "request-1"


def test_safe_invalid_and_missing_errors_never_echo_sensitive_input(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    database = tmp_path / "attempts.sqlite3"
    invalid = tmp_path / "invalid.json"
    sensitive = "https://user:password@example.invalid/path?token=secret"
    invalid.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "command_type": "CREATE_ATTEMPT",
                "command_id": "not-a-uuid",
                "attempt_id": ATTEMPT_ID,
                "expected_revision": 0,
                "secret": sensitive,
            }
        ),
        encoding="utf-8",
    )

    invalid_status, invalid_output, invalid_error = _run(
        capsys, database, "attempt", "create", "--command-json", str(invalid)
    )
    assert invalid_status == 2
    assert invalid_output == {}
    assert sensitive not in invalid_error
    assert "ValidationError" not in invalid_error
    assert "Traceback" not in invalid_error

    missing_status, missing_output, missing_error = _run(
        capsys, database, "attempt", "status", "missing-attempt"
    )
    assert missing_status == 3
    assert missing_output == {}
    assert missing_error


def test_create_rejects_invalid_command_uuid_as_the_only_invalid_field(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    database = tmp_path / "attempts.sqlite3"
    command_path = tmp_path / "invalid-command-id.json"
    payload = _create_command().model_dump(mode="json")
    payload["command_id"] = "not-a-uuid"
    command_path.write_text(json.dumps(payload), encoding="utf-8")

    status, output, error = _run(
        capsys,
        database,
        "attempt",
        "create",
        "--command-json",
        str(command_path),
    )

    assert status == 2
    assert output == {}
    assert error == "Invalid command arguments.\n"


def test_create_rejects_a_valid_non_create_command(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    database = tmp_path / "attempts.sqlite3"
    command_path = tmp_path / "start-command.json"
    command_path.write_text(
        StartAttempt(
            schema_version=1,
            command_type="START_ATTEMPT",
            command_id=UUID("10000000-0000-0000-0000-000000000009"),
            attempt_id=ATTEMPT_ID,
            expected_revision=1,
        ).model_dump_json(),
        encoding="utf-8",
    )

    status, output, error = _run(
        capsys,
        database,
        "attempt",
        "create",
        "--command-json",
        str(command_path),
    )

    assert status == 2
    assert output == {}
    assert error == "Invalid command arguments.\n"


def test_submit_input_shape_errors_return_exit_two(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    database = tmp_path / "attempts.sqlite3"
    command_path = tmp_path / "create.json"
    _write_command(command_path, _create_command())
    assert _run(capsys, database, "attempt", "create", "--command-json", str(command_path))[0] == 0
    asyncio.run(_start_attempt(database))

    status, output, error = _run(
        capsys,
        database,
        "attempt",
        "submit-input",
        ATTEMPT_ID,
        "--request-id",
        "request-1",
        "--response-kind",
        "PROVIDE_INPUT",
    )
    assert status == 2
    assert output == {}
    assert error == "Invalid command arguments.\n"


@pytest.mark.parametrize(
    ("response_kind", "flag", "payload"),
    (
        (
            "PROVIDE_INPUT",
            "--response-ref-json",
            {
                "capture_class": "hashed",
                "content_hash": "a" * 64,
                "media_type": "text/plain",
                "byte_size": 1,
                "prompt": "raw-secret-prompt",
            },
        ),
        (
            "CONFIRM_FAILED",
            "--error-json",
            {
                "code": "FAILED",
                "retryable": False,
                "safe_message": "failed",
                "response": "raw-secret-response",
            },
        ),
    ),
)
def test_submit_input_rejects_raw_content_in_strict_summary_json(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    response_kind: str,
    flag: str,
    payload: dict[str, object],
) -> None:
    serialized = json.dumps(payload)
    status, output, error = _run(
        capsys,
        tmp_path / "attempts.sqlite3",
        "attempt",
        "submit-input",
        ATTEMPT_ID,
        "--request-id",
        "request-1",
        "--response-kind",
        response_kind,
        flag,
        serialized,
    )

    assert status == 2
    assert output == {}
    assert error == "Invalid command arguments.\n"
    assert serialized not in error


def test_submit_input_commits_additional_input_to_sqlite(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database = tmp_path / "attempts.sqlite3"
    command_path = tmp_path / "create.json"
    _write_command(command_path, _create_command())
    assert _run(
        capsys,
        database,
        "attempt",
        "create",
        "--command-json",
        str(command_path),
    )[0] == 0
    asyncio.run(_start_attempt(database))
    asyncio.run(_request_additional_input(database))

    response_ref = json.dumps(_artifact("operator-input").model_dump(mode="json"))
    status, result, error = _run(
        capsys,
        database,
        "attempt",
        "submit-input",
        ATTEMPT_ID,
        "--request-id",
        "request-1",
        "--response-kind",
        "PROVIDE_INPUT",
        "--response-ref-json",
        response_ref,
    )

    assert status == 0
    assert error == ""
    assert result["accepted"] is True
    assert result["phase"] == "RUNNING"
    loaded = asyncio.run(SQLiteAttemptRepository(database).load(ATTEMPT_ID))
    assert loaded is not None
    assert loaded.state.pending_external == ()


@pytest.mark.parametrize(
    ("error_code", "expected_exit"),
    (
        ("ATTEMPT_NOT_FOUND", 3),
        ("REVISION_CONFLICT", 4),
        ("UNSUPPORTED_COMMAND", 5),
    ),
)
def test_mutation_maps_rejected_results_without_hidden_retry(
    error_code: str,
    expected_exit: int,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    class RejectedEngine(_Engine):
        async def handle(self, command: object) -> CommandResult:
            self.handle_calls += 1
            self.command = command
            return CommandResult(
                command_id=command.command_id,
                attempt_id=command.attempt_id,
                accepted=False,
                revision=7,
                phase=AttemptPhase.RUNNING,
                error=ErrorSummary(
                    code=error_code,
                    retryable=False,
                    safe_message="The command was rejected.",
                ),
            )

    repository = _Repository()
    engine = RejectedEngine()
    ids = _Ids()
    dependencies = _dependencies(engine, repository=repository, ids=ids)
    monkeypatch.setattr(cli_module, "_dependencies", lambda database: dependencies)

    status = main(
        ["--database", "ignored.sqlite3", "attempt", "pause", ATTEMPT_ID]
    )
    captured = capsys.readouterr()

    assert status == expected_exit
    assert json.loads(captured.out)["error"]["code"] == error_code
    assert captured.err == ""
    assert repository.load_calls == 1
    assert engine.handle_calls == 1
    assert ids.calls == 1


def test_direct_repository_revision_conflict_returns_exit_four(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    class ConflictingEngine(_Engine):
        async def handle(self, command: object) -> CommandResult:
            self.handle_calls += 1
            raise RevisionConflict()

    dependencies = _dependencies(ConflictingEngine())
    monkeypatch.setattr(cli_module, "_dependencies", lambda database: dependencies)

    status = main(
        ["--database", "ignored.sqlite3", "attempt", "pause", ATTEMPT_ID]
    )
    captured = capsys.readouterr()

    assert status == 4
    assert captured.out == ""
    assert captured.err == "Command could not be completed.\n"


def test_parser_error_does_not_echo_invalid_command(
    capsys: pytest.CaptureFixture[str],
) -> None:
    sensitive = "https://user:password@example.invalid/?token=secret"
    status = main(["--database", "ignored.sqlite3", "attempt", sensitive])
    captured = capsys.readouterr()

    assert status == 2
    assert captured.out == ""
    assert captured.err == "Invalid command arguments.\n"
    assert sensitive not in captured.err


def test_non_contract_result_is_never_serialized(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    sensitive = "https://user:password@example.invalid/?token=secret"

    class UnsafeRecovery:
        async def recover_startup(self, max_actions: int) -> ErrorSummary:
            assert max_actions == 100
            return ErrorSummary(
                code="UNSAFE",
                retryable=False,
                safe_message=sensitive,
            )

    dependencies = _dependencies(_Engine(), recovery=UnsafeRecovery())
    monkeypatch.setattr(cli_module, "_dependencies", lambda database: dependencies)

    status = main(["--database", "ignored.sqlite3", "recover"])
    captured = capsys.readouterr()

    assert status == 5
    assert captured.out == ""
    assert captured.err == "Command could not be completed.\n"
    assert sensitive not in captured.err


def test_base_exception_from_dependency_is_not_caught(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    class CrashSentinel(BaseException):
        __slots__ = ()

    class CrashingRecovery:
        async def recover_startup(self, max_actions: int) -> RecoverySummary:
            assert max_actions == 100
            raise CrashSentinel()

    dependencies = _dependencies(_Engine(), recovery=CrashingRecovery())
    monkeypatch.setattr(cli_module, "_dependencies", lambda database: dependencies)

    with pytest.raises(CrashSentinel):
        main(["--database", "ignored.sqlite3", "recover"])

    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_recover_emits_structured_summary(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    status, summary, error = _run(capsys, tmp_path / "attempts.sqlite3", "recover")
    assert status == 0
    assert error == ""
    assert summary == {
        "failed_attempt_ids": [],
        "recovered_attempt_ids": [],
        "terminal_attempt_ids": [],
        "waiting_attempt_ids": [],
    }


def test_recover_without_backend_fails_closed_with_structured_summary(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    database = tmp_path / "attempts.sqlite3"
    repository = SQLiteAttemptRepository(database)
    asyncio.run(
        _persist_action(
            repository,
            RecoveryPolicy.REPLAY_SAFE,
            started=False,
        )
    )

    status, summary, error = _run(capsys, database, "recover")

    assert status == 5
    assert error == ""
    assert summary == {
        "failed_attempt_ids": [ATTEMPT_ID],
        "recovered_attempt_ids": [],
        "terminal_attempt_ids": [],
        "waiting_attempt_ids": [],
    }
