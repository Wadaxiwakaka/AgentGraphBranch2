from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

from pydantic import TypeAdapter, ValidationError

from .artifacts import ArtifactStore
from .commands import (
    CancelAttempt,
    Command,
    CreateAttempt,
    PauseAttempt,
    ResumeAttempt,
    SubmitExternalInput,
)
from .contract import CommandResult, IdFactory, SystemClock, Uuid4IdFactory
from .engine import AttemptEngine
from .executor import ActionExecutor
from .recovery import RecoveryCoordinator, RecoverySummary
from .state import (
    ArtifactRef,
    ErrorSummary,
    ExternalResponseKind,
    OperatorView,
    to_operator_view,
)
from .store import AttemptRepository, LoadedAttempt, RevisionConflict
from .stores import SQLiteAttemptRepository


_COMMAND_ADAPTER = TypeAdapter(Command)
_ARTIFACT_REF_ADAPTER = TypeAdapter(ArtifactRef)
_ERROR_SUMMARY_ADAPTER = TypeAdapter(ErrorSummary)
_SAFE_INVALID_ARGUMENTS = "Invalid command arguments.\n"
_SAFE_MISSING_ATTEMPT = "Attempt was not found.\n"
_SAFE_EXPECTED_FAILURE = "Command could not be completed.\n"


class _CliArgumentError(ValueError):
    pass


class _HelpRequested(Exception):
    pass


class _MissingAttempt(ValueError):
    pass


class _SafeArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        del message
        raise _CliArgumentError()

    def exit(self, status: int = 0, message: str | None = None) -> None:
        del message
        if status == 0:
            raise _HelpRequested()
        raise _CliArgumentError()


CliOutput = CommandResult | OperatorView | RecoverySummary


@dataclass(frozen=True, slots=True)
class CliDependencies:
    repository: AttemptRepository
    engine: AttemptEngine
    id_factory: IdFactory
    recovery_coordinator: RecoveryCoordinator


def build_parser() -> argparse.ArgumentParser:
    parser = _SafeArgumentParser(prog="python -m experiment_system")
    parser.add_argument("--database", required=True)
    commands = parser.add_subparsers(dest="operation", required=True)

    attempt = commands.add_parser("attempt")
    attempt_commands = attempt.add_subparsers(dest="attempt_operation", required=True)

    create = attempt_commands.add_parser("create")
    create.add_argument("--command-json", required=True)

    for name in ("status", "pause", "resume", "cancel"):
        command = attempt_commands.add_parser(name)
        command.add_argument("attempt_id")

    submit_input = attempt_commands.add_parser("submit-input")
    submit_input.add_argument("attempt_id")
    submit_input.add_argument("--request-id", required=True)
    submit_input.add_argument("--response-kind", required=True)
    submit_input.add_argument("--response-ref-json")
    submit_input.add_argument("--error-json")

    commands.add_parser("recover")
    return parser


def _dependencies(database: str) -> CliDependencies:
    database_path = Path(database)
    repository = SQLiteAttemptRepository(database_path)
    clock = SystemClock()
    id_factory = Uuid4IdFactory()
    engine = AttemptEngine(
        repository=repository,
        clock=clock,
        id_factory=id_factory,
        artifact_verifier=ArtifactStore(database_path.parent / "artifacts"),
        allowed_edges=frozenset(),
    )
    executor = ActionExecutor(
        backends={},
        repository=repository,
        engine=engine,
        clock=clock,
        lease_seconds=30,
    )
    return CliDependencies(
        repository=repository,
        engine=engine,
        id_factory=id_factory,
        recovery_coordinator=RecoveryCoordinator(
            repository=repository,
            engine=engine,
            executor=executor,
            clock=clock,
            worker_id="local-operator-recovery",
            lease_seconds=30,
        ),
    )


def _read_create_command(path: str) -> CreateAttempt:
    try:
        raw = Path(path).read_text(encoding="utf-8")
        command = _COMMAND_ADAPTER.validate_json(raw, strict=True)
    except (OSError, UnicodeError, ValidationError, ValueError):
        raise _CliArgumentError() from None
    if not isinstance(command, CreateAttempt):
        raise _CliArgumentError()
    return command


def _attempt_id(value: str) -> str:
    if type(value) is not str or not value:
        raise _CliArgumentError()
    return value


def _response_kind(value: str) -> ExternalResponseKind:
    try:
        return ExternalResponseKind(value)
    except ValueError:
        raise _CliArgumentError() from None


def _strict_json(value: str | None, adapter: TypeAdapter[Any]) -> Any | None:
    if value is None:
        return None
    try:
        return adapter.validate_json(value, strict=True)
    except (ValidationError, ValueError):
        raise _CliArgumentError() from None


async def _loaded_attempt(
    dependencies: CliDependencies,
    attempt_id: str,
) -> LoadedAttempt:
    loaded = await dependencies.repository.load(attempt_id)
    if loaded is None:
        raise _MissingAttempt()
    return loaded


async def _mutate(
    dependencies: CliDependencies,
    attempt_id: str,
    command_type: type[PauseAttempt] | type[ResumeAttempt] | type[CancelAttempt],
    command_name: str,
) -> CommandResult:
    loaded = await _loaded_attempt(dependencies, attempt_id)
    command = command_type(
        schema_version=1,
        command_type=command_name,
        command_id=dependencies.id_factory.new_uuid(),
        attempt_id=attempt_id,
        expected_revision=loaded.state.revision,
    )
    return await dependencies.engine.handle(command)


async def run_command(
    args: argparse.Namespace,
    dependencies: CliDependencies,
) -> CliOutput:
    if args.operation == "recover":
        return await dependencies.recovery_coordinator.recover_startup(max_actions=100)

    if args.attempt_operation == "create":
        return await dependencies.engine.handle(_read_create_command(args.command_json))

    attempt_id = _attempt_id(args.attempt_id)
    if args.attempt_operation == "status":
        loaded = await _loaded_attempt(dependencies, attempt_id)
        return to_operator_view(loaded.state)
    if args.attempt_operation == "pause":
        return await _mutate(dependencies, attempt_id, PauseAttempt, "PAUSE_ATTEMPT")
    if args.attempt_operation == "resume":
        return await _mutate(dependencies, attempt_id, ResumeAttempt, "RESUME_ATTEMPT")
    if args.attempt_operation == "cancel":
        return await _mutate(dependencies, attempt_id, CancelAttempt, "CANCEL_ATTEMPT")

    response_kind = _response_kind(args.response_kind)
    response_ref = _strict_json(args.response_ref_json, _ARTIFACT_REF_ADAPTER)
    error = _strict_json(args.error_json, _ERROR_SUMMARY_ADAPTER)
    loaded = await _loaded_attempt(dependencies, attempt_id)
    try:
        command = SubmitExternalInput(
            schema_version=1,
            command_type="SUBMIT_EXTERNAL_INPUT",
            command_id=dependencies.id_factory.new_uuid(),
            attempt_id=attempt_id,
            expected_revision=loaded.state.revision,
            request_id=args.request_id,
            response_kind=response_kind,
            response_ref=response_ref,
            error=error,
        )
    except ValidationError:
        raise _CliArgumentError() from None
    return await dependencies.engine.handle(command)


def _write_json(value: CliOutput) -> None:
    payload = value.model_dump(mode="json", exclude_none=True)
    print(
        json.dumps(
            payload,
            allow_nan=False,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    )


def _result_exit_code(value: CliOutput) -> int:
    if isinstance(value, CommandResult):
        if value.accepted:
            return 0
        if value.error is not None and value.error.code == "ATTEMPT_NOT_FOUND":
            return 3
        if value.error is not None and value.error.code == "REVISION_CONFLICT":
            return 4
        return 5
    if isinstance(value, RecoverySummary) and value.failed_attempt_ids:
        return 5
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    try:
        args = parser.parse_args(argv)
        value = asyncio.run(run_command(args, _dependencies(args.database)))
    except _HelpRequested:
        return 0
    except _CliArgumentError:
        print(_SAFE_INVALID_ARGUMENTS, end="", file=__import__("sys").stderr)
        return 2
    except _MissingAttempt:
        print(_SAFE_MISSING_ATTEMPT, end="", file=__import__("sys").stderr)
        return 3
    except RevisionConflict:
        print(_SAFE_EXPECTED_FAILURE, end="", file=__import__("sys").stderr)
        return 4
    except (ValidationError, ValueError, OSError):
        print(_SAFE_EXPECTED_FAILURE, end="", file=__import__("sys").stderr)
        return 5
    except Exception:
        print(_SAFE_EXPECTED_FAILURE, end="", file=__import__("sys").stderr)
        return 5
    if not isinstance(value, (CommandResult, OperatorView, RecoverySummary)):
        print(_SAFE_EXPECTED_FAILURE, end="", file=__import__("sys").stderr)
        return 5
    _write_json(value)
    return _result_exit_code(value)


__all__ = ["build_parser", "main", "run_command"]
