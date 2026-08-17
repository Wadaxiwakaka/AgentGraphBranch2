from __future__ import annotations

import asyncio
import json
import math
import sqlite3
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any
from uuid import UUID

from ..actions import NormalizedAction
from ..contract import (
    CommandResult,
    FaultInjector,
    FaultPoint,
    NoOpFaultInjector,
)
from ..events import (
    ActionAccepted,
    ActionCancelled,
    ActionFailed,
    ActionOutcomeUnknown,
    ActionStarted,
    ActionSucceeded,
    ActionTimedOut,
    BudgetReleased,
    BudgetSettled,
    DomainEvent,
    parse_domain_event,
)
from ..reducer import replay_events
from ..state import (
    ActionStatus,
    ArtifactRef,
    AttemptPhase,
    AttemptState,
    TERMINAL_ACTION_STATUSES,
    canonical_state_bytes,
)
from ..store import (
    ArtifactRegistration,
    ClaimedAction,
    CommandRecord,
    CommandReuseError,
    CommitRequest,
    CommitResult,
    CorruptEventStream,
    DeliveryClaim,
    InvalidCommit,
    LoadedAttempt,
    OutboxActionStatus,
    RejectedCommandRequest,
    RepositoryOperationUncertain,
    RevisionConflict,
    StaleDeliveryClaim,
    UnsupportedRepositorySchema,
)


_SCHEMA_VERSION = 2
_TERMINAL_PHASES = frozenset(
    {
        AttemptPhase.SUCCEEDED,
        AttemptPhase.FAILED,
        AttemptPhase.CANCELLED,
        AttemptPhase.TIMED_OUT,
        AttemptPhase.INTERRUPTED,
    }
)
_TERMINAL_ACTION_EVENT_TYPES = (
    ActionSucceeded,
    ActionFailed,
    ActionTimedOut,
    ActionCancelled,
    ActionOutcomeUnknown,
)

_ACTION_OUTBOX_V2_DDL = """
    CREATE TABLE action_outbox (
        attempt_id TEXT NOT NULL,
        action_id TEXT NOT NULL,
        action_json TEXT NOT NULL,
        action_status TEXT NOT NULL
            CHECK (action_status IN ('ACCEPTED', 'STARTED')),
        recovery_policy TEXT NOT NULL,
        idempotency_key TEXT NOT NULL,
        accepted_sequence_no INTEGER NOT NULL CHECK (accepted_sequence_no >= 1),
        delivery_state TEXT NOT NULL CHECK (delivery_state IN ('PENDING', 'LEASED')),
        lease_owner TEXT,
        lease_expires_at TEXT,
        PRIMARY KEY (attempt_id, action_id),
        FOREIGN KEY (attempt_id) REFERENCES attempt_heads(attempt_id),
        CHECK (
            (delivery_state = 'PENDING' AND lease_owner IS NULL AND lease_expires_at IS NULL) OR
            (delivery_state = 'LEASED' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
        )
    )
    """

_SCHEMA_STATEMENTS = (
    """
    CREATE TABLE schema_meta (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        version INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE events (
        attempt_id TEXT NOT NULL CHECK (length(attempt_id) > 0),
        sequence_no INTEGER NOT NULL CHECK (sequence_no >= 1),
        event_id TEXT NOT NULL UNIQUE,
        event_json TEXT NOT NULL,
        event_hash TEXT NOT NULL
            CHECK (length(event_hash) = 64 AND event_hash NOT GLOB '*[^0-9a-f]*'),
        previous_event_hash TEXT
            CHECK (
                previous_event_hash IS NULL OR
                (length(previous_event_hash) = 64 AND
                 previous_event_hash NOT GLOB '*[^0-9a-f]*')
            ),
        PRIMARY KEY (attempt_id, sequence_no),
        CHECK (
            (sequence_no = 1 AND previous_event_hash IS NULL) OR
            (sequence_no > 1 AND previous_event_hash IS NOT NULL)
        )
    )
    """,
    """
    CREATE TABLE attempt_heads (
        attempt_id TEXT PRIMARY KEY CHECK (length(attempt_id) > 0),
        revision INTEGER NOT NULL CHECK (revision >= 1),
        state_json TEXT NOT NULL,
        state_hash TEXT NOT NULL
            CHECK (length(state_hash) = 64 AND state_hash NOT GLOB '*[^0-9a-f]*'),
        schema_version INTEGER NOT NULL CHECK (schema_version >= 1),
        latest_event_id TEXT NOT NULL,
        latest_event_hash TEXT NOT NULL
            CHECK (length(latest_event_hash) = 64 AND
                   latest_event_hash NOT GLOB '*[^0-9a-f]*')
    )
    """,
    """
    CREATE TABLE attempt_checkpoints (
        attempt_id TEXT NOT NULL CHECK (length(attempt_id) > 0),
        revision INTEGER NOT NULL CHECK (revision >= 1),
        state_json TEXT NOT NULL,
        state_hash TEXT NOT NULL
            CHECK (length(state_hash) = 64 AND state_hash NOT GLOB '*[^0-9a-f]*'),
        schema_version INTEGER NOT NULL CHECK (schema_version >= 1),
        latest_event_id TEXT NOT NULL,
        latest_event_hash TEXT NOT NULL
            CHECK (length(latest_event_hash) = 64 AND
                   latest_event_hash NOT GLOB '*[^0-9a-f]*'),
        PRIMARY KEY (attempt_id, revision)
    )
    """,
    """
    CREATE TABLE commands (
        attempt_id TEXT NOT NULL CHECK (length(attempt_id) > 0),
        command_id TEXT NOT NULL,
        request_hash TEXT NOT NULL
            CHECK (length(request_hash) = 64 AND request_hash NOT GLOB '*[^0-9a-f]*'),
        result_json TEXT NOT NULL,
        committed_revision INTEGER NOT NULL CHECK (committed_revision >= 0),
        checkpoint_written INTEGER NOT NULL CHECK (checkpoint_written IN (0, 1)),
        PRIMARY KEY (attempt_id, command_id)
    )
    """,
    """
    CREATE TABLE artifacts (
        artifact_key TEXT PRIMARY KEY
            CHECK (length(artifact_key) = 64 AND artifact_key NOT GLOB '*[^0-9a-f]*'),
        content_hash TEXT
            CHECK (content_hash IS NULL OR
                   (length(content_hash) = 64 AND content_hash NOT GLOB '*[^0-9a-f]*')),
        media_type TEXT NOT NULL CHECK (length(media_type) > 0),
        byte_size INTEGER NOT NULL CHECK (byte_size >= 0),
        relative_path TEXT,
        capture_class TEXT NOT NULL
            CHECK (capture_class IN ('full', 'hashed', 'metadata_only')),
        CHECK (
            (capture_class = 'full' AND content_hash IS NOT NULL AND relative_path IS NOT NULL) OR
            (capture_class = 'hashed' AND content_hash IS NOT NULL AND relative_path IS NULL) OR
            (capture_class = 'metadata_only' AND content_hash IS NULL AND relative_path IS NULL)
        )
    )
    """,
    """
    CREATE TABLE attempt_artifacts (
        attempt_id TEXT NOT NULL,
        artifact_key TEXT NOT NULL,
        ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
        PRIMARY KEY (attempt_id, artifact_key),
        UNIQUE (attempt_id, ordinal),
        FOREIGN KEY (attempt_id) REFERENCES attempt_heads(attempt_id),
        FOREIGN KEY (artifact_key) REFERENCES artifacts(artifact_key)
    )
    """,
    _ACTION_OUTBOX_V2_DDL,
    """
    CREATE INDEX action_outbox_claim_order
    ON action_outbox(accepted_sequence_no, action_id, attempt_id)
    """,
    """
    CREATE INDEX commands_attempt_order
    ON commands(attempt_id, committed_revision, command_id)
    """,
)


@dataclass(frozen=True, slots=True)
class _EventRow:
    event: DomainEvent
    event_json: str
    event_hash: str
    previous_event_hash: str | None


@dataclass(frozen=True, slots=True)
class _CheckpointRepair:
    revision: object
    state: AttemptState | None
    event: _EventRow | None
    present: bool


@dataclass(frozen=True, slots=True)
class _Projection:
    state: AttemptState
    state_hash: str
    rows: tuple[_EventRow, ...]
    checkpoint_revisions: tuple[int, ...]
    head_needs_repair: bool
    checkpoint_repairs: tuple[_CheckpointRepair, ...]

    @property
    def events(self) -> tuple[DomainEvent, ...]:
        return tuple(row.event for row in self.rows)

    @property
    def needs_repair(self) -> bool:
        return self.head_needs_repair or bool(self.checkpoint_repairs)


@dataclass(slots=True)
class _CommandGate:
    lock: asyncio.Lock
    users: int = 0


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _event_json(event: DomainEvent) -> str:
    return _canonical_json(event.model_dump(mode="json"))


def _model_json(model: Any) -> str:
    return _canonical_json(model.model_dump(mode="json"))


def _artifact_key(ref: ArtifactRef) -> str:
    return sha256(_model_json(ref).encode("utf-8")).hexdigest()


def _state_values(state: AttemptState) -> tuple[str, str]:
    state_bytes = canonical_state_bytes(state)
    return state_bytes.decode("utf-8"), sha256(state_bytes).hexdigest()


def _utc_text(value: datetime) -> str:
    normalized = value.astimezone(timezone.utc)
    return normalized.isoformat(timespec="microseconds").replace("+00:00", "Z")


def _parse_utc_text(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError("stored lease timestamp is not UTC")
    return parsed


@contextmanager
def _repository_boundary() -> Iterator[None]:
    try:
        yield
    except sqlite3.IntegrityError:
        raise InvalidCommit() from None
    except sqlite3.OperationalError:
        raise RepositoryOperationUncertain() from None


class SQLiteAttemptRepository:
    def __init__(
        self,
        database_path: str | Path,
        *,
        busy_timeout_ms: int = 5_000,
        fault_injector: FaultInjector | None = None,
    ) -> None:
        if isinstance(busy_timeout_ms, bool) or not isinstance(busy_timeout_ms, int):
            raise ValueError("busy_timeout_ms must be a nonnegative integer")
        if busy_timeout_ms < 0:
            raise ValueError("busy_timeout_ms must be a nonnegative integer")
        if fault_injector is not None and not isinstance(
            fault_injector, FaultInjector
        ):
            raise ValueError("fault_injector must implement FaultInjector")
        if fault_injector is not None and not callable(
            getattr(fault_injector, "hit", None)
        ):
            raise ValueError("fault_injector.hit must be callable")
        self._database_path = Path(database_path)
        self._busy_timeout_ms = busy_timeout_ms
        self._fault_injector = (
            fault_injector
            if fault_injector is not None
            else NoOpFaultInjector()
        )
        self._initialized = False
        self._initialization_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._gate_lock = asyncio.Lock()
        self._command_gates: dict[str, _CommandGate] = {}

    @asynccontextmanager
    async def command_scope(self, attempt_id: str) -> AsyncIterator[None]:
        self._validate_attempt_id(attempt_id)
        async with self._gate_lock:
            gate = self._command_gates.setdefault(
                attempt_id,
                _CommandGate(lock=asyncio.Lock()),
            )
            gate.users += 1
        try:
            async with gate.lock:
                yield
        finally:
            async with self._gate_lock:
                gate.users -= 1
                if gate.users == 0:
                    del self._command_gates[attempt_id]

    async def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        async with self._initialization_lock:
            if self._initialized:
                return
            await asyncio.to_thread(self._initialize_sync)
            self._initialized = True

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = sqlite3.connect(
            self._database_path,
            timeout=self._busy_timeout_ms / 1_000,
            isolation_level=None,
        )
        try:
            connection.row_factory = sqlite3.Row
            connection.execute(f"PRAGMA busy_timeout={self._busy_timeout_ms}")
            connection.execute("PRAGMA foreign_keys=ON")
            journal_mode = connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
            if str(journal_mode).lower() != "wal":
                raise UnsupportedRepositorySchema()
            yield connection
        finally:
            connection.close()

    def _initialize_sync(self) -> None:
        self._database_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                tables = {
                    str(row[0])
                    for row in connection.execute(
                        """
                        SELECT name FROM sqlite_master
                        WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
                        """
                    )
                }
                if "schema_meta" not in tables:
                    if tables:
                        raise UnsupportedRepositorySchema()
                    for statement in _SCHEMA_STATEMENTS:
                        connection.execute(statement)
                    connection.execute(
                        "INSERT INTO schema_meta(singleton, version) VALUES (1, ?)",
                        (_SCHEMA_VERSION,),
                    )
                rows = tuple(connection.execute("SELECT version FROM schema_meta"))
                if len(rows) != 1 or type(rows[0][0]) is not int:
                    raise UnsupportedRepositorySchema()
                version = rows[0][0]
                if version == 1:
                    self._migrate_v1_to_v2(connection)
                elif version != _SCHEMA_VERSION:
                    raise UnsupportedRepositorySchema()
                connection.commit()
            except UnsupportedRepositorySchema:
                connection.rollback()
                raise
            except sqlite3.Error:
                connection.rollback()
                raise UnsupportedRepositorySchema() from None
            except BaseException:
                connection.rollback()
                raise

    @staticmethod
    def _migrate_v1_to_v2(connection: sqlite3.Connection) -> None:
        expected_columns = (
            "attempt_id",
            "action_id",
            "action_json",
            "action_status",
            "recovery_policy",
            "idempotency_key",
            "accepted_sequence_no",
            "delivery_state",
            "lease_owner",
            "lease_expires_at",
        )
        columns = tuple(
            row[1] for row in connection.execute("PRAGMA table_info(action_outbox)")
        )
        statuses = tuple(
            row[0]
            for row in connection.execute(
                "SELECT DISTINCT action_status FROM action_outbox"
            )
        )
        if columns != expected_columns or any(status != "ACCEPTED" for status in statuses):
            raise UnsupportedRepositorySchema()

        row_count = connection.execute(
            "SELECT COUNT(*) FROM action_outbox"
        ).fetchone()[0]
        connection.execute("DROP INDEX action_outbox_claim_order")
        connection.execute("ALTER TABLE action_outbox RENAME TO action_outbox_v1")
        connection.execute(_ACTION_OUTBOX_V2_DDL)
        connection.execute(
            """
            INSERT INTO action_outbox(
                attempt_id, action_id, action_json, action_status,
                recovery_policy, idempotency_key, accepted_sequence_no,
                delivery_state, lease_owner, lease_expires_at
            )
            SELECT attempt_id, action_id, action_json, action_status,
                   recovery_policy, idempotency_key, accepted_sequence_no,
                   delivery_state, lease_owner, lease_expires_at
            FROM action_outbox_v1
            """
        )
        copied_count = connection.execute(
            "SELECT COUNT(*) FROM action_outbox"
        ).fetchone()[0]
        if copied_count != row_count:
            raise UnsupportedRepositorySchema()
        connection.execute("DROP TABLE action_outbox_v1")
        connection.execute(
            """
            CREATE INDEX action_outbox_claim_order
            ON action_outbox(accepted_sequence_no, action_id, attempt_id)
            """
        )
        if tuple(connection.execute("PRAGMA foreign_key_check")):
            raise UnsupportedRepositorySchema()
        updated = connection.execute(
            "UPDATE schema_meta SET version = ? WHERE singleton = 1 AND version = 1",
            (_SCHEMA_VERSION,),
        )
        if updated.rowcount != 1:
            raise UnsupportedRepositorySchema()

    async def _inspect_connection_settings(self) -> tuple[str, int, int]:
        await self._ensure_initialized()
        return await asyncio.to_thread(self._inspect_connection_settings_sync)

    def _inspect_connection_settings_sync(self) -> tuple[str, int, int]:
        with self._connection() as connection:
            journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0])
            foreign_keys = int(connection.execute("PRAGMA foreign_keys").fetchone()[0])
            busy_timeout = int(connection.execute("PRAGMA busy_timeout").fetchone()[0])
            return journal_mode, foreign_keys, busy_timeout

    @staticmethod
    def _validate_attempt_id(attempt_id: str) -> None:
        if type(attempt_id) is not str or not attempt_id:
            raise ValueError("attempt_id must be a nonempty string")

    @staticmethod
    def _read_event_rows(
        connection: sqlite3.Connection,
        attempt_id: str,
    ) -> tuple[_EventRow, ...]:
        try:
            database_rows = tuple(
                connection.execute(
                    """
                    SELECT sequence_no, event_id, event_json, event_hash,
                           previous_event_hash
                    FROM events WHERE attempt_id = ? ORDER BY sequence_no
                    """,
                    (attempt_id,),
                )
            )
            event_rows: list[_EventRow] = []
            previous_hash: str | None = None
            for expected_sequence, row in enumerate(database_rows, start=1):
                sequence_no = row["sequence_no"]
                event_id = row["event_id"]
                raw_json = row["event_json"]
                event_hash = row["event_hash"]
                stored_previous_hash = row["previous_event_hash"]
                if type(sequence_no) is not int or sequence_no != expected_sequence:
                    raise CorruptEventStream()
                if not isinstance(raw_json, str) or not isinstance(event_hash, str):
                    raise CorruptEventStream()
                payload = json.loads(raw_json)
                if _canonical_json(payload) != raw_json:
                    raise CorruptEventStream()
                if sha256(raw_json.encode("utf-8")).hexdigest() != event_hash:
                    raise CorruptEventStream()
                if stored_previous_hash != previous_hash:
                    raise CorruptEventStream()
                event = parse_domain_event(payload)
                if (
                    event.attempt_id != attempt_id
                    or event.sequence_no != sequence_no
                    or str(event.event_id) != event_id
                ):
                    raise CorruptEventStream()
                event_rows.append(
                    _EventRow(
                        event=event,
                        event_json=raw_json,
                        event_hash=event_hash,
                        previous_event_hash=stored_previous_hash,
                    )
                )
                previous_hash = event_hash
            if event_rows:
                replay_events(row.event for row in event_rows)
            return tuple(event_rows)
        except CorruptEventStream:
            raise
        except Exception:
            raise CorruptEventStream() from None

    @classmethod
    def _read_projection(
        cls,
        connection: sqlite3.Connection,
        attempt_id: str,
    ) -> _Projection | None:
        rows = cls._read_event_rows(connection, attempt_id)
        if not rows:
            derived_rows = connection.execute(
                """
                SELECT
                    EXISTS(SELECT 1 FROM attempt_heads WHERE attempt_id = ?),
                    EXISTS(SELECT 1 FROM attempt_checkpoints WHERE attempt_id = ?)
                """,
                (attempt_id, attempt_id),
            ).fetchone()
            if derived_rows[0] or derived_rows[1]:
                raise CorruptEventStream()
            return None

        events = tuple(row.event for row in rows)
        try:
            state = replay_events(events)
        except Exception:
            raise CorruptEventStream() from None
        state_json, state_hash = _state_values(state)
        final_row = rows[-1]

        head = connection.execute(
            """
            SELECT revision, state_json, state_hash, schema_version,
                   latest_event_id, latest_event_hash
            FROM attempt_heads WHERE attempt_id = ?
            """,
            (attempt_id,),
        ).fetchone()
        head_valid = head is not None and cls._snapshot_matches(
            head,
            attempt_id=attempt_id,
            state=state,
            state_json=state_json,
            state_hash=state_hash,
            event=final_row,
        )

        checkpoint_rows = tuple(
            connection.execute(
                """
                SELECT revision, state_json, state_hash, schema_version,
                       latest_event_id, latest_event_hash
                FROM attempt_checkpoints
                WHERE attempt_id = ? ORDER BY revision
                """,
                (attempt_id,),
            )
        )
        expected_checkpoint_revisions = tuple(
            row[0]
            for row in connection.execute(
                """
                SELECT committed_revision FROM commands
                WHERE attempt_id = ? AND checkpoint_written = 1
                ORDER BY committed_revision
                """,
                (attempt_id,),
            )
        )
        if (
            any(
                type(revision) is not int or not 1 <= revision <= len(rows)
                for revision in expected_checkpoint_revisions
            )
            or tuple(sorted(set(expected_checkpoint_revisions)))
            != expected_checkpoint_revisions
        ):
            raise CorruptEventStream()

        checkpoint_repairs: list[_CheckpointRepair] = []
        checkpoint_by_revision: dict[int, sqlite3.Row] = {}
        expected_revision_set = set(expected_checkpoint_revisions)
        for checkpoint in checkpoint_rows:
            revision = checkpoint["revision"]
            if type(revision) is not int or revision not in expected_revision_set:
                checkpoint_repairs.append(
                    _CheckpointRepair(
                        revision=revision,
                        state=None,
                        event=None,
                        present=True,
                    )
                )
                continue
            checkpoint_by_revision[revision] = checkpoint

        states_by_revision: dict[int, AttemptState] = {}
        for revision in expected_checkpoint_revisions:
            prefix_state = states_by_revision.get(revision)
            if prefix_state is None:
                try:
                    prefix_state = replay_events(events[:revision])
                except Exception:
                    raise CorruptEventStream() from None
                states_by_revision[revision] = prefix_state
            prefix_json, prefix_hash = _state_values(prefix_state)
            prefix_event = rows[revision - 1]
            checkpoint = checkpoint_by_revision.get(revision)
            if checkpoint is None or not cls._snapshot_matches(
                checkpoint,
                attempt_id=attempt_id,
                state=prefix_state,
                state_json=prefix_json,
                state_hash=prefix_hash,
                event=prefix_event,
            ):
                checkpoint_repairs.append(
                    _CheckpointRepair(
                        revision=revision,
                        state=prefix_state,
                        event=prefix_event,
                        present=checkpoint is not None,
                    )
                )

        return _Projection(
            state=state,
            state_hash=state_hash,
            rows=rows,
            checkpoint_revisions=expected_checkpoint_revisions,
            head_needs_repair=not head_valid,
            checkpoint_repairs=tuple(checkpoint_repairs),
        )

    @staticmethod
    def _snapshot_matches(
        row: sqlite3.Row,
        *,
        attempt_id: str,
        state: AttemptState,
        state_json: str,
        state_hash: str,
        event: _EventRow,
    ) -> bool:
        return (
            state.attempt_id == attempt_id
            and row["revision"] == state.revision
            and row["state_json"] == state_json
            and row["state_hash"] == state_hash
            and row["schema_version"] == state.schema_version
            and row["latest_event_id"] == str(event.event.event_id)
            and row["latest_event_hash"] == event.event_hash
        )

    @classmethod
    def _repair_projection(
        cls,
        connection: sqlite3.Connection,
        attempt_id: str,
        projection: _Projection,
    ) -> None:
        if projection.head_needs_repair:
            cls._write_head(connection, projection.state, projection.rows[-1])
        for repair in projection.checkpoint_repairs:
            if repair.state is None or repair.event is None:
                connection.execute(
                    """
                    DELETE FROM attempt_checkpoints
                    WHERE attempt_id = ? AND revision = ?
                    """,
                    (attempt_id, repair.revision),
                )
                continue
            if not repair.present:
                cls._insert_checkpoint(connection, repair.state, repair.event)
                continue
            state_json, state_hash = _state_values(repair.state)
            connection.execute(
                """
                UPDATE attempt_checkpoints
                SET state_json = ?, state_hash = ?, schema_version = ?,
                    latest_event_id = ?, latest_event_hash = ?
                WHERE attempt_id = ? AND revision = ?
                """,
                (
                    state_json,
                    state_hash,
                    repair.state.schema_version,
                    str(repair.event.event.event_id),
                    repair.event.event_hash,
                    attempt_id,
                    repair.revision,
                ),
            )

    @staticmethod
    def _write_head(
        connection: sqlite3.Connection,
        state: AttemptState,
        event: _EventRow,
    ) -> None:
        state_json, state_hash = _state_values(state)
        connection.execute(
            """
            INSERT INTO attempt_heads(
                attempt_id, revision, state_json, state_hash, schema_version,
                latest_event_id, latest_event_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(attempt_id) DO UPDATE SET
                revision = excluded.revision,
                state_json = excluded.state_json,
                state_hash = excluded.state_hash,
                schema_version = excluded.schema_version,
                latest_event_id = excluded.latest_event_id,
                latest_event_hash = excluded.latest_event_hash
            """,
            (
                state.attempt_id,
                state.revision,
                state_json,
                state_hash,
                state.schema_version,
                str(event.event.event_id),
                event.event_hash,
            ),
        )

    async def load(self, attempt_id: str) -> LoadedAttempt | None:
        self._validate_attempt_id(attempt_id)
        with _repository_boundary():
            await self._ensure_initialized()
            loaded, needs_repair = await asyncio.to_thread(
                self._load_sync,
                attempt_id,
                False,
            )
            if not needs_repair:
                return loaded
            async with self._write_lock:
                loaded, _ = await asyncio.to_thread(self._load_sync, attempt_id, True)
                return loaded

    def _load_sync(
        self,
        attempt_id: str,
        repair: bool,
    ) -> tuple[LoadedAttempt | None, bool]:
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                projection = self._read_projection(connection, attempt_id)
                if projection is None:
                    connection.commit()
                    return None, False
                if repair and projection.needs_repair:
                    self._repair_projection(connection, attempt_id, projection)
                loaded = self._build_loaded_attempt(connection, attempt_id, projection)
                needs_repair = projection.needs_repair and not repair
                connection.commit()
                return loaded, needs_repair
            except BaseException:
                connection.rollback()
                raise

    @staticmethod
    def _read_command_result(row: sqlite3.Row) -> CommandResult:
        return CommandResult.model_validate_json(row["result_json"])

    @classmethod
    def _build_loaded_attempt(
        cls,
        connection: sqlite3.Connection,
        attempt_id: str,
        projection: _Projection,
    ) -> LoadedAttempt:
        command_records = tuple(
            CommandRecord(
                command_id=UUID(row["command_id"]),
                request_hash=row["request_hash"],
                committed_revision=row["committed_revision"],
                result=cls._read_command_result(row),
            )
            for row in connection.execute(
                """
                SELECT command_id, request_hash, result_json, committed_revision
                FROM commands WHERE attempt_id = ?
                ORDER BY committed_revision, command_id
                """,
                (attempt_id,),
            )
        )
        registrations: list[ArtifactRegistration] = []
        for row in connection.execute(
            """
            SELECT a.artifact_key, a.capture_class, a.content_hash, a.media_type,
                   a.byte_size, a.relative_path
            FROM attempt_artifacts AS aa
            JOIN artifacts AS a ON a.artifact_key = aa.artifact_key
            WHERE aa.attempt_id = ? ORDER BY aa.ordinal
            """,
            (attempt_id,),
        ):
            ref = ArtifactRef(
                capture_class=row["capture_class"],
                content_hash=row["content_hash"],
                media_type=row["media_type"],
                byte_size=row["byte_size"],
                relative_path=row["relative_path"],
            )
            if _artifact_key(ref) != row["artifact_key"]:
                raise InvalidCommit()
            registrations.append(ArtifactRegistration(ref=ref))
        return LoadedAttempt(
            state=projection.state,
            state_hash=projection.state_hash,
            events=projection.events,
            latest_event_id=projection.events[-1].event_id,
            command_records=command_records,
            artifact_registrations=tuple(registrations),
            checkpoint_revisions=projection.checkpoint_revisions,
        )

    async def find_command(
        self,
        attempt_id: str,
        command_id: UUID,
    ) -> CommandRecord | None:
        self._validate_attempt_id(attempt_id)
        if not isinstance(command_id, UUID):
            raise ValueError("command_id must be a UUID")
        with _repository_boundary():
            await self._ensure_initialized()
            return await asyncio.to_thread(
                self._find_command_sync,
                attempt_id,
                command_id,
            )

    def _find_command_sync(
        self,
        attempt_id: str,
        command_id: UUID,
    ) -> CommandRecord | None:
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                row = connection.execute(
                    """
                    SELECT command_id, request_hash, result_json, committed_revision
                    FROM commands WHERE attempt_id = ? AND command_id = ?
                    """,
                    (attempt_id, str(command_id)),
                ).fetchone()
                if row is None:
                    connection.commit()
                    return None

                result = self._read_command_result(row)
                event_rows = self._read_event_rows(connection, attempt_id)
                if result.accepted:
                    revision = row["committed_revision"]
                    if (
                        type(revision) is not int
                        or not 1 <= revision <= len(event_rows)
                        or event_rows[revision - 1].event.command_id != command_id
                    ):
                        raise CorruptEventStream()
                record = CommandRecord(
                    command_id=UUID(row["command_id"]),
                    request_hash=row["request_hash"],
                    committed_revision=row["committed_revision"],
                    result=result,
                )
                connection.commit()
                return record
            except BaseException:
                connection.rollback()
                raise

    async def record_rejection(
        self,
        request: RejectedCommandRequest,
    ) -> CommandResult:
        validated = RejectedCommandRequest.model_validate_json(request.model_dump_json())
        with _repository_boundary():
            await self._ensure_initialized()
            async with self._write_lock:
                return await asyncio.to_thread(self._record_rejection_sync, validated)

    def _record_rejection_sync(
        self,
        request: RejectedCommandRequest,
    ) -> CommandResult:
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                existing = connection.execute(
                    """
                    SELECT request_hash, result_json FROM commands
                    WHERE attempt_id = ? AND command_id = ?
                    """,
                    (request.attempt_id, str(request.command_id)),
                ).fetchone()
                if existing is not None:
                    if existing["request_hash"] != request.request_hash:
                        raise CommandReuseError()
                    result = CommandResult.model_validate_json(existing["result_json"])
                    connection.commit()
                    return result

                projection = self._read_projection(connection, request.attempt_id)
                current_revision = 0 if projection is None else projection.state.revision
                current_phase = None if projection is None else projection.state.phase
                if request.result.revision != current_revision:
                    raise RevisionConflict()
                if request.result.phase is not current_phase:
                    raise InvalidCommit()
                if projection is not None and projection.needs_repair:
                    self._repair_projection(connection, request.attempt_id, projection)

                connection.execute(
                    """
                    INSERT INTO commands(
                        attempt_id, command_id, request_hash, result_json,
                        committed_revision, checkpoint_written
                    ) VALUES (?, ?, ?, ?, ?, 0)
                    """,
                    (
                        request.attempt_id,
                        str(request.command_id),
                        request.request_hash,
                        _model_json(request.result),
                        request.result.revision,
                    ),
                )
                connection.commit()
                return CommandResult.model_validate_json(_model_json(request.result))
            except (CommandReuseError, RevisionConflict, InvalidCommit, CorruptEventStream):
                connection.rollback()
                raise
            except sqlite3.IntegrityError:
                connection.rollback()
                raise InvalidCommit() from None
            except sqlite3.OperationalError:
                connection.rollback()
                raise RepositoryOperationUncertain() from None
            except BaseException:
                connection.rollback()
                raise

    async def commit(self, request: CommitRequest) -> CommitResult:
        validated = CommitRequest.model_validate_json(request.model_dump_json())
        with _repository_boundary():
            await self._ensure_initialized()
            async with self._write_lock:
                return await asyncio.to_thread(self._commit_sync, validated)

    def _commit_sync(self, request: CommitRequest) -> CommitResult:
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                existing_command = connection.execute(
                    """
                    SELECT request_hash, result_json, committed_revision,
                           checkpoint_written
                    FROM commands WHERE attempt_id = ? AND command_id = ?
                    """,
                    (request.attempt_id, str(request.command_id)),
                ).fetchone()
                if existing_command is not None:
                    result = self._duplicate_commit_result(
                        connection,
                        request,
                        existing_command,
                    )
                    connection.commit()
                    return result

                projection = self._read_projection(connection, request.attempt_id)
                current_revision = 0 if projection is None else projection.state.revision
                if request.expected_revision != current_revision:
                    raise RevisionConflict()

                prior_events = () if projection is None else projection.events
                candidate_events = prior_events + request.events
                candidate_state = replay_events(candidate_events)
                self._validate_projection(request, candidate_state)
                self._validate_event_ids(connection, request.events)
                accepted_sequences = self._validate_outbox_changes(
                    connection,
                    request=request,
                    candidate_state=candidate_state,
                )

                if projection is not None and projection.needs_repair:
                    self._repair_projection(connection, request.attempt_id, projection)

                previous_hash = None if projection is None else projection.rows[-1].event_hash
                new_rows: list[_EventRow] = []
                for event in request.events:
                    raw_json = _event_json(event)
                    event_hash = sha256(raw_json.encode("utf-8")).hexdigest()
                    connection.execute(
                        """
                        INSERT INTO events(
                            attempt_id, sequence_no, event_id, event_json,
                            event_hash, previous_event_hash
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (
                            request.attempt_id,
                            event.sequence_no,
                            str(event.event_id),
                            raw_json,
                            event_hash,
                            previous_hash,
                        ),
                    )
                    event_row = _EventRow(
                        event=event,
                        event_json=raw_json,
                        event_hash=event_hash,
                        previous_event_hash=previous_hash,
                    )
                    new_rows.append(event_row)
                    previous_hash = event_hash

                final_row = new_rows[-1]
                self._write_head(connection, candidate_state, final_row)
                if request.checkpoint:
                    self._insert_checkpoint(connection, candidate_state, final_row)
                self._register_artifacts(
                    connection,
                    request.attempt_id,
                    request.artifact_registrations,
                )
                self._write_outbox_changes(
                    connection,
                    request=request,
                    accepted_sequences=accepted_sequences,
                )
                connection.execute(
                    """
                    INSERT INTO commands(
                        attempt_id, command_id, request_hash, result_json,
                        committed_revision, checkpoint_written
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        request.attempt_id,
                        str(request.command_id),
                        request.request_hash,
                        _model_json(request.result),
                        request.result.revision,
                        int(request.checkpoint),
                    ),
                )
                self._fault_injector.hit(FaultPoint.BEFORE_TRANSACTION_COMMIT)
                connection.commit()

                state_json, state_hash = _state_values(candidate_state)
                del state_json
                return CommitResult(
                    state=candidate_state,
                    state_hash=state_hash,
                    events=candidate_events,
                    latest_event_id=candidate_events[-1].event_id,
                    command_result=request.result,
                    checkpoint_written=request.checkpoint,
                )
            except (
                CommandReuseError,
                RevisionConflict,
                InvalidCommit,
                CorruptEventStream,
            ):
                connection.rollback()
                raise
            except sqlite3.IntegrityError:
                connection.rollback()
                raise InvalidCommit() from None
            except sqlite3.OperationalError:
                connection.rollback()
                raise RepositoryOperationUncertain() from None
            except BaseException:
                connection.rollback()
                raise

    def _duplicate_commit_result(
        self,
        connection: sqlite3.Connection,
        request: CommitRequest,
        command_row: sqlite3.Row,
    ) -> CommitResult:
        if command_row["request_hash"] != request.request_hash:
            raise CommandReuseError()
        command_result = CommandResult.model_validate_json(command_row["result_json"])
        if not command_result.accepted:
            raise InvalidCommit()
        projection = self._read_projection(connection, request.attempt_id)
        if projection is None:
            raise CorruptEventStream()
        if projection.needs_repair:
            self._repair_projection(connection, request.attempt_id, projection)
        revision = command_row["committed_revision"]
        if type(revision) is not int or not 1 <= revision <= len(projection.rows):
            raise CorruptEventStream()
        events = projection.events[:revision]
        try:
            state = replay_events(events)
        except Exception:
            raise CorruptEventStream() from None
        _, state_hash = _state_values(state)
        return CommitResult(
            state=state,
            state_hash=state_hash,
            events=events,
            latest_event_id=events[-1].event_id,
            command_result=command_result,
            checkpoint_written=bool(command_row["checkpoint_written"]),
        )

    @staticmethod
    def _validate_projection(request: CommitRequest, state: AttemptState) -> None:
        if (
            state.attempt_id != request.attempt_id
            or state.revision != request.result.revision
            or state.phase is not request.result.phase
        ):
            raise InvalidCommit()

    @staticmethod
    def _validate_event_ids(
        connection: sqlite3.Connection,
        events: tuple[DomainEvent, ...],
    ) -> None:
        for event in events:
            if connection.execute(
                "SELECT 1 FROM events WHERE event_id = ?",
                (str(event.event_id),),
            ).fetchone() is not None:
                raise InvalidCommit()

    @staticmethod
    def _validate_outbox_changes(
        connection: sqlite3.Connection,
        *,
        request: CommitRequest,
        candidate_state: AttemptState,
    ) -> dict[str, int]:
        accepted_events = tuple(
            event for event in request.events if isinstance(event, ActionAccepted)
        )
        accepted_action_ids = tuple(event.action.action_id for event in accepted_events)
        outbox_action_ids = tuple(action.action_id for action in request.outbox_actions)
        if (
            len(accepted_action_ids) != len(set(accepted_action_ids))
            or len(outbox_action_ids) != len(set(outbox_action_ids))
            or set(accepted_action_ids) != set(outbox_action_ids)
        ):
            raise InvalidCommit()

        existing_entries = {
            row["action_id"]: row
            for row in connection.execute(
                """
                SELECT action_id, action_status, delivery_state,
                       lease_owner, lease_expires_at
                FROM action_outbox WHERE attempt_id = ?
                """,
                (request.attempt_id,),
            )
        }
        existing_action_ids = set(existing_entries)
        accepted_sequences: dict[str, int] = {}
        for action in request.outbox_actions:
            if action.action_id in existing_action_ids:
                raise InvalidCommit()
            matching_events = [
                event
                for event in accepted_events
                if event.action.action_id == action.action_id and event.action == action
            ]
            matching_state_actions = [
                state_action
                for state_action in candidate_state.actions
                if state_action.action_id == action.action_id
                and state_action.status is ActionStatus.ACCEPTED
            ]
            if len(matching_events) != 1 or len(matching_state_actions) != 1:
                raise InvalidCommit()
            accepted_sequences[action.action_id] = matching_events[0].sequence_no

        started_events = tuple(
            event for event in request.events if isinstance(event, ActionStarted)
        )
        if not started_events and request.delivery_claim is not None:
            raise InvalidCommit()
        for event in started_events:
            entry = existing_entries.get(event.action_id)
            claim = request.delivery_claim
            state_action = next(
                (
                    action
                    for action in candidate_state.actions
                    if action.action_id == event.action_id
                ),
                None,
            )
            if (
                entry is None
                or claim is None
                or claim.attempt_id != request.attempt_id
                or claim.action_id != event.action_id
                or claim.worker_id != entry["lease_owner"]
                or entry["lease_expires_at"] is None
                or claim.lease_expires_at
                != _parse_utc_text(entry["lease_expires_at"])
                or event.wall_time_utc >= claim.lease_expires_at
                or entry["action_status"] != OutboxActionStatus.ACCEPTED.value
                or entry["delivery_state"] != "LEASED"
                or entry["lease_owner"] is None
                or entry["lease_expires_at"] is None
                or state_action is None
                or (
                    state_action.status is not ActionStatus.STARTED
                    and state_action.status not in TERMINAL_ACTION_STATUSES
                )
            ):
                raise StaleDeliveryClaim()

        terminal_events = tuple(
            event
            for event in request.events
            if isinstance(event, _TERMINAL_ACTION_EVENT_TYPES)
            and event.action_id in existing_action_ids
        )
        completed_action_ids = request.completed_delivery_action_ids
        if tuple(event.action_id for event in terminal_events) != completed_action_ids:
            raise InvalidCommit()
        for event in terminal_events:
            completed_action_id = event.action_id
            if completed_action_id not in existing_action_ids:
                raise InvalidCommit()
            entry_status = existing_entries[completed_action_id]["action_status"]
            if any(
                event.action_id == completed_action_id for event in started_events
            ):
                entry_status = OutboxActionStatus.STARTED.value
            predispatch_cancellation = (
                isinstance(event, ActionCancelled)
                and entry_status == OutboxActionStatus.ACCEPTED.value
            )
            if (
                entry_status != OutboxActionStatus.STARTED.value
                and not predispatch_cancellation
            ):
                raise InvalidCommit()
        remaining_action_ids = (
            existing_action_ids
            | {action.action_id for action in request.outbox_actions}
        ) - set(completed_action_ids)
        if candidate_state.phase in _TERMINAL_PHASES and remaining_action_ids:
            raise InvalidCommit()
        return accepted_sequences

    @staticmethod
    def _insert_checkpoint(
        connection: sqlite3.Connection,
        state: AttemptState,
        event: _EventRow,
    ) -> None:
        state_json, state_hash = _state_values(state)
        connection.execute(
            """
            INSERT INTO attempt_checkpoints(
                attempt_id, revision, state_json, state_hash, schema_version,
                latest_event_id, latest_event_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                state.attempt_id,
                state.revision,
                state_json,
                state_hash,
                state.schema_version,
                str(event.event.event_id),
                event.event_hash,
            ),
        )

    @staticmethod
    def _register_artifacts(
        connection: sqlite3.Connection,
        attempt_id: str,
        registrations: tuple[ArtifactRegistration, ...],
    ) -> None:
        next_ordinal = connection.execute(
            """
            SELECT COALESCE(MAX(ordinal), -1) + 1
            FROM attempt_artifacts WHERE attempt_id = ?
            """,
            (attempt_id,),
        ).fetchone()[0]
        for registration in registrations:
            ref = registration.ref
            artifact_key = _artifact_key(ref)
            connection.execute(
                """
                INSERT INTO artifacts(
                    artifact_key, content_hash, media_type, byte_size,
                    relative_path, capture_class
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(artifact_key) DO NOTHING
                """,
                (
                    artifact_key,
                    ref.content_hash,
                    ref.media_type,
                    ref.byte_size,
                    ref.relative_path,
                    ref.capture_class,
                ),
            )
            stored = connection.execute(
                """
                SELECT content_hash, media_type, byte_size, relative_path, capture_class
                FROM artifacts WHERE artifact_key = ?
                """,
                (artifact_key,),
            ).fetchone()
            if stored is None or tuple(stored) != (
                ref.content_hash,
                ref.media_type,
                ref.byte_size,
                ref.relative_path,
                ref.capture_class,
            ):
                raise InvalidCommit()
            exists = connection.execute(
                """
                SELECT 1 FROM attempt_artifacts
                WHERE attempt_id = ? AND artifact_key = ?
                """,
                (attempt_id, artifact_key),
            ).fetchone()
            if exists is not None:
                continue
            connection.execute(
                """
                INSERT INTO attempt_artifacts(attempt_id, artifact_key, ordinal)
                VALUES (?, ?, ?)
                """,
                (attempt_id, artifact_key, next_ordinal),
            )
            next_ordinal += 1

    @staticmethod
    def _write_outbox_changes(
        connection: sqlite3.Connection,
        *,
        request: CommitRequest,
        accepted_sequences: dict[str, int],
    ) -> None:
        for action in request.outbox_actions:
            connection.execute(
                """
                INSERT INTO action_outbox(
                    attempt_id, action_id, action_json, action_status,
                    recovery_policy, idempotency_key, accepted_sequence_no,
                    delivery_state, lease_owner, lease_expires_at
                ) VALUES (?, ?, ?, 'ACCEPTED', ?, ?, ?, 'PENDING', NULL, NULL)
                """,
                (
                    request.attempt_id,
                    action.action_id,
                    _model_json(action),
                    action.recovery_policy.value,
                    action.idempotency_key,
                    accepted_sequences[action.action_id],
                ),
            )
        for event in request.events:
            if not isinstance(event, ActionStarted):
                continue
            cursor = connection.execute(
                """
                UPDATE action_outbox SET action_status = 'STARTED'
                WHERE attempt_id = ? AND action_id = ?
                  AND action_status = 'ACCEPTED'
                  AND delivery_state = 'LEASED'
                  AND lease_owner IS NOT NULL
                  AND lease_expires_at IS NOT NULL
                """,
                (request.attempt_id, event.action_id),
            )
            if cursor.rowcount != 1:
                raise InvalidCommit()
        for completed_action_id in request.completed_delivery_action_ids:
            cursor = connection.execute(
                """
                DELETE FROM action_outbox WHERE attempt_id = ? AND action_id = ?
                """,
                (request.attempt_id, completed_action_id),
            )
            if cursor.rowcount != 1:
                raise InvalidCommit()

    async def list_events(
        self,
        attempt_id: str,
        *,
        after_sequence_no: int = 0,
    ) -> tuple[DomainEvent, ...]:
        self._validate_attempt_id(attempt_id)
        if type(after_sequence_no) is not int or after_sequence_no < 0:
            raise ValueError("after_sequence_no must be a nonnegative integer")
        with _repository_boundary():
            await self._ensure_initialized()
            return await asyncio.to_thread(
                self._list_events_sync,
                attempt_id,
                after_sequence_no,
            )

    def _list_events_sync(
        self,
        attempt_id: str,
        after_sequence_no: int,
    ) -> tuple[DomainEvent, ...]:
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                rows = self._read_event_rows(connection, attempt_id)
                events = tuple(
                    row.event
                    for row in rows
                    if row.event.sequence_no > after_sequence_no
                )
                connection.commit()
                return events
            except BaseException:
                connection.rollback()
                raise

    async def claim_action(
        self,
        *,
        worker_id: str,
        now_utc: datetime,
        lease_seconds: float,
    ) -> ClaimedAction | None:
        if type(worker_id) is not str or not worker_id:
            raise ValueError("worker_id must be a nonempty string")
        if (
            not isinstance(now_utc, datetime)
            or now_utc.tzinfo is None
            or now_utc.utcoffset() != timedelta(0)
        ):
            raise ValueError("now_utc must be timezone-aware UTC")
        if (
            isinstance(lease_seconds, bool)
            or not isinstance(lease_seconds, (int, float))
            or not math.isfinite(lease_seconds)
            or lease_seconds <= 0
        ):
            raise ValueError("lease_seconds must be a positive finite number")
        with _repository_boundary():
            await self._ensure_initialized()
            async with self._write_lock:
                return await asyncio.to_thread(
                    self._claim_action_sync,
                    worker_id,
                    now_utc,
                    float(lease_seconds),
                )

    async def confirm_action_claim(
        self,
        *,
        claim: DeliveryClaim,
        action_status: OutboxActionStatus,
        now_utc: datetime,
    ) -> bool:
        validated = DeliveryClaim.model_validate_json(claim.model_dump_json())
        if not isinstance(action_status, OutboxActionStatus):
            raise ValueError("action_status must be an OutboxActionStatus")
        if (
            not isinstance(now_utc, datetime)
            or now_utc.tzinfo is None
            or now_utc.utcoffset() != timedelta(0)
        ):
            raise ValueError("now_utc must be timezone-aware UTC")
        with _repository_boundary():
            await self._ensure_initialized()
            return await asyncio.to_thread(
                self._confirm_action_claim_sync,
                validated,
                action_status,
                now_utc,
            )

    def _confirm_action_claim_sync(
        self,
        claim: DeliveryClaim,
        action_status: OutboxActionStatus,
        now_utc: datetime,
    ) -> bool:
        with self._connection() as connection:
            row = connection.execute(
                """
                SELECT action_status, delivery_state, lease_owner,
                       lease_expires_at
                FROM action_outbox
                WHERE attempt_id = ? AND action_id = ?
                """,
                (claim.attempt_id, claim.action_id),
            ).fetchone()
            if row is None:
                return False
            lease_expires_at = row["lease_expires_at"]
            if not isinstance(lease_expires_at, str):
                return False
            try:
                stored_expiry = _parse_utc_text(lease_expires_at)
            except ValueError:
                raise InvalidCommit() from None
            return (
                row["action_status"] == action_status.value
                and row["delivery_state"] == "LEASED"
                and row["lease_owner"] == claim.worker_id
                and stored_expiry == claim.lease_expires_at
                and now_utc < claim.lease_expires_at
            )

    def _claim_action_sync(
        self,
        worker_id: str,
        now_utc: datetime,
        lease_seconds: float,
    ) -> ClaimedAction | None:
        with self._connection() as connection:
            connection.execute("BEGIN IMMEDIATE")
            try:
                candidates = tuple(
                    connection.execute(
                        """
                        SELECT attempt_id, action_id, action_json, recovery_policy,
                               idempotency_key, accepted_sequence_no,
                               action_status, delivery_state, lease_owner,
                               lease_expires_at
                        FROM action_outbox
                        ORDER BY accepted_sequence_no, action_id, attempt_id
                        """
                    )
                )
                for row in candidates:
                    lease_expires_at = row["lease_expires_at"]
                    if (
                        lease_expires_at is not None
                        and _parse_utc_text(lease_expires_at) > now_utc
                    ):
                        continue
                    action = NormalizedAction.model_validate_json(row["action_json"])
                    if (
                        action.action_id != row["action_id"]
                        or action.recovery_policy.value != row["recovery_policy"]
                        or action.idempotency_key != row["idempotency_key"]
                    ):
                        raise InvalidCommit()
                    try:
                        action_status = OutboxActionStatus(row["action_status"])
                    except ValueError:
                        raise InvalidCommit() from None
                    projection = self._read_projection(connection, row["attempt_id"])
                    if projection is None:
                        raise CorruptEventStream()
                    if projection.needs_repair:
                        self._repair_projection(connection, row["attempt_id"], projection)
                    if not self._is_dispatchable(
                        projection,
                        action,
                        action_status,
                    ):
                        continue

                    expires_at = now_utc + timedelta(seconds=lease_seconds)
                    connection.execute(
                        """
                        UPDATE action_outbox
                        SET delivery_state = 'LEASED', lease_owner = ?,
                            lease_expires_at = ?
                        WHERE attempt_id = ? AND action_id = ?
                        """,
                        (
                            worker_id,
                            _utc_text(expires_at),
                            row["attempt_id"],
                            row["action_id"],
                        ),
                    )
                    claimed = ClaimedAction(
                        attempt_id=row["attempt_id"],
                        action=action,
                        accepted_sequence_no=row["accepted_sequence_no"],
                        action_status=action_status,
                        worker_id=worker_id,
                        lease_expires_at=expires_at,
                    )
                    connection.commit()
                    return claimed
                connection.commit()
                return None
            except (InvalidCommit, CorruptEventStream):
                connection.rollback()
                raise
            except sqlite3.Error:
                connection.rollback()
                raise InvalidCommit() from None
            except BaseException:
                connection.rollback()
                raise

    @staticmethod
    def _is_dispatchable(
        projection: _Projection,
        action: NormalizedAction,
        action_status: OutboxActionStatus,
    ) -> bool:
        state_action = next(
            (
                candidate
                for candidate in projection.state.actions
                if candidate.action_id == action.action_id
            ),
            None,
        )
        if state_action is None or state_action.reservation_id != action.reservation_id:
            raise InvalidCommit()
        if state_action.status.value != action_status.value:
            raise InvalidCommit()
        if (
            action_status is OutboxActionStatus.ACCEPTED
            and projection.state.phase is not AttemptPhase.RUNNING
        ):
            return False
        return not any(
            isinstance(event, (BudgetSettled, BudgetReleased))
            and event.reservation.reservation_id == state_action.reservation_id
            for event in projection.events
        )

    async def list_nonterminal_attempt_ids(self) -> tuple[str, ...]:
        with _repository_boundary():
            await self._ensure_initialized()
            attempt_ids = await asyncio.to_thread(self._list_event_attempt_ids_sync)
            nonterminal: list[str] = []
            for attempt_id in attempt_ids:
                loaded = await self.load(attempt_id)
                if loaded is not None and loaded.state.phase not in _TERMINAL_PHASES:
                    nonterminal.append(attempt_id)
            return tuple(nonterminal)

    def _list_event_attempt_ids_sync(self) -> tuple[str, ...]:
        with self._connection() as connection:
            return tuple(
                row[0]
                for row in connection.execute(
                    "SELECT DISTINCT attempt_id FROM events ORDER BY attempt_id"
                )
            )


__all__ = ["SQLiteAttemptRepository"]
