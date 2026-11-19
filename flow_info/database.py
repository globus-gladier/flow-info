import decimal
import json
import logging
import pathlib
import typing as t
import zlib

from sqlalchemy import (
    LargeBinary,
    String,
    create_engine,
    delete,
    func,
    select,
    type_coerce,
)
from sqlalchemy.dialects.sqlite import insert
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column
from sqlalchemy.types import TypeDecorator

log = logging.getLogger(__name__)


def _json_default(value):
    # ijson (used by flow_info.migration) parses non-integer numbers as Decimal.
    # float() of it matches what json.load would have read from the same text.
    if isinstance(value, decimal.Decimal):
        return float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


class CompressedJSON(TypeDecorator):
    """JSON stored as a zlib-compressed blob. Logs repeat state inputs and outputs in
    every entry, so they compress well."""

    impl = LargeBinary
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        return zlib.compress(
            json.dumps(value, separators=(",", ":"), default=_json_default).encode(), 6
        )

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        return json.loads(zlib.decompress(value))


class Base(DeclarativeBase):
    pass


class Run(Base):
    __tablename__ = "runs"

    run_id: Mapped[str] = mapped_column(String, primary_key=True)
    start_time: Mapped[str] = mapped_column(String, index=True)
    status: Mapped[t.Optional[str]]
    flow_id: Mapped[t.Optional[str]]
    # The full run document from Globus Search.
    data: Mapped[dict] = mapped_column(CompressedJSON)


class RunLog(Base):
    __tablename__ = "run_logs"

    # Not a foreign key to runs: migrated months can have logs for runs that are no
    # longer in the runs file.
    run_id: Mapped[str] = mapped_column(String, primary_key=True)
    fetched_at: Mapped[str]
    # The full response from the Flows run logs API.
    data: Mapped[dict] = mapped_column(CompressedJSON)


class Flow(Base):
    __tablename__ = "flows"

    flow_id: Mapped[str] = mapped_column(String, primary_key=True)
    fetched_at: Mapped[str]
    # The full flow document from the Flows API.
    data: Mapped[dict] = mapped_column(CompressedJSON)


class MonthDB:
    """SQLite database holding one month of flows, runs and run logs for a beamline.

    Flows are a snapshot of the beamline's flows, replaced each time they're updated
    during the month.

    Every row is stored and read on its own, so memory use doesn't grow with the size
    of the month. SQLite's default rollback journal is used, so no -wal/-shm files sit
    next to the database between commands.
    """

    # Rows loaded at a time when iterating over a month.
    YIELD_PER = 500
    # Run logs looked up per query in get_run_logs.
    LOG_LOOKUP_BATCH = 500

    def __init__(self, path: pathlib.Path):
        self.path = pathlib.Path(path)
        self.engine = create_engine(f"sqlite:///{self.path}")
        Base.metadata.create_all(self.engine)

    def close(self):
        self.engine.dispose()

    def save_flows(self, flows: list, fetched_at: str):
        """Replace the stored flows with this snapshot."""
        rows = [{"flow_id": f["id"], "fetched_at": fetched_at, "data": f} for f in flows]
        stmt = insert(Flow)
        stmt = stmt.on_conflict_do_update(
            index_elements=[Flow.flow_id],
            set_={col: stmt.excluded[col] for col in ("fetched_at", "data")},
        )
        with Session(self.engine) as session, session.begin():
            session.execute(delete(Flow))
            if rows:
                session.execute(stmt, rows)

    def get_flows(self) -> list:
        with Session(self.engine) as session:
            return list(session.scalars(select(Flow.data).order_by(Flow.flow_id)))

    def count_flows(self) -> int:
        with Session(self.engine) as session:
            return session.scalar(select(func.count()).select_from(Flow))

    def save_runs(self, runs: list):
        """Insert or update runs. Runs already stored but missing from `runs` are kept,
        so runs that have aged out of Globus Search aren't lost on a refetch."""
        if not runs:
            return
        stmt = insert(Run)
        stmt = stmt.on_conflict_do_update(
            index_elements=[Run.run_id],
            set_={col: stmt.excluded[col] for col in ("start_time", "status", "flow_id", "data")},
        )
        rows = [
            {
                "run_id": r["run_id"],
                "start_time": r["start_time"],
                "status": r.get("status"),
                "flow_id": r.get("flow_id"),
                "data": r,
            }
            for r in runs
        ]
        with Session(self.engine) as session, session.begin():
            session.execute(stmt, rows)

    def save_run_logs(self, logs: list, fetched_at: str):
        """Insert or replace (run_id, log) pairs."""
        if not logs:
            return
        stmt = insert(RunLog)
        stmt = stmt.on_conflict_do_update(
            index_elements=[RunLog.run_id],
            set_={col: stmt.excluded[col] for col in ("fetched_at", "data")},
        )
        rows = [
            {"run_id": run_id, "fetched_at": fetched_at, "data": run_log}
            for run_id, run_log in logs
        ]
        with Session(self.engine) as session, session.begin():
            session.execute(stmt, rows)

    def iter_runs(self):
        """Every run document, oldest first, loaded YIELD_PER rows at a time."""
        stmt = (
            select(Run.data)
            .order_by(Run.start_time, Run.run_id)
            .execution_options(yield_per=self.YIELD_PER)
        )
        with Session(self.engine) as session:
            yield from session.scalars(stmt)

    def get_run_logs(self, run_ids: list):
        """Yield (run_id, run_log) for each id in order, run_log None if not stored.

        Logs are looked up LOG_LOOKUP_BATCH ids per query, but only decompressed as
        they're yielded, so memory holds one batch of compressed logs and one decoded log.
        """
        compressed = CompressedJSON()
        # Select the raw blob, so the column type doesn't decode the whole batch.
        raw_data = type_coerce(RunLog.data, LargeBinary)
        with Session(self.engine) as session:
            for i in range(0, len(run_ids), self.LOG_LOOKUP_BATCH):
                batch = run_ids[i : i + self.LOG_LOOKUP_BATCH]
                found = dict(
                    session.execute(
                        select(RunLog.run_id, raw_data).where(RunLog.run_id.in_(batch))
                    ).all()
                )
                for run_id in batch:
                    blob = found.pop(run_id, None)
                    yield run_id, compressed.process_result_value(blob, None)

    def get_run_log(self, run_id: str):
        with Session(self.engine) as session:
            return session.scalar(select(RunLog.data).where(RunLog.run_id == run_id))

    def _missing_logs_query(self, *columns):
        return (
            select(*columns)
            .outerjoin(RunLog, RunLog.run_id == Run.run_id)
            .where(RunLog.run_id.is_(None))
        )

    def runs_missing_logs(self) -> list:
        """(run_id, start_time) for every run without a stored log, oldest first."""
        stmt = self._missing_logs_query(Run.run_id, Run.start_time).order_by(
            Run.start_time, Run.run_id
        )
        with Session(self.engine) as session:
            return [tuple(row) for row in session.execute(stmt)]

    def count_runs(self) -> int:
        with Session(self.engine) as session:
            return session.scalar(select(func.count()).select_from(Run))

    def count_run_logs(self) -> int:
        with Session(self.engine) as session:
            return session.scalar(select(func.count()).select_from(RunLog))

    def count_missing_logs(self) -> int:
        with Session(self.engine) as session:
            return session.scalar(
                self._missing_logs_query(func.count()).select_from(Run)
            )
