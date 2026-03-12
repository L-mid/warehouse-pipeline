from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, LiteralString, cast

from psycopg import Connection

from warehouse_pipeline.db.sql_runner import run_sql_file

DEFAULT_PUBLISH_SQL_DIR = Path(__file__).resolve().parents[3] / "sql" / "publish"
DEFAULT_METRICS_SQL_DIR = DEFAULT_PUBLISH_SQL_DIR / "metrics"
# Only one for now
DEFAULT_VIEWS_FILE_NAME = "900_views.sql"


@dataclass(frozen=True)
class PublishResult:
    """
    Summary of what happened when applying the publish view.
    """

    files_ran: tuple[str, ...]
    metrics_available: tuple[str, ...]


@dataclass(frozen=True)
class MetricQueryResult:
    """
    Results of a single named metric SQL query.
    """

    name: str
    columns: tuple[str, ...]
    rows: tuple[dict[str, Any], ...]


def _resolve_publish_dir(sql_dir: Path | None = None) -> Path:
    """Resolve and validate the publish SQL directory."""
    resolved = (sql_dir or DEFAULT_PUBLISH_SQL_DIR).resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"publish SQL dir does not exist: {resolved}")
    if not resolved.is_dir():
        raise NotADirectoryError(f"publish SQL path is not a directory: {resolved}")

    return resolved


def _resolve_views_file(sql_dir: Path | None = None) -> Path:
    """Resolve only the main publish views SQL file. For just running this view specifically."""
    publish_dir = _resolve_publish_dir(sql_dir)
    path = publish_dir / DEFAULT_VIEWS_FILE_NAME
    if not path.exists():
        raise FileNotFoundError(f"publish views SQL file does not exist: {path}")
    return path


def _resolve_metrics_dir(metrics_dir: Path | None = None) -> Path:
    """Resolves and validates the metric query SQL directory."""
    resolved = (metrics_dir or DEFAULT_METRICS_SQL_DIR).resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"publish metrics dir does not exist: {resolved}")
    if not resolved.is_dir():
        raise NotADirectoryError(f"publish metrics path is not a directory: {resolved}")

    return resolved


def _metric_files(metrics_dir: Path | None = None) -> tuple[Path, ...]:
    """Return all `.sql` metric files using for views in ASC order."""
    directory = _resolve_metrics_dir(metrics_dir)
    return tuple(sorted(directory.glob("*.sql")))


def _resolve_metric_path(name: str, metrics_dir: Path | None = None) -> Path:
    """
    Resolve a metric query by stem OR filename.
    """
    directory = _resolve_metrics_dir(metrics_dir)

    # `010_revenue_by_day_country` or else try `010_revenue_by_day_country.sql`
    normalized = name if name.endswith(".sql") else f"{name}.sql"
    path = directory / normalized

    if not path.exists():
        available = ", ".join(p.stem for p in _metric_files(directory))
        raise FileNotFoundError(
            f"metric SQL file not found: {normalized}. Available metrics: {available}"
        )

    return path


def list_metric_queries(metrics_dir: Path | None = None) -> tuple[str, ...]:
    """Return a tuple containing every available metric query name found by stem."""
    return tuple(path.stem for path in _metric_files(metrics_dir))


def apply_views(conn: Connection, *, sql_dir: Path | None = None) -> PublishResult:
    """
    Creates and applies the SQL view layer from the `sql/publish/900_views.sql`.
    Returns the result of what happened in `PublishResult`.
    """
    views_file = _resolve_views_file(sql_dir)
    run_sql_file(conn, views_file)

    return PublishResult(
        files_ran=(views_file.name,),
        metrics_available=list_metric_queries(),
    )


def run_metric_query(
    conn: Connection,
    *,
    name: str,
    metrics_dir: Path | None = None,
) -> MetricQueryResult:
    """
    Executes one named metric query from `sql/publish/metrics/*.sql`.

    Returns rows as a tuple of dicts keyed by the column name.
    """
    path = _resolve_metric_path(name, metrics_dir)
    sql_text = path.read_text(encoding="utf-8")

    with conn.cursor() as cur:
        sql_text = cast(LiteralString, sql_text)
        cur.execute(sql_text)
        fetched_rows = cur.fetchall()

        if cur.description is None:
            # tuple
            columns: tuple[str, ...] = ()
            rows: tuple[dict[str, Any], ...] = ()
        else:
            columns = tuple(desc.name for desc in cur.description)
            rows = tuple(dict(zip(columns, row, strict=True)) for row in fetched_rows)

    return MetricQueryResult(
        name=path.stem,
        columns=columns,
        rows=rows,
    )
