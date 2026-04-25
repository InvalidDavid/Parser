#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

import aiosqlite


SCHEMA = """
PRAGMA foreign_keys = ON;
PRAGMA synchronous = NORMAL;

DROP TABLE IF EXISTS source_checks;
DROP TABLE IF EXISTS source_domains;
DROP TABLE IF EXISTS sources;
DROP TABLE IF EXISTS summary;
DROP TABLE IF EXISTS metadata;

CREATE TABLE metadata (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE summary (
  key TEXT PRIMARY KEY,
  value INTEGER NOT NULL
);

CREATE TABLE sources (
  id TEXT PRIMARY KEY,
  key TEXT NOT NULL,
  title TEXT NOT NULL,
  language TEXT NOT NULL,
  language_name TEXT NOT NULL,
  engine TEXT,
  content_type TEXT,
  broken INTEGER NOT NULL DEFAULT 0,
  broken_reason TEXT,
  nsfw INTEGER NOT NULL DEFAULT 0,
  path TEXT NOT NULL,
  repo_url TEXT NOT NULL,
  raw_url TEXT NOT NULL,
  search_text TEXT NOT NULL,

  health_status TEXT NOT NULL,
  health_reason TEXT,
  checked_at TEXT,
  latency_ms INTEGER,
  http_status INTEGER,
  final_url TEXT,
  result_count INTEGER,
  health_details TEXT
);

CREATE TABLE source_domains (
  source_id TEXT NOT NULL,
  domain TEXT NOT NULL,
  position INTEGER NOT NULL,
  PRIMARY KEY (source_id, domain),
  FOREIGN KEY (source_id) REFERENCES sources(id) ON DELETE CASCADE
);

CREATE TABLE source_checks (
  source_id TEXT NOT NULL,
  check_name TEXT NOT NULL,
  status TEXT NOT NULL,
  reason TEXT,
  latency_ms INTEGER,
  count INTEGER,
  details TEXT,
  PRIMARY KEY (source_id, check_name),
  FOREIGN KEY (source_id) REFERENCES sources(id) ON DELETE CASCADE
);

CREATE INDEX idx_sources_title ON sources(title);
CREATE INDEX idx_sources_key ON sources(key);
CREATE INDEX idx_sources_language ON sources(language);
CREATE INDEX idx_sources_status ON sources(health_status);
CREATE INDEX idx_sources_content_type ON sources(content_type);
CREATE INDEX idx_sources_nsfw ON sources(nsfw);
CREATE INDEX idx_sources_search_text ON sources(search_text);
CREATE INDEX idx_domains_domain ON source_domains(domain);
CREATE INDEX idx_checks_status ON source_checks(status);
"""


def clean_int(value: Any) -> int | None:
    if value is None:
        return None

    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def clean_bool(value: Any) -> int:
    return 1 if bool(value) else 0


def clean_text(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    return text or None


def clean_required_text(value: Any, fallback: str = "") -> str:
    text = clean_text(value)
    return text if text is not None else fallback


async def insert_metadata(db: aiosqlite.Connection, payload: dict[str, Any]) -> None:
    source_repo = payload.get("sourceRepo") or {}

    rows = [
        ("generatedAt", clean_required_text(payload.get("generatedAt"))),
        ("generatedBy", clean_required_text(payload.get("generatedBy"))),
        ("disclaimer", clean_required_text(payload.get("disclaimer"))),
        ("sourceOwner", clean_required_text(source_repo.get("owner"))),
        ("sourceRepo", clean_required_text(source_repo.get("repo"))),
        ("sourceBranch", clean_required_text(source_repo.get("branch"))),
    ]

    await db.executemany(
        "INSERT INTO metadata(key, value) VALUES (?, ?)",
        rows,
    )


async def insert_summary(db: aiosqlite.Connection, payload: dict[str, Any]) -> None:
    summary = payload.get("summary") or {}

    rows = [
        (str(key), clean_int(value) or 0)
        for key, value in summary.items()
    ]

    await db.executemany(
        "INSERT INTO summary(key, value) VALUES (?, ?)",
        rows,
    )


async def insert_source(db: aiosqlite.Connection, source: dict[str, Any]) -> str:
    health = source.get("health") or {}
    source_id = clean_required_text(source.get("id") or source.get("key"))

    await db.execute(
        """
        INSERT INTO sources (
          id,
          key,
          title,
          language,
          language_name,
          engine,
          content_type,
          broken,
          broken_reason,
          nsfw,
          path,
          repo_url,
          raw_url,
          search_text,
          health_status,
          health_reason,
          checked_at,
          latency_ms,
          http_status,
          final_url,
          result_count,
          health_details
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            source_id,
            clean_required_text(source.get("key")),
            clean_required_text(source.get("title")),
            clean_required_text(source.get("language"), "multi"),
            clean_required_text(source.get("languageName")),
            clean_text(source.get("engine")),
            clean_text(source.get("contentType")),
            clean_bool(source.get("broken")),
            clean_text(source.get("brokenReason")),
            clean_bool(source.get("nsfw")),
            clean_required_text(source.get("path")),
            clean_required_text(source.get("repoUrl")),
            clean_required_text(source.get("rawUrl")),
            clean_required_text(source.get("searchText")),
            clean_required_text(health.get("status"), "unknown"),
            clean_text(health.get("reason")),
            clean_text(health.get("checkedAt")),
            clean_int(health.get("latencyMs")),
            clean_int(health.get("httpStatus")),
            clean_text(health.get("finalUrl")),
            clean_int(health.get("resultCount")),
            clean_text(health.get("details")),
        ),
    )

    return source_id


async def insert_domains(
    db: aiosqlite.Connection,
    source_id: str,
    source: dict[str, Any],
) -> None:
    domains = source.get("domains") or []

    rows = [
        (source_id, str(domain).strip().lower(), index)
        for index, domain in enumerate(domains)
        if str(domain).strip()
    ]

    await db.executemany(
        """
        INSERT OR IGNORE INTO source_domains(source_id, domain, position)
        VALUES (?, ?, ?)
        """,
        rows,
    )


async def insert_checks(
    db: aiosqlite.Connection,
    source_id: str,
    source: dict[str, Any],
) -> None:
    health = source.get("health") or {}
    checks = health.get("checks") or {}

    if not isinstance(checks, dict):
        return

    rows = []

    for check_name, check in checks.items():
        if not isinstance(check, dict):
            continue

        rows.append(
            (
                source_id,
                str(check_name),
                clean_required_text(check.get("status"), "unknown"),
                clean_text(check.get("reason")),
                clean_int(check.get("latencyMs")),
                clean_int(check.get("count")),
                clean_text(check.get("details")),
            )
        )

    await db.executemany(
        """
        INSERT INTO source_checks (
          source_id,
          check_name,
          status,
          reason,
          latency_ms,
          count,
          details
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )


async def write_database(input_path: Path, output_path: Path) -> None:
    payload = json.loads(input_path.read_text(encoding="utf-8"))

    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists():
        output_path.unlink()

    async with aiosqlite.connect(output_path) as db:
        await db.executescript(SCHEMA)

        await insert_metadata(db, payload)
        await insert_summary(db, payload)

        sources = payload.get("sources") or []

        for source in sources:
            if not isinstance(source, dict):
                continue

            source_id = await insert_source(db, source)
            await insert_domains(db, source_id, source)
            await insert_checks(db, source_id, source)

        await db.execute("PRAGMA optimize")
        await db.commit()

    print(f"wrote {output_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert generated sources.json into a SQLite catalog database."
    )
    parser.add_argument(
        "--input",
        default="public/data/sources.json",
        help="Input sources.json path.",
    )
    parser.add_argument(
        "--output",
        default="public/data/sources.sqlite",
        help="Output SQLite database path.",
    )
    return parser.parse_args()


async def async_main() -> None:
    args = parse_args()
    await write_database(
        input_path=Path(args.input),
        output_path=Path(args.output),
    )


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
