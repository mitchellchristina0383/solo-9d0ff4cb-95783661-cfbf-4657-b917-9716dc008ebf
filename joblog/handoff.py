"""Incremental log handoff for remote audit consumers.

A remote audit endpoint holds a backup and needs every log committed *after*
that backup.  This module implements the handoff protocol:

Establishment
    ``establish(consumer_id, backup_id)`` verifies the backup (manifest,
    SHA-256, SQLite integrity) and only then records the handoff, anchored at
    the backup's last log id (``snapshot_max_id``, or 0 for an empty backup).
    Retrying with the same consumer identifier returns the original anchor
    unchanged; the same identifier with a different backup is a conflict.  A
    backup that does not verify never creates a handoff.

Batched claiming
    ``claim`` delivers the next logs in id order together with a batch
    credential (token).  The batch boundary is fixed at claim time: logs
    appended while a batch is open are not part of it and are delivered by
    the next batch.  While a batch is unconfirmed, ``claim`` re-serves the
    same token and the same content (the range is re-read from the
    append-only log, so it cannot drift).

Confirmation
    Only the token of the currently open batch advances the durable progress
    cursor ``next_id``.  A late, repeated or foreign token is rejected and
    changes nothing.  All state transitions run inside a single
    ``BEGIN IMMEDIATE`` transaction, so concurrent claims/confirmations
    serialise and can never skip a log, and a restart simply resumes the
    persisted state.
"""

from __future__ import annotations

import secrets
import sqlite3
from typing import Any

from .backup import BackupManager, utc_now
from .db import Database

__all__ = [
    "HandoffError",
    "HandoffNotFound",
    "HandoffConflict",
    "StaleBatchError",
    "HandoffManager",
]


class HandoffError(Exception):
    """Base class for expected handoff failures."""


class HandoffNotFound(HandoffError):
    """No handoff exists for the consumer identifier."""


class HandoffConflict(HandoffError):
    """The consumer identifier is already bound to a different backup."""


class StaleBatchError(HandoffError):
    """The confirmation token is not the token of the currently open batch."""


class HandoffManager:
    def __init__(self, database: Database, backups: BackupManager):
        self.database = database
        self.backups = backups

    # ------------------------------------------------------------------ views
    @staticmethod
    def _view(row: sqlite3.Row) -> dict[str, Any]:
        open_batch = None
        if row["batch_token"] is not None:
            open_batch = {
                "batch_token": row["batch_token"],
                "from_id": row["batch_from_id"],
                "to_id": row["batch_to_id"],
            }
        return {
            "consumer_id": row["consumer_id"],
            "backup_id": row["backup_id"],
            "start_after_id": row["start_after_id"],
            "next_id": row["next_id"],
            "open_batch": open_batch,
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    @staticmethod
    def _fetch(conn: sqlite3.Connection, consumer_id: str) -> sqlite3.Row | None:
        return conn.execute(
            "SELECT * FROM handoffs WHERE consumer_id = ?", (consumer_id,)
        ).fetchone()

    # ----------------------------------------------------------- establishment
    def establish(self, consumer_id: str, backup_id: str) -> tuple[dict[str, Any], bool]:
        """Create the handoff for *consumer_id* anchored at *backup_id*.

        Returns ``(view, created)``.  A retry with the same backup returns the
        persisted view (original anchor, current progress) with
        ``created == False``; a different backup raises :class:`HandoffConflict`.
        A backup that does not verify raises and records nothing.
        """
        with self.database.reader() as conn:
            existing = self._fetch(conn, consumer_id)
        if existing is not None:
            if existing["backup_id"] != backup_id:
                raise HandoffConflict(
                    f"consumer {consumer_id!r} is bound to backup "
                    f"{existing['backup_id']!r}, not {backup_id!r}"
                )
            return self._view(existing), False

        # Only a backup that verifies right now may anchor a new handoff.
        # Raises BackupNotFound / BackupVerificationError otherwise.
        info = self.backups.verify(backup_id)
        start_after_id = int(info["snapshot_max_id"] or 0)

        with self.database.writer() as conn:
            # Re-check under the write lock: a concurrent establish for the
            # same consumer serialises here and must not be overwritten.
            existing = self._fetch(conn, consumer_id)
            if existing is not None:
                if existing["backup_id"] != backup_id:
                    raise HandoffConflict(
                        f"consumer {consumer_id!r} is bound to backup "
                        f"{existing['backup_id']!r}, not {backup_id!r}"
                    )
                return self._view(existing), False
            now = utc_now()
            conn.execute(
                "INSERT INTO handoffs"
                " (consumer_id, backup_id, start_after_id, next_id, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (consumer_id, backup_id, start_after_id, start_after_id + 1, now, now),
            )
            row = self._fetch(conn, consumer_id)
        assert row is not None
        return self._view(row), True

    # ---------------------------------------------------------------- reading
    def get(self, consumer_id: str) -> dict[str, Any]:
        with self.database.reader() as conn:
            row = self._fetch(conn, consumer_id)
        if row is None:
            raise HandoffNotFound(f"no handoff for consumer {consumer_id!r}")
        return self._view(row)

    # --------------------------------------------------------------- claiming
    def claim(self, consumer_id: str, limit: int) -> dict[str, Any]:
        """Claim the next batch, or re-serve the open unconfirmed batch.

        The batch is an inclusive id range fixed at claim time; appends that
        land while the batch is open are delivered by the next batch.
        """
        with self.database.writer() as conn:
            row = self._fetch(conn, consumer_id)
            if row is None:
                raise HandoffNotFound(f"no handoff for consumer {consumer_id!r}")

            if row["batch_token"] is not None:
                # Unconfirmed batch: re-serve identical content.  The log is
                # append-only, so re-reading the stored range cannot drift.
                items = conn.execute(
                    "SELECT id, job_id, message, created_at FROM job_logs"
                    " WHERE id BETWEEN ? AND ? ORDER BY id",
                    (row["batch_from_id"], row["batch_to_id"]),
                ).fetchall()
                return {
                    "consumer_id": consumer_id,
                    "batch_token": row["batch_token"],
                    "from_id": row["batch_from_id"],
                    "to_id": row["batch_to_id"],
                    "items": [dict(r) for r in items],
                }

            items = conn.execute(
                "SELECT id, job_id, message, created_at FROM job_logs"
                " WHERE id >= ? ORDER BY id LIMIT ?",
                (row["next_id"], limit),
            ).fetchall()
            if not items:
                # Nothing new: no batch is opened, nothing to confirm.
                return {
                    "consumer_id": consumer_id,
                    "batch_token": None,
                    "from_id": None,
                    "to_id": None,
                    "items": [],
                }

            token = secrets.token_urlsafe(16)
            from_id = int(items[0]["id"])
            to_id = int(items[-1]["id"])
            conn.execute(
                "UPDATE handoffs SET batch_token=?, batch_from_id=?, batch_to_id=?,"
                " updated_at=? WHERE consumer_id=?",
                (token, from_id, to_id, utc_now(), consumer_id),
            )
            return {
                "consumer_id": consumer_id,
                "batch_token": token,
                "from_id": from_id,
                "to_id": to_id,
                "items": [dict(r) for r in items],
            }

    # ------------------------------------------------------------- confirming
    def confirm(self, consumer_id: str, batch_token: str) -> dict[str, Any]:
        """Advance progress past the open batch if *batch_token* matches.

        Any other token — late, repeated or foreign — raises
        :class:`StaleBatchError` and leaves the persisted state untouched.
        """
        with self.database.writer() as conn:
            row = self._fetch(conn, consumer_id)
            if row is None:
                raise HandoffNotFound(f"no handoff for consumer {consumer_id!r}")
            if row["batch_token"] is None:
                raise StaleBatchError("no open batch to confirm")
            if not secrets.compare_digest(row["batch_token"], batch_token):
                raise StaleBatchError(
                    "batch token does not match the currently open batch"
                )
            conn.execute(
                "UPDATE handoffs SET next_id=?, batch_token=NULL, batch_from_id=NULL,"
                " batch_to_id=NULL, updated_at=? WHERE consumer_id=?",
                (int(row["batch_to_id"]) + 1, utc_now(), consumer_id),
            )
            updated = self._fetch(conn, consumer_id)
        assert updated is not None
        return self._view(updated)
