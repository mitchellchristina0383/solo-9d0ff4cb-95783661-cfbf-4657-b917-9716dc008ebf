# Job Log Service

FastAPI + SQLite service for an append-only job log with crash-safe backup and
verified restore.  All key flows are usable from the interactive docs at
`/docs`.

## Run

```bash
docker compose up --build
```

The API listens on `http://localhost:8000` (`JOBLOG_PORT` overrides the port),
docs at `http://localhost:8000/docs`.  State lives in the `joblog-data` volume:
source database `/data/joblogs/source.db`, backups `/data/backups`, restored
targets `/data/restores`.

Without Docker (Python 3.11+):

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
JOBLOG_DATA_DIR=./data uvicorn joblog.main:app --port 8000
```

## API

| Method | Path | Purpose |
| ------ | ---- | ------- |
| `POST` | `/logs` | Append a log entry; returns only after the commit is durable |
| `GET`  | `/logs` | Query committed logs (`job_id`, `after_id`, `limit`) |
| `POST` | `/backups` | Create, verify and publish a backup; returns its backup id |
| `GET`  | `/backups` | List published backups |
| `GET`  | `/backups/{id}` | Get one published backup (id, SHA-256s, snapshot row count) |
| `GET`  | `/backups/{id}/verify` | Re-check manifest, SHA-256 and SQLite integrity |
| `GET`  | `/backups/{id}/download` | Download the verified `.db` file |
| `POST` | `/backups/{id}/restore` | Restore into an independent target (`target_name`, `overwrite`) |
| `GET`  | `/restores/{target_name}/logs` | Read logs from a restored target (read-only) |
| `POST` | `/handoffs` | Establish an incremental handoff (`consumer_id`, `backup_id`) |
| `GET`  | `/handoffs/{consumer_id}` | Handoff state: anchor, progress cursor, open batch |
| `POST` | `/handoffs/{consumer_id}/claim` | Claim the next batch (`limit`); idempotent while unconfirmed |
| `POST` | `/handoffs/{consumer_id}/confirm` | Confirm the open batch (`batch_token`) and advance progress |
| `GET`  | `/health` | Liveness/readiness |

### Typical flow (`/docs` order)

1. `POST /logs` a few entries.
2. `POST /backups` — the response contains the published `id`, the database
   SHA-256 and the manifest SHA-256.
3. `POST /logs` more entries (they must not appear in the backup).
4. `GET /backups/{id}/verify` — confirms the manifest still matches.
5. `POST /backups/{id}/restore` with `{"target_name": "incident-2026.db"}`.
6. `GET /restores/incident-2026.db/logs` — shows the logs committed before the
   backup and none appended afterwards.

### Incremental audit handoff (`/docs` order)

A remote audit endpoint that already holds backup `bkp-...` collects every log
committed after it:

1. `POST /handoffs` with `{"consumer_id": "auditor-eu", "backup_id": "bkp-..."}`.
   The backup is verified first; the response anchors at the backup's last log
   id (`start_after_id`, `0` for an empty backup). Retrying with the same
   identifiers returns the original anchor (`200`); the same `consumer_id`
   with a different backup is a `409` conflict.
2. `POST /handoffs/auditor-eu/claim?limit=100` — returns the next logs in id
   order plus a `batch_token`. Logs appended while the batch is open are left
   for the next batch. Re-claiming before confirming returns the same token
   and content.
3. `POST /handoffs/auditor-eu/confirm` with `{"batch_token": "..."}` — only
   the token of the currently open batch advances the progress cursor; a late
   or foreign token is rejected (`409`) and changes nothing.
4. Repeat claim → confirm until `claim` returns an empty `items` list (no
   token is issued for an empty batch).

Progress is stored in the source database, so a service restart simply
resumes: an unconfirmed batch is re-served identically and confirmed batches
are never re-delivered.

## Guarantees and how they are achieved

* **Consistent snapshot under concurrent appends.**  A backup opens a
  read-only connection, starts a read transaction (`BEGIN` + a `SELECT`, which
  pins one committed WAL snapshot) and copies with SQLite's online-backup API.
  Writers keep appending on `BEGIN IMMEDIATE` transactions; rows committed
  after the snapshot are excluded, rows committed before are included.  The
  copy's `COUNT(*)`/`MAX(id)` are cross-checked against the frozen snapshot.
* **Backup id published only after verification.**  The copy is staged under
  `backups/.staging/`, checked with `PRAGMA integrity_check`, a
  `manifest.json` with SHA-256 digests is written, and the whole directory is
  re-verified from disk.  Only then is it atomically renamed to
  `backups/<id>` and the database row flipped to `ready`.  The API only ever
  exposes `ready` backups.
* **Interruption / restart safety.**  A backup row is inserted as `staging`
  before any copying.  Startup runs `recover_incomplete()`: staging directories
  are deleted, non-`ready` rows become `failed`, and `ready` rows whose files
  vanished are demoted to `failed`.  A partial or interrupted backup can never
  be listed, downloaded or restored.
* **Published backups are immutable.**  Backup files are made read-only
  (`0444`), stored outside the source database, and never touched by later
  appends.  The source is a normal WAL database and stays writable.
* **Verified restore.**  `POST /backups/{id}/restore` re-verifies the manifest
  hash, the database hash and `integrity_check` *before* touching the target.
  The restored copy is prepared and hashed in a private temp directory, then
  renamed into place.  An existing target (when `overwrite=true`) is moved
  aside first and put back on any failure, so the previous target survives
  bit-for-bit.
* **Independent target.**  Restore writes a separate database file under
  `/data/restores`; later appends to the source and later writes to the target
  are isolated from each other.  The restore endpoint never opens the source
  for writing.
* **Verified handoff anchor.**  `POST /handoffs` verifies the referenced
  backup (manifest, SHA-256, `integrity_check`) before recording anything; a
  corrupt backup establishes no handoff.  The anchor is the verified backup's
  `snapshot_max_id` (`0` when empty) and is immutable afterwards.
* **Ordered, confirmed delivery.**  Batches are claimed and confirmed inside
  single `BEGIN IMMEDIATE` transactions, so concurrent claims serialise and
  see the same open batch, and only the current batch token can move the
  progress cursor.  The batch's id range is fixed at claim time and re-read
  from the append-only log on re-claim, so content cannot drift and no log
  can be skipped or delivered twice.  Failed operations (stale token,
  conflicting backup, corrupt backup) roll back and leave the persisted
  progress untouched.

## Tests

```bash
pip install -r requirements.txt pytest httpx
python -m pytest
```

The suite covers concurrent appends during a backup, frozen-snapshot
behaviour, immutable published backups, independent restore, corrupt-backup
restore with the target preserved, interrupted backups after restart,
`ready`-row demotion, backup failure, restart persistence and `/docs`.  The
handoff suite covers anchoring at the verified backup end (empty backups
start at 0), idempotent establish and backup conflicts, corrupt-backup
rejection, in-order claim/re-claim/confirm with appends landing in the next
batch, stale-token rejection, concurrent claims and confirmations, restart
resumption, and failure operations leaving progress untouched.

## Notes / limits

* The service is a single process.  `recover_incomplete()` assumes it is the
  only writer to the backup directory at startup; do not run several API
  instances against the same `/data` volume.
* Backup files are hash-checked on every verify/download/restore, which is
  O(backup size).  Fine for job-log volumes; for very large databases this
  should be cached.
* Restore takes an explicit `target_name` (a filename under `/data/restores`);
  absolute host paths are deliberately not accepted through the API.
