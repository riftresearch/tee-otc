Replica-targeted SQL migrations for `sauron`.

Use this directory for logical-subscriber / replica objects that do not belong in
the primary OTC schema migration stream, such as:

- `pg_notify(...)` trigger functions
- replica-side row triggers enabled for logical replication apply
- replica-local views or helper objects used only by Sauron

The Rust migrator for this directory lives in `src/replica_db.rs`.
