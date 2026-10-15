# Journal migrations

Alembic revisions run through the engine's locked SQLAlchemy connection. Current
head is `0004_orders_recent`, logical schema version 9. Configuration, ownership
compatibility and backup/restore behavior live in the [engine README](../README.md).

- [0001 journal baseline](versions/0001_journal_v6.py): frozen version-6 schema.
- [0002 single user](versions/0002_single_user.py): owner, foreign keys and preserved history.
- [0003 bucket member enabled](versions/0003_bucket_member_enabled.py): `bucket_members.enabled`, existing members on.
- [0004 orders recent](versions/0004_orders_recent.py): `orders_recent` and `orders_market_recent` indexes for listing every order newest first; undated rows take `updated_ts`.

Add a reviewed revision for schema/data changes; never edit deployed revisions or
import mutable runtime definitions into them. Verify both databases and rollback
of failures. Unrestricted autogeneration is not supported by the partial runtime
metadata. Migration execution requires the application's owner/locking context.
