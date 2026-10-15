"""Orders in every state, newest first: `orders_recent` and `orders_market_recent`.

`GET /trading/orders?scope=all` pages the `orders` table by `(created_ts,
venue, id)` descending, optionally for one market. Every row written by the
journal has a `created_ts`; a row from before that guarantee takes its
`updated_ts`, the closest time on record, so it sorts and pages like the rest.
"""
from alembic import op
import sqlalchemy as sa

revision = "0004_orders_recent"
down_revision = "0003_bucket_member_enabled"
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()
    connection.execute(sa.text("UPDATE orders SET created_ts = updated_ts WHERE created_ts IS NULL"))
    connection.execute(sa.text("CREATE INDEX IF NOT EXISTS orders_recent ON orders(created_ts, venue, id)"))
    connection.execute(sa.text(
        "CREATE INDEX IF NOT EXISTS orders_market_recent ON orders(market_id, created_ts, venue, id)"))
    connection.execute(sa.text("UPDATE meta SET value='9' WHERE key='schema_version'"))


def downgrade():
    connection = op.get_bind()
    connection.execute(sa.text("DROP INDEX IF EXISTS orders_market_recent"))
    connection.execute(sa.text("DROP INDEX IF EXISTS orders_recent"))
    connection.execute(sa.text("UPDATE meta SET value='8' WHERE key='schema_version'"))
