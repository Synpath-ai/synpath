"""Fair values are gone: drop the `fair_values` table.

A position is marked from the live book by the engine (`Engine.marks`, the
price it could be closed at), never from a stored value, so nothing reads or
writes this table any more. Downgrade recreates it empty.
"""
from alembic import op
import sqlalchemy as sa

revision = "0005_drop_fair_values"
down_revision = "0004_orders_recent"
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()
    connection.execute(sa.text("DROP TABLE IF EXISTS fair_values"))
    connection.execute(sa.text("UPDATE meta SET value='10' WHERE key='schema_version'"))


def downgrade():
    connection = op.get_bind()
    connection.execute(sa.text(
        "CREATE TABLE IF NOT EXISTS fair_values (account_key TEXT NOT NULL, market_id TEXT NOT NULL, "
        "value TEXT NOT NULL, source TEXT, ts INTEGER NOT NULL, PRIMARY KEY (account_key, market_id))"))
    connection.execute(sa.text("UPDATE meta SET value='9' WHERE key='schema_version'"))
