"""Seed one owner and reference it from all authoritative instructions."""
import json
from alembic import op, context
import sqlalchemy as sa
# Revision definitions are frozen; future runtime models must not rewrite history.
metadata = sa.MetaData()
users = sa.Table("users", metadata,
    sa.Column("user_id", sa.Text, primary_key=True, nullable=False),
    sa.Column("singleton", sa.Integer, nullable=False, server_default="1", unique=True),
    sa.CheckConstraint("singleton = 1", name="users_single_owner"),
    sa.CheckConstraint("length(trim(user_id)) > 0", name="users_nonempty_id"))
historical_order_records = sa.Table("historical_order_records", metadata,
    sa.Column("kind", sa.Text, primary_key=True),
    sa.Column("venue", sa.Text, primary_key=True),
    sa.Column("id", sa.Text, primary_key=True),
    sa.Column("user_id", sa.Text, nullable=False),
    sa.Column("record", sa.Text, nullable=False))

revision = "0002_single_user"
down_revision = "0001_journal_v6"
branch_labels = None
depends_on = None
ORDER_TABLES = {"market": "market_orders", "limit": "limit_orders", "stop_market": "stop_market_orders", "stop_limit": "stop_limit_orders"}


def upgrade():
    connection = op.get_bind()
    owner = context.config.attributes["user_id"]
    _check_instruction_ownership(connection, owner)
    users.create(connection)
    historical_order_records.create(connection)
    connection.execute(users.insert().values(user_id=owner))
    for kind, table_name in ORDER_TABLES.items():
        table = sa.Table(table_name, sa.MetaData(), autoload_with=connection)
        _preserve_observations(connection, table, kind, owner)
        # Named reflected checks are retained when SQLite rebuilds the table.
        checks = sorted((constraint for constraint in table.constraints if isinstance(constraint, sa.CheckConstraint)),
                        key=lambda constraint: str(constraint.sqltext))
        for index, constraint in enumerate(checks):
            if constraint.name is None:
                constraint.name = f"{table_name}_check_{index}"
        with op.batch_alter_table(table_name, copy_from=table) as batch:
            batch.create_foreign_key(f"{table_name}_user", "users", ["user_id"], ["user_id"])
    submissions = sa.Table("submissions", sa.MetaData(), autoload_with=connection)
    with op.batch_alter_table("submissions", copy_from=submissions) as batch:
        batch.create_foreign_key("submissions_user", "users", ["user_id"], ["user_id"])
    connection.execute(sa.text("UPDATE meta SET value='7' WHERE key='schema_version'"))


def _check_instruction_ownership(connection, owner):
    for table_name in (*ORDER_TABLES.values(), "submissions"):
        table = sa.Table(table_name, sa.MetaData(), autoload_with=connection)
        query = sa.select(table.c.user_id).where(table.c.user_id != owner)
        if table_name != "submissions":
            query = query.where(table.c.request.is_not(None))
        if connection.execute(query.limit(1)).first():
            raise ValueError("existing instruction ownership conflicts with configured user_id; explicit ownership review required")


def _preserve_observations(connection, table, kind, owner):
    rows = connection.execute(sa.select(table).where(table.c.user_id != owner, table.c.request.is_(None))).mappings().all()
    for row in rows:
        connection.execute(historical_order_records.insert().values(kind=kind, venue=row["venue"], id=row["id"], user_id=row["user_id"], record=json.dumps(dict(row))))
        connection.execute(table.delete().where(table.c.venue == row["venue"], table.c.id == row["id"]))


def downgrade():
    raise RuntimeError("owner migration downgrade requires an explicit backup/restore plan")
