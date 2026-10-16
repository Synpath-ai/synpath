"""SQLAlchemy schema objects for execution ownership and preserved history."""
from sqlalchemy import Column, Integer, Text, Table, MetaData, CheckConstraint

metadata = MetaData()
users = Table(
    "users", metadata,
    Column("user_id", Text, primary_key=True, nullable=False),
    Column("singleton", Integer, nullable=False, server_default="1", unique=True),
    CheckConstraint("singleton = 1", name="users_single_owner"),
    CheckConstraint("length(trim(user_id)) > 0", name="users_nonempty_id"),
)
historical_order_records = Table(
    "historical_order_records", metadata,
    Column("kind", Text, primary_key=True),
    Column("venue", Text, primary_key=True),
    Column("id", Text, primary_key=True),
    Column("user_id", Text, nullable=False),
    Column("record", Text, nullable=False),
)
