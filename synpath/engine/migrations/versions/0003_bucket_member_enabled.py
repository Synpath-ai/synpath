"""A bucket member can be switched off: `bucket_members.enabled`, on by default."""
from alembic import op
import sqlalchemy as sa

revision = "0003_bucket_member_enabled"
down_revision = "0002_single_user"
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()
    columns = {column["name"] for column in sa.inspect(connection).get_columns("bucket_members")}
    if "enabled" not in columns:
        # Every member stored before this revision was routed, so it stays on.
        op.add_column("bucket_members", sa.Column("enabled", sa.Integer, nullable=False, server_default="1"))
    connection.execute(sa.text("UPDATE meta SET value='8' WHERE key='schema_version'"))


def downgrade():
    with op.batch_alter_table("bucket_members") as batch:
        batch.drop_column("enabled")
    op.get_bind().execute(sa.text("UPDATE meta SET value='7' WHERE key='schema_version'"))
