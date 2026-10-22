"""Application-managed migrations require its selected connection and owner."""
from alembic import context
from synpath.engine.models import metadata

configuration = context.config
connection = configuration.attributes.get("connection")
if connection is None or context.is_offline_mode():
    raise RuntimeError("journal migrations require the application's locked connection and configured user_id")
context.configure(connection=connection, target_metadata=metadata, transactional_ddl=True)
with context.begin_transaction():
    context.run_migrations()
