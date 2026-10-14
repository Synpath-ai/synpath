"""Run Alembic on the journal's existing, locked SQLAlchemy transaction."""
from pathlib import Path
from alembic import command
from alembic.config import Config
from sqlalchemy import select
from .models import users
from .database import finish_database_operation


async def upgrade(database, user_id):
    def run(connection):
        configuration = Config()
        configuration.set_main_option("script_location", str(Path(__file__).parent / "migrations"))
        configuration.attributes.update(connection=connection, user_id=user_id)
        command.upgrade(configuration, "head")
        owner = connection.execute(select(users.c.user_id)).scalar_one()
        if owner != user_id:
            raise ValueError("configured user_id differs from the stored execution owner")
    await finish_database_operation(database.connection.run_sync(run))
