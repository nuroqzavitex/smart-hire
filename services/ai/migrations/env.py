"""Alembic environment: the DSN comes from AI_DB_DSN only (no other settings needed)."""

import os

from alembic import context
from sqlalchemy import create_engine

from ai_service.config import normalize_dsn


def run() -> None:
    dsn = normalize_dsn(os.environ["AI_DB_DSN"])
    if context.is_offline_mode():
        context.configure(url=dsn, literal_binds=True)
        with context.begin_transaction():
            context.run_migrations()
        return
    engine = create_engine(dsn)
    with engine.connect() as connection:
        context.configure(connection=connection)
        with context.begin_transaction():
            context.run_migrations()
    engine.dispose()


run()
