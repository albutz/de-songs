"""Data onboarding for the million songs dataset with Docker, SQLAlchemy and Postgres."""
import logging

from sqlalchemy import create_engine, inspect

from etl import run_etl_pipeline
from tables import metadata

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":

    engine = create_engine(
        "postgresql+psycopg2://postgres:postgres@localhost:6543/sparkifydb", future=True
    )

    logging.info("Engine created.")

    # Create schema if necessary
    inspector = inspect(engine)
    tbls = [
        "artists_init",
        "artists",
        "songs_init",
        "songs",
        "locations",
        "artists_locations",
        "albums",
    ]
    if not all([tbl in inspector.get_table_names() for tbl in tbls]):
        with engine.begin() as conn:
            metadata.create_all(conn)
        logging.info("Schema created.")
    else:
        logging.info("Schema already exists.")

    run_etl_pipeline(engine)

    engine.dispose()
