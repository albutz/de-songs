"""Data onboarding for the million songs dataset with Docker, SQLAlchemy and Postgres."""
import logging

from sqlalchemy import create_engine, inspect

from etl import Pipeline
from tables import (
    artist_location_table,
    artist_table,
    artist_table_init,
    location_table,
    metadata,
    song_table_init,
)

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":

    engine = create_engine(
        "postgresql+psycopg2://postgres:postgres@sparkify/sparkifydb", future=True
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

    tables = {
        "artists_init": artist_table_init,
        "songs_init": song_table_init,
        "locations": location_table,
        "artists": artist_table,
        "artists_locations": artist_location_table,
    }

    etl_pipeline = Pipeline(engine, tables)

    etl_pipeline.run_initial_pipeline()
    etl_pipeline.run_artist_pipeline()
    etl_pipeline.run_location_pipeline()
    etl_pipeline.run_artist_location_pipeline()
    etl_pipeline.run_album_pipeline()
    etl_pipeline.run_song_pipeline()
    etl_pipeline.drop_init_tables()

    engine.dispose()
