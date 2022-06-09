"""Data onboarding for the million songs dataset with Docker, SQLAlchemy and Postgres."""
import logging

from python_on_whales import DockerClient
from sqlalchemy import create_engine, inspect

from etl import run_initial_pipeline
from setup import Docker
from tables import metadata

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":

    docker_client = DockerClient(compose_files=["docker-compose.yml"])

    with Docker(docker_client) as container:

        engine = create_engine(
            "postgresql+psycopg2://postgres:postgres@localhost:6543/sparkifydb", future=True
        )

        logging.info("Engine created.")

        # Create schema if necessary
        inspector = inspect(engine)
        if not all([tbl in inspector.get_table_names() for tbl in ["artists", "songs"]]):
            with engine.begin() as conn:
                metadata.create_all(conn)

        # ETL pipeline
        run_initial_pipeline(engine)

        engine.dispose()
