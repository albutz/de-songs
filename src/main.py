"""Main script for data modeling with Postgres."""
# from pathlib import Path

from time import sleep

import psycopg2
from python_on_whales import DockerClient

if __name__ == "__main__":
    # Start container
    docker_client = DockerClient(compose_files=["docker-compose.yml"])
    docker_client.compose.up(detach=True)

    # Wait for container starting up
    sleep(5)

    # Connect to database
    conn = psycopg2.connect(
        database="sparkifydb",
        user="postgres",
        password="postgres",
        host="localhost",
        port=6543,
    )

    # Set autocommit to True
    conn.set_session(autocommit=True)

    # Check connection
    cur = conn.cursor()
    cur.execute("SHOW data_directory;")
    print(cur.fetchall())

    # Stop container
    docker_client.compose.down()
