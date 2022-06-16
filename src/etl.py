"""ETL pipeline."""
from pathlib import Path

from sqlalchemy import Column, select
from sqlalchemy.future.engine import Engine
from tqdm import tqdm

from read import get_file_paths, read_h5
from tables import artist_table, artist_table_init, location_table, song_table_init
from utils import cast_numeric, encode_str


def run_initial_pipeline(engine: Engine) -> None:
    """Initial ETL pipeline.

    Minimal processing to load songs and artists into the database.

    Args:
        engine: Engine to connect to the database
    """
    file_paths = get_file_paths(Path("data"))

    for file_path in tqdm(file_paths):

        # Load individual file
        song_df = read_h5(file_path)

        # Insert artist
        # fmt: off
        insert_artist_stmt = (
            artist_table_init.
            insert().
            values(
                name=encode_str(song_df.artist_name),
                location=encode_str(song_df.artist_location),
                latitude=cast_numeric(song_df.artist_latitude),
                longitude=cast_numeric(song_df.artist_longitude)
            )
        )
        # fmt: on

        with engine.connect() as conn:
            conn.execute(insert_artist_stmt)
            conn.commit()

        # Insert song
        # fmt:off
        insert_song_stmt = (
            song_table_init.
            insert().
            values(
                title=encode_str(song_df.title),
                year=cast_numeric(song_df.year),
                danceability=cast_numeric(song_df.danceability),
                duration=cast_numeric(song_df.duration),
                end_of_fade_in=cast_numeric(song_df.end_of_fade_in),
                start_of_fade_out=cast_numeric(song_df.start_of_fade_out),
                loudness=cast_numeric(song_df.loudness),
                bpm=cast_numeric(song_df.tempo),
                album_name=encode_str(song_df.release),
                artist_name=encode_str(song_df.artist_name),
            )
        )
        # fmt: on

        with engine.connect() as conn:
            conn.execute(insert_song_stmt)
            conn.commit()


def run_artist_pipeline(engine: Engine) -> None:
    """Transform artists table.

    Args:
        engine: Engine to connect to the database
    """
    stmt = select(artist_table_init.c.name.distinct())

    with engine.connect() as conn:
        res = conn.execute(stmt)

    artist_names = [row[0] for row in res.all()]

    for artist_name in artist_names:
        insert_artist_stmt = artist_table.insert().values(name=artist_name)

        with engine.connect() as conn:
            conn.execute(insert_artist_stmt)
            conn.commit()


def run_location_pipeline(engine: Engine) -> None:
    """Locations table.

    Insert values into the locations table.

    Args:
        engine: Engine to connect to the database
    """
    stmt = select(artist_table_init.c.location.distinct())

    with engine.connect() as conn:
        res = conn.execute(stmt)

    locations = [row[0] for row in res.all()]

    def get_lng_lat_val(col: Column, location: str) -> list | None:
        """Helper to insert unique latitude and longitude values only.

        Args:
            col: Column object for latitude or longitude
            location: Single location as a string

        Returns:
            Latitude or longitude value if it is unique for a location and else None.
        """
        stmt = select(col).where(artist_table_init.c.location == location)

        with engine.connect() as conn:
            res = conn.execute(stmt)

        val = [row[0] for row in res.all()]
        val = list(set(val))
        insert_val = val[0] if len(val) == 1 else None

        return insert_val

    for location in locations:

        if location is None:
            continue

        lat = get_lng_lat_val(artist_table_init.c.latitude, location)
        lng = get_lng_lat_val(artist_table_init.c.longitude, location)

        insert_loc_stmt = location_table.insert().values(name=location, latitude=lat, longitude=lng)

        with engine.connect() as conn:
            conn.execute(insert_loc_stmt)
            conn.commit()
