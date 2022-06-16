"""Metadata / database schema."""

from sqlalchemy import Column, Integer, MetaData, Numeric, String, Table

metadata = MetaData()

artist_table_init = Table(
    "artists_init",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String),
    Column("location", String),
    Column("latitude", Numeric),
    Column("longitude", Numeric),
)

artist_table = Table(
    "artists",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String, unique=True, nullable=False),
)

location_table = Table(
    "locations",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("name", String, unique=True, nullable=False),
    Column("latitude", Numeric),
    Column("longitude", Numeric),
)

song_table_init = Table(
    "songs_init",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("title", String),
    Column("year", Integer),
    Column("danceability", Numeric),
    Column("duration", Numeric),
    Column("end_of_fade_in", Numeric),
    Column("start_of_fade_out", Numeric),
    Column("loudness", Numeric),
    Column("bpm", Numeric),
    Column("album_name", String),
    Column("artist_name", String),
)
