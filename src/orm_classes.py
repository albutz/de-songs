"""ORM classes."""
from sqlalchemy import Column, ForeignKey, Integer, Numeric, String
from sqlalchemy.orm import registry, relationship

mapper_reqistry = registry()


@mapper_reqistry.mapped
class Artist:
    """ORM class for artists table."""

    __tablename__ = "artists"

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    location = Column(String)
    latitude = Column(Numeric)
    longitude = Column(Numeric)

    songs = relationship("Song")


@mapper_reqistry.mapped
class Song:
    """ORM class for songs table."""

    __tablename__ = "songs"

    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    year = Column(Integer)
    danceability = Column(Numeric)
    duration = Column(Numeric)
    end_of_fade_in = Column(Numeric)
    start_of_fade_out = Column(Numeric)
    loudness = Column(Numeric)
    bpm = Column(Numeric)
    album_name = Column(String)
    artist_name = Column(String, ForeignKey("artists.name"))
