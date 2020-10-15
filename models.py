"""
model.py : database row <-> objet python
"""
from sqlalchemy import Boolean, Column, Integer, String, Numeric, SmallInteger, Date, ForeignKey, Table
from sqlalchemy.orm import relationship

from database import Base

play_table = Table('play', Base.metadata,
    Column('id_actor', Integer, ForeignKey('stars.id')),
    Column('id_movie', Integer, ForeignKey('movies.id'))
)


class Movie(Base):
    __tablename__ = "movies"

    id = Column(Integer, primary_key=True, index=True)
    title = Column(String(length=400), nullable=False)
    year = Column(SmallInteger, nullable=False)
    duration = Column(SmallInteger, nullable=True)
    id_director = Column(Integer, ForeignKey('stars.id'))
    #Many to one
    director = relationship("Star")
    #Many to Many
    actors = relationship("Star", secondary=play_table)



class Star(Base):
    __tablename__ = "stars"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(length=150), nullable=False)
    birthdate = Column(Date, nullable=False)
