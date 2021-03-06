"""
file crud.py
manage CRUD and adapt model data from db to schema data to api rest
"""

from typing import Optional, List
from sqlalchemy.orm import Session
from sqlalchemy import desc, extract, between
from sqlalchemy import func
from fastapi.logger import logger
import models, schemas

def get_movie(db: Session, movie_id: int):
    db_movie = db.query(models.Movie).filter(models.Movie.id == movie_id).first()
    logger.error("Movie retrieved from DB: {} ; director: {}".format( 
              db_movie.title, 
              db_movie.director.name if db_movie.director is not None else "no director"))
    return db_movie

def get_movies(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.Movie).offset(skip).limit(limit).all()

def get_allMovies(db: Session):
    return db.query(models.Movie).all()

def get_movies_by_title(db: Session, title: str):
    return db.query(models.Movie).filter(models.Movie.title == title).all()

def get_movies_by_parttitle(db: Session, title: str):
    return db.query(models.Movie).filter(models.Movie.title.like(f'%{title}%')).all()



def get_movies_by_title_year(db: Session, title: str, year: int):
    return db.query(models.Movie).filter(models.Movie.title == title, models.Movie.year == year).order_by(models.Movie.year, models.Movie.title).all()


def get_movies_by_director_endname(db: Session, endname: str):
    return db.query(models.Movie).join(models.Movie.director)      \
            .filter(models.Star.name.like(f'%{endname}')) \
            .order_by(desc(models.Movie.year))  \
            .all()

def get_movies_by_actor_endname(db: Session, endname: str):
    return db.query(models.Movie).join(models.Movie.actors) \
            .filter(models.Star.name.like(f'%{endname}'))   \
            .order_by(desc(models.Movie.year))              \
            .all()


def get_director_by_movie(db: Session, idMovie: str):
    movie_director = db.query(models.Movie).filter(models.Movie.id == idMovie).join(models.Movie.director).first()
    return movie_director.director

def get_actor_by_movie_title(db: Session, movieTitle: str):
    movie_actors = db.query(models.Movie).filter(models.Movie.title.like(f'%{movieTitle}%')).join(models.Movie.actors).all()

    allactors = [movie_actor.actors for movie_actor in movie_actors]

    return allactors




def create_movie(db: Session, movie: schemas.MovieCreate):
    db_movie = models.Movie(title=movie.title, year=movie.year, duration=movie.duration)
    db.add(db_movie)
    db.commit()
    db.refresh(db_movie)
    return db_movie



def update_movie(db: Session, movie: schemas.Movie):
    db_movie = db.query(models.Movie).filter(models.Movie.id == movie.id).first()
    if db_movie is not None:
        db_movie.title = movie.title
        db_movie.year = movie.year
        db_movie.duration = movie.duration
        db.commit()
    return db_movie



def delete_movie(db: Session, movie_id: int):
     db_movie = db.query(models.Movie).filter(models.Movie.id == movie_id).first()
     if db_movie is not None:
         db.delete(db_movie)
         db.commit()
     return db_movie




def get_movies_by_range_year(db: Session, year_min: Optional[int] = None, year_max: Optional[int] = None):
    if year_min is None and year_max is None:
        return None
    elif year_min is None:
        return db.query(models.Movie).filter(models.Movie.year <= year_max).all()
    elif year_max is None:
        return db.query(models.Movie).filter(models.Movie.year >= year_min).all()
    else:
        return db.query(models.Movie) \
                .filter(
                    models.Movie.year >= year_min,
                    models.Movie.year <= year_max) \
                .all()

#movie's stat

def get_movies_count_by_year(db: Session):
    result_query = db.query(models.Movie.year,func.count().label("countMovies")) \
    .group_by(models.Movie.year)\
    .order_by(models.Movie.year)\
    .all()

    return [{'year' : year,'countMovies':countMovies} for year,countMovies in result_query]

def get_movies_stat_duration(db: Session):
    result_query =  db.query(models.Movie.year,
        func.max(models.Movie.duration).label("max_duration"),
        func.min(models.Movie.duration).label("min_duration"),
        func.avg(models.Movie.duration).label("mean_duration")) \
    .group_by(models.Movie.year)\
    .order_by(models.Movie.year)\
    .all()

    return [{'year' : year,'min_duration':minduration, 'max_duration' : maxduration, 'mean_duration' : mean_duration} for year,minduration,maxduration,mean_duration in result_query]


# Stars's query

def get_star(db: Session, star_id: int):
    return db.query(models.Star).filter(models.Star.id == star_id).first()


def get_stars(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.Star).offset(skip).limit(limit).all()


def get_stars_by_name(db: Session, name: str):
    return db.query(models.Star).filter(models.Star.name == name).all()

def get_stars_by_partname(db: Session, name: str):
    return db.query(models.Star).filter(models.Star.name.like(f'%{name}%')).all()


def create_star(db: Session, star: schemas.StarCreate):
    db_star = models.Star(name=star.name, birthdate=star.birthdate)
    db.add(db_star)
    db.commit()
    db.refresh(db_star)
    return db_star


def update_star(db: Session, star: schemas.Star):
    db_star = db.query(models.Star).filter(models.Star.id == star.id).first()
    if db_star is not None:
        db_star.name = star.name
        db_star.birthdate = star.birthdate
        db.commit()
    return db_star


def delete_star(db: Session, star_id: int):
     db_star = db.query(models.Star).filter(models.Star.id == star_id).first()
     if db_star is not None:
         db.delete(db_star)
         db.commit()
     return db_star


def get_star_by_birthyear(db: Session, year: int):
    return db.query(models.Star).filter(extract('year', models.Star.birthdate) == year).all()



# Stars's stat

def get_movie_stat_director(db: Session, min_count: int ):
    
    result_query =  db.query(models.Star,
        func.count(models.Movie.id).label("count_movies"))\
    .join(models.Movie.director)\
    .group_by(models.Star)\
    .having(func.count(models.Movie.id) >= min_count)\
    .order_by(desc("count_movies"))\
    .all()

    return result_query


def get_count_movie_by_actor(db: Session, min_count: int ):
    
    result_query =  db.query(models.Star,
        func.count(models.Movie.id).label("count_movies"))\
    .join(models.Movie.actors)\
    .group_by(models.Star)\
    .having(func.count(models.Movie.id) >= min_count)\
    .order_by(desc("count_movies"))\
    .all()

    return result_query

def get_first_movie_by_actor(db: Session, min_count: int ):
    
    result_query =  db.query(models.Star,
        func.min(models.Movie.year).label("first_year"))\
    .join(models.Movie.actors)\
    .group_by(models.Star)\
    .having(func.count(models.Movie.id) >= min_count)\
    .all()

    return result_query

def get_last_movie_by_actor(db: Session, min_count: int ):
    
    result_query =  db.query(models.Star,
        func.max(models.Movie.year).label("last_year"))\
    .join(models.Movie.actors)\
    .group_by(models.Star)\
    .having(func.count(models.Movie.id) >= min_count)\
    .all()

    return result_query

def get_stat_movie_by_actor(db: Session,min_count: int):
    
    result_query =  db.query(models.Star,
        func.min(models.Movie.year),func.max(models.Movie.year), func.count(models.Movie.id))\
    .join(models.Movie.actors)\
    .group_by(models.Star)\
    .having(func.count(models.Movie.id) >= min_count)\
    .all()
    return result_query



#query with join 

def update_movie_director(db: Session, movie_id: int, director_id: int):
    db_movie = get_movie(db=db, movie_id=movie_id)
    db_star =  get_star(db=db, star_id=director_id)
    if db_movie is None or db_star is None:
        return None
    db_movie.director = db_star
    db.commit()
    return db_movie

def add_movie_actor(db: Session, movie_id: int, actor_id: int):
    db_movie = get_movie(db=db, movie_id=movie_id)
    db_star =  get_star(db=db, star_id=actor_id)
    if db_movie is None or db_star is None:
        return None
    db_movie.actors.append(db_star)
    db.commit()
    return db_movie


def update_movie_actor(db: Session, movie_id: int, actors_id: List[int]):
    db_movie = get_movie(db=db, movie_id=movie_id)
    db_stars =  [] 
    for actor_id in actors_id:
        actor = get_star(db=db, star_id=actor_id)
        if actor is None:
            return None
        db_stars.append(actor)

    if db_movie is None :
        return None
    db_movie.actors = db_stars
    db.commit()
    return db_movie