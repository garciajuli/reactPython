from typing import List, Optional
import logging

from fastapi import Depends, FastAPI, HTTPException
from fastapi.logger import logger as fastapi_logger
from sqlalchemy.orm import Session

import crud, models, schemas
from database import SessionLocal, engine

models.Base.metadata.create_all(bind=engine)
app = FastAPI()

logger = logging.getLogger("uvicorn")
fastapi_logger.handlers = logger.handlers
fastapi_logger.setLevel(logger.level)
logger.error("API Started")

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/movies/", response_model=List[schemas.Movie])
def read_movies(skip: Optional[int] = 0, limit: Optional[int] = 100, db: Session = Depends(get_db)):
    # read items from database
    movies = crud.get_movies(db, skip=skip, limit=limit)
    # return them as json
    return movies

@app.get("/movies/by_id/{movie_id}", response_model=schemas.MovieDetail)
def read_movie(movie_id: int, db: Session = Depends(get_db)):
    db_movie = crud.get_movie(db, movie_id=movie_id)
    if db_movie is None:
        raise HTTPException(status_code=404, detail="Movie to read not found")
    return db_movie



@app.get("/stars", response_model=List[schemas.Star])
def read_stars(skip: Optional[int] = 0, limit: Optional[int] = 100, db: Session = Depends(get_db)):
    # read items from database
    stars = crud.get_stars(db, skip=skip, limit=limit)
    # return them as json
    return stars

@app.get("/stars/by_id/{star_id}", response_model=schemas.Star)
def read_star(star_id: int, db: Session = Depends(get_db)):
    db_star = crud.get_star(db, star_id=star_id)
    if db_star is None:
        raise HTTPException(status_code=404, detail="Star to read not found")
    return db_star


@app.get("/movie/by_title", response_model=List[schemas.Movie])
def read_movie_by_title(searchTitle: Optional[str] = None, db: Session = Depends(get_db)):
    #  read movie from database
    movie = crud.get_movies_by_title(db=db, title=searchTitle)
    # return them as json
    return movie
    # return n

@app.get("/movie/by_partTitle", response_model=List[schemas.Movie])
def read_movie_by_partTitle(searchTitle: Optional[str] = None, db: Session = Depends(get_db)):
    # read movie from database
    movie = crud.get_movies_by_title(db=db, title=searchTitle)
    # return them as json
    return movie

@app.get("/movies/by_title_year", response_model=List[schemas.Movie])
def read_movies_by_title_year(t: str, y: int, db: Session = Depends(get_db)):
    return crud.get_movies_by_title_year(db=db, title=t, year=y)


@app.get("/movies/by_director", response_model=List[schemas.Movie])
def read_movies_by_director(n: str, db: Session = Depends(get_db)):
    return crud.get_movies_by_director_endname(db=db, endname=n)

@app.get("/movies/by_actor", response_model=List[schemas.Movie])
def read_movies_by_actor(n: str, db: Session = Depends(get_db)):
    return crud.get_movies_by_actor_endname(db=db, endname=n)



@app.get("/director/by_id_movie/{idMovie}", response_model=schemas.Star)
def read_director_by_movie(idMovie: int, db: Session = Depends(get_db)):
    director = crud.get_director_by_movie(db=db, idMovie=idMovie)
    if director is None:
        raise HTTPException(status_code=404, detail="Star to read not found")
    return director

@app.get("/actor/by_movie_title", response_model=List[List[schemas.Star]])
def read_actor_by_movie_title(movieTitle: str, db: Session = Depends(get_db)):
    actors = crud.get_actor_by_movie_title(db=db, movieTitle=movieTitle)
    if actors is None:
        raise HTTPException(status_code=404, detail="Star to read not found")
    return actors


@app.get("/stars/by_name") #, response_model=List[schemas.Star])
def read_star_by_name(starName: Optional[str] = None, db: Session = Depends(get_db)):
    #  read movie from database
    star = crud.get_stars_by_name(db=db, name=starName)
    # return them as json
    return star
    # return n

@app.get("/stars/by_partname", response_model=List[schemas.Star])
def read_movie_by_partname(starName: Optional[str] = None, db: Session = Depends(get_db)):
    # read movie from database
    star = crud.get_stars_by_partname(db=db, name=starName)
    # return them as json
    return star


@app.post("/movie/", response_model=schemas.Movie)
def create_movie(movie: schemas.MovieCreate, db: Session = Depends(get_db)):
    # receive json movie without id and return json movie from database with new id
    return crud.create_movie(db=db, movie=movie)


@app.post("/star/", response_model=schemas.Star)
def create_star(star: schemas.StarCreate, db: Session = Depends(get_db)):
    # receive json movie without id and return json movie from database with new id
    return crud.create_star(db=db, star=star)


@app.put("/movie/", response_model=schemas.Movie)
def update_movie(movie: schemas.Movie, db: Session = Depends(get_db)):
    db_movie = crud.update_movie(db, movie=movie)
    if db_movie is None:
        raise HTTPException(status_code=404, detail="Movie to update not found")
    return db_movie


@app.put("/star/", response_model=schemas.Star)
def update_star(star: schemas.Star, db: Session = Depends(get_db)):
    db_star = crud.update_star(db, star=star)
    if db_star is None:
        raise HTTPException(status_code=404, detail="Star to update not found")
    return db_star

@app.delete("/movie/{movie_id}", response_model=schemas.Movie)
def delete_movie(movie_id: int, db: Session = Depends(get_db)):
    db_movie = crud.delete_movie(db, movie_id=movie_id)
    if db_movie is None:
        raise HTTPException(status_code=404, detail="Movie to delete not found")
    return db_movie

@app.delete("/star/{star_id}", response_model=schemas.Star)
def delete_star(star_id: int, db: Session = Depends(get_db)):
    db_star = crud.delete_star(db, star_id=star_id)
    if db_star is None:
        raise HTTPException(status_code=404, detail="Star to delete not found")
    return db_star


@app.get("/movies/by_range_year", response_model=List[schemas.Movie])
def get_movie_by_range_year(year_min: Optional[int] = None, year_max: Optional[int] = None, db: Session = Depends(get_db)):
    # read movie from database
    movie = crud.get_movies_by_range_year(db = db, year_min = year_min, year_max = year_max)
    # return them as json
    return movie

@app.get("/stars/by_birthyear/{year}", response_model=List[schemas.Star])
def get_star_by_birthyear(year: int, db: Session = Depends(get_db)):
    # read star from database
    star = crud.get_star_by_birthyear(db = db, year = year)
    # return them as json
    return star



@app.put("/movies/director/", response_model=schemas.MovieDetail)
def update_movie_director(mid: int, sid: int, db: Session = Depends(get_db)):
    db_movie = crud.update_movie_director(db=db, movie_id=mid, director_id=sid)
    if db_movie is None:
        raise HTTPException(status_code=404, detail="Movie or Star not found")
    return db_movie

@app.post("/movies/actor/", response_model=schemas.MovieDetail)
def add_movie_actor(mid: int, sid: int, db: Session = Depends(get_db)):
    """ add one actor to a movie
        mid (query param): movie id
        sid (query param): star id to add in movie.actors
    """
    db_movie = crud.add_movie_actor(db=db, movie_id=mid, actor_id=sid)
    if db_movie is None:
        raise HTTPException(status_code=404, detail="Movie or Star not found or star already in actors")
    return db_movie

@app.put("/movies/actors/", response_model=schemas.MovieDetail)
def update_movie_actors(mid: int, sids: List[int], db: Session = Depends(get_db)):
    """ replace actors from a movie
        mid (query param): movie id
        sids (body param): list of star id to replace movie.actors
    """
    db_movie = crud.update_movie_actor(db=db, movie_id=mid, actors_id=sids)
    if db_movie is None:
        raise HTTPException(status_code=404, detail="Movie or Star not found or star already in actors")
    return db_movie
