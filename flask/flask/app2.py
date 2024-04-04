

#To run the program in the terminal
#python -m flask --app app2 run --port 8080 --debug



from flask import Flask, render_template
from flask_basicauth import BasicAuth
import pymysql
import os
from flask import abort
from flask import request
import json
import math

app = Flask(__name__)
app.config.from_file("flask_config.json", load=json.load)
auth = BasicAuth(app)

from flask_swagger_ui import get_swaggerui_blueprint
from xlwings import App
swaggerui_blueprint = get_swaggerui_blueprint(
    base_url='/docs',
    api_url='/static/openapi.yaml',
)
app.register_blueprint(swaggerui_blueprint)

def remove_null_fields(obj):
        return {k:v for k, v in obj.items() if v is not None}

@app.route("/movies/<int:movie_id>")
@auth.required
def movie(movie_id):
    db_conn = pymysql.connect(host="localhost", 
                              user="root", 
                              password=os.getenv('sql_password'), 
                              database="bechdel",
                              cursorclass=pymysql.cursors.DictCursor)
    with db_conn.cursor() as cursor:
        cursor.execute("""
                       SELECT M.movieId, 
                       M.originalTitle,
                       M.primaryTitle AS englishTitle,
                       B.rating AS bechdelScore,
                       M.runtimeMinutes,
                       M.startYear AS Year,
                       M.movieType,
                       M.isAdult
                       FROM Movies M
                       JOIN Bechdel B ON B.movieId = M.movieId 
                       WHERE M.movieId=%s""", (movie_id, ))
        movie = cursor.fetchone()
        if not movie:
            abort(404)
        movie = remove_null_fields(movie)
    with db_conn.cursor() as cursor:
        cursor.execute("SELECT * FROM MoviesGenres WHERE movieId=%s", (movie_id, ))
        genres = cursor.fetchall()
        movie['genres'] = [g['genre'] for g in genres]
    with db_conn.cursor() as cursor:
        cursor.execute("""
                       SELECT
                       P.personId,
                       P.primaryName AS name,
                       P.birthYear,
                       P.deathYear,
                       MP.job,
                       MP.category AS rol
                       FROM MoviesPeople MP
                       JOIN People P on P.personId = MP.personId
                       WHERE MP.movieId=%s""", (movie_id, ))
        people = cursor.fetchall()
        movie['people'] = people
        movie['people'] = [remove_null_fields(p) for p in people]
    db_conn.close()
    return movie

MAX_PAGE_SIZE = 100

@app.route("/movies")
@auth.required
def movies():
    include_details = int(request.args.get('include_details', 0))
    page = int(request.args.get('page', 1))
    page_size = int(request.args.get('page_size', MAX_PAGE_SIZE))
    page_size = min(page_size, MAX_PAGE_SIZE)

    db_conn = pymysql.connect(host="localhost", 
                              user="root",
                              password=os.getenv('sql_password'), 
                              database="bechdel",
                              cursorclass=pymysql.cursors.DictCursor)
    movie_ids = []
    movies = []
    try:
        with db_conn.cursor() as cursor:
            cursor.execute("""
                        SELECT M.movieId, 
                        M.originalTitle,
                        M.primaryTitle AS englishTitle,
                        B.rating AS bechdelScore,
                        M.runtimeMinutes,
                        M.startYear AS Year,
                        M.movieType,
                        M.isAdult
                        FROM Movies M
                        JOIN Bechdel B ON B.movieId = M.movieId
                        ORDER BY movieId
                        LIMIT %s
                        OFFSET %s
                        """, (page_size, (page-1) * page_size))
            movies = cursor.fetchall()
            movie_ids = [movie['movieId'] for movie in movies]
        
        if include_details:
            placeholders = ','.join(['%s'] * len(movie_ids))
            if movie_ids:
                with db_conn.cursor() as cursor:
                    cursor.execute(f"SELECT * FROM MoviesGenres WHERE movieId IN ({placeholders})", tuple(movie_ids))
                    genres = cursor.fetchall()
                    genres_by_movie = {}
                    for genre in genres:
                            genres_by_movie.setdefault(genre['movieId'], []).append(genre['genre'])
                    for movie in movies:
                        movie['genres'] = genres_by_movie.get(movie['movieId'], [])
                
                with db_conn.cursor() as cursor:
                    cursor.execute(f"""SELECT MP.movieId, P.personId, P.primaryName AS name, P.birthYear, P.deathYear,
                                    MP.job, MP.category AS role
                                    FROM MoviesPeople MP
                                    JOIN People P ON P.personId = MP.personId
                                    WHERE MP.movieId IN ({placeholders})
                                    """, tuple(movie_ids))
                    people = cursor.fetchall()
                    people_by_movie = {}
                    for person in people:
                        people_by_movie.setdefault(person['movieId'], []).append(remove_null_fields(person))
                    for movie in movies:
                        movie['people'] = people_by_movie.get(movie['movieId'], [])

                with db_conn.cursor() as cursor:
                    cursor.execute("SELECT COUNT(*) AS total FROM Movies")
                    total = cursor.fetchone()
                    last_page = math.ceil(total['total'] / page_size)
    finally: db_conn.close()

    return {
        'movies': movies,
        'next_page': f'/movies?page={page+1}&page_size={page_size}',
        'last_page': '',  
    }

