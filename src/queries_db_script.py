def query_1(connector, text: str, limit: int = 20):
    # [1] (Full-text): movies by title/overview/tagline
    # The query receives a text and performs a full-text search over the movies table,
    # using the combined text fields: title, overview, tagline.
    # The user can also determine the most number of rows he'd like to receive back.
    query = """
          SELECT 
              tmdb_id, title, release_year, overview, vote_average,
              # calculate the relevance score for this movie, relative to text:
              MATCH(title, overview, tagline) AGAINST (%s IN NATURAL LANGUAGE MODE) AS score
          FROM 
              movies
          WHERE  # keep only rows where the full-text search matches at all:
              MATCH(title, overview, tagline) AGAINST (%s IN NATURAL LANGUAGE MODE)
          ORDER BY 
              score DESC,  # first: most relevant movies 
              vote_average DESC,  # tie-breaker: higher vote_average first
              vote_count DESC # tie-breaker: higher vote_count first
          LIMIT 
              %s; 
          """
    cursor = connector.cursor()
    cursor.execute(query, (text, text, limit))  # in order to avoid SQL injection
    rows = cursor.fetchall()
    cursor.close()
    return rows


def query_2(connector, keyword_text: str, limit: int = 20):
    # [2] (Full-text): keywords full-text -> movies
    # The query receives a full-text boolean query applied to keywords.name, and returns
    # a list of tuples with the attributes (id, title, release year, vote average, matched_keywords)
    # where matched_keywords equals how many keyword rows matched user's search and are linked to
    # that movie. The rows are ordered by matched_keywords.
    # The keywords data came straight from the database.
    query = """
          SELECT 
              movies.tmdb_id, movies.title, movies.release_year, movies.overview, movies.vote_average,
              # counts how many matching keyword rows that movie had:
              COUNT(DISTINCT keywords.name) AS matched_keywords
          FROM 
              keywords, # keywords(keyword_id (PK), name)
              movie_keywords,  # movie_keywords(tmdb_id, keyword_id) (keywords <-> movies bridge)
              movies # movies(tmdb_id (PK), title, release_year, vote_average, ...)
          WHERE
              movie_keywords.keyword_id = keywords.keyword_id AND
              movies.tmdb_id = movie_keywords.tmdb_id AND
              # keep only those keyword rows whose name matched user's boolean search expression: 
              MATCH(keywords.name) AGAINST (%s IN BOOLEAN MODE)
          GROUP BY # collapse all "movie-keyword" rows into one row per movie:
              movies.tmdb_id, movies.title, movies.release_year, movies.vote_average
          ORDER BY 
              matched_keywords DESC,  # first: movies with more matched keywords
              movies.vote_average DESC  # tie-breaker: higher TMDB rating
          LIMIT 
              %s;
          """
    cursor = connector.cursor()
    cursor.execute(query, (keyword_text, limit))
    rows = cursor.fetchall()
    cursor.close()
    return rows


def query_3(connector, min_movies: int = 5, limit: int = 20):
    # [3] (Complex): actors with roles across many genres
    # This query finds who acted in at least min_movies movies, and ranks them by how genre-diverse
    # they are (which actors appear in as many movies, across many genres).
    query = """
          SELECT 
              people.person_id,
              people.name,
              people.profile_path,
              # count how many different genres they have appeared in (based on the movies they acted in):
              COUNT(DISTINCT movie_genres.genre_id) AS distinct_genres,
              # count how many different movies they acted in:
              COUNT(DISTINCT movie_cast.tmdb_id) AS distinct_movies
          FROM 
              people,  # people(person_id (pk), name, gender, profile_path)
              movie_cast,  # movie_cast(tmdb_id, credit_id, person_id, cast_id, cast_order, character_name)
              movie_genres  # movie_genres(tmdb_id, genre_id) 
          WHERE
              people.person_id = movie_cast.person_id AND
              movie_cast.tmdb_id = movie_genres.tmdb_id  
              # after that, each row represents: 1 x actor, 1 x movie they acted in, 1 x genre of that movie
          GROUP BY  # collapse all those rows into one row per actor
              people.person_id, people.name
          HAVING # keep only actors who appear in at least <min_movies> distinct movies:
              distinct_movies >= %s
          ORDER BY 
              distinct_genres DESC,  # first: actors with more genre diversity
              distinct_movies DESC  # tie-breaker: actors who acted in more movies
          LIMIT 
              %s;
          """
    cursor = connector.cursor()
    cursor.execute(query, (min_movies, limit))
    rows = cursor.fetchall()
    cursor.close()
    return rows


def query_4(connector, min_ratings: int = 30, limit: int = 20):
    # [4] (Complex): overrated on TMDB (MovieLens avg is lower than TMDB vote_average)
    # This query tries to find movies that MovieLens users rate much lower than TMDB's vote_average, but only
    # if the movie has enough ratings (<min_ratings>) - to be meaningful.
    # The ranking is the biggest gap first.
    query = """
          # we want to create a temporary table movie_avg, with one row per MovieLens movie:
          WITH movie_avg AS (
              SELECT
                  ratings.ml_movie_id,  # movie identifier
                  (AVG(ratings.rating) - 0.5) * (10.0 / 4.5) AS avg_rating,  # average user rating for that movie (adjusted to TMDB scale)
                  COUNT(*) AS cnt  # number of ratings it has (to determine how reflective the avg rating is)
              FROM 
                  ratings  # ratings(user_id, ml_movie_id, rating, ts_unix)
              GROUP BY
                  ratings.ml_movie_id
              HAVING # keeps only movies with at least <min_ratings> ratings (we want it to be reflective)
                  cnt >= %s
          )
          SELECT 
              movies.tmdb_id, movies.title, movies.release_year,  # TMDB info
              movie_avg.avg_rating, movie_avg.cnt,  # MovieLens aggregated rating
              movies.vote_average AS tmdb_vote_avg,
              movies.vote_count AS tmdb_vote_count,
              (movie_avg.avg_rating - movies.vote_average) AS gap  # the computed gap (positive = overrated, negative = underrated)
          FROM 
              movie_avg,  # movie_avg(ml_movie_id, avg_rating, cnt)
              movielens_links,  # movielens_links(ml_movie_id (pk), imdb_id, tmdb_id)
              movies  # movies(tmdb_id (PK), title, release_year, vote_average, ...)
          WHERE 
              movielens_links.ml_movie_id = movie_avg.ml_movie_id AND
              movies.tmdb_id = movielens_links.tmdb_id AND
              movies.vote_count >= %s
              # after that, each row represents: 1 x movie that exists in MovieLens ratings AND has a matching TMDB movie row
          ORDER BY 
              ABS(gap) DESC,  # first: biggest absolute difference first.
              movie_avg.avg_rating DESC  # tie-breaker: higher MovieLens average.
          LIMIT 
              %s;
          """
    cursor = connector.cursor()
    cursor.execute(query, (min_ratings, min_ratings, limit))
    rows = cursor.fetchall()
    cursor.close()
    return rows


def query_5(connector, genre_name: str, min_movies: int = 5, limit: int = 20):
    # [5] (Complex): actors with >= min_movies who NEVER acted in given genre
    # This query finds actors who have acted in at least min_movies movies, and have never acted in the given genre.
    # e.g. actors with >= 100 movies who never did Horror.
    query = """
          SELECT 
              people.person_id, people.name, people.profile_path, # person information
              # counts how many different movies each actor appears in. 
              COUNT(DISTINCT movie_cast.tmdb_id) AS movies_count
          FROM 
              people,  # people(person_id (pk), name, gender, profile_path) 
              movie_cast  # movie_cast(tmdb_id, credit_id, person_id, cast_id, cast_order, character_name) (only actors)
          WHERE 
              movie_cast.person_id = people.person_id
              # at this point, we have one row per actor with their movies_count
          GROUP BY 
              people.person_id, people.name
          HAVING 
              # keeps only actors with enough movies (min_movies)
              movies_count >= %s AND
              # keep only actors for whom the subquery finds no movie in that genre (genre_name)
              NOT EXISTS (
                  # for the current actor from the outer query, look for any row that proves they acted in the
                  # "forbidden" genre (genre_name). If at least one such row exists, the actor has acted in a movie in
                  # that genre -> NOT EXISTS fails -> actor is excluded. 
                  SELECT 1
                  FROM 
                      movie_cast mc2,  # movie_cast(tmdb_id, credit_id, person_id, cast_id, cast_order, character_name)
                      movie_genres,  # movie_genres(tmdb_id, genre_id) 
                      genres  # genres(genre_id, name)
                  WHERE
                      movie_genres.tmdb_id = mc2.tmdb_id AND
                      genres.genre_id = movie_genres.genre_id AND
                      mc2.person_id = people.person_id AND
                      genres.name = %s
          )
          ORDER BY # show actors with the most movies first
              movies_count DESC
          LIMIT 
              %s;
          """
    cursor = connector.cursor()
    cursor.execute(query, (min_movies, genre_name, limit))
    rows = cursor.fetchall()
    cursor.close()
    return rows


def query_6(connector, user_id: int, ml_movie_id: int, new_rating: float):
    """
    Updates the rating for (user_id, ml_movie_id) and refreshes ts_unix to 'now'.
    Returns number of affected rows (0 if no such row exists).
    """
    query = """
          UPDATE ratings
          SET 
              rating = %s, 
              ts_unix = UNIX_TIMESTAMP()
          WHERE 
              user_id = %s AND 
              ml_movie_id = %s;
          """
    cursor = connector.cursor()
    cursor.execute(query, (new_rating, user_id, ml_movie_id))
    connector.commit()
    print(f"Updated rows: {cursor.rowcount}")
    query = """SELECT * FROM ratings WHERE user_id = %s AND ml_movie_id = %s;"""
    cursor.execute(query, (user_id, ml_movie_id))
    rows = cursor.fetchall()
    cursor.close()
    return rows

