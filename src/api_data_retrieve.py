# Loads processed CSVs from data/csvs into the movie_app database.
import argparse
from pathlib import Path
import pandas as pd
import mysql.connector

# DB connection settings
MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3307
MYSQL_USER = "abuhatzira"
MYSQL_PASSWORD = "123123"
DB_NAME = "abuhatzira"

# path to CSVs
PATH = "data/csvs"

# Batch sizes
BATCH_SIZE = 2000
RATINGS_BATCH_SIZE = 5000


def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def df_to_rows(df: pd.DataFrame):
    # convert NaN to None for mysql-connector
    df = df.where(pd.notna(df), None)
    return [tuple(x) for x in df.itertuples(index=False, name=None)]


def reset_all_tables(cur):
    # removes all rows from all database tables
    for table in [
        "ratings",
        "movielens_links",
        "movie_crew",
        "movie_cast",
        "people",
        "movie_keywords",
        "keywords",
        "movie_genres",
        "genres",
        "movies",
    ]:
        cur.execute(f"TRUNCATE TABLE {table};")


def read_csv(pdir: Path, filename: str) -> pd.DataFrame:
    return pd.read_csv(pdir / filename, low_memory=False)


def main():
    argument_parser = argparse.ArgumentParser()
    # Connection parameters (user-provided)
    argument_parser.add_argument("--host", default=MYSQL_HOST)
    argument_parser.add_argument("--port", type=int, default=MYSQL_PORT)
    argument_parser.add_argument("--user", default=MYSQL_USER)
    argument_parser.add_argument("--db-pass", default=MYSQL_PASSWORD)
    argument_parser.add_argument("--db-name", default=DB_NAME)
    args = argument_parser.parse_args()
    pdir = Path(PATH)

    con = mysql.connector.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.db_pass,
        database=args.db_name,
        autocommit=False
    )
    cursor = con.cursor()

    try:
        cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
        reset_all_tables(cursor)

        # [1] movies
        movies = read_csv(pdir, "movies.csv")
        sql_movies = """
                     INSERT INTO movies
                     (tmdb_id, imdb_id, title, original_title, overview, tagline, release_date, release_year,
                      original_language, runtime, status, budget, revenue, popularity, vote_average, vote_count, adult)
                     VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) \
                     """
        for batch in chunked(df_to_rows(movies), BATCH_SIZE):
            cursor.executemany(sql_movies, batch)

        # [2] genres (only 20 rows)
        genres = read_csv(pdir, "genres.csv")
        cursor.executemany(
            "INSERT INTO genres(genre_id, name) VALUES (%s,%s)",
            df_to_rows(genres),
        )

        # [3] movie_genres
        movie_genres = read_csv(pdir, "movie_genres.csv")
        for batch in chunked(df_to_rows(movie_genres), BATCH_SIZE):
            cursor.executemany(
                "INSERT INTO movie_genres(tmdb_id, genre_id) VALUES (%s,%s)",
                batch,
            )

        # [4] keywords
        keywords = read_csv(pdir, "keywords.csv")[["keyword_id", "name"]]
        for batch in chunked(df_to_rows(keywords), BATCH_SIZE):
            cursor.executemany(
                "INSERT IGNORE INTO keywords(keyword_id, name) VALUES (%s,%s)",
                batch,
            )

        # [5] movie_keywords
        movie_keywords = read_csv(pdir, "movie_keywords.csv")[["tmdb_id", "keyword_id"]]
        for batch in chunked(df_to_rows(movie_keywords), BATCH_SIZE):
            cursor.executemany(
                "INSERT IGNORE INTO movie_keywords(tmdb_id, keyword_id) VALUES (%s,%s)",
                batch,
            )

        # [6] people
        people = read_csv(pdir, "people.csv")
        for batch in chunked(df_to_rows(people), BATCH_SIZE):
            cursor.executemany(
                "INSERT INTO people(person_id, name, gender, profile_path) VALUES (%s,%s,%s,%s)",
                batch,
            )

        # [7] movie_cast
        movie_cast = read_csv(pdir, "movie_cast.csv")
        sql_cast = """
                   INSERT INTO movie_cast(tmdb_id, credit_id, person_id, cast_id, cast_order, character_name)
                   VALUES (%s,%s,%s,%s,%s,%s) \
                   """
        for batch in chunked(df_to_rows(movie_cast), BATCH_SIZE):
            cursor.executemany(sql_cast, batch)

        # [8] movie_crew
        movie_crew = read_csv(pdir, "movie_crew.csv")
        sql_crew = """
                   INSERT INTO movie_crew(tmdb_id, credit_id, person_id, department, job)
                   VALUES (%s,%s,%s,%s,%s) \
                   """
        for batch in chunked(df_to_rows(movie_crew), BATCH_SIZE):
            cursor.executemany(sql_crew, batch)

        # [9] movielens_links
        links = read_csv(pdir, "movielens_links.csv")
        for batch in chunked(df_to_rows(links), BATCH_SIZE):
            cursor.executemany(
                "INSERT INTO movielens_links(ml_movie_id, imdb_id, tmdb_id) VALUES (%s,%s,%s)",
                batch,
            )

        # [10] ratings
        ratings = read_csv(pdir, "ratings.csv")
        sql_ratings = "INSERT INTO ratings(user_id, ml_movie_id, rating, ts_unix) VALUES (%s,%s,%s,%s)"
        for batch in chunked(df_to_rows(ratings), RATINGS_BATCH_SIZE):
            cursor.executemany(sql_ratings, batch)

        cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
        con.commit()
        print("Done: loaded CSVs into database.")

    except mysql.connector.Error as error:
        print(f"Failed loading with error: {str(error)}")
        con.rollback()

    finally:
        cursor.close()
        con.close()


if __name__ == "__main__":
    main()
