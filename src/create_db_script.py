import argparse
import mysql.connector

# Creates (or resets) the movie_app database and all tables.

# DB connection settings
MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3307
MYSQL_USER = "abuhatzira"
MYSQL_PASSWORD = "123123"
DB_NAME = "abuhatzira"


def main():
    argument_parser = argparse.ArgumentParser()
    # Connection parameters (user-provided)
    argument_parser.add_argument("--host", default=MYSQL_HOST)
    argument_parser.add_argument("--port", type=int, default=MYSQL_PORT)
    argument_parser.add_argument("--user", default=MYSQL_USER)
    argument_parser.add_argument("--db-pass", default=MYSQL_PASSWORD)
    argument_parser.add_argument("--db-name", default=DB_NAME)
    args = argument_parser.parse_args()
    # Connection:
    con = mysql.connector.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.db_pass,
        autocommit=True
    )
    cursor = con.cursor()

    try:
        cursor.execute(f"DROP DATABASE IF EXISTS {args.db_name};")
        # Create DB:
        # [0] Database + encoding --------------------------------------------------------------------------------------
        # we create the dedicated database for the project.
        cursor.execute(f"""CREATE DATABASE IF NOT EXISTS {args.db_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;""".strip())
        cursor.execute(f"USE {args.db_name}")
        tables = {}
        # [1] movies table ---------------------------------------------------------------------------------------------
        # this will be the main entity in our project: one row per movie (TMDB = The Movies DataBase)
        tables["movies"] = """
            CREATE TABLE IF NOT EXISTS movies (
                        tmdb_id           INT PRIMARY KEY,  # (PK) TMDB movie id 
                        imdb_id           VARCHAR(12), # IMDB id for the movie 
                        title             VARCHAR(255) NOT NULL,  # the public title 
                        original_title    VARCHAR(255),  # original-language title 
                        overview          TEXT, # plot_summary - used for full-text search 
                        tagline           TEXT, # marketing tagline - used for full-text search 
                        release_date      DATE, # the date the movie released 
                        release_year      SMALLINT, # the year the movie was released 
                        original_language CHAR(5), # short language code. e.g. en 
                        runtime           SMALLINT, # minutes 
                        status            VARCHAR(40),  # e.g. Released/Post-Production 
                        budget            BIGINT, # numeric budget 
                        revenue           BIGINT, # numeric revenue 
                        popularity        DOUBLE, # TMDB popularity score (not a rating. Based on views/traffic/trending)
                        vote_average      DOUBLE, # TMDB average rating (movie's avg user rating on TMDB 0-10 scale)
                        vote_count        INT, # number of TMDB votes (how many TMDB votes that avg is based on)
                        adult             TINYINT(1), # whether the movie is adult content 

                        FULLTEXT KEY ft_movies_text (title, overview, tagline) # supports MATCH(...) AGAINST(...) searches
            ) ENGINE=InnoDB
            """.strip()
        # [2] genres ---------------------------------------------------------------------------------------------------
        # lookup table of unique genres.
        tables["genres"] = """
            CREATE TABLE IF NOT EXISTS genres (
                                genre_id INT PRIMARY KEY, # (PK) genre id from TMDB 
                                name     VARCHAR(60) NOT NULL, # genre name 

                                UNIQUE KEY uq_genre_name (name)
            ) ENGINE=InnoDB
            """.strip()
        # [3] movie_genres----------------------------------------------------------------------------------------------
        # bridge table (many to many) connecting movies <-> genres
        tables["movie_genres"] = """
            CREATE TABLE IF NOT EXISTS movie_genres (
                              tmdb_id  INT NOT NULL, # which movie (FK to movies.tmdb_id)
                              genre_id INT NOT NULL, # which genre (FK to genres.genre_id)

                              PRIMARY KEY (tmdb_id, genre_id), # (PK) preventing duplicates
                              CONSTRAINT fk_mg_movie FOREIGN KEY (tmdb_id) REFERENCES movies(tmdb_id),
                              CONSTRAINT fk_mg_genre FOREIGN KEY (genre_id) REFERENCES genres(genre_id)
            ) ENGINE=InnoDB
            """.strip()
        # [4] keywords -------------------------------------------------------------------------------------------------
        # lookup table of unique keywords (topics/tags)
        tables["keywords"] = """
            CREATE TABLE IF NOT EXISTS keywords (
                          keyword_id INT PRIMARY KEY,  # (PK) keyword id 
                          name       VARCHAR(100) NOT NULL, # keyword text 

                          FULLTEXT KEY ft_keywords_name (name) # supports MATCH(name) AGAINST(...) in query 2
            ) ENGINE=InnoDB
            """.strip()
        # [5] movie_keywords -------------------------------------------------------------------------------------------
        # bridge table movies <-> keywords
        tables["movie_keywords"] = """
            CREATE TABLE IF NOT EXISTS movie_keywords (
                                tmdb_id     INT NOT NULL, # movie id (FK to movies.tmdb_id)
                                keyword_id  INT NOT NULL, # keyword id (FK keywords.keyword_id)

                                PRIMARY KEY (tmdb_id, keyword_id),
                                KEY idx_mk_keyword (keyword_id),
                                KEY idx_mk_keyword_tmdb (keyword_id, tmdb_id),
                                CONSTRAINT fk_mk_movie   FOREIGN KEY (tmdb_id) REFERENCES movies(tmdb_id),
                                CONSTRAINT fk_mk_keyword FOREIGN KEY (keyword_id) REFERENCES keywords(keyword_id)
            ) ENGINE=InnoDB
            """.strip()
        # [6] people ---------------------------------------------------------------------------------------------------
        # unique people (actors, directors, writers, etc)
        tables["people"] = """
            CREATE TABLE IF NOT EXISTS people (
                        person_id    INT PRIMARY KEY, # (PK): TMDB person id 
                        name         VARCHAR(255) NOT NULL,  # person's name 
                        gender       TINYINT, # numeric gender code from TMDB (can be NULL) 
                        profile_path VARCHAR(255) # path to profile image (can be NULL) 
            ) ENGINE=InnoDB
            """.strip()
        # [7] movie_cast -----------------------------------------------------------------------------------------------
        # cast members in movies (actors + roles). Many-to-many with extra attributes
        tables["movie_cast"] = """
            CREATE TABLE IF NOT EXISTS movie_cast (
                            tmdb_id        INT NOT NULL, # movie id (FK to movies.tmdb_id)
                            credit_id      VARCHAR(32) NOT NULL, # unique id for this credit record
                            person_id      INT NOT NULL, # which person acted (FK to people.person_id)
                            cast_id        INT, # TMDB cast id field (can be null)
                            cast_order     INT, # ordering in credits (0 = main star, etc.)
                            character_name VARCHAR(255),  # character portrayed

                            PRIMARY KEY (tmdb_id, credit_id),
                            KEY idx_cast_person (person_id),
                            KEY idx_cast_person_movie (person_id, tmdb_id),
                            CONSTRAINT fk_cast_movie  FOREIGN KEY (tmdb_id) REFERENCES movies(tmdb_id),
                            CONSTRAINT fk_cast_person FOREIGN KEY (person_id) REFERENCES people(person_id)
            ) ENGINE=InnoDB
            """.strip()
        # [8] movie_crew -----------------------------------------------------------------------------------------------
        # crew members in movies (director/writer etc.). Many-to-many with extra attributes
        tables["movie_crew"] = """
            CREATE TABLE IF NOT EXISTS movie_crew (
                            tmdb_id    INT NOT NULL, # movie id (FK to movies.tmdb_id)
                            credit_id  VARCHAR(32) NOT NULL, # unique credit record id
                            person_id  INT NOT NULL, # which person worked on this movie (FK to people.person_id)
                            department VARCHAR(100), # department (e.g. Directing, Writing)
                            job        VARCHAR(100), # job title (e.g. director, Screenplay)

                            PRIMARY KEY (tmdb_id, credit_id),
                            CONSTRAINT fk_crew_movie  FOREIGN KEY (tmdb_id) REFERENCES movies(tmdb_id),
                            CONSTRAINT fk_crew_person FOREIGN KEY (person_id) REFERENCES people(person_id)
            ) ENGINE=InnoDB
            """.strip()
        # [9] movielens_links ------------------------------------------------------------------------------------------
        # mapping between MovieLens ids and TMDB ids so ratings can join to movies.
        tables["movielens_links"] = """
            CREATE TABLE IF NOT EXISTS movielens_links (
                                 ml_movie_id INT PRIMARY KEY, # (PK) MovieLens movieId (FK to movies.tmdb_id)
                                 imdb_id     VARCHAR(12), # IMDB id constructed from links' numeric imdbId.
                                 tmdb_id     INT, # TMDB id for the same movie

                                 KEY idx_links_tmdb (tmdb_id),
                                 CONSTRAINT fk_links_movie FOREIGN KEY (tmdb_id) REFERENCES movies(tmdb_id)
            ) ENGINE=InnoDB
            """.strip()
        # [10] ratings -------------------------------------------------------------------------------------------------
        # user ratings from MovieLens
        tables["ratings"] = """
            CREATE TABLE IF NOT EXISTS ratings (
                         user_id     INT NOT NULL, # MovieLens user id 
                         ml_movie_id INT NOT NULL, # MovieLens movie id (FK to movielens.ml_movie_id)
                         rating      DECIMAL(2,1) NOT NULL, # rating value (e.g. 3.5)
                         ts_unix     INT NOT NULL, # timestamp (unix seconds)

                PRIMARY KEY (user_id, ml_movie_id),
                KEY idx_ratings_movie (ml_movie_id),
                CONSTRAINT fk_ratings_link FOREIGN KEY (ml_movie_id) REFERENCES movielens_links(ml_movie_id)
            ) ENGINE=InnoDB
            """.strip()

        i = 0
        for table_name, table_creation_stmt in tables.items():
            try:
                i += 1
                print(f"Creating table [{i}/10]: {table_name}")
                cursor.execute(table_creation_stmt)
            except mysql.connector.Error as err:
                print(f"Error while creating {table_name}")
                raise Exception(str(err))

        print("Done: database + schema created.")

    finally:
        cursor.close()
        con.close()


if __name__ == "__main__":
    main()