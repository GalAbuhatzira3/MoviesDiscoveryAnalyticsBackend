import argparse
from decimal import Decimal
from sys import float_repr_style

import mysql.connector
import queries_db_script as q

# DB connection settings (fill these once; no CLI args needed)
MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 3307
MYSQL_USER = "abuhatzira"
MYSQL_PASSWORD = "123123"
DB_NAME = "abuhatzira"


# command-line runner: the user chooses --q 1...5, it connects to MySQL, call the matching function
# from queries_db_script.py, then prints the result in a readable table using pandas.


# The function decides the column headers based on which query was run and the tuple length:
def columns_for_query(qnum: int, rows):
    # Define column headers per query (adjusted by row length when needed)
    if not rows:
        return []

    n = len(rows[0])  # the number of attributes (columns)
    if qnum == 1:
        return ["TMDB movie id", "Movie title", "Release year", "Overview", "Vote average", "Relevance score"]
    if qnum == 2:
        return ["TMDB movie id", "Movie title", "Release year", "Overview", "Vote average", "Matched keywords"][:n]
    if qnum == 3:
        return ["Actor id", "Actor name", "Profile", "Number of distinct genres", "Number of movies"][:n]
    if qnum == 4:
        return ["TMDB movie id", "Movie title", "Release year", "MovieLens average ratings",
                "MovieLens number of reviews", "TMDB Vote average", "TMDB number of reviews", "Gap"][:n]
    if qnum == 5:
        return ["Person id", "Name", "Profile", "Movies count"][:n]
    if qnum == 6:
        return ["User id", "MovieLens movie id", "New rating", "Time stamp"][:n]
    return None  # never gets here anyway


# outputs the rows as pandas
def print_rows_as_table(qnum: int, rows, max_str: int):
    # no results:
    if not rows:
        print("No results.")
        return
    # getting the columns name from the helper function:
    cols = columns_for_query(qnum, rows)

    # converts each value to a clean string:
    def clean(value):
        if isinstance(value, Decimal):
            value = float(value)
        if value is None:
            s = ""
        elif isinstance(value, float):
            s = f"{value:.6f}".rstrip("0").rstrip(".")
        else:
            s = str(value)
        # Truncate long text fields (overview/tagline)
        if max_str and isinstance(s, str) and len(s) > max_str:
            s = s[:max_str] + "…"
        return s

    # Convert all cells to strings
    data = [[clean(value) for value in row] for row in rows]
    # Compute column widths
    widths = []
    for i, column in enumerate(cols):
        width = len(column)
        for row in data:
            if i < len(row):
                width = max(width, len(row[i]))
        widths.append(width)
    # Print header
    header = "  ".join(f"{cols[i]:<{widths[i]}}" for i in range(len(cols)))
    print(header)
    print("-" * len(header))
    # Print rows
    for row in data:
        line = "  ".join(f"{row[i]:<{widths[i]}}" for i in range(len(cols)))
        print(line)


def dispatch_query(args, con):
    try:
        if args.q == 1:
            if args.example:
                print("Example query 1: py src\queries_execution.py --q 1 --text 'America' --limit 10")
                rows = q.query_1(con, text="America", limit=10)
            else:
                rows = q.query_1(con, args.text, args.limit)
        elif args.q == 2:
            if args.example:
                print(
                    "Example query 2: py src\queries_execution.py --q 2 --text '+(love tragedy musical) -secret' --limit 10")
                rows = q.query_2(con, keyword_text='+(love tragedy musical) -secret', limit=10)
            else:
                rows = q.query_2(con, args.text, args.limit)
        elif args.q == 3:
            if args.example:
                print("Example query 3: py src\queries_execution.py --q 3 --min-movies 30 --limit 15")
                rows = q.query_3(con, min_movies=30, limit=15)
            else:
                rows = q.query_3(con, args.min_movies, args.limit)
        elif args.q == 4:
            if args.example:
                print("Example query 4: py src\queries_execution.py --q 4 --min-ratings 100 --limit 15")
                rows = q.query_4(con, min_ratings=100, limit=15)
            else:
                rows = q.query_4(con, args.min_ratings, args.limit)
        elif args.q == 5:
            if args.example:
                print("Example query 5: py src\queries_execution.py --q 5 --genre 'Western' --min-movies 20 --limit 15")
                rows = q.query_5(con, genre_name="Western", min_movies=20, limit=15)
            else:
                rows = q.query_5(con, args.genre, args.min_movies, args.limit)
        elif args.q == 6:
            if args.example:
                print("Example query 5: py src\queries_execution.py --q 6 --user-id 7 --ml-movie-id 10 --new-rating 4")
                rows = q.query_6(con, user_id=7, ml_movie_id=10, new_rating=4)
            else:
                rows = q.query_6(con, args.user_id, args.ml_movie_id, args.new_rating)
        else:
            raise SystemExit("q must be one of 1,2,3,4,5")
        print_rows_as_table(args.q, rows, args.max_str)
    finally:
        con.close()


def parse_rating(rating: str):
    value = float(rating)
    # MovieLens rating can only be between 0.5 and 5
    if value < 0.5 or value > 5.0:
        raise argparse.ArgumentTypeError("rating must be between 0.5 and 5.0 (MovieLens scale)")
    # allow only 0.5 steps
    if (value * 2) % 1 != 0:
        raise argparse.ArgumentTypeError("rating must be in steps of 0.5 (e.g., 3.5, 4, 4.5)")
    return value


def parse_pos_int(x: str):
    value = int(x)
    if value <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return value


def main():
    # Argument parsing:
    argument_parser = argparse.ArgumentParser()
    # DB connection:
    argument_parser.add_argument("--host", default=MYSQL_HOST)
    argument_parser.add_argument("--port", type=int, default=MYSQL_PORT)
    argument_parser.add_argument("--user", default=MYSQL_USER)
    argument_parser.add_argument("--db_name", default=DB_NAME)
    argument_parser.add_argument("--db-pass", default=MYSQL_PASSWORD)
    # Queries 1-5 arguments:
    argument_parser.add_argument("--q", type=int, required=True)
    argument_parser.add_argument("--text", default="love")
    argument_parser.add_argument("--genre", default="Horror")
    argument_parser.add_argument("--limit", type=int, default=10)
    argument_parser.add_argument("--min-movies", type=int, default=5)
    argument_parser.add_argument("--min-ratings", type=int, default=30)
    argument_parser.add_argument("--max-str", type=int, default=120)  # max length of strings
    # Query 6 (update):
    argument_parser.add_argument("--user-id", type=parse_pos_int, default=1)
    argument_parser.add_argument("--ml-movie-id", type=parse_pos_int, default=31)
    argument_parser.add_argument("--new-rating", type=parse_rating, default=1)
    # Example Queries:
    argument_parser.add_argument("--example", action="store_true", help="dispatch example query")
    args = argument_parser.parse_args()

    # connection:
    con = mysql.connector.connect(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.db_pass,
        database=args.db_name,
        autocommit=False
    )

    # query dispatch:
    dispatch_query(args, con)


if __name__ == "__main__":
    main()
