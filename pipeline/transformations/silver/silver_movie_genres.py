from pyspark import pipelines as dp
from pyspark.sql import functions as F


# Fixed MovieLens genre list (same 18 genres the Separate_Genre notebook discovers dynamically).
# .collect() is not allowed in SDP, so the list is hardcoded here.
_GENRES = [
    "Action", "Adventure", "Animation", "Children's", "Comedy", "Crime",
    "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror", "Musical",
    "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western",
]


@dp.materialized_view(
    comment="Silver: movie genre one-hot encoding with release year extracted from title."
)
def silver_movie_genres():
    movie_df = spark.read.table("bronze_movies")

    # Build one boolean column per genre
    movies_genre_df = movie_df
    for genre in _GENRES:
        col_name = f"genre_{genre.replace('-', '_').replace(' ', '_').lower()}"
        movies_genre_df = movies_genre_df.withColumn(
            col_name,
            F.array_contains(F.split(F.col("genres"), "\\|"), genre),
        )

    # Drop the original string column since each genre now has its own column
    movies_genre_df = movies_genre_df.drop("genres")

    # Extract the 4-digit year in parentheses at the end of the title, e.g. "Movie Name (2002)"
    movies_genre_df = movies_genre_df.withColumn(
        "release_year",
        F.regexp_extract(F.col("title"), "\\((\\d{4})\\)\\s*$", 1).cast("int"),
    )

    # Clean the year out of the title, leaving just the movie name
    movies_genre_df = movies_genre_df.withColumn(
        "title",
        F.regexp_replace(F.col("title"), "\\s*\\(\\d{4}\\)\\s*$", ""),
    )

    return movies_genre_df
