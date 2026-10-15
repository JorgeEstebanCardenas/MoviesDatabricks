from pyspark import pipelines as dp
from pyspark.sql import functions as F


# Genre column names in silver_movie_genres and their display labels.
# .collect() is not allowed in SDP, so the list is hardcoded.
_GENRE_COLS = [
    ("genre_action", "Action"),
    ("genre_adventure", "Adventure"),
    ("genre_animation", "Animation"),
    ("genre_children's", "Children's"),
    ("genre_comedy", "Comedy"),
    ("genre_crime", "Crime"),
    ("genre_documentary", "Documentary"),
    ("genre_drama", "Drama"),
    ("genre_fantasy", "Fantasy"),
    ("genre_film_noir", "Film-Noir"),
    ("genre_horror", "Horror"),
    ("genre_musical", "Musical"),
    ("genre_mystery", "Mystery"),
    ("genre_romance", "Romance"),
    ("genre_sci_fi", "Sci-Fi"),
    ("genre_thriller", "Thriller"),
    ("genre_war", "War"),
    ("genre_western", "Western"),
]


@dp.materialized_view(
    comment="Gold: count of movies per genre (unpivoted from silver_movie_genres one-hot columns)."
)
def gold_genre_counts():
    movie_df = spark.read.table("silver_movie_genres")

    col_names = [c for c, _ in _GENRE_COLS]
    agg_exprs = [F.sum(F.col(c).cast("int")).alias(c) for c in col_names]
    wide_counts_df = movie_df.agg(*agg_exprs)

    genre_counts_df = (
        wide_counts_df
        .unpivot([], col_names, "genre", "count")
        .withColumn("count", F.col("count").cast("int"))
        .orderBy(F.desc("count"))
    )

    return genre_counts_df
