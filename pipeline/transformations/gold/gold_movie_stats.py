from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


@dp.materialized_view(
    comment="Gold: per-movie stats — avg rating, avg age, and top occupation by avg rating."
)
def gold_movie_stats():
    ratings_df = spark.read.table("bronze_ratings")
    user_df = spark.read.table("bronze_users")
    movies_df = spark.read.table("bronze_movies")
    occupation_df = spark.read.table("bronze_occupations")

    # Join ratings + users + occupations + movies
    joined = (
        ratings_df
        .join(user_df, on="userId", how="inner")
        .join(occupation_df, F.col("occupation") == F.col("occupation_id"), how="left")
        .join(movies_df.select("movieId", "title", "genres"), on="movieId", how="inner")
    )

    # 1. Average rating and average age per movie
    movie_stats = (
        joined.groupBy("movieId", "title", "genres")
        .agg(
            F.round(F.avg("rating"), 2).alias("avg_rating"),
            F.round(F.avg("age"), 1).alias("avg_age"),
        )
    )

    # 2. Occupation that rated each movie the highest (by avg rating)
    occ_movie = (
        joined.groupBy("movieId", "occupation_name")
        .agg(F.avg("rating").alias("occ_avg_rating"))
    )

    w = Window.partitionBy("movieId").orderBy(F.desc("occ_avg_rating"))
    top_occupation = (
        occ_movie
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .select(
            "movieId",
            F.col("occupation_name").alias("top_occupation"),
            F.round(F.col("occ_avg_rating"), 2).alias("top_occupation_avg_rating"),
        )
    )

    # Final: one row per movie
    movie_final = movie_stats.join(top_occupation, on="movieId", how="left")

    return movie_final
