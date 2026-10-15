from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    comment="Gold: number of movies released per year."
)
def gold_movies_per_year():
    movie_df = spark.read.table("silver_movie_genres")

    return (
        movie_df
        .groupBy("release_year")
        .count()
        .withColumnRenamed("count", "movie_count")
        .orderBy(F.desc("release_year"))
    )
