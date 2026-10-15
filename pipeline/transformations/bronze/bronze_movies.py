from pyspark import pipelines as dp


@dp.materialized_view(
    comment="Bronze: raw MovieLens movies loaded from the archived CSV."
)
def bronze_movies():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/Workspace/Users/jorgestebance@gmail.com/MoviesDatabricks/data/archive/movies.csv")
    )
