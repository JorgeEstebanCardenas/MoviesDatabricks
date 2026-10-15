from pyspark import pipelines as dp


@dp.materialized_view(
    comment="Bronze: raw MovieLens users loaded from the archived CSV."
)
def bronze_users():
    return (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/Workspace/Users/jorgestebance@gmail.com/MoviesDatabricks/data/archive/users.csv")
    )
