from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    comment="Gold: per-user rating stats with demographics and occupation name."
)
def gold_user_stats():
    ratings_df = spark.read.table("bronze_ratings")
    user_df = spark.read.table("bronze_users")
    occupation_df = spark.read.table("bronze_occupations")

    user_stats_df = (
        ratings_df
        .groupBy("userId")
        .agg(
            F.round(F.avg("rating"), 2).alias("avg_rating"),
            F.count("rating").alias("num_ratings"),
        )
    )

    user_stats_df = (
        user_stats_df
        .join(user_df, on="userId", how="left")
        .join(
            occupation_df,
            F.col("occupation") == F.col("occupation_id"),
            how="left",
        )
        .select(
            "userId",
            "avg_rating",
            "num_ratings",
            "age",
            "gender",
            F.col("occupation_name").alias("occupation"),
        )
    )

    return user_stats_df
