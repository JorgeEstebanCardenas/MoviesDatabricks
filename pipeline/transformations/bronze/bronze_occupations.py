from pyspark import pipelines as dp


# Occupation IDs map to this list (index = occupation_id) from the MovieLens dataset
_OCCUPATION_LIST = [
    "other",                    # 0
    "academic/educator",        # 1
    "artist",                   # 2
    "clerical/admin",           # 3
    "college/grad student",     # 4
    "customer service",         # 5
    "doctor/health care",       # 6
    "executive/managerial",     # 7
    "farmer",                   # 8
    "homemaker",                # 9
    "K-12 student",             # 10
    "lawyer",                   # 11
    "programmer",               # 12
    "retired",                  # 13
    "sales/marketing",          # 14
    "scientist",                # 15
    "self-employed",            # 16
    "technician/engineer",      # 17
    "tradesman/craftsman",      # 18
    "unemployed",               # 19
    "writer",                   # 20
]


@dp.materialized_view(
    comment="Bronze: MovieLens occupation lookup (index = occupation_id)."
)
def bronze_occupations():
    rows = [(i, name) for i, name in enumerate(_OCCUPATION_LIST)]
    return spark.createDataFrame(
        rows, "occupation_id INT, occupation_name STRING"
    )
