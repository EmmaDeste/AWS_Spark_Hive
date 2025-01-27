from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Read CSV from S3") \
    .config("spark.hadoop.fs.s3a.access.key", "<your-access-key>") \
    .config("spark.hadoop.fs.s3a.secret.key", "<your-secret-key>") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

moviesInfoDf = spark.read.csv("s3://dai-2025-paris/deste/output/movielens/moviesInfoDf.csv",
                              header=True, inferSchema=True)

# ########################### DATA ANALYSIS #######################################

clean_movies_path = "s3://dai-2025-paris/deste/output/movielens/moviesInfoDf.csv/"
movies_df = spark.read.csv(clean_movies_path, header=True, inferSchema=True)

from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
releaseYear_window_descRating = Window.partitionBy("releaseYear").orderBy(col("averageRating").desc())
ranked_movies = movies_df.withColumn("rank", row_number().over(releaseYear_window_descRating))
best_movies_per_year = ranked_movies.filter(col("rank") == 1)
best_movies_per_year.show()

genre_window_descRating = Window.partitionBy("Genre").orderBy(col("averageRating").desc())
ranked_movies = movies_df.withColumn("rank", row_number().over(genre_window_descRating))
best_movies_per_genre = ranked_movies.filter(col("rank") == 1)
best_movies_per_genre.show()

action_movies_df = movies_df.filter(movies_df.Genre == "Action")
ranked_action_movies = action_movies_df.withColumn("rank", row_number().over(releaseYear_window_descRating))
best_action_movies_per_year = ranked_action_movies.filter(col("rank") == 1)
best_action_movies_per_year.show()

romance_movies_df = movies_df.filter(movies_df.Genre == "Romance")
ranked_romance_movies = romance_movies_df.withColumn("rank", row_number().over(releaseYear_window_descRating))
best_romance_movies_per_year = ranked_romance_movies.filter(col("rank") == 1)
best_romance_movies_per_year.show()
