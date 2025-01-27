import spark as spark

# 1 - EXTRACT

movies_path = "s3://nahle-bucket-datalake/movielens/input/movies/movies.csv"
moviesDf = spark.read.csv(movies_path, header=True, inferSchema=True)

ratings_path = "s3:// nahle-bucket-datalake/movielens/input/ratings/ratings.csv"
ratingsDf = spark.read.csv(ratings_path, header=True, inferSchema=True)

# 2 - TRANSFORM

moviesInfoDf = moviesDf.select("movieId", "title", "genres")

from pyspark.sql.functions import regexp_extract
moviesInfoDf = moviesInfoDf.withColumn("releaseYear", regexp_extract("title", r"\((\d{4})\)", 1))

from pyspark.sql.functions import split, explode
moviesInfoDf = (moviesInfoDf
                .withColumn("genresArray", split("genres", r"\|"))
                .select("movieId", "title", "releaseYear", "genresArray"))

moviesInfoDf = (moviesInfoDf
                .withColumn("Genre", explode("genresArray"))
                .select("movieId", "title", "releaseYear", "Genre"))

ratings_count = ratingsDf.groupBy("movieId").count()
moviesInfoDf = moviesInfoDf.join(ratings_count, on="movieId", how="left")

from pyspark.sql.functions import sum, col
rating_sum = ratingsDf.groupBy("movieId").agg(sum("rating").alias("total_rating"))
moviesInfoDf = moviesInfoDf.join(rating_sum, on="movieId", how="left")
moviesInfoDf = moviesInfoDf.withColumn("averageRating", col("total_rating") / col("count"))
moviesInfoDf = moviesInfoDf.select("movieId", "title", "releaseYear", "Genre", "count", "averageRating")

moviesInfoDf = moviesInfoDf.filter(moviesInfoDf["count"] >= 500)

moviesInfoDf = moviesInfoDf.dropna()

moviesInfoDf = moviesInfoDf.filter(moviesInfoDf["Genre"] != '(no genres listed)')

# 3 - LOAD

moviesInfoDf.write.mode("overwrite").option("header", "true").csv("s3://dai-2025-paris/deste/output/movielens/moviesInfoDf.csv")
