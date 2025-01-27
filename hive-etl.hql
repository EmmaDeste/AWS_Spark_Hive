/* LOAD CORRECT FILE FROM S3 BUCKET */

CREATE EXTERNAL TABLE moviesInfo (
    movieId INT,
    title STRING,
    releaseyear INT,
    genre STRING,
    rating_count INT,
    rating_average DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://dai-2025-paris/deste/output/movielens/moviesInfoDf.csv/';
