DROP TABLE IF EXISTS moviesInfo;
CREATE EXTERNAL TABLE new_moviesInfo (
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


WITH best_movies_per_year AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY m.releaseyear ORDER BY m.rating_average DESC) AS rank
    FROM moviesInfo m
)
SELECT *
FROM best_movies_per_year
WHERE rank = 1
ORDER BY releaseyear;

WITH best_movies_per_genre AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY m.genre ORDER BY m.rating_average DESC) AS rank
    FROM moviesInfo m
)
SELECT *
FROM best_movies_per_genre
WHERE rank = 1
ORDER BY genre;

WITH best_action_movies_per_year AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY m.releaseyear ORDER BY m.rating_average DESC) AS rank
    FROM moviesInfo m
    WHERE m.genre LIKE '%Action%')
SELECT *
FROM best_action_movies_per_year
WHERE rank = 1
ORDER BY releaseyear;

WITH best_romance_movies_per_year AS (
    SELECT*, ROW_NUMBER() OVER (PARTITION BY m.releaseyear ORDER BY m.rating_average DESC) AS rank
    FROM moviesInfo m
    WHERE m.genre LIKE '%Romance%'
)
SELECT *
FROM best_romance_movies_per_year
WHERE rank = 1
ORDER BY releaseyear;
