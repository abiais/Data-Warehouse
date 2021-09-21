-- Here are some example queries that could be run on the database 
 
-- Top 10 artists by number of songplays

SELECT
	dim_artist.name,
    count(fact_songplay.id)
FROM
	fact_songplay
JOIN
	dim_artist
    ON fact_songplay.artist_id = dim_artist.id
GROUP BY
	dim_artist.name
LIMIT 10

-- Year of origin of the most played song

SELECT
    dim_song.year,
    count(fact_songplay.id)
FROM
	fact_songplay
JOIN
	dim_song
    ON fact_songplay.song_id = dim_song.id
GROUP BY
	dim_song.year
LIMIT 1

-- Average number of songplay per weekday

SELECT
	dim_time.weekday,
  	count(fact_songplay.id)::FLOAT/count(distinct dim_time.day) AS average_nb_of_sonplays_per_dow
FROM	
  	fact_songplay
JOIN
  	dim_time
  	ON fact_songplay.start_time = dim_time.start_time
GROUP BY
  	dim_time.weekday
ORDER BY
  	weekday