SELECT
 last_table.user_id,
 last_table.tweet_date,
 ROUND(SUM(last_table.tweets) OVER(PARTITION BY last_table.user_id ORDER BY tweet_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)/ SUM(last_table.rep) OVER(PARTITION BY last_table.user_id ORDER BY tweet_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS runningavg
FROM (
 WITH
   final_table AS (
   SELECT
     user_id,
     tweet_date,
     COUNT(tweet_id) AS tweets
   FROM
     tweets
   GROUP BY
     user_id,
     tweet_date
   ORDER BY
     tweet_date)
 SELECT
   final_table.user_id,
   final_table.tweet_date,
   final_table.tweets,
   CASE
     WHEN final_table.tweet_date IS NOT NULL THEN 1
 END
   AS rep
 FROM
   final_table)last_table