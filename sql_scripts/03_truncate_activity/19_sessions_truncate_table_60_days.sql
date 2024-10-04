CREATE TABLE customer_analysis_se.sessions_data_excl_60d AS
SELECT 
    a.*
FROM 
    customer_analysis_se.combined_sessions_train a
INNER JOIN 
    (SELECT customer_id, label_date, MAX(timestamp) AS max_timestamp
     FROM customer_analysis_se.combined_sessions_train
     GROUP BY customer_id, label_date) b 
ON 
    a.customer_id = b.customer_id AND a.label_date = b.label_date
WHERE 
    a.timestamp <= b.max_timestamp - INTERVAL '60 days';
