CREATE TEMP TABLE max_timestamp_per_label AS
SELECT 
    customer_id, 
    label_date,
    MAX(timestamp) AS max_timestamp
FROM 
    customer_analysis_se.combined_bets_train
GROUP BY 
    customer_id, label_date;

CREATE TABLE customer_analysis_se.bets_data_excl_90d AS
SELECT 
    a.*
FROM 
    customer_analysis_se.combined_bets_train a
INNER JOIN 
    max_timestamp_per_label b 
ON 
    a.customer_id = b.customer_id AND a.label_date = b.label_date
WHERE 
    a.timestamp <= b.max_timestamp - INTERVAL '90 days';
