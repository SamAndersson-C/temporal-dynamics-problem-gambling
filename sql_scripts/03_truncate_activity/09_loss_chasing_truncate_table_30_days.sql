-- Create a temporary table to store the maximum period end timestamp for each customer and label date

CREATE TEMP TABLE max_timestamp_per_label AS
SELECT 
    customer_id, 
    label_date,
    MAX(period_end) AS max_timestamp
FROM 
    customer_analysis_se.combined_loss_chasing_train
GROUP BY 
    customer_id, label_date;

-- Create a table to store loss chasing data excluding records within 30 days of the maximum period end timestamp

CREATE TABLE customer_analysis_se.loss_chasing_data_excl_30d AS
SELECT 
    a.*
FROM 
    customer_analysis_se.combined_loss_chasing_train a
INNER JOIN 
    max_timestamp_per_label b 
ON 
    a.customer_id = b.customer_id AND a.label_date = b.label_date
WHERE 
    a.period_end <= b.max_timestamp - INTERVAL '30 days';
