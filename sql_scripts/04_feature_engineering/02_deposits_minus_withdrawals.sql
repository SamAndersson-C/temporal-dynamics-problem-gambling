ALTER TABLE customer_analysis_se.combined_transactions_train
ADD COLUMN net_balance NUMERIC;

UPDATE customer_analysis_se.combined_transactions_train
SET net_balance = total_deposits - total_withdrawals;

-- Convert label_date to an integer if needed
ALTER TABLE customer_analysis_se.combined_transactions_train
ADD COLUMN label_date_numeric INT;

UPDATE customer_analysis_se.combined_transactions_train
SET label_date_numeric = EXTRACT(EPOCH FROM label_date) / 86400; -- days since Unix epoch

ALTER TABLE customer_analysis_se.combined_transactions_train
ADD COLUMN net_balance_trend NUMERIC;

WITH regression_data AS (
    SELECT 
        customer_id,
        COUNT(*) AS n,
        SUM(label_date_numeric) AS sum_x,
        SUM(net_balance) AS sum_y,
        SUM(label_date_numeric * net_balance) AS sum_xy,
        SUM(label_date_numeric * label_date_numeric) AS sum_xx
    FROM 
        customer_analysis_se.combined_transactions_train
    GROUP BY 
        customer_id
)
UPDATE customer_analysis_se.combined_transactions_train ct
SET net_balance_trend = rd.slope
FROM (
    SELECT 
        customer_id,
        CASE 
            WHEN (n * sum_xx - sum_x * sum_x) = 0 THEN NULL
            ELSE (n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x * sum_x)
        END AS slope
    FROM 
        regression_data
) rd
WHERE 
    ct.customer_id = rd.customer_id;

-- test

ALTER TABLE customer_analysis_se.combined_transactions_test
ADD COLUMN net_balance NUMERIC;

UPDATE customer_analysis_se.combined_transactions_test
SET net_balance = total_deposits - total_withdrawals;





-- Convert label_date to an integer if needed
ALTER TABLE customer_analysis_se.combined_transactions_test
ADD COLUMN label_date_numeric INT;

UPDATE customer_analysis_se.combined_transactions_test
SET label_date_numeric = EXTRACT(EPOCH FROM label_date) / 86400; -- days since Unix epoch


ALTER TABLE customer_analysis_se.combined_transactions_test
ADD COLUMN net_balance_trend NUMERIC;

WITH regression_data AS (
    SELECT 
        customer_id,
        COUNT(*) AS n,
        SUM(label_date_numeric) AS sum_x,
        SUM(net_balance) AS sum_y,
        SUM(label_date_numeric * net_balance) AS sum_xy,
        SUM(label_date_numeric * label_date_numeric) AS sum_xx
    FROM 
        customer_analysis_se.combined_transactions_test
    GROUP BY 
        customer_id
)
UPDATE customer_analysis_se.combined_transactions_test ct
SET net_balance_trend = rd.slope
FROM (
    SELECT 
        customer_id,
        CASE 
            WHEN (n * sum_xx - sum_x * sum_x) = 0 THEN NULL
            ELSE (n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x * sum_x)
        END AS slope
    FROM 
        regression_data
) rd
WHERE 
    ct.customer_id = rd.customer_id;
