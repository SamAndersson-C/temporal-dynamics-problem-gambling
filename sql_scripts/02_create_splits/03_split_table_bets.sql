-- Create a table for training data including records up to June 1, 2022

CREATE TABLE customer_analysis_se.combined_bets_train AS
SELECT *
FROM customer_analysis_se.vw_combined_bets_with_labels
WHERE label_date <= '2022-06-01' AND timestamp <= '2022-06-01';

-- Create a table for testing data with records after June 1, 2022

CREATE TABLE customer_analysis_se.combined_bets_test AS
SELECT *
FROM customer_analysis_se.vw_combined_bets_with_labels
WHERE label_date > '2022-06-01' AND timestamp > '2022-06-01';
