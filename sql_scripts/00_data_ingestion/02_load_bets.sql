-- SQL commands go here
CREATE TABLE customer_analysis_se.combined_bets AS
SELECT cb.*
FROM combined_bets cb
JOIN customer_analysis_se.swedish_customers sc ON cb.customer_id = sc.customer_id;
