-- SQL commands go here
CREATE TABLE customer_analysis_se.combined_payments AS
SELECT cp.*
FROM combined_payments cp
JOIN customer_analysis_se.swedish_customers sc ON cp.customer_id = sc.customer_id;
