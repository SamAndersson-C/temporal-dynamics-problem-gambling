CREATE TABLE customer_analysis_se.multiple_accounts AS
SELECT ma.*
FROM multiple_accounts ma
JOIN customer_analysis_se.swedish_customers sc ON ma.customer_id = sc.customer_id;
