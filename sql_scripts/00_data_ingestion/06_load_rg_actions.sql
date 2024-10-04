CREATE TABLE customer_analysis_se.rg_actions AS
SELECT ra.*
FROM rg_actions ra
JOIN customer_analysis_se.swedish_customers sc ON ra.customer_id = sc.customer_id;
