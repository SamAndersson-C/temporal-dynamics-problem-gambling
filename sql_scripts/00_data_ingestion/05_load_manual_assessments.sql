CREATE TABLE customer_analysis_se.manual_assessments AS
SELECT ma.*
FROM manual_assessments ma
JOIN customer_analysis_se.swedish_customers sc ON ma.customer_id = sc.customer_id;
