CREATE TABLE customer_analysis_se.swedish_customer_background AS
SELECT *
FROM customer_background
WHERE country_code = 'SE';
