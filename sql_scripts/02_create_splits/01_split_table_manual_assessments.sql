--train labels
CREATE TABLE customer_analysis_se.manual_assessments_train AS
SELECT *
FROM customer_analysis_se.manual_assessments
WHERE date <= '2022-06-01';

--test labels
CREATE TABLE customer_analysis_se.manual_assessments_test AS
SELECT *
FROM customer_analysis_se.manual_assessments
WHERE date > '2022-06-01';
