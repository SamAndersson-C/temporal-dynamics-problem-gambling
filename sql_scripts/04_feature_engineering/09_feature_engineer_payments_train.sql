-- Create or replace a view to consolidate payment data, filtered to only include transactions up to the label date

CREATE OR REPLACE VIEW customer_analysis_se.vw_combined_payments_features AS
SELECT 
    p.customer_id,
    p.timestamp_min,
    p.timestamp_max,
    p.deposit_num,
    p.deposit_sum,
    p.deposit_approved_num,
    p.deposit_approved_sum,
    p.deposit_canceled_num,
    p.deposit_canceled_sum,
    p.deposit_denied_num,
    p.deposit_denied_sum,
    p.deposit_failed_num,
    p.deposit_failed_sum,
    p.withdrawal_num,
    p.withdrawal_sum,
    p.withdrawal_approved_num,
    p.withdrawal_approved_sum,
    p.withdrawal_canceled_num,
    p.withdrawal_canceled_sum,
    p.withdrawal_denied_num,
    p.withdrawal_denied_sum,
    m.date AS label_date
FROM 
    customer_analysis_se.combined_payments_train p
INNER JOIN 
    customer_analysis_se.manual_assessments_train m ON p.customer_id = m.customer_id
WHERE 
    p.timestamp_max <= m.date;


CREATE TABLE customer_analysis_se.combined_payments_features_aggregated AS
WITH payment_quantiles AS (
    SELECT
        customer_id,
        label_date,
        
        -- Quantiles for deposit-related metrics
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY deposit_num) AS deposit_num_p25,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY deposit_num) AS deposit_num_p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY deposit_num) AS deposit_num_p75,
        
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY deposit_sum) AS deposit_sum_p25,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY deposit_sum) AS deposit_sum_p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY deposit_sum) AS deposit_sum_p75,
        
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY deposit_approved_num) AS deposit_approved_num_p25,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY deposit_approved_num) AS deposit_approved_num_p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY deposit_approved_num) AS deposit_approved_num_p75,

        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY deposit_approved_sum) AS deposit_approved_sum_p25,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY deposit_approved_sum) AS deposit_approved_sum_p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY deposit_approved_sum) AS deposit_approved_sum_p75,

        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY deposit_canceled_num) AS deposit_canceled_num_p25,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY deposit_canceled_num) AS deposit_canceled_num_p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY deposit_canceled_num) AS deposit_canceled_num_p75,

        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY deposit_canceled_sum) AS deposit_canceled_sum_p25,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY deposit_canceled_sum) AS deposit_canceled_sum_p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY deposit_canceled_sum) AS deposit_canceled_sum_p75,

        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY deposit_denied_num) AS deposit_denied_num_p25,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY deposit_denied_num) AS deposit_denied_num_p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY deposit_denied_num) AS deposit_denied_num_p75,

        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY deposit_denied_sum) AS deposit_denied_sum_p25,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY deposit_denied_sum) AS deposit_denied_sum_p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY deposit_denied_sum) AS deposit_denied_sum_p75,

        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY deposit_failed_num) AS deposit_failed_num_p25,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY deposit_failed_num) AS deposit_failed_num_p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY deposit_failed_num) AS deposit_failed_num_p75,

        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY deposit_failed_sum) AS deposit_failed_sum_p25,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY deposit_failed_sum) AS deposit_failed_sum_p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY deposit_failed_sum) AS deposit_failed_sum_p75,

        -- Quantiles for withdrawal-related metrics
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY withdrawal_num) AS withdrawal_num_p25,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY withdrawal_num) AS withdrawal_num_p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY withdrawal_num) AS withdrawal_num_p75,

        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY withdrawal_sum) AS withdrawal_sum_p25,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY withdrawal_sum) AS withdrawal_sum_p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY withdrawal_sum) AS withdrawal_sum_p75,

        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY withdrawal_approved_num) AS withdrawal_approved_num_p25,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY withdrawal_approved_num) AS withdrawal_approved_num_p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY withdrawal_approved_num) AS withdrawal_approved_num_p75,

        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY withdrawal_canceled_num) AS withdrawal_canceled_num_p25,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY withdrawal_canceled_num) AS withdrawal_canceled_num_p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY withdrawal_canceled_num) AS withdrawal_canceled_num_p75,

        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY withdrawal_canceled_sum) AS withdrawal_canceled_sum_p25,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY withdrawal_canceled_sum) AS withdrawal_canceled_sum_p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY withdrawal_canceled_sum) AS withdrawal_canceled_sum_p75
        -- Repeat for other withdrawal metrics

    FROM customer_analysis_se.vw_combined_payments_features
    GROUP BY customer_id, label_date
),
payment_stats AS (
    SELECT
        customer_id,
        label_date,
        
        -- Average and Standard Deviation for deposit-related metrics
        AVG(deposit_num) AS avg_deposit_num,
        STDDEV(deposit_num) AS stddev_deposit_num,
        AVG(deposit_sum) AS avg_deposit_sum,
        STDDEV(deposit_sum) AS stddev_deposit_sum,
        AVG(deposit_approved_num) AS avg_deposit_approved_num,
        STDDEV(deposit_approved_num) AS stddev_deposit_approved_num,
        AVG(deposit_approved_sum) AS avg_deposit_approved_sum,
        STDDEV(deposit_approved_sum) AS stddev_deposit_approved_sum,
        AVG(deposit_canceled_num) AS avg_deposit_canceled_num,
        STDDEV(deposit_canceled_num) AS stddev_deposit_canceled_num,
        AVG(deposit_canceled_sum) AS avg_deposit_canceled_sum,
        STDDEV(deposit_canceled_sum) AS stddev_deposit_canceled_sum,
        AVG(deposit_denied_num) AS avg_deposit_denied_num,
        STDDEV(deposit_denied_num) AS stddev_deposit_denied_num,
        AVG(deposit_denied_sum) AS avg_deposit_denied_sum,
        STDDEV(deposit_denied_sum) AS stddev_deposit_denied_sum,
        AVG(deposit_failed_num) AS avg_deposit_failed_num,
        STDDEV(deposit_failed_num) AS stddev_deposit_failed_num,
        AVG(deposit_failed_sum) AS avg_deposit_failed_sum,
        STDDEV(deposit_failed_sum) AS stddev_deposit_failed_sum,
        
        -- Averages and Standard Deviations for withdrawal metrics
        AVG(withdrawal_num) AS avg_withdrawal_num,
        STDDEV(withdrawal_num) AS stddev_withdrawal_num,
        AVG(withdrawal_sum) AS avg_withdrawal_sum,
        STDDEV(withdrawal_sum) AS stddev_withdrawal_sum,
        AVG(withdrawal_approved_num) AS avg_withdrawal_approved_num,
        STDDEV(withdrawal_approved_num) AS stddev_withdrawal_approved_num,
        AVG(withdrawal_approved_sum) AS avg_withdrawal_approved_sum,
        STDDEV(withdrawal_approved_sum) AS stddev_withdrawal_approved_sum,
        AVG(withdrawal_canceled_num) AS avg_withdrawal_canceled_num,
        STDDEV(withdrawal_canceled_num) AS stddev_withdrawal_canceled_num,
        AVG(withdrawal_canceled_sum) AS avg_withdrawal_canceled_sum,
        STDDEV(withdrawal_canceled_sum) AS stddev_withdrawal_canceled_sum,
        AVG(withdrawal_denied_num) AS avg_withdrawal_denied_num,
        STDDEV(withdrawal_denied_num) AS stddev_withdrawal_denied_num,
        AVG(withdrawal_denied_sum) AS avg_withdrawal_denied_sum,
        STDDEV(withdrawal_denied_sum) AS stddev_withdrawal_denied_sum,
        
        -- Calculate session duration variability
        AVG(EXTRACT(EPOCH FROM (timestamp_max - timestamp_min))) AS avg_session_duration,
        STDDEV(EXTRACT(EPOCH FROM (timestamp_max - timestamp_min))) AS stddev_session_duration
        
    FROM customer_analysis_se.vw_combined_payments_features
    GROUP BY customer_id, label_date
)
SELECT 
    ps.customer_id,
    ps.label_date,
    ps.avg_deposit_num,
    ps.stddev_deposit_num,
    ps.avg_deposit_sum,
    ps.stddev_deposit_sum,
    ps.avg_deposit_approved_num,
    ps.stddev_deposit_approved_num,
    ps.avg_deposit_approved_sum,
    ps.stddev_deposit_approved_sum,
    ps.avg_deposit_canceled_num,
    ps.stddev_deposit_canceled_num,
    ps.avg_deposit_canceled_sum,
    ps.stddev_deposit_canceled_sum,
    ps.avg_deposit_denied_num,
    ps.stddev_deposit_denied_num,
    ps.avg_deposit_denied_sum,
    ps.stddev_deposit_denied_sum,
    ps.avg_deposit_failed_num,
    ps.stddev_deposit_failed_num,
    ps.avg_deposit_failed_sum,
    ps.stddev_deposit_failed_sum,
    ps.avg_withdrawal_num,
    ps.stddev_withdrawal_num,
    ps.avg_withdrawal_sum,
    ps.stddev_withdrawal_sum,
    ps.avg_withdrawal_approved_num,
    ps.stddev_withdrawal_approved_num,
    ps.avg_withdrawal_approved_sum,
    ps.stddev_withdrawal_approved_sum,
    ps.avg_withdrawal_canceled_num,
    ps.stddev_withdrawal_canceled_num,
    ps.avg_withdrawal_canceled_sum,
    ps.stddev_withdrawal_canceled_sum,
    ps.avg_withdrawal_denied_num,
    ps.stddev_withdrawal_denied_num,
    ps.avg_withdrawal_denied_sum,
    ps.stddev_withdrawal_denied_sum,

    -- Quantiles for deposit-related metrics
    pq.deposit_num_p25,
    pq.deposit_num_p50,
    pq.deposit_num_p75,
    pq.deposit_sum_p25,
    pq.deposit_sum_p50,
    pq.deposit_sum_p75,
    pq.deposit_approved_num_p25,
    pq.deposit_approved_num_p50,
    pq.deposit_approved_num_p75,
    pq.deposit_approved_sum_p25,
    pq.deposit_approved_sum_p50,
    pq.deposit_approved_sum_p75,
    pq.deposit_canceled_num_p25,
    pq.deposit_canceled_num_p50,
    pq.deposit_canceled_num_p75,
    pq.deposit_canceled_sum_p25,
    pq.deposit_canceled_sum_p50,
    pq.deposit_canceled_sum_p75,
    pq.deposit_denied_num_p25,
    pq.deposit_denied_num_p50,
    pq.deposit_denied_num_p75,
    pq.deposit_denied_sum_p25,
    pq.deposit_denied_sum_p50,
    pq.deposit_denied_sum_p75,
    pq.deposit_failed_num_p25,
    pq.deposit_failed_num_p50,
    pq.deposit_failed_num_p75,
    pq.deposit_failed_sum_p25,
    pq.deposit_failed_sum_p50,
    pq.deposit_failed_sum_p75,

    -- Quantiles for withdrawal-related metrics
    pq.withdrawal_num_p25,
    pq.withdrawal_num_p50,
    pq.withdrawal_num_p75,
    pq.withdrawal_sum_p25,
    pq.withdrawal_sum_p50,
    pq.withdrawal_sum_p75,
    pq.withdrawal_approved_num_p25,
    pq.withdrawal_approved_num_p50,
    pq.withdrawal_approved_num_p75,
    pq.withdrawal_canceled_num_p25,
    pq.withdrawal_canceled_num_p50,
    pq.withdrawal_canceled_num_p75,
    pq.withdrawal_canceled_sum_p25,
    pq.withdrawal_canceled_sum_p50,
    pq.withdrawal_canceled_sum_p75

FROM 
    payment_stats ps
JOIN 
    payment_quantiles pq ON ps.customer_id = pq.customer_id AND ps.label_date = pq.label_date;

