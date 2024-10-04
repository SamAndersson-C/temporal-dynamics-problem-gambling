-- Create a view for combined betting features by joining bets data with manual assessments

CREATE OR REPLACE VIEW customer_analysis_se.vw_combined_bets_features AS
SELECT 
    b.customer_id,
    b.timestamp,
    b.winning_cash_num,
    b.winning_cash_sum,
    b.turnover_cash_sum,
    b.turnover_cash_num,
    b.cancel_wager_num,
    m.date
FROM 
    customer_analysis_se.bets_data_excl_30d b
INNER JOIN 
    customer_analysis_se.manual_assessments_train m ON b.customer_id = m.customer_id
WHERE 
    b.timestamp <= m.date;

-- Temporarily store combined betting features from the view for further processing

CREATE TEMP TABLE temp_combined_bets_features AS
SELECT 
    customer_id,
    timestamp,
    winning_cash_num,
    winning_cash_sum,
    turnover_cash_sum,
    turnover_cash_num,
    cancel_wager_num,
    date AS label_date
FROM 
    customer_analysis_se.vw_combined_bets_features;


-- Aggregate betting features over 30 days, including quantiles and averages for various betting metrics



CREATE TABLE customer_analysis_se.combined_bets_features_aggregated_30d AS
WITH quantiles AS (
    SELECT
        customer_id,
        label_date,
        
        -- Winning cash sum quantiles
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY winning_cash_sum) AS winning_cash_sum_p25,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY winning_cash_sum) AS winning_cash_sum_p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY winning_cash_sum) AS winning_cash_sum_p75,
        
        -- Winning cash num quantiles
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY winning_cash_num) AS winning_cash_num_p25,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY winning_cash_num) AS winning_cash_num_p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY winning_cash_num) AS winning_cash_num_p75,

        -- Turnover cash sum quantiles
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY turnover_cash_sum) AS turnover_cash_sum_p25,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY turnover_cash_sum) AS turnover_cash_sum_p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY turnover_cash_sum) AS turnover_cash_sum_p75,
        
        -- Turnover cash num quantiles
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY turnover_cash_num) AS turnover_cash_num_p25,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY turnover_cash_num) AS turnover_cash_num_p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY turnover_cash_num) AS turnover_cash_num_p75,

        -- Cancel wager num quantiles
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY cancel_wager_num) AS cancel_wager_num_p25,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY cancel_wager_num) AS cancel_wager_num_p50,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY cancel_wager_num) AS cancel_wager_num_p75

    FROM temp_combined_bets_features
    GROUP BY customer_id, label_date
)
SELECT 
    bf.customer_id,
    bf.date,
    
    -- Dynamic Proportional/Rate-based Features
    COALESCE(SUM(bf.winning_cash_sum) / NULLIF(COUNT(DISTINCT bf.timestamp), 0), 0) AS avg_daily_winning_cash_all,
    COALESCE(SUM(bf.winning_cash_num) / NULLIF(COUNT(DISTINCT bf.timestamp), 0), 0) AS avg_daily_winning_bets_all,
    COALESCE(SUM(bf.turnover_cash_sum) / NULLIF(COUNT(DISTINCT bf.timestamp), 0), 0) AS avg_daily_turnover_cash_all,
    COALESCE(SUM(bf.turnover_cash_num) / NULLIF(COUNT(DISTINCT bf.timestamp), 0), 0) AS avg_daily_turnover_bets_all,
    COALESCE(SUM(bf.cancel_wager_num) / NULLIF(COUNT(DISTINCT bf.timestamp), 0), 0) AS avg_daily_canceled_wagers_all,
    
    -- Standard Deviations
    COALESCE(STDDEV(bf.winning_cash_sum), 0) AS stddev_daily_winning_cash_all,
    COALESCE(STDDEV(bf.winning_cash_num), 0) AS stddev_daily_winning_bets_all,
    COALESCE(STDDEV(bf.turnover_cash_sum), 0) AS stddev_daily_turnover_cash_all,
    COALESCE(STDDEV(bf.turnover_cash_num), 0) AS stddev_daily_turnover_bets_all,
    COALESCE(STDDEV(bf.cancel_wager_num), 0) AS stddev_daily_canceled_wagers_all,

    -- Quantiles from CTE
    q.winning_cash_sum_p25,
    q.winning_cash_sum_p50,
    q.winning_cash_sum_p75,
    q.winning_cash_num_p25,
    q.winning_cash_num_p50,
    q.winning_cash_num_p75,
    q.turnover_cash_sum_p25,
    q.turnover_cash_sum_p50,
    q.turnover_cash_sum_p75,
    q.turnover_cash_num_p25,
    q.turnover_cash_num_p50,
    q.turnover_cash_num_p75,
    q.cancel_wager_num_p25,
    q.cancel_wager_num_p50,
    q.cancel_wager_num_p75

FROM 
    customer_analysis_se.vw_combined_bets_features bf
JOIN 
    quantiles q ON bf.customer_id = q.customer_id AND bf.date = q.label_date
GROUP BY 
    bf.customer_id, bf.date, 
    q.winning_cash_sum_p25, q.winning_cash_sum_p50, q.winning_cash_sum_p75, 
    q.winning_cash_num_p25, q.winning_cash_num_p50, q.winning_cash_num_p75, 
    q.turnover_cash_sum_p25, q.turnover_cash_sum_p50, q.turnover_cash_sum_p75, 
    q.turnover_cash_num_p25, q.turnover_cash_num_p50, q.turnover_cash_num_p75, 
    q.cancel_wager_num_p25, q.cancel_wager_num_p50, q.cancel_wager_num_p75;



