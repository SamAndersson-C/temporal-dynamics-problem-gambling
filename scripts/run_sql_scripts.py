import os
import logging
import sqlalchemy
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables from 'se_env.txt'
if load_dotenv('se_env.txt'):
    logging.info("Environment variables loaded successfully.")
else:
    logging.error("Failed to load environment variables from 'se_env.txt'.")

# Database connection parameters
DATABASE_NAME = os.getenv('DB_NAME')
DATABASE_USER = os.getenv('DB_USER')
DATABASE_PASSWORD = os.getenv('DB_PASS')
DATABASE_HOST = os.getenv('DB_HOST')
DATABASE_PORT = os.getenv('DB_PORT')

# Construct the DATABASE_URL
DATABASE_URL = f"postgresql+psycopg2://{DATABASE_USER}:{DATABASE_PASSWORD}@{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_NAME}"

# Set up logging
logging.basicConfig(
    filename='sql_execution.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

# Function to run SQL scripts
def run_sql_file(engine, sql_file_path):
    """Executes an SQL script."""
    logging.info(f"Running SQL script: {sql_file_path}")
    
    try:
        with open(sql_file_path, 'r') as file:
            sql_script = file.read()
        
        # Execute SQL script
        with engine.connect() as connection:
            connection.execute(sqlalchemy.text(sql_script))
        
        logging.info(f"Successfully executed: {sql_file_path}")
    except Exception as e:
        logging.error(f"Failed to execute {sql_file_path}: {e}")
        raise

def main():
    logging.info("Starting SQL script execution")

    # Initialize the database connection
    try:
        engine = create_engine(DATABASE_URL)
        logging.info("SQLAlchemy engine created successfully.")
    except Exception as e:
        logging.error(f"Failed to create SQLAlchemy engine: {e}")
        raise

    # List of SQL scripts in the order you wish to run them
    sql_scripts = [
        # 00_data_ingestion scripts
        'sql_scripts/00_data_ingestion/01_load_sessions.sql',
        'sql_scripts/00_data_ingestion/02_load_bets.sql',
        'sql_scripts/00_data_ingestion/03_load_payments.sql',
        'sql_scripts/00_data_ingestion/04_load_multiple_accounts.sql',
        'sql_scripts/00_data_ingestion/05_load_manual_assessments.sql',
        'sql_scripts/00_data_ingestion/06_load_rg_actions.sql',
        'sql_scripts/00_data_ingestion/07_load_daily_aggregate_transactions.sql',
        'sql_scripts/00_data_ingestion/08_load_loss_chasing.sql',
        'sql_scripts/00_data_ingestion/09_load_customer_background.sql',
        'sql_scripts/00_data_ingestion/10_load_rg_predictions.sql',

        # 01_general_preprocessing scripts
        'sql_scripts/01_general_preprocessing/00_add_label_dates.sql',
        'sql_scripts/01_general_preprocessing/01_background.sql',
        'sql_scripts/01_general_preprocessing/02_filter_customers_by_country.sql',
        'sql_scripts/01_general_preprocessing/03_create_index.sql',

        # 02_create_splits scripts
        'sql_scripts/02_create_splits/01_split_table_manual_assessments.sql',
        'sql_scripts/02_create_splits/02_split_table_background.sql',
        'sql_scripts/02_create_splits/03_split_table_bets.sql',
        'sql_scripts/02_create_splits/04_split_table_payments.sql',
        'sql_scripts/02_create_splits/05_split_table_sessions.sql',
        'sql_scripts/02_create_splits/06_split_table_transactions.sql',
        'sql_scripts/02_create_splits/07_split_table_loss_chasing.sql',
        'sql_scripts/02_create_splits/08_split_table_rg_actions.sql',
        'sql_scripts/02_create_splits/09_split_table_rg_predictions.sql',

        # 03_truncate_activity scripts
        'sql_scripts/03_truncate_activity/00_bets_truncate_table_30_days.sql',
        'sql_scripts/03_truncate_activity/01_bets_truncate_table_60_days.sql',
        'sql_scripts/03_truncate_activity/02_bets_truncate_table_90_days.sql',
        'sql_scripts/03_truncate_activity/03_payments_truncate_table_30_days.sql',
        'sql_scripts/03_truncate_activity/04_payments_truncate_table_60_days.sql',
        'sql_scripts/03_truncate_activity/05_payments_truncate_table_90_days.sql',
        'sql_scripts/03_truncate_activity/06_transactions_truncate_table_30_days.sql',
        'sql_scripts/03_truncate_activity/07_transactions_truncate_table_60_days.sql',
        'sql_scripts/03_truncate_activity/08_transactions_truncate_table_90_days.sql',
        'sql_scripts/03_truncate_activity/09_loss_chasing_truncate_table_30_days.sql',
        'sql_scripts/03_truncate_activity/10_loss_chasing_truncate_table_60_days.sql',
        'sql_scripts/03_truncate_activity/11_loss_chasing_truncate_table_90_days.sql',
        'sql_scripts/03_truncate_activity/12_rg_actions_truncate_table_30_days.sql',
        'sql_scripts/03_truncate_activity/13_rg_actions_truncate_table_60_days.sql',
        'sql_scripts/03_truncate_activity/14_rg_actions_truncate_table_90_days.sql',
        'sql_scripts/03_truncate_activity/15_sessions_truncate_table_30_days.sql',
        'sql_scripts/03_truncate_activity/16_sessions_truncate_table_60_days.sql',
        'sql_scripts/03_truncate_activity/17_sessions_truncate_table_90_days.sql',
        'sql_scripts/03_truncate_activity/18_rg_predictions_truncate_table_90_days.sql',

        # 04_feature_engineering scripts
        'sql_scripts/04_feature_engineering/00_feature_engineer_background_train.sql',
        'sql_scripts/04_feature_engineering/01_deposits_minus_withdrawals.sql',
        'sql_scripts/04_feature_engineering/02_feature_engineer_background_test.sql',
        'sql_scripts/04_feature_engineering/03_feature_engineer_bets_30d_train.sql',
        'sql_scripts/04_feature_engineering/04_feature_engineer_bets_60d_train.sql',
        'sql_scripts/04_feature_engineering/05_feature_engineer_bets_90d_train.sql',
        'sql_scripts/04_feature_engineering/06_feature_engineer_bets_train.sql',
        'sql_scripts/04_feature_engineering/07_feature_engineer_bets_30d_test.sql',
        'sql_scripts/04_feature_engineering/08_feature_engineer_bets_60d_test.sql',
        'sql_scripts/04_feature_engineering/09_feature_engineer_bets_90d_test.sql',
        'sql_scripts/04_feature_engineering/10_feature_engineer_bets_test.sql',
        'sql_scripts/04_feature_engineering/11_feature_engineer_transactions_30d_train.sql',
        'sql_scripts/04_feature_engineering/12_feature_engineer_transactions_60d_train.sql',
        'sql_scripts/04_feature_engineering/13_feature_engineer_transactions_train.sql',
        'sql_scripts/04_feature_engineering/14_feature_engineer_transactions_30d_test.sql',
        'sql_scripts/04_feature_engineering/15_feature_engineer_transactions_60d_test.sql',
        'sql_scripts/04_feature_engineering/16_feature_engineer_transactions_test.sql',
        'sql_scripts/04_feature_engineering/17_feature_engineer_rg_actions_30d_train.sql',
        'sql_scripts/04_feature_engineering/18_feature_engineer_rg_actions_60d_train.sql',
        'sql_scripts/04_feature_engineering/19_feature_engineer_rg_actions_90d_train.sql',
        'sql_scripts/04_feature_engineering/20_feature_engineer_rg_actions_train.sql',
        'sql_scripts/04_feature_engineering/21_feature_engineer_rg_actions_30d_test.sql',
        'sql_scripts/04_feature_engineering/22_feature_engineer_rg_actions_60d_test.sql',
        'sql_scripts/04_feature_engineering/23_feature_engineer_rg_actions_90d_test.sql',
        'sql_scripts/04_feature_engineering/24_feature_engineer_rg_actions_test.sql',
        'sql_scripts/04_feature_engineering/25_feature_engineer_rg_predictions_30d_train.sql',
        'sql_scripts/04_feature_engineering/26_feature_engineer_rg_predictions_60d_train.sql',
        'sql_scripts/04_feature_engineering/27_feature_engineer_rg_predictions_90d_train.sql',
        'sql_scripts/04_feature_engineering/28_feature_engineer_rg_predictions_train.sql',
        'sql_scripts/04_feature_engineering/29_feature_engineer_rg_predictions_30d_test.sql',
        'sql_scripts/04_feature_engineering/30_feature_engineer_rg_predictions_60d_test.sql',
        'sql_scripts/04_feature_engineering/31_feature_engineer_rg_predictions_90d_test.sql',
        'sql_scripts/04_feature_engineering/32_feature_engineer_rg_predictions_test.sql',
        'sql_scripts/04_feature_engineering/33_feature_engineer_multiple_accounts_train.sql',
        'sql_scripts/04_feature_engineering/34_feature_engineer_multiple_accounts_test.sql'
    ]

    # Execute each SQL script in order
    for script in sql_scripts:
        try:
            run_sql_file(engine, script)
        except Exception as e:
            logging.error(f"Error executing {script}: {e}")

    logging.info("SQL script execution completed successfully")

if __name__ == "__main__":
    main()
