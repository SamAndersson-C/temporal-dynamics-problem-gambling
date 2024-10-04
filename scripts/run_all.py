import subprocess
import logging

# Set up logging
logging.basicConfig(
    filename='run_all.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s:%(message)s'
)

def main():
    logging.info("Starting full pipeline execution")

    try:
        # Step 1: Run SQL scripts
        logging.info("Running SQL scripts...")
        subprocess.run(["python", "scripts/run_sql_scripts.py"], check=True)
        logging.info("SQL scripts completed successfully")

        # Step 2: Run preprocessing and selection notebook
        logging.info("Running preprocessing and selection notebook...")
        subprocess.run(["python", "scripts/run_sql_scripts.py"], check=True)
        logging.info("Preprocessing and selection completed successfully")

        # Step 3: Run hyperparameter tuning notebook
        logging.info("Running hyperparameter tuning notebook...")
        subprocess.run(["python", "scripts/run_hyperparameter_tuning.py"], check=True)
        logging.info("Hyperparameter tuning completed successfully")

    except subprocess.CalledProcessError as e:
        logging.error(f"Pipeline execution failed: {e}")
        raise

if __name__ == "__main__":
    main()
