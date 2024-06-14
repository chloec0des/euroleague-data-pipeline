import os
from datetime import date
from google.cloud import bigquery

# Get the current directory of this script
current_directory = os.path.dirname(os.path.abspath(__file__))

# Construct the absolute path to the credentials.json file
credentials_path = os.path.join(current_directory, '../nba/credentials.json')

# Set the environment variable
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path

# Define the Google Cloud project ID and dataset ID
PROJECT_ID = 'keen-genre-365508'
DATASET_ID = 'euroleague_data'
HOME = os.path.expanduser('~')

def load_data_into_bigquery(file_path, table_id):
    bq_client = bigquery.Client(project=PROJECT_ID)
    dataset_ref = bq_client.dataset(DATASET_ID)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite the table if it exists
        source_format=bigquery.SourceFormat.PARQUET,  # Specify the source format as PARQUET
    )

    # Load data from file into BigQuery
    with open(file_path, "rb") as source_file:
        load_job = bq_client.load_table_from_file(source_file, dataset_ref.table(table_id), job_config=job_config)

    load_job.result()  # Waits for the job to complete.

    print(f"Loaded data from {file_path} into BigQuery table {table_id}")

def load_euroleague_data_to_bigquery():
    current_day = date.today().strftime("%Y%m%d")
    formatted_folder = f"{HOME}/datalake/formatted/euroleague/{current_day}"
    
    # Print formatted folder path for debugging
    print(f"Formatted folder path: {formatted_folder}")
    
    if not os.path.exists(formatted_folder):
        print(f"No formatted Euroleague data found for {current_day}")
        return

    for file_name in os.listdir(formatted_folder):
        if file_name.endswith(".parquet"):
            file_path = os.path.join(formatted_folder, file_name)
            table_id = f"euroleague_{current_day}_{file_name.split('.')[0]}"  # Table name based on file name and current date
            
            # Print file path and table ID for debugging
            print(f"Processing file: {file_path} into table: {table_id}")
            
            load_data_into_bigquery(file_path, table_id)

if __name__ == "__main__":
    load_euroleague_data_to_bigquery()
