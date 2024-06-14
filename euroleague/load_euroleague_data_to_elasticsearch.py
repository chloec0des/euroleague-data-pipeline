import os
import json
from datetime import date
from elasticsearch import Elasticsearch, helpers
import pandas as pd

# Define the Elasticsearch client
es = Elasticsearch([{'host': 'localhost', 'port': 9200, 'scheme': 'http'}])

HOME = os.path.expanduser('~')

def load_data_into_elasticsearch(file_path, index_name):
    # Read the Parquet file into a DataFrame
    df = pd.read_parquet(file_path)
    
    # Convert the DataFrame to a list of dictionaries
    records = df.to_dict(orient='records')
    
    # Prepare the data for bulk upload to Elasticsearch
    actions = [
        {
            "_index": index_name,
            "_source": record
        }
        for record in records
    ]
    
    try:
        # Bulk upload data to Elasticsearch
        helpers.bulk(es, actions)
        print(f"Loaded data from {file_path} into Elasticsearch index {index_name}")
    except helpers.BulkIndexError as bulk_error:
        print(f"Failed to index data: {bulk_error.errors}")
        for error in bulk_error.errors:
            print(json.dumps(error, indent=2))

def load_euroleague_data_to_elasticsearch():
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
            index_name = f"euroleague_{current_day}_{file_name.split('.')[0]}"  # Index name based on file name and current date
            
            # Print file path and index name for debugging
            print(f"Processing file: {file_path} into index: {index_name}")
            
            load_data_into_elasticsearch(file_path, index_name)

if __name__ == "__main__":
    load_euroleague_data_to_elasticsearch()
