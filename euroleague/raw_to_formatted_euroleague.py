import os
import pandas as pd
from datetime import date

HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"

def replace_special_characters(column_names):
    return [name.replace(' ', '_').replace('.', '_').replace('+', 'plus').replace('-', '_') for name in column_names]

def convert_function(file_name, current_day):
    RAW_PATH = DATALAKE_ROOT_FOLDER + "raw/euroleague/" + current_day + "/" + file_name
    FORMATTED_FOLDER = DATALAKE_ROOT_FOLDER + "formatted/euroleague/" + current_day + "/"
    if not os.path.exists(FORMATTED_FOLDER):
        os.makedirs(FORMATTED_FOLDER)
    print(f"Reading raw file: {RAW_PATH}")
    df = pd.read_csv(RAW_PATH)
    df.columns = replace_special_characters(df.columns)
    parquet_file_name = file_name.replace(".csv", ".snappy.parquet")
    df.to_parquet(FORMATTED_FOLDER + parquet_file_name)
    print(f"Converted {file_name} to {parquet_file_name}")

def convert_raw_to_formatted_euroleague():
    current_day = date.today().strftime("%Y%m%d")
    raw_folder = os.path.join(DATALAKE_ROOT_FOLDER, "raw/euroleague", current_day)
    if not os.path.exists(raw_folder):
        print(f"No raw Euroleague data found for {current_day}")
        return
    formatted_folder = os.path.join(DATALAKE_ROOT_FOLDER, "formatted/euroleague", current_day)
    if not os.path.exists(formatted_folder):
        os.makedirs(formatted_folder)
    for file_name in os.listdir(raw_folder):
        if file_name.endswith(".csv"):
            convert_function(file_name, current_day)
            print(f"Converted {file_name} to Parquet format")

if __name__ == "__main__":
    convert_raw_to_formatted_euroleague()
