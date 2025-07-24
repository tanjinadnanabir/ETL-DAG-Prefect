from prefect import flow, task
from datetime import timedelta
import pandas as pd
import requests
import csv

# Task: Extract
@task
def extract_data():
    print("Extracting data...")
    url = "https://jsonplaceholder.typicode.com/users"
    response = requests.get(url)
    return response.json()

# Task: Transform
@task
def transform_data(raw_data):
    print("Transforming data...")
    df = pd.DataFrame(raw_data)
    df = df[["id", "name", "email"]]
    return df

# Task: Load
@task
def load_data(data):
    print("Loading data to CSV...")
    data.to_csv("etl_output.csv", index=False)
    print("Saved to etl_output.csv")

# Main flow
@flow(name="ETL")
def etl_flow():
    raw = extract_data()
    cleaned = transform_data(raw)
    load_data(cleaned)

# Optional: You can test run locally
if __name__ == "__main__":
    etl_flow()
