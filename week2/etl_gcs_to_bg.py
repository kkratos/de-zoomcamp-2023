from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(retries=3, log_prints=True)
def extract_from_gcs(color: str, dataset_file:str) -> Path:
    """Download trip data from GCS"""

    gcs_path = f"data/{color}/{dataset_file}.parquet"  # GCS path as a string
    local_dir = Path("./data") / color  # Local directory to save the file
    local_dir.mkdir(parents=True, exist_ok=True)  # Ensure local directory exists

    gcs_block = GcsBucket.load("zoom-gcs")
    local_path = local_dir / f"{dataset_file}.parquet"  # Full local path to save the file

    gcs_block.get_directory(from_path=gcs_path, local_path=local_path)
    return local_path


@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    print(f"pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0, inplace=True)
    print(f"post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BigQuery"""
    
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-cred")

    df.to_gbq(
        destination_table="destination-folder",
        project_id="project-id",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500000,
        if_exists="append"
    )

# ht354@rutgers.edu
@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""
    color = "yellow"
    year = 2021
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"

    path = extract_from_gcs(color, dataset_file)
    df = transform(path)
    write_bq(df)

if __name__ == "__main__":
    etl_gcs_to_bq()