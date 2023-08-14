from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(year, dataset) -> Path:
    """Download qb data from GCS"""
    # year, data = item
    gcs_path = f"data/{dataset}_{year}.parquet"
    gcs_block = GcsBucket.load("data-pipeline-block")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"data/")
    # print(Path(f"data/{gcs_path}"), year)
    return Path(gcs_path), year


@task()
def transform(dataset: str, path: Path, year: int) -> pd.DataFrame:
    """Data cleaning example"""
    print('path name', path.name)
    df = pd.read_parquet(path)
    # if 'ngs' in path.name:
    #     df.attrs['name'] = f'ngs_{year}'
    # if 'adv_stats' in path.name:
    #     df.attrs['name'] = f'advstats_{year}'
        
    
    
    df.attrs['name'] = f'{dataset}_{year}'



    print('df name within transform', df.attrs['name'])
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("data-pipeline-creds")

    if 'ngs' in df.attrs['name']:

        df.to_gbq(
            destination_table="nfl.ngs",
            project_id="astute-atlas-387920",
            credentials=gcp_credentials_block.get_credentials_from_service_account(),
            chunksize=500_000,
            if_exists="replace",
        )
    if 'advstats' in df.attrs['name']:
        df.to_gbq(
            destination_table="nfl.advstats",
            project_id="astute-atlas-387920",
            credentials=gcp_credentials_block.get_credentials_from_service_account(),
            chunksize=500_000,
            if_exists="replace",
        )
    if 'pbp' in df.attrs['name']:
        df.to_gbq(
            destination_table="nfl.pbp",
            project_id="astute-atlas-387920",
            credentials=gcp_credentials_block.get_credentials_from_service_account(),
            chunksize=500_000,
            if_exists="replace",
        )


@flow()
def etl_gcs_to_bq(year: int = 2019, dataset: str = 'ngs'):
    """Main ETL flow to load data into Big Query"""
    path, year = extract_from_gcs(year, dataset)
    df = transform(dataset, path, year)
    write_bq(df)

@flow()
def etl_parent_flow(years: list[int] = [2019]):
    for year in years:
        etl_gcs_to_bq(year, 'ngs')
        etl_gcs_to_bq(year, 'adv_stats')
        etl_gcs_to_bq(year, 'pbp')

if __name__ == "__main__":
    years = [2019]
    # dataset = ['ngs', 'adv_stats']
    # data = list(zip(years*len(dataset), dataset))
    # , 2020, 2021, 2022]
    etl_parent_flow(years)

    # for item in data:
    #     etl_gcs_to_bq(item)
