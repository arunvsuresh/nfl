from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint

# ng_url_base = 'https://github.com/nflverse/nflverse-data/releases/download/nextgen_stats/'

# adv_stats_url_base = 'https://github.com/nflverse/nflverse-data/releases/download/pfr_advstats/'

# repo = Dolt('.')

# ng_df = pd.DataFrame()
# year  = 2022
# while year <= 2022:
#     ng_lst = []
#     url = ng_url_base + 'ngs_' + str(year) + '_passing.csv.gz'
#     ng_data = pd.read_csv(url, compression='gzip', low_memory=False)
#     ng_lst.append(ng_data)

#     ngs_parquet_file = f'ngs_{year}.parquet'

#     ng_df.to_parquet(f'../../data/{ngs_parquet_file}')
#     year += 1
# ng_df = pd.concat(ng_lst)


# adv_stats_df = pd.DataFrame()
# year  = 2022
# while year <= 2022:
#     adv_stats_lst = []
#     url = adv_stats_url_base + 'advstats_week_pass_' + str(year) + '.parquet'
#     adv_stats_data = pd.read_parquet(url, compression='gzip', low_memory=False)
#     adv_stats_lst.append(adv_stats_data)

#     adv_stats_parquet_file = f'adv_stats_{year}.parquet'

#     adv_stats_df.to_parquet(f'../../data/{adv_stats_parquet_file}')
#     year += 1
# adv_stats_df = pd.concat(adv_stats_lst)

# gcp_block = GcsBucket.load("data-pipeline-block")
# gcp_block.upload_from_path(from_path=f'../../data/{plays_csv_file}', to_path=f'data/{plays_csv_file}')

# since we're pulling from the web, enable retries in case of bad network connection
@task(retries=3)
def fetch(dataset: str, dataset_url: str, year: int) -> pd.DataFrame:
    # if 'ngs' in dataset_url:
    #     df = pd.read_csv(dataset_url, compression='gzip', low_memory=False)

    #     df.name = f'ngs_{year}'
    # if 'advstats' in dataset_url:
    #     df = pd.read_parquet(dataset_url)
    #     df.name = f'advstats_{year}'
    # if 'pbp' in dataset_url:
    #     df = pd.read_parquet(dataset_url)
    #     df.name = f'pbp_{year}'
    #     print('dataset url...', dataset_url)
    #     print('df name within fetch...', df.name)


    if 'ngs' in dataset:
        df = pd.read_csv(dataset_url, compression='gzip', low_memory=False)
    else:
        df = pd.read_parquet(dataset_url)

    df.attrs['name'] = f'{dataset}_{year}'


    return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    print('within clean function, df.name', df.attrs['name'])
    if 'advstats' in df.attrs['name']:
        # drop nan cols
        df = df.drop(columns=['receiving_drop', 'receiving_drop_pct', \
                        'def_times_blitzed', 'def_times_hurried', 'def_times_hitqb'])

    elif 'pbp' in df.attrs['name']:
        # keep only relevant fields for analysis
        pbp_fields_to_keep = [
            'game_date', 'game_id', 'season', 'week', 'posteam', 'passer_id', 'passer',
            'qb_hit', 'pass_length', 'pass_location', 'passing_yards', 'pass_touchdown', 'interception', 'sack', 'pass_attempt',
            'complete_pass', 'epa', 'success'
        ]

        pbp_fields = [pbp_field for pbp_field in pbp_fields_to_keep if pbp_field in df.columns]

        df = df[pbp_fields]

        # clean up data & impute missing values
        df = df[~df['passer'].isin([None])]

        df['passing_yards'] = df['passing_yards'].fillna(0)
        df['game_date'] = pd.to_datetime(df['game_date'])
        print(df['game_date'].dtype)


        print('LEN OF PBP DF COLUMNS...', len(df.columns))
    return df

@task()
def write_to_local(df: pd.DataFrame, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    # print('df.name', df.name)
    print('dataset_file', dataset_file)
    path = Path(f"data/{dataset_file}.parquet")

    print('path parent is dir: ', path.parent.is_dir())
    if not path.parent.is_dir():
        path.parent.mkdir(parents=True)
    print('PATH: ', path)

    df.to_parquet(path, compression="gzip")
    return path

@task()
def write_to_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("data-pipeline-block")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return

@flow()
def etl_web_to_gcs(item):

    year, dataset = item

    file = f'{dataset}_{year}'
    base_url = 'https://github.com/nflverse/nflverse-data/releases/download/'

    if dataset == 'ngs':
        url = base_url + 'nextgen_stats/' + 'ngs_' + str(year) + '_passing.csv.gz'

    if dataset == 'adv_stats':
        url = base_url + 'pfr_advstats/' + 'advstats_week_pass_' + str(year) + '.parquet'
    if dataset == 'pbp':
        url = base_url + 'pbp/' + 'play_by_play_' + str(year) + '.parquet'

    print('urlllll....', url)
    df = fetch(dataset, url, year)
    clean_df = clean(df)
    local_path = write_to_local(clean_df, file)
    write_to_gcs(local_path)


if __name__ == '__main__':

    years = [2019]
    dataset = ['ngs', 'adv_stats', 'pbp']
    data = list(zip(years*len(dataset), dataset))
    # , 2020, 2021, 2022]
    for item in data:
        etl_web_to_gcs(item)
