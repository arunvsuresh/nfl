-- create external table from gcs
create or replace external table 'astute-atlas-387920.nfl.external_pbp'
options (
  format = 'PARQUET',
  uris = ['gs://nfl_data_lake_astute-atlas-387920/data/pbp*']
);

select * from 'astute-atlas-387920.nfl.external_pbp' limit 10;

-- create non-partitioned table from existing external table
create or replace table 'astute-atlas-387920.nfl.pbp_non_partitioned' as select * from 'astute-atlas-387920.nfl.external_pbp';


-- create partitioned table from external table
create or replace table 'astute-atlas-387920.nfl.pbp_partitioned' 
partition by
    date(game_date) as 
select * from 'astute-atlas-387920.nfl.external_pbp';

-- impact of partition
-- scanning 528.12 KB when run
select distinct(passer_id)
from 'astute-atlas-387920.nfl.pbp_non_partitioned'
where date(game_date) between '2019-09-08 00:00:00' and '2019-11-10 00:00:00'

-- impact of partition 
-- scanning 289.32 KB when run
select distinct(game_id)
from `astute-atlas-387920.nfl.pbp_partitioned`
where date(game_date) between '2019-09-08' and '2019-11-10'

-- let's look into the partitions
select table_name, partition_id, total_rows
from `astute-atlas-387920.INFORMATION_SCHEMA.PARTITIONS`
where table_name = `astute-atlas-387920.nfl.pbp_partitioned`
order by total_rows desc;

-- create a partition & cluster table
create or replace table `astute-atlas-387920.nfl.pbp_partitioned_clustered`
partition by date(game_date)
cluster by game_id as select * from 'astute-atlas-387920.nfl.external_pbp';

query scans 

