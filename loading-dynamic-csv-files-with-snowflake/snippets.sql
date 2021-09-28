
copy into mytable
from s3://big-time-data/files
credentials=(aws_key_id='$AWS_ACCESS_KEY_ID' 
aws_secret_key='$AWS_SECRET_ACCESS_KEY')
FILE_FORMAT = ( TYPE = CSV );


-- csv_lines represents the raw CSV lines
CREATE OR REPLACE TABLE csv_lines (
  LOADED_AT timestamp,
  FILENAME string,
  FILE_ROW_NUMBER int,
  DATA VARIANT
);


-- this will ingest csv files with a maximum on 20 columns
COPY INTO csv_lines
from (
  SELECT
    CURRENT_TIMESTAMP as LOADED_AT,
    METADATA$FILENAME as FILENAME,
    METADATA$FILE_ROW_NUMBER as FILE_ROW_NUMBER,
    object_construct(
      'col_001', T.$1, 'col_002', T.$2, 'col_003', T.$3, 'col_004', T.$4,
      'col_005', T.$5, 'col_006', T.$6, 'col_007', T.$7, 'col_008', T.$8,
      'col_009', T.$9, 'col_010', T.$10, 'col_011', T.$11, 'col_012', T.$12,
      'col_013', T.$13, 'col_014', T.$14, 'col_015', T.$15, 'col_016', T.$16,
      'col_017', T.$17, 'col_018', T.$18, 'col_019', T.$19, 'col_020', T.$20
    ) as data
  FROM @mystage/files T
)
FILE_FORMAT = (
  TYPE = CSV
  RECORD_DELIMITER = '\n'
  ESCAPE_UNENCLOSED_FIELD = NONE
  FIELD_OPTIONALLY_ENCLOSED_BY='0x22'
  EMPTY_FIELD_AS_NULL=TRUE
  NULL_IF = ''
);

create table csv_records as
with headers as (
  select 
    source.filename,
    k.value::string as key,
    source.data[k.value::string] as value
  from csv_lines source,
  lateral flatten(input => object_keys(source.data)) k
  where file_row_number = 1 -- first line must be column headers
)
, records as (
  select
    source.loaded_at,
    source.filename,
    source.filename || '-' || source.file_row_number as unique_key,
    object_agg(headers.value, source.data[headers.key]) data
  from csv_lines source
  inner join headers
    on headers.filename = source.filename
  where source.file_row_number > 1
  group by 1, 2, 3, 4
)
select * from record;


select
  data['account_id']::varchar as account_id
from csv_records
-- where filename like '%/myfilter/%' -- we can filter by file name if we wish
