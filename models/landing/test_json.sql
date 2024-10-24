{{ config(
    materialized='incremental' 
 , incremental_strategy = "insert_overwrite"
 
)
}}

SELECT    Replace( JSON_EXTRACT(parsed_records.after, '$.table'),'"','') as op_type , 
cast(Replace( JSON_EXTRACT(parsed_records.after, '$.MEMBERSHIPNUMBER'),'"','') as INT64) as MEMBERSHIPNUMBER,
cast(Replace( JSON_EXTRACT(parsed_records.after, '$.CHGSTAMP'),'"','') as Datetime) as CHGSTAMP,
cast(Replace( JSON_EXTRACT(parsed_records.after, '$.REDEMPTIONAMOUNT'),'"','') as INT64) as REDEMPTIONAMOUNT,
cast(Replace( JSON_EXTRACT(parsed_records.after, '$.DBACTION'),'"','') as STRING) as DBACTION,
current_datetime () as updated_ts
FROM `studio-de-accelerator.dbt_mkumari.raw_cdc_test_json` 