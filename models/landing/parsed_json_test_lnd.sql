{{ config(
    materialized='incremental' 
 , incremental_strategy = "insert_overwrite"
  ,partition_by={"field": "updated_ts", "data_type": "Datetime"} 
)
}}

SELECT    Replace( JSON_EXTRACT(parsed_records.after, '$.op_type'),'"','') as op_type , 
cast(Replace( JSON_EXTRACT(parsed_records.after, '$.MEMBERSHIPNUMBER'),'"','') as INT64) as MEMBERSHIPNUMBER,
cast(Replace( JSON_EXTRACT(parsed_records.after, '$.CHGSTAMP'),'"','') as Datetime) as CHGSTAMP,
cast(Replace( JSON_EXTRACT(parsed_records.after, '$.REDEMPTIONAMOUNT'),'"','') as FLOAT64) as REDEMPTIONAMOUNT,
 JSON_VALUE(parsed_records.after, '$.DBACTION') as DBACTION,
current_datetime () as updated_ts
FROM `studio-de-accelerator.dbt_mkumari.raw_cdc_test_json` 