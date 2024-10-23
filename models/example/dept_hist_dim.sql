

{{ config(
    materialized='incremental' 
 , incremental_strategy = "merge"
 ,unique_key=['department_num']
)
}}

with source_data as (

    Select * from studio-de-accelerator.dbt_mkumari.dept_hist_stg where hist_cost_department_ind !=0

),
max_sk as (
    Select max(department_num) as max_skey from studio-de-accelerator.dbt_mkumari.dept_hist_dim
)

select 
-- (select max_skey from max_sk) + Row_number() over(order by  1) as 
department_num,hist_department_short_desc, hist_department_long_desc, hist_cost_department_ind, hist_major_dept_grp_num, hist_major_dept_grp_short_desc, hist_major_dept_grp_long_desc, hist_mega_dept_grp_num, hist_mega_dept_grp_short_desc, hist_mega_dept_grp_long_desc, op_ts,
current_datetime() as updated_ts
from source_data

