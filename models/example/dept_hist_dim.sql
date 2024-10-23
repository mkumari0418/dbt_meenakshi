

{{ config(materialized='table') }}

with source_data as (

    Select * from studio-de-accelerator.dbt_mkumari.dept_hist_stg where hist_cost_department_ind !=0

)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
