-- Use the `ref` function to select from other models

select *
from hive_metastore.default.my_first_dbt_model
where id = 1