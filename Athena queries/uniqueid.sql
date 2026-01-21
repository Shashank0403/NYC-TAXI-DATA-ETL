create table transformed_taxi_dataset as 
select *, row_number() over() as id
from "taxi_dataset_demo"."taxi_dataset_demo_raw_taxi_dataset_demo"
