select id, fare_amount, total_amount, max(tip_amount) as highest_tip
from "taxi_dataset_demo"."transformed_taxi_dataset"
group by id, fare_amount, total_amount
order by highest_tip desc
limit 10;
