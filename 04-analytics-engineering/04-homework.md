## 1
since we set 
```bash
export DBT_BIGQUERY_PROJECT=myproject
export DBT_BIGQUERY_DATASET=my_nyc_tripdata
```

```sql
select * 
from {{ source('raw_nyc_tripdata', 'ext_green_taxi' ) }}
```
would be 

```sql
select *
from  myproject.raw_nyc_tripdata.ext_green_taxi
```
because we defined DBT_BIGQUERY_DATASET and not DBT_BIGQUERY_SOURCE_DATASET
## 2
```sql
where pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY
```

```
dbt run --vars '{days_back: 7}'
```
ovewrite the initial value of days_back else it uses the env var of DAYS_BACK else it uses 30


## 3
```
dbt run --select models/staging/+
```

it would have been correct if we used as it would process all children
```
dbt build --select models/staging/+
```

## 4
All answers are correct except B i.e
Setting a value for DBT_BIGQUERY_STAGING_DATASET env var is mandatory, or it'll fail to compile." (because it returns DBT_BIGQUERY_TARGET_DATASET if not set).

## 5
Green: 1 is best and 2 is worst, same for yellow

## 6 
