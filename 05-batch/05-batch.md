processing data: batch (80%) and streaming (<20%)
batch processing data in batch (say all Jan data): weekly, daily (all day data after the day is over), hourly, every 5 minutes, 3 times per hour
streaming: gets data as it comes (on the fly)

batch technologies: python scripts (wk1, kubernetes, aws), sql, spark, flink

workflow: Data lake (csv) --> python --> sql (dbt) ---> spark ---> python

Advantages: easy to manage, retry, easier to scale, 
disadvantages: delay (wait for time to get another batch or time to run each process in the workflow)

## Spark (can also be used for streaming a sequence of data)

data lake ---> cluster of spark ---> data lake
(s3/gcs/parquet)

you can execute sql in data lake using Hive, presto/athena

raw data --> lake --> sql athena --> spark ---> python (train ml) --> model--> apply spark ml --> lake

## setup vm instance (compute engine) and select vm instance

### generate ssh keys
cloud.google.com/compute/docs/connect/create-ssh-keys
cd .ssh/
ssh-keygen -t rsa -f ~/.ssh/hmdgcp -C hammedgcp -b 2048

go to metadata under compute engine, ssh keys, add, save

go to vm instances and create an instance

general purpoose, e2, standard-4, change OS to Ubuntu 20.04, you create increase disk size to 30. 
once created, copy the external IP and connect from your local terminal at ~ folder using: ssh -i  ~/.ssh/hmdgcp hammedgcp@ext-ip
ssh -i  ~/.ssh/hmdgcp hammedgcp@35.202.135.56

once loggedin, download anaconda: wget https://repo.anaconda.com/archive/Anaconda3-2024.10-1-Linux-x86_64.sh
run with: bash Anaconda3-2024.10-1-Linux-x86_64.sh

https://github.com/sindresorhus/guides/blob/main/docker-without-sudo.md

# install spark using [link](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-batch/setup/linux.md)

install spark3.5.5 rather using 

wget https://dlcdn.apache.org/spark/spark-3.5.5/spark-3.5.5-bin-hadoop3.tgz

and change python path to 
```bash
export PYTHONPATH="${SPARK_HOME}/python/:$PYTHONPATH"
export PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH"
```

Things not executed right-away are transformations, they are lazy (select, filter, groupby, and so on)
actions are things executed right-away, they are eager e.g show, take, head

RDD: resilient distributed dataset

## Spark_gcs
copy data to gcp bucket using: gsutil -m cp -r pq/ gs://spark_datalake/pq
make a lib dir and download: gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop3-2.2.5.jar gcs-connector-hadoop3-2.2.5.jar
see (09_spark_gcp.ipynb)

## local spark cluster (running spark in standalone mode)
see (06_spark_sql2.py) expose to port 8080 instead of 4040

## setting up cluster with google Dataproc 
create a dataproc and copy (06_spark_sql2.py) to the gcp bucket
gsutil cp 06_spark_sql2.py gs://spark_datalake/code/06_spark_sql2.py

and submit a job on your dataproc cluster: 

argument: supply python file args

--input_green=gs://spark_datalake/pq/green/2021/* \
--input_yellow=gs://spark_datalake/pq/yellow/2021/* \
--output=gs://spark_datalake/report-2021

## we can submit dataproc cluster job using gcloud, python, and others. See [link](https://cloud.google.com/dataproc/docs/guides/submit-job#dataproc-submit-job-console)

gcloud dataproc jobs submit job-command \
    --cluster=cluster-name \
    --region=region \
    other dataproc-flags \
    -- job-args

"pysparkJob": {
    "mainPythonFileUri": "gs://spark_datalake/code/06_spark_sql2.py",
    "properties": {
      "spark.dataproc.enhanced.optimizer.enabled": "true",
      "spark.dataproc.enhanced.execution.enabled": "true"
    },
    "args": [
      "--input_green=gs://spark_datalake/pq/green/2021/*",
      "--input_yellow=gs://spark_datalake/pq/yellow/2021/*",
      "--output=gs://spark_datalake/report-2021"
    ]

## to run, update your service account permission to submit jobs (dataproc admin)

gcloud dataproc jobs submit pyspark \
    --cluster=dezoom-cluster \
    --region=us-central1 \
    gs://spark_datalake/code/06_spark_sql2.py \
    -- \
        --input_green=gs://spark_datalake/pq/green/2020/*/ \
        --input_yellow=gs://spark_datalake/pq/yellow/2020/*/ \
        --output=gs://spark_datalake/report-2020


## connecting pyspark to bigquery (see [link](https://github.com/GoogleCloudDataproc/spark-bigquery-connector) and [link](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/05-batch/code/cloud.md))
gsutil cp 06_spark_sql_bigquery.py gs://spark_datalake/code/06_spark_sql_bigquery.py

This did not work.
gcloud dataproc jobs submit pyspark \
    --cluster=dezoom-cluster \
    --region=us-central1 \
    --jars=gs://spark-lib/bigquery/spark-3.5-bigquery-0.42.0.jar \
    gs://spark_datalake/code/06_spark_sql_bigquery.py \
    -- \
        --input_green=gs://spark_datalake/pq/green/2020/*/ \
        --input_yellow=gs://spark_datalake/pq/yellow/2020/*/ \
        --output=de-zoomcamp-449719.trips_data_all.reports-2020

This worked see [link](https://github.com/GoogleCloudDataproc/spark-bigquery-connector/issues/1295).

gcloud dataproc jobs submit pyspark \
    --cluster=dezoom-cluster \
    --region=us-central1 \
    gs://spark_datalake/code/06_spark_sql_bigquery.py \
    -- \
        --input_green=gs://spark_datalake/pq/green/2020/*/ \
        --input_yellow=gs://spark_datalake/pq/yellow/2020/*/ \
        --output=de-zoomcamp-449719.trips_data_all.reports-2020