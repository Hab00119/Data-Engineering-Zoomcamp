## Roles in data team
data engineer prepares and maintains the data infrastructure
data analyst uses data to answer questions and solve problems
analytics engineer introduces the good software engineering practices to the to efforts of data analyst and data scientists

## Analytics tool
data loading
data storing (cloud data warehouses like snowflake, bigquery and redshift)
data modelling (tools like dbt or dataform)
data presentation (bi tools like google data studio, looker, mode, tableau)

### Data Modelling

ETL (Extract Transform Load) Vs ELT (Extract Load Transform)
ETL: extract from data source (database), transform, and load to data warehouse
ELT: extract from data source (database),  load to data warehouse and transform in data warehouse

ETL ensures that we transform data and load clean data to data warehouse (it is more stable and data analysis compliant, has higher storage and compute cost)

ELT is faster and more flexible as we have loaded the data to the warehouse. It has lower cost and lower maintenance

#### ETL (Extract, Transform, Load)
Process:

- Extract: Data is pulled from various sources (databases, APIs, files).
- Transform: Data is cleaned, filtered, and transformed before being loaded.
- Load: The transformed data is then stored in the data warehouse.

##### Advantages:

- The data warehouse contains clean, structured, and analysis-ready data.
- Because transformations happen before loading, the warehouse remains optimized and stable.
- Ensures data quality by eliminating duplicates, handling missing values, and standardizing formats before storage.

##### Disadvantages:

- Higher compute and storage costs since processing happens externally before loading.
- Slower since transformations are done before loading.
- Less flexibility if data needs reprocessing, as it must go through ETL again.


#### ELT (Extract, Load, Transform)
Process:

- Extract: Raw data is pulled from sources.
- Load: The raw data is loaded directly into the warehouse.
- Transform: The transformation happens inside the warehouse using SQL or other processing tools.

##### Advantages:

- Faster and more flexible: Since data is loaded before transformation, users can run transformations as needed.
- Lower storage and compute costs: Modern cloud warehouses (BigQuery, Snowflake, Redshift) separate storage from compute, making it more cost-efficient.
- Easier maintenance: Since data remains raw, it can be reprocessed for different use cases without re-extracting.

##### Disadvantages:

- Raw data can be messy, requiring careful governance to ensure accurate analysis.
- More complex queries may be needed for transformation inside the warehouse.
- Potential performance impact on queries if large transformations are frequently run.

## Kimball's Dimensional Modeling
obj
- deliver understandable data and fast query performance to the business users

Approach
prioritise user understandability and query performance over non redundant data 

Other approaches
- Bill Immont
- Data Vault

### Elements of Dimensional Modelling

- facts tables: Store quantitative data (metrics, measurements, or business facts) that are analyzed. 
    - Sales Fact Table: Stores total sales, revenue, discount, and profit.
- dimension tables: Provide context to fact tables by storing descriptive attributes.
    - Customer Dimension: Stores customer name, age, location, and segment.

### Architecture of Dimensional Modeling

- Stage Area (contains raw data, not exposed to everyone)
- Processing Area (processed data)
- Presentation Area (presentation of the final processed data)

## dbt
a transformation workflow that allows anyone to use SQL to deploy analytics code following software engineering practices

## how to use dbt
dbt Core (open-source, builds and run a dbt project, cli interface, free to use)
dbt cloud (SaaS application to develop and manage dbt projects, web-based IDE and cloud CLI, logging and alerting, admin and metadata API, semantic layer)

## how we will use dbt in the project
Data Source (EL): NYC Taxi trip
Data transformation (T): bigquery or postgres
- bigquery, development using cloud IDE, no local installation of dbt Core
- postgres, use local IDE, install dbt core, run dbt models using CLI

## materialization in dbt
- view: virtual table
- table: real table
- ephemeral
- incremental

## starting a dbt project
create a GCP service account (add BigQuery admin and storage admin access), generate json key, check [https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/dbt_cloud_setup.md](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/dbt_cloud_setup.md)
open user account, click on project, and create a new project named taxi_rides_ny. connect it to bigquery under connections. 

From the dbt croud, initialize the project
in bigquery, create a dataset trips_data_all dataset in bigquery manually and use multi regions in US. Run the query in [/workspaces/Data-Engineering-Zoomcamp/04-analytics-engineering/load_data_to_bigquery_hack.sql](/workspaces/Data-Engineering-Zoomcamp/04-analytics-engineering/load_data_to_bigquery_hack.sql) to create the green and yellow tables.

to generate a model, use 
{% set models_to_generate = codegen.get_models(directory='staging', prefix='stg) %}
{{ codegen.generate_model_yaml(
    model_names = models_to_generate
)}}

and compile

dbt docs generate (generates dbt docs)

### deployment
create a new environment named (Production), dataset prod, then create a deploy job under Production env with:
job name: Nightly
description: This is where the data hits production
environment: Production
Commands: dbt build
generate docs on run
run source freshness
run on schedule

add nightly to project under artifacts

you can also add CI/CD job and set commands to: dbt build --select state:modified+

+ is the children