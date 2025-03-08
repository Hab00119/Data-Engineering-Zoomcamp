# Project: Postgres Setup & Data Ingestion (Week 1)

This project demonstrates how to set up a PostgreSQL database using Docker, load data into it using Python, and inspect the database using pgcli or pgAdmin. There are two main approaches provided:

- **Option 1:** Manually run a Python script with Docker containers for PostgreSQL and pgAdmin.
- **Option 2:** Wrap the Python script in a Docker image and use Docker Compose to orchestrate PostgreSQL, pgAdmin, and your ingestion service.
> **Note:** SQL queries and further database operations are covered in a separate [SQL.md](/workspaces/Data-Engineering-Zoomcamp/01-Docker_Sql_terraform/Docker_SQL/SQL-notes.md) file.

---

## Prerequisites

- Docker installed on your system.
- Python environment (if running scripts directly).
- Internet access to download datasets.

---

## Option 1: Manual Setup Using Docker and Python

This option involves manually setting up the PostgreSQL and pgAdmin containers, then running the Python ingestion script to load data.

### 1. Create a Docker Network

Create a network to allow containers to communicate:

```bash
docker network create pg-network
```

### 2. Start the PostgreSQL Container

Run PostgreSQL with the following command:

```bash
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v /workspaces/Data-Engineering-Zoomcamp/Docker_SQL/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name pg-database \
  postgres:13
```

### 3. Load Data into PostgreSQL

Download taxi and zone data using the provided Python script. The script supports CSV, CSV.GZ, and Parquet files.

**Load Taxi Data:**

```bash
python ./dataloader2.py \
  --user root \
  --password root \
  --host localhost \
  --port 5432 \
  --db ny_taxi \
  --table-name yellow_taxi_data \
  --url "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz" \
  --batch-size 10000 \
  --max-workers 1
```

**Load Zones Data:**

```bash
python ./dataloader2.py \
  --user root \
  --password root \
  --host localhost \
  --port 5432 \
  --db ny_taxi \
  --table-name zones \
  --url "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv" \
  --batch-size 10000 \
  --max-workers 1
```

### 4. Access the Database

You can view and interact with the data using either pgcli or pgAdmin.

#### Using pgcli:

```bash
pgcli -h localhost -p 5432 -u root -d ny_taxi
```

#### Using pgAdmin:

Run the pgAdmin container:

```bash
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -e PGADMIN_CONFIG_PROXY_X_HOST_COUNT=1 \
  -e PGADMIN_CONFIG_PROXY_X_PREFIX_COUNT=1 \
  -p 8080:80 \
  --network=pg-network \
  --name pgadmin \
  dpage/pgadmin4
```

Then, open your browser and navigate to [http://localhost:8080](http://localhost:8080).  
Log in with:
- **Email:** admin@admin.com
- **Password:** root

**Configure the Server Connection:**
- **Name:** (Any name you prefer, e.g., Docker)
- **Hostname:** `pg-database`
- **Username:** `root`
- **Password:** `root`
- **Port:** `5432`

After connecting, explore the `ny_taxi` database, review the tables (such as `yellow_taxi_data` and `zones`), and inspect data using the built-in viewer.

---

## Option 2: Using Docker Compose and a Wrapped Python Image

This option uses Docker Compose to bundle PostgreSQL, pgAdmin, and your Python-based ingestion tool into a multi-container setup.

### 1. Prepare the Docker Compose File

I already created `docker-compose.yaml` file [here](/workspaces/Data-Engineering-Zoomcamp/01-Docker_Sql_terraform/Docker_SQL/docker-compose.yaml) :


### 2. Build Your Ingestion Docker Image

Assuming you have a `Dockerfile` that wraps your Python ingestion script (which I already created [here](/workspaces/Data-Engineering-Zoomcamp/01-Docker_Sql_terraform/Docker_SQL/Dockerfile)), build your image:

```bash
docker build -t taxi_ingest:v001 .
```

### 3. Run the Ingestion Container

Use the built image to load data into the PostgreSQL database. Make sure to connect to the Docker network `pg-network`:

```bash
docker-compose up -d
```

### 4. Access the Database

- **pgcli:**

  ```bash
  pgcli -h localhost -p 5432 -u root -d ny_taxi
  ```

- **pgAdmin:**  
  Navigate to [http://localhost:8080](http://localhost:8080) and configure the connection.

---

## Next Steps

Continue with SQL operations and queries in [SQL.md](/workspaces/Data-Engineering-Zoomcamp/01-Docker_Sql_terraform/Docker_SQL/SQL-notes.md).