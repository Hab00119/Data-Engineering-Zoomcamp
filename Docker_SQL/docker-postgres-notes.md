# Docker and PostgreSQL Setup Guide - NYC Taxi Data Project

## Docker Basic Commands

### Container Management
```bash
# List containers
docker ps        # Running containers
docker ps -a     # All containers

# User management
docker exec <container-name> id #current user info, 0 is root

# Container operations
docker start -a <container-name>     # Start container
docker stop <container-name>         # Stop container
docker rm [-f] <container-name>      # Remove container
docker rename <old-name> <new-name>  # Rename container
docker container prune               # Remove all stopped containers
```

### Image Management
```bash
# List images
docker images

# Remove image
docker rmi [-f] <image-name>:<tag>

# Build image
docker build -t <image-name>:<tag> .
```

### Network Management
network is used to put different containers in the same group, in our case, we created a pg-network to put pg-admin and pgdatabase in the same network for easy communication
```bash
# Network operations
docker network create <network-name>
docker network ls
docker network inspect <network-name>
```

## PostgreSQL Setup

### Database Container
```bash
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name pg-database \
  postgres:13
```

### pgAdmin Setup
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

## Data Ingestion

### Running Data Loader
```bash
python dataloader.py \
  --user root \
  --password root \
  --host localhost \
  --port 5432 \
  --db ny_taxi \
  --table-name yellow_taxi_data \
  --url "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet" \
  --max-workers 4
```

## Docker Compose

### Basic Docker Compose Configuration
```yaml
version: '3.8'

services:
  pgdatabase:
    image: postgres:13
    environment:
      - POSTGRES_USER=root
      - POSTGRES_PASSWORD=root
      - POSTGRES_DB=ny_taxi
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"

  pgadmin:
    image: dpage/pgadmin4
    environment:
      - PGADMIN_DEFAULT_EMAIL=admin@admin.com
      - PGADMIN_DEFAULT_PASSWORD=root
    ports:
      - "8080:80"

volumes:
  postgres_data:
```

### Docker Compose Commands
```bash
docker-compose up [-d]    # Start services (detached mode with -d)
docker-compose down       # Stop and remove containers
```

## Port Mapping Notes
- Host port mapping: `-p host_port:container_port`
- When connecting from host machine: use `host_port`
- When connecting within Docker network: use `container_port`
- Without `-p` flag, container ports are only accessible within Docker network

### Docker Port Mapping

When running a container with the `-p <host>:<container_port>` flag, use `<host>` to connect from within the host machine.  
If `-p` is not specified, the container won't expose its ports to the host, and the database can only be accessed within the Docker network or the container itself.

Example:  
```bash
docker run -d -p 5431:5432 postgres
```
. Within the host:
```bash
pgcli -h localhost -p 5431 -u user -d db
```
. Within another container in the same network (e.g., pgAdmin):

```bash
pgcli -h localhost -p 5432 -u user -d db
```

## Connection Commands
```bash
# CLI Database Access
pgcli -h localhost -p 5432 -u root -d ny_taxi

# pgAdmin Web Interface
# Access at localhost:8080
# Login: admin@admin.com
# Password: root
```
# Summary on how to use the project
## CD to Docker_SQL folder
```bash
cd Docker_SQL
```
## Create a docker network named pg-network
```bash
docker network pg-network
```

## Create a Postgres Container  
Set up the container with the following:  
- **Username**: `root`  
- **Password**: `root`  
- **Database Name**: `ny_taxi`  
- **Volume Mapping**: `ny_taxi_postgres_data` to the Postgres container data  
- **Container Name**: `pg-database` (for easy reference)  

### Method 1: Using Postgres and pgcli (no docker network required)  
1. **Run the Postgres container**  
   You can run it in detached mode by adding the `-d` flag.  
```bash
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name pg-database \
  postgres:13
```

2. **Run pgcli in another terminal**  
   (Assuming `pgcli` is installed on your machine)  
   Use the following connection details:  
   - **Host**: `localhost`  
   - **Username**: `root`  
   - **Database**: `ny_taxi`  
   - **Password**: `root` (when prompted)  
```bash
# CLI Database Access
pgcli -h localhost -p 5432 -u root -d ny_taxi
```

You should now be connected. For web interface access, use pgAdmin instead of pgcli.

### Method 2: Using Postgres and pgAdmin Containers (requires a docker network)  
1. **Run the Postgres container**  
   You can run it in detached mode by adding the `-d` flag.  
```bash
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  --network=pg-network \
  --name pg-database \
  postgres:13
```

2. Run the pgAdmin container. I included two proxy environment variables to address issues encountered when running the code on Codespaces. Codespaces creates its own port, which can lead to conflicts (refer to: [Proxy](https://github.com/orgs/community/discussions/17918)).
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

Optional but recommended: Steps [1] and [2] can be performed using Docker Compose, which automatically creates a network for all containers (unless specified otherwise). In this setup, we explicitly used the pg-network created earlier.
```yaml
networks:
  pg-network:
    external: true
    
services:
  pgdatabase:
    image: postgres:13
    container_name: pg_database
    environment:
      - POSTGRES_USER=root
      - POSTGRES_PASSWORD=root
      - POSTGRES_DB=ny_taxi
    volumes:
      - "./ny_taxi_postgres_data:/var/lib/postgresql/data:rw"
    ports:
      - "5432:5432"
    restart: unless-stopped
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U root -h localhost -d ny_taxi"]
      interval: 10s
      timeout: 5s
      retries: 5
    networks:
      - pg-network

  pgadmin:
    image: dpage/pgadmin4
    container_name: pg_admin
    environment:
      - PGADMIN_DEFAULT_EMAIL=admin@admin.com
      - PGADMIN_DEFAULT_PASSWORD=root
    ports:
      - "8080:80"
    depends_on:
      pgdatabase:
        condition: service_healthy
    restart: unless-stopped
    networks:
      - pg-network
```

Ensure the current user has permission to access the host volume ./ny_taxi_postgres_data. If not, you may need to create a new volume.
```bash
sudo rm -rf ./ny_taxi_postgres_data
mkdir ny_taxi_postgres_data
chown $(id -u):$(id -g) ny_taxi_postgres_data
```

run docker-compose
```bash
docker-compose up
```

3. Setting Up Postgres and pgAdmin  

Ensure both the Postgres and pgAdmin containers are running. To view pgAdmin:  
1. Click on the forwarded port (typically 8080, but Codespaces generates its own port).  
2. Log in using the credentials specified in Docker:  
   - **Username**: `admin@admin.com`  
   - **Password**: `root`  

Once logged in, you will be directed to the landing page:  
- Navigate to **Servers > Register > Server**.  
  - **Name**: Use a name of your choice (e.g., `Docker`).  
- Go to the **Connection** tab and enter the following details:  
  - **Hostname**: `pg-database` (the Postgres container name used in Docker)  
  - **Username**: `root` (Docker Postgres username)  
  - **Password**: `root` (Docker Postgres password)  
  - **Port**: `5432` (the container port, not the host port).  
    - Note: It may take multiple attempts to log in successfully.  

After logging in:  
1. Navigate to **Docker (or the name you used) > Databases > ny_taxi > Schemas > Tables > yellow_taxi_data**.  
2. Right-click on `yellow_taxi_data` and select **View/Edit Data > First 100 Rows**.  

**Note**: If no data has been ingested into the Postgres database, the `yellow_taxi_data` table will not appear.

## Data Ingestion with docker
I created a Dockerfile to load `dataloader.py` (a Python script for ingesting data into the database). You can find the code here: [link](https://github.com/Hab00119/Data-Engineering-Zoomcamp/tree/main/Docker_SQL).  

To run the Dockerfile:  
1. **Build a Docker image**  
```bash
docker build -t taxi_ingest:v001 .
```

2. **Run the image with the dataloader parameters**  
   The dataloader supports both `.csv` and `.parquet` files. For parallelization, you can use the `--max-workers` parameter. However, if memory is limited, set `--max-workers` to `1`.  
```bash
docker run -it --network=pg-network taxi_ingest:v001
    --user root 
    --password root
    --host pg-database
    --port 5432
    --db ny_taxi
    --table-name zones
    --url "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    --max-workers 1
```