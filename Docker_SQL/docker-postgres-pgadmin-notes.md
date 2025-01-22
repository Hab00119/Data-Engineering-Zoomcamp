# Docker and PostgreSQL Setup Guide - NYC Taxi Data Project

## Table of Contents
1. [Docker Basics](#docker-basics)
    - [Container Management](#container-management)
    - [Image Management](#image-management)
    - [Network Management](#network-management)
2. [PostgreSQL Setup](#postgresql-setup)
    - [Database Container](#database-container)
    - [pgAdmin Setup](#pgadmin-setup)
3. [Data Ingestion](#data-ingestion)
    - [Running Data Loader](#running-data-loader)
4. [Docker Compose](#docker-compose)
    - [Basic Docker Compose Configuration](#basic-docker-compose-configuration)
    - [Docker Compose Commands](#docker-compose-commands)
5. [Port Mapping Notes](#port-mapping-notes)
6. [Connection Commands](#connection-commands)
7. [Summary on How to Use the Project](#summary-on-how-to-use-the-project)
    - [Method 1: Using Postgres and pgcli](#method-1-using-postgres-and-pgcli)
    - [Method 2: Using Postgres and pgAdmin Containers](#method-2-using-postgres-and-pgadmin-containers)

---

## Docker Basics

### Container Management
```bash
# List containers
docker ps        # Running containers
docker ps -a     # All containers

# User management
docker exec <container-name> id  # Current user info, 0 is root

# Container operations
docker start -a <container-name>      # Start container
docker stop <container-name>          # Stop container
docker rm [-f] <container-name>       # Remove container
docker rename <old-name> <new-name>   # Rename container
docker container prune                # Remove all stopped containers
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
```bash
# Network operations
docker network create <network-name>
docker network ls
docker network inspect <network-name>
```
Note: Networks are used to group containers for communication. For this project, we created a pg-network to connect pg-database and pgAdmin for easy interaction.

