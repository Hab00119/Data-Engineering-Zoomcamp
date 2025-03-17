### Question 1
```bash
docker exec -it redpanda-1 rpk help
```

```bash
docker exec -it redpanda-1 rpk version
```
Version:     v24.2.18
Git ref:     f9a22d4430
Build date:  2025-02-14T12:52:55Z
OS/Arch:     linux/amd64
Go version:  go1.23.1

Redpanda Cluster
  node-1  v24.2.18 - f9a22d443087b824803638623d6b7492ec8221f9

### Question 2
create a topic with name green-trips

```bash
docker exec -it redpanda-1 rpk topic --help
```
create topic
```bash
docker exec -it redpanda-1 rpk topic create green-trips
```
check available topics
```bash
docker exec -it redpanda-1 rpk topic list
```

TOPIC        STATUS
green-trips  OK


### Question 3
True

### Question 4
```bash
python /workspaces/Data-Engineering-Zoomcamp/06-streaming/src/producer/green_producer.py
```
36.71 seconds

debug if redpanda receives the data
```bash
docker exec -it redpanda-1 rpk topic consume green-trips --brokers=redpanda-1:29092
```

### Question 5
use the schema
```sql
CREATE TABLE location_streaks (
    pickup_location_id INT,
    dropoff_location_id INT,
    streak_length BIGINT,
    session_start TIMESTAMP,
    session_end TIMESTAMP,
    PRIMARY KEY (session_start, session_end, pickup_location_id, dropoff_location_id)
);
```

session_job2.py

docker compose exec jobmanager ./bin/flink run -py /opt/src/job/session_job2.py --pyFiles /opt/src -d

this worked (see [link](https://www.youtube.com/watch?v=mmdHCjjl2dA&t=297s))
docker compose exec jobmanager ./bin/flink run -py /opt/src/job/session_job3.py --pyFiles /opt/src -d


```bash
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration

def create_streak_sink(t_env):
    table_name = 'location_streaks'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            pickup_location_id INT,
            dropoff_location_id INT,
            streak_length BIGINT,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            PRIMARY KEY (session_start, session_end, pickup_location_id, dropoff_location_id) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://postgres:5432/postgres',
            'table-name' = '{table_name}',
            'username' = 'postgres',
            'password' = 'postgres',
            'driver' = 'org.postgresql.Driver'
        );
        """
    t_env.execute_sql(sink_ddl)
    return table_name

def create_green_trips_source(t_env):
    table_name = "green_trips"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime STRING,
            lpep_dropoff_datetime STRING,
            PULocationID INT,
            DOLocationID INT,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            processed_dropoff_time AS CAST(TO_TIMESTAMP(lpep_dropoff_datetime, 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP(3)),
            WATERMARK for processed_dropoff_time as processed_dropoff_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'green-trips',
            'scan.startup.mode' = 'earliest-offset',
            'properties.auto.offset.reset' = 'earliest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name

def streak_analysis():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(1)
    
    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    
    try:
        # Create Kafka table for green trips
        source_table = create_green_trips_source(t_env)
        streak_table = create_streak_sink(t_env)
        
        # Use SESSION window with 5-minute gap to find streaks - fixed version
        t_env.execute_sql(f"""
        INSERT INTO {streak_table}
        SELECT
            PULocationID as pickup_location_id,
            DOLocationID as dropoff_location_id,
            COUNT(*) AS streak_length,
            SESSION_START(processed_dropoff_time, INTERVAL '5' MINUTES) as session_start,
            SESSION_END(processed_dropoff_time, INTERVAL '5' MINUTES) as session_end
        FROM {source_table}
        GROUP BY 
            SESSION(processed_dropoff_time, INTERVAL '5' MINUTES),
            PULocationID, 
            DOLocationID;
        """).wait()
        
    except Exception as e:
        print("Processing green trips data failed:", str(e))

if __name__ == '__main__':
    streak_analysis()
```

```sql
SELECT 
    pickup_location_id,
    dropoff_location_id,
    MAX(streak_length) as longest_streak
FROM location_streaks
GROUP BY pickup_location_id, dropoff_location_id
ORDER BY longest_streak DESC;
```

ANS:95-95 (44 longest streak)