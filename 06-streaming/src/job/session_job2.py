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
    env.set_parallelism(3)
    
    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)
    
    try:
        # Create Kafka table for green trips
        source_table = create_green_trips_source(t_env)
        streak_table = create_streak_sink(t_env)
        
        # Use SESSION window with 5-minute gap to find streaks
        t_env.execute_sql(f"""
        INSERT INTO {streak_table}
        SELECT
            PULocationID as pickup_location_id,
            DOLocationID as dropoff_location_id,
            COUNT(*) AS streak_length,
            window_start as session_start,
            window_end as session_end
        FROM TABLE(
            SESSION(TABLE {source_table}, DESCRIPTOR(processed_dropoff_time), INTERVAL '5' MINUTES)
        )
        GROUP BY window_start, window_end, PULocationID, DOLocationID;
        """).wait()
        
    except Exception as e:
        print("Processing green trips data failed:", str(e))

if __name__ == '__main__':
    streak_analysis()