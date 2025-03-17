from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common.time import Duration

def create_trips_source_kafka(t_env):
    table_name = "green_trips"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            lpep_pickup_datetime STRING,
            lpep_dropoff_datetime STRING,
            PULocationID BIGINT,
            DOLocationID BIGINT,
            passenger_count INT,
            trip_distance DOUBLE,
            tip_amount DOUBLE,
            event_watermark AS TO_TIMESTAMP(lpep_dropoff_datetime, 'yyyy-MM-dd HH:mm:ss'),
            WATERMARK FOR event_watermark AS event_watermark - INTERVAL '5' SECOND
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

def create_session_aggregated_sink(t_env):
    table_name = "processed_sessions"
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            PULocationID BIGINT,
            DOLocationID BIGINT,
            trip_count BIGINT,
            PRIMARY KEY (session_start, PULocationID, DOLocationID) NOT ENFORCED
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

def process_sessions():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create Kafka table
        source_table = create_trips_source_kafka(t_env)
        session_table = create_session_aggregated_sink(t_env)

        t_env.execute_sql(f"""
        INSERT INTO {session_table}
        SELECT
            window_start AS session_start,
            window_end AS session_end,
            PULocationID,
            DOLocationID,
            COUNT(*) AS trip_count
        FROM TABLE(
            SESSION(TABLE {source_table}, DESCRIPTOR(event_watermark), INTERVAL '5' MINUTE)
        )
        GROUP BY window_start, window_end, PULocationID, DOLocationID;
        """).wait()

    except Exception as e:
        print("Processing session data from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    process_sessions()
