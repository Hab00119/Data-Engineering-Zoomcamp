from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, TableEnvironment, StreamTableEnvironment


def create_processed_events_sink_postgres(t_env):
    table_name = 'processed_events'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            test_data INTEGER,
            event_timestamp TIMESTAMP
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


def create_events_source_kafka(t_env):
    table_name = "events"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            test_data INTEGER,
            event_timestamp BIGINT,
            event_watermark AS TO_TIMESTAMP_LTZ(event_timestamp, 3),
            WATERMARK FOR event_watermark AS event_watermark - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = 'redpanda-1:29092',
            'topic' = 'test-topic',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
        """
    t_env.execute_sql(source_ddl)
    return table_name


def log_processing():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10 * 1000)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create Kafka source and PostgreSQL sink tables
        source_table = create_events_source_kafka(t_env)
        postgres_sink = create_processed_events_sink_postgres(t_env)

        # Print the received data from Kafka before processing
        print("🔍 Debugging: Printing received events from Kafka:")
        t_env.execute_sql(f"SELECT * FROM {source_table}").print()

        # Create a print sink for debugging purposes
        t_env.execute_sql(
            f"""
            CREATE TEMPORARY TABLE print_table WITH ('connector' = 'print')
            LIKE {source_table} (EXCLUDING ALL)
            """
        )

        # Insert into print table to check data before writing to PostgreSQL
        print("🔍 Debugging: Writing data to print table...")
        t_env.execute_sql(f"INSERT INTO print_table SELECT * FROM {source_table}").wait()

        # Insert into PostgreSQL
        print("🔍 Debugging: Writing data to PostgreSQL...")
        t_env.execute_sql(
            f"""
            INSERT INTO {postgres_sink}
            SELECT
                test_data,
                TO_TIMESTAMP_LTZ(event_timestamp, 3) AS event_timestamp
            FROM {source_table}
            """
        ).wait()

        print("✅ Data successfully written to PostgreSQL.")

    except Exception as e:
        print("❌ ERROR: Writing records from Kafka to JDBC failed:", str(e))


if __name__ == '__main__':
    log_processing()

