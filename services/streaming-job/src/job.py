import os
import logging
import sys
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

# Configure logging to see what's happening
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    logger.info("=== Starting Flink Streaming Job ===")
    
    # Setup environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(int(os.getenv("FLINK_PARALLELISM", "1")))
    
    # Enable Latency Tracking (as described in the paper)
    env.get_config().set_latency_tracking_interval(1000)
    
    # Enable Checkpointing (every 10s)
    env.enable_checkpointing(10000)
    
    # Table environment
    t_env = StreamTableEnvironment.create(env)
    
    # Config
    kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    source_topic = os.getenv("SOURCE_TOPIC", "trips_in")
    sink_topic = os.getenv("SINK_TOPIC", "trips_stats")
    
    logger.info(f"Configuration: Source={source_topic}, Sink={sink_topic}, Kafka={kafka_servers}")

    # Define Source Table (Kafka)
    logger.info("Creating source table...")
    t_env.execute_sql(f"""
        CREATE TABLE trips_source (
            event_id STRING,
            event_time_ms BIGINT,
            sent_time_ms BIGINT,
            payload STRING,
            ts AS TO_TIMESTAMP_LTZ(sent_time_ms, 3),
            WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{source_topic}',
            'properties.bootstrap.servers' = '{kafka_servers}',
            'properties.group.id' = 'flink-consumer',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json'
        )
    """)

    # Define Sink Table (Regular Kafka - simpler than upsert)
    logger.info("Creating sink table...")
    t_env.execute_sql(f"""
        CREATE TABLE trips_sink (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            cnt BIGINT
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{sink_topic}',
            'properties.bootstrap.servers' = '{kafka_servers}',
            'format' = 'json'
        )
    """)

    # Execute Query: Count trips per 10-second tumbling window
    logger.info("Starting streaming query...")
    query = """
        INSERT INTO trips_sink
        SELECT
            window_start,
            window_end,
            COUNT(*) as cnt
        FROM TABLE(
            TUMBLE(TABLE trips_source, DESCRIPTOR(ts), INTERVAL '10' SECOND)
        )
        GROUP BY window_start, window_end
    """
    
    logger.info(f"Query: {query}")
    t_env.execute_sql(query)
    
    logger.info("Job submitted successfully!")

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.error(f"Job failed with error: {e}", exc_info=True)
        sys.exit(1)
