import os
import sys

# Set JAVA_HOME if not already set (needed for PyFlink's embedded mini-cluster)
if not os.getenv("JAVA_HOME"):
    local_jdk = os.path.join(os.path.dirname(__file__), "flink_setup", "jdk11", "jdk-11.0.30+7")
    if os.path.isdir(local_jdk):
        os.environ["JAVA_HOME"] = local_jdk
        os.environ["PATH"] = os.path.join(local_jdk, "bin") + os.pathsep + os.environ["PATH"]
        print(f"[INFO] Set JAVA_HOME to {local_jdk}")

from pyflink.table import EnvironmentSettings, TableEnvironment

# --- Configuration ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
OUTPUT_PATH = os.getenv('FLINK_OUTPUT_PATH', os.path.abspath(os.path.join(os.path.dirname(__file__), "data", "output")))
MAPPING_PATH = os.getenv('FLINK_MAPPING_PATH', os.path.abspath(os.path.join(os.path.dirname(__file__), "data", "mappings")))


def to_uri(path):
    """Convert a local path to a file:/// URI for Flink."""
    return "file:///" + os.path.abspath(path).replace("\\", "/")


def load_connector_jars():
    """Load only the connector/format JARs that are NOT bundled with PyFlink."""
    if os.getenv("DOCKER_ENV"):
        lib_dir = "/opt/flink/usrlib"
        return [f"file://{lib_dir}/{f}" for f in os.listdir(lib_dir) if f.endswith('.jar')]

    # Local development: use the version-matched JARs in flink_setup/connectors/
    conn_dir = os.path.join(os.path.dirname(__file__), "flink_setup", "connectors")
    if not os.path.isdir(conn_dir):
        print(f"[ERROR] Connector JAR directory not found: {conn_dir}")
        print("  Run the setup instructions in the README to download connector JARs.")
        sys.exit(1)

    jars = []
    for f in sorted(os.listdir(conn_dir)):
        if f.endswith('.jar'):
            jars.append(to_uri(os.path.join(conn_dir, f)))
    return jars


def run_pipeline():
    """Main Flink pipeline: Kafka → Join with bus stop mapping → Parquet sink."""

    # ── 1. Table Environment ──────────────────────────────────────────────
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = TableEnvironment.create(settings)

    # Checkpointing: filesystem sink only commits files on checkpoint completion
    t_env.get_config().set("execution.checkpointing.interval", "30000")
    t_env.get_config().set("execution.checkpointing.mode", "EXACTLY_ONCE")
    t_env.get_config().set("parallelism.default", "1")
    # Limit restart attempts so errors surface quickly during testing
    t_env.get_config().set("restart-strategy.type", "fixed-delay")
    t_env.get_config().set("restart-strategy.fixed-delay.attempts", "3")
    t_env.get_config().set("restart-strategy.fixed-delay.delay", "5s")

    # ── 2. Load Connector JARs ────────────────────────────────────────────
    jars = load_connector_jars()
    print(f"\n[INFO] Loading {len(jars)} connector JARs:")
    for j in jars:
        print(f"  - {j.split('/')[-1]}")

    t_env.get_config().set("pipeline.jars", ";".join(jars))

    # ── 3. Kafka Source ───────────────────────────────────────────────────
    # Reads the raw JSON string from each Kafka message
    t_env.execute_sql(f"""
        CREATE TABLE kafka_source (
            raw_message STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'lta-bus-arrivals',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'properties.group.id' = 'flink_bus_pipeline',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'raw'
        )
    """)
    print("[OK] Created kafka_source table")

    # ── 4. Bus Stop Mapping (static Parquet lookup) ───────────────────────
    mapping_uri = to_uri(MAPPING_PATH)
    t_env.execute_sql(f"""
        CREATE TABLE bus_stop_mapping (
            BusStopCode STRING,
            Description STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{mapping_uri}',
            'format' = 'parquet'
        )
    """)
    print(f"[OK] Created bus_stop_mapping table  -> {mapping_uri}")

    # ── 5. Parquet File Sink ──────────────────────────────────────────────
    sink_uri = to_uri(OUTPUT_PATH)
    t_env.execute_sql(f"""
        CREATE TABLE parquet_sink (
            bus_stop_code        STRING,
            bus_stop_description STRING,
            raw_json             STRING,
            ingested_at          TIMESTAMP(3)
        ) WITH (
            'connector' = 'filesystem',
            'path' = '{sink_uri}',
            'format' = 'parquet'
        )
    """)
    print(f"[OK] Created parquet_sink table      -> {sink_uri}")

    # ── 6. Pipeline: Read → Extract → Join → Write ───────────────────────
    # Extract BusStopCode from the raw JSON, join to mapping for description
    pipeline_sql = """
        INSERT INTO parquet_sink
        SELECT
            JSON_VALUE(k.raw_message, '$.BusStopCode'),
            m.Description,
            k.raw_message,
            CURRENT_TIMESTAMP
        FROM kafka_source k
        JOIN bus_stop_mapping m
          ON JSON_VALUE(k.raw_message, '$.BusStopCode') = m.BusStopCode
    """

    print("\n" + "=" * 55)
    print("  Pipeline Starting")
    print("=" * 55)
    print(f"  Kafka broker : {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"  Mapping path : {MAPPING_PATH}")
    print(f"  Output path  : {OUTPUT_PATH}")
    print(f"  Checkpoint   : every 30s")
    print("-" * 55)
    print("  Waiting for data & first checkpoint...")
    print("  (Parquet files will appear in the output folder")
    print("   after the first checkpoint completes)")
    print("  Press Ctrl+C to stop.\n")

    t_env.execute_sql(pipeline_sql).wait()


if __name__ == "__main__":
    print("=" * 55)
    print("  LTA Bus Arrivals — Flink Streaming Pipeline")
    print("=" * 55)

    os.makedirs(OUTPUT_PATH, exist_ok=True)

    try:
        run_pipeline()
    except KeyboardInterrupt:
        print("\n[INFO] Pipeline stopped by user.")
    except Exception as e:
        print(f"\n[FAILED] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
