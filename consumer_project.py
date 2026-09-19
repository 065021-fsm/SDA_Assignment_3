"""
Airline Cargo Kafka Consumer

Consumes:
    cargo_manifest
    sensor_readings
    flight_tracking

and stores them in MySQL.
"""

import io
import json
import logging
import os
import signal
import sys

from kafka import KafkaConsumer

from mysql_storage import MySQLStorage


# ---------------------------------------------------------
# LOGGING
# ---------------------------------------------------------

# On Windows, the console's default codepage (cp1252) can't encode
# characters like the checkmark used below, which throws
# UnicodeEncodeError and spams "--- Logging error ---". Force UTF-8 on
# both the file handler and the console stream to avoid that.
_console_stream = sys.stdout
if hasattr(sys.stdout, 'buffer'):
    _console_stream = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("consumer.log", encoding="utf-8"),
        logging.StreamHandler(_console_stream)
    ]
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

KAFKA_TOPICS = [
    "cargo_manifest",
    "sensor_readings",
    "flight_tracking"
]

CONSUMER_GROUP = "airline-cargo-mysql-consumer"


# ---------------------------------------------------------
# MYSQL CONFIGURATION
# ---------------------------------------------------------

MYSQL_CONFIG = {

    "host": os.getenv("MYSQL_HOST", "localhost"),

    "port": int(os.getenv("MYSQL_PORT", "3306")),

    "user": os.getenv("MYSQL_USER", "root"),

    # Falls back to the hardcoded value so nothing breaks today, but set
    # the MYSQL_PASSWORD environment variable and remove the fallback
    # before this code goes anywhere near version control.
    "password": os.getenv("MYSQL_PASSWORD", "248920b@G"),

    "database": os.getenv("MYSQL_DATABASE", "airline_cargo")

}


# ---------------------------------------------------------
# GLOBAL OBJECTS
# ---------------------------------------------------------

mysql_storage = None
consumer = None


# ---------------------------------------------------------
# CREATE KAFKA CONSUMER
# ---------------------------------------------------------

def create_consumer():

    try:

        consumer = KafkaConsumer(

            *KAFKA_TOPICS,

            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,

            group_id=CONSUMER_GROUP,

            auto_offset_reset="earliest",

            enable_auto_commit=False,

            value_deserializer=lambda x: json.loads(
                x.decode("utf-8")
            ),

            key_deserializer=lambda x:
                x.decode("utf-8")
                if x
                else None

        )

        logger.info(
            "Connected to Kafka"
        )

        logger.info(
            f"Subscribed to topics: {KAFKA_TOPICS}"
        )

        return consumer

    except Exception as e:

        logger.error(
            f"Failed to create Kafka consumer: {e}"
        )

        return None


# ---------------------------------------------------------
# PROCESS MESSAGE
# ---------------------------------------------------------

def process_message(topic, data):

    try:

        logger.info(
            f"Processing message from topic: {topic}"
        )

        if topic == "cargo_manifest":

            return mysql_storage.insert_cargo_manifest(
                data
            )

        elif topic == "sensor_readings":

            return mysql_storage.insert_sensor_reading(
                data
            )

        elif topic == "flight_tracking":

            return mysql_storage.insert_flight_tracking(
                data
            )

        else:

            logger.warning(
                f"Unknown Kafka topic: {topic}"
            )

            return False

    except Exception as e:

        logger.error(
            f"Error processing message: {e}"
        )

        return False


# ---------------------------------------------------------
# MAIN CONSUMER LOOP
# ---------------------------------------------------------

def run_consumer():

    global mysql_storage
    global consumer

    # -----------------------------------------------------
    # CONNECT TO MYSQL
    # -----------------------------------------------------

    logger.info(
        "Connecting to MySQL..."
    )

    mysql_storage = MySQLStorage(
        host=MYSQL_CONFIG["host"],
        port=MYSQL_CONFIG["port"],
        user=MYSQL_CONFIG["user"],
        password=MYSQL_CONFIG["password"],
        database=MYSQL_CONFIG["database"]
    )

    if not mysql_storage.connect():

        logger.error(
            "Could not connect to MySQL"
        )

        return

    # -----------------------------------------------------
    # CONNECT TO KAFKA
    # -----------------------------------------------------

    consumer = create_consumer()

    if consumer is None:

        mysql_storage.close()

        return

    logger.info("=" * 70)

    logger.info(
        "AIRLINE CARGO KAFKA CONSUMER STARTED"
    )

    logger.info("=" * 70)

    logger.info(
        f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}"
    )

    logger.info(
        f"MySQL: {MYSQL_CONFIG['database']}"
    )

    logger.info(
        f"Topics: {KAFKA_TOPICS}"
    )

    logger.info("=" * 70)

    # -----------------------------------------------------
    # CONSUME MESSAGES
    # -----------------------------------------------------

    try:

        for message in consumer:

            logger.info(
                f"Received message | "
                f"topic={message.topic} | "
                f"partition={message.partition} | "
                f"offset={message.offset}"
            )

            success = process_message(
                message.topic,
                message.value
            )

            # Only commit after successful MySQL insertion
            if success:

                try:

                    consumer.commit()

                    logger.info(
                        f"✓ Message stored successfully | "
                        f"{message.topic} | "
                        f"offset={message.offset}"
                    )

                except Exception as e:

                    logger.error(
                        f"Failed to commit offset | "
                        f"{message.topic} | "
                        f"offset={message.offset} | "
                        f"error={e}"
                    )

            else:

                logger.error(
                    f"✗ Failed to store message | "
                    f"{message.topic} | "
                    f"offset={message.offset}"
                )

    except KeyboardInterrupt:

        logger.info(
            "Shutdown signal received"
        )

    except Exception as e:

        logger.error(
            f"Consumer error: {e}"
        )

    finally:

        shutdown()


# ---------------------------------------------------------
# SHUTDOWN
# ---------------------------------------------------------

def shutdown():

    global mysql_storage
    global consumer

    logger.info(
        "Shutting down consumer..."
    )

    try:

        if consumer:

            consumer.close()

            logger.info(
                "Kafka consumer closed"
            )

    except Exception as e:

        logger.error(
            f"Error closing Kafka consumer: {e}"
        )

    try:

        if mysql_storage:

            mysql_storage.close()

    except Exception as e:

        logger.error(
            f"Error closing MySQL: {e}"
        )

    logger.info(
        "Consumer stopped"
    )


# ---------------------------------------------------------
# SIGNAL HANDLERS
# ---------------------------------------------------------

def signal_handler(sig, frame):

    logger.info(
        "Termination signal received"
    )

    shutdown()

    sys.exit(0)


signal.signal(
    signal.SIGINT,
    signal_handler
)

signal.signal(
    signal.SIGTERM,
    signal_handler
)


# ---------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------

if __name__ == "__main__":

    run_consumer()