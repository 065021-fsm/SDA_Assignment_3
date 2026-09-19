"""
MySQL Storage Layer for Airline Cargo Streaming Project

Receives processed Kafka messages and stores them in MySQL.
"""

import mysql.connector
from mysql.connector import Error
import logging
import json
from datetime import datetime


logger = logging.getLogger(__name__)


class MySQLStorage:

    def __init__(
        self,
        host="localhost",
        port=3306,
        user="root",
        password="248920b@G",
        database="airline_cargo"
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None

    # ---------------------------------------------------------
    # CONNECT TO MYSQL
    # ---------------------------------------------------------

    def connect(self):

        try:

            # First connect without database
            connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password
            )

            cursor = connection.cursor()

            cursor.execute(
                f"CREATE DATABASE IF NOT EXISTS `{self.database}`"
            )

            cursor.close()
            connection.close()

            # Connect to the actual database
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )

            if self.connection.is_connected():

                logger.info(
                    f"Connected to MySQL database: {self.database}"
                )

                self.create_tables()

                return True

        except Error as e:

            logger.error(f"MySQL connection error: {e}")

            return False

    # ---------------------------------------------------------
    # CREATE TABLES
    # ---------------------------------------------------------

    def create_tables(self):

        cursor = self.connection.cursor()

        # -----------------------------------------------------
        # CARGO MANIFEST TABLE
        # -----------------------------------------------------

        cargo_table = """
        CREATE TABLE IF NOT EXISTS cargo_manifests (

            id BIGINT AUTO_INCREMENT PRIMARY KEY,

            cargo_id VARCHAR(50),
            awb_number VARCHAR(50),

            shipper VARCHAR(255),
            consignee VARCHAR(255),

            destination_airport VARCHAR(10),

            weight_kg DOUBLE,

            length_cm DOUBLE,
            width_cm DOUBLE,
            height_cm DOUBLE,

            hazmat_class VARCHAR(20),
            hazmat_code VARCHAR(20),

            special_handling JSON,

            insurance_value_usd DOUBLE,

            booking_timestamp DATETIME NULL,
            status VARCHAR(50),

            estimated_delivery DATETIME NULL,

            kafka_received_at DATETIME DEFAULT CURRENT_TIMESTAMP,

            UNIQUE KEY unique_awb (awb_number)

        )
        """

        cursor.execute(cargo_table)

        # -----------------------------------------------------
        # SENSOR READINGS TABLE
        # -----------------------------------------------------

        sensor_table = """
        CREATE TABLE IF NOT EXISTS sensor_readings (

            id BIGINT AUTO_INCREMENT PRIMARY KEY,

            cargo_id VARCHAR(50),
            flight_number VARCHAR(50),

            event_timestamp DATETIME NULL,

            temperature_celsius DOUBLE,
            humidity_percent DOUBLE,

            latitude DOUBLE,
            longitude DOUBLE,
            altitude_meters DOUBLE,

            impact_detected BOOLEAN,
            door_open BOOLEAN,

            pressure_hpa DOUBLE,

            source VARCHAR(100),
            data_quality_score DOUBLE,

            destination_airport VARCHAR(10),

            external_temperature_celsius DOUBLE,
            external_humidity_percent DOUBLE,
            wind_speed_kmh DOUBLE,
            weather_condition VARCHAR(100),

            weather_timestamp DATETIME NULL,

            kafka_received_at DATETIME DEFAULT CURRENT_TIMESTAMP,

            INDEX idx_sensor_cargo (cargo_id),
            INDEX idx_sensor_timestamp (event_timestamp)

        )
        """

        cursor.execute(sensor_table)

        # -----------------------------------------------------
        # FLIGHT TRACKING TABLE
        # -----------------------------------------------------

        flight_table = """
        CREATE TABLE IF NOT EXISTS flight_tracking (

            id BIGINT AUTO_INCREMENT PRIMARY KEY,

            flight_number VARCHAR(50),

            aircraft_icao VARCHAR(50),
            airline VARCHAR(20),

            event_timestamp DATETIME NULL,

            latitude DOUBLE,
            longitude DOUBLE,

            altitude_ft DOUBLE,
            speed_knots DOUBLE,

            flight_status VARCHAR(50),

            origin VARCHAR(10),
            destination VARCHAR(10),

            scheduled_arrival DATETIME NULL,
            estimated_arrival DATETIME NULL,

            cargo_hold_temperature DOUBLE,
            cargo_hold_humidity DOUBLE,

            source VARCHAR(100),

            kafka_received_at DATETIME DEFAULT CURRENT_TIMESTAMP,

            INDEX idx_flight_number (flight_number),
            INDEX idx_flight_timestamp (event_timestamp)

        )
        """

        cursor.execute(flight_table)

        self.connection.commit()

        cursor.close()

        logger.info("MySQL tables created successfully")

    # ---------------------------------------------------------
    # HELPER: CONVERT ISO TIMESTAMP
    # ---------------------------------------------------------

    @staticmethod
    def parse_datetime(value):

        if not value:
            return None

        try:

            # Remove Z
            value = value.replace("Z", "")

            # Handle ISO datetime
            return datetime.fromisoformat(value)

        except Exception:

            return None

    # ---------------------------------------------------------
    # STORE CARGO MANIFEST
    # ---------------------------------------------------------

    def insert_cargo_manifest(self, data):

        try:

            cursor = self.connection.cursor()

            sql = """
            INSERT INTO cargo_manifests (

                cargo_id,
                awb_number,
                shipper,
                consignee,
                destination_airport,
                weight_kg,
                length_cm,
                width_cm,
                height_cm,
                hazmat_class,
                hazmat_code,
                special_handling,
                insurance_value_usd,
                booking_timestamp,
                status,
                estimated_delivery

            )

            VALUES (

                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s

            )

            ON DUPLICATE KEY UPDATE

                status = VALUES(status),
                estimated_delivery = VALUES(estimated_delivery)

            """

            dimensions = data.get("dimensions", {})

            values = (

                data.get("cargo_id"),

                data.get("awb_number"),

                data.get("shipper"),

                data.get("consignee"),

                data.get("destination_airport"),

                data.get("weight_kg"),

                dimensions.get("length_cm"),

                dimensions.get("width_cm"),

                dimensions.get("height_cm"),

                data.get("hazmat_class"),

                data.get("hazmat_code"),

                json.dumps(
                    data.get("special_handling", [])
                ),

                data.get("insurance_value_usd"),

                self.parse_datetime(
                    data.get("booking_timestamp")
                ),

                data.get("status"),

                self.parse_datetime(
                    data.get("estimated_delivery")
                )
            )

            cursor.execute(sql, values)

            self.connection.commit()

            cursor.close()

            logger.info(
                f"Stored cargo manifest: "
                f"{data.get('cargo_id')}"
            )

            return True

        except Error as e:

            logger.error(
                f"Error storing cargo manifest: {e}"
            )

            self.connection.rollback()

            return False

    # ---------------------------------------------------------
    # STORE SENSOR READING
    # ---------------------------------------------------------

    def insert_sensor_reading(self, data):

        try:

            cursor = self.connection.cursor()

            sensor = data.get(
                "sensor_readings", {}
            )

            location = sensor.get(
                "location", {}
            )

            weather = data.get(
                "environmental_enrichment", {}
            )

            sql = """
            INSERT INTO sensor_readings (

                cargo_id,
                flight_number,
                event_timestamp,

                temperature_celsius,
                humidity_percent,

                latitude,
                longitude,
                altitude_meters,

                impact_detected,
                door_open,
                pressure_hpa,

                source,
                data_quality_score,

                destination_airport,

                external_temperature_celsius,
                external_humidity_percent,
                wind_speed_kmh,
                weather_condition,

                weather_timestamp

            )

            VALUES (

                %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s,
                %s, %s, %s, %s,
                %s

            )
            """

            values = (

                data.get("cargo_id"),

                data.get("flight_number"),

                self.parse_datetime(
                    data.get("timestamp")
                ),

                sensor.get(
                    "temperature_celsius"
                ),

                sensor.get(
                    "humidity_percent"
                ),

                location.get("latitude"),

                location.get("longitude"),

                location.get("altitude_meters"),

                sensor.get(
                    "impact_detected",
                    False
                ),

                sensor.get(
                    "door_open",
                    False
                ),

                sensor.get(
                    "pressure_hpa"
                ),

                data.get("source"),

                data.get(
                    "data_quality_score"
                ),

                weather.get(
                    "destination_airport"
                ),

                weather.get(
                    "external_temperature_celsius"
                ),

                weather.get(
                    "external_humidity_percent"
                ),

                weather.get(
                    "wind_speed_kmh"
                ),

                weather.get(
                    "weather_condition"
                ),

                self.parse_datetime(
                    weather.get(
                        "weather_timestamp"
                    )
                )
            )

            cursor.execute(sql, values)

            self.connection.commit()

            cursor.close()

            logger.info(
                f"Stored sensor reading: "
                f"{data.get('cargo_id')}"
            )

            return True

        except Error as e:

            logger.error(
                f"Error storing sensor reading: {e}"
            )

            self.connection.rollback()

            return False

    # ---------------------------------------------------------
    # STORE FLIGHT TRACKING
    # ---------------------------------------------------------

    def insert_flight_tracking(self, data):

        try:

            cursor = self.connection.cursor()

            position = data.get(
                "position", {}
            )

            sql = """
            INSERT INTO flight_tracking (

                flight_number,
                aircraft_icao,
                airline,
                event_timestamp,

                latitude,
                longitude,

                altitude_ft,
                speed_knots,

                flight_status,

                origin,
                destination,

                scheduled_arrival,
                estimated_arrival,

                cargo_hold_temperature,
                cargo_hold_humidity,

                source

            )

            VALUES (

                %s, %s, %s, %s,
                %s, %s,
                %s, %s,
                %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s

            )
            """

            values = (

                data.get("flight_number"),

                data.get("aircraft_icao"),

                data.get("airline"),

                self.parse_datetime(
                    data.get("timestamp")
                ),

                position.get("latitude"),

                position.get("longitude"),

                position.get("altitude_ft"),

                position.get("speed_knots"),

                data.get("flight_status"),

                data.get("origin"),

                data.get("destination"),

                self.parse_datetime(
                    data.get("scheduled_arrival")
                ),

                self.parse_datetime(
                    data.get("estimated_arrival")
                ),

                data.get(
                    "cargo_hold_temperature"
                ),

                data.get(
                    "cargo_hold_humidity"
                ),

                data.get(
                    "source",
                    "SIMULATED"
                )
            )

            cursor.execute(sql, values)

            self.connection.commit()

            cursor.close()

            logger.info(
                f"Stored flight tracking: "
                f"{data.get('flight_number')}"
            )

            return True

        except Error as e:

            logger.error(
                f"Error storing flight tracking: {e}"
            )

            self.connection.rollback()

            return False

    # ---------------------------------------------------------
    # CLOSE CONNECTION
    # ---------------------------------------------------------

    def close(self):

        if self.connection:

            if self.connection.is_connected():

                self.connection.close()

                logger.info(
                    "MySQL connection closed"
                )