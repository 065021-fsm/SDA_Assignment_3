"""
Airline Cargo Alerts Consumer

Reads the same Kafka topics as consumer_project.py

    cargo_manifest
    sensor_readings
    flight_tracking

runs alert rules on every message and stores the resulting alerts in a new
`alerts` table in the same MySQL database (airline_cargo).

It uses its OWN consumer group, so it can run next to consumer_project.py
and both will receive every message.

Alert lifecycle (column `status`):
    OPEN      a condition is currently active (e.g. temperature out of range).
              Set to RESOLVED automatically once the condition has cleared.
    RESOLVED  the condition cleared; `resolved_at` is filled in.
    EVENT     one-shot occurrence with no lifecycle (impact, door open,
              temperature swing).

All timestamps in the alerts table are UTC.

Run:
    1. Enter your MySQL password at MYSQL_PASSWORD below (the only place)
    2. python consumer_alerts_store_mysql.py
"""

import json
import logging
import sys
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaConsumer
from mysql.connector import Error as MySQLError

from mysql_storage import MySQLStorage


# =========================================================
# CONNECTION SETTINGS
# =========================================================

MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_USER = "root"
MYSQL_PASSWORD = "248920b@G"   # <-- enter your password here
MYSQL_DATABASE = "airline_cargo"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

KAFKA_TOPICS = ["cargo_manifest", "sensor_readings", "flight_tracking"]

# Separate group from consumer_project.py ("airline-cargo-mysql-consumer")
CONSUMER_GROUP = "airline-cargo-alerts-consumer"

# "earliest" replays the whole backlog on first start (alerts get their
# original event times). Use "latest" to only alert on new messages.
AUTO_OFFSET_RESET = "earliest"


# ---------------------------------------------------------
# LOGGING
# ---------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("consumer_alerts.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger("alerts_consumer")

HOUSEKEEPING_INTERVAL_S = 30   # how often time-based rules are evaluated
RETRY_DELAY_S = 5              # wait before retrying after a MySQL error
CONTEXT_RETRY_S = 60           # how long to remember "no manifest found"


# ---------------------------------------------------------
# ALERT THRESHOLDS  (edit these to tune the rules)
# ---------------------------------------------------------

# Allowed internal temperature range (min, max) in Celsius per hazmat_class.
# Only classes listed here get a cold-chain alert. PHARM 2-8 C is the standard
# pharmaceutical cold-chain range; add e.g. "FOOD": (0.0, 10.0) if you want it.
TEMP_LIMITS_C = {
    "PHARM": (2.0, 8.0),
}

THRESHOLDS = {
    "humidity_max_pct": 60.0,
    "temp_swing_c": 5.0,                 # change between consecutive readings
    "data_quality_min": 0.90,
    "door_open_min_altitude_m": 300.0,   # door open above this = "in flight"
    "sensor_silent_seconds": 300,        # no reading for this long
    "flight_delay_warn_min": 60,
    "flight_delay_critical_min": 120,
    "flight_scheduled_speed_kt": 50,     # moving while status is "scheduled"
    "flight_low_altitude_ft": 3000,      # low while en-route
    "stuck_booked_within_hours": 12,     # still BOOKED this close to delivery
    "high_value_usd": 1_000_000,         # insurance value that escalates severity
}

# Rule -> (seconds the condition must hold before alerting,
#          seconds it must stay clear before the alert is resolved)
RULE_TIMING = {
    "COLD_CHAIN_BREACH":      (300, 120),
    "HUMIDITY_HIGH":          (300, 120),
    "DATA_QUALITY_LOW":       (300, 120),
    "SENSOR_SILENT":          (0, 0),
    "FLIGHT_DELAY":           (120, 120),
    "FLIGHT_STATUS_MISMATCH": (60, 60),
    "FLIGHT_LOW_ALTITUDE":    (300, 120),
    "STUCK_SHIPMENT":         (0, 0),
}

# Producer sends DEPARTED / IN_FLIGHT / DESCENDING (simulated) or
# en-route / scheduled / landed (AirLabs). Only these mean "cruising".
EN_ROUTE_STATUSES = {"en-route", "in-flight"}

SEVERITIES = ["INFO", "WARNING", "CRITICAL"]


# ---------------------------------------------------------
# HELPERS
# ---------------------------------------------------------

def utcnow():
    """Naive UTC datetime, whole seconds (matches how event times are stored)."""
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


def parse_ts(value):
    """ISO string (with or without Z) -> naive UTC datetime, or None."""
    dt = MySQLStorage.parse_datetime(value)
    if dt is None:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.replace(microsecond=0)


def num(value):
    """Best-effort float conversion; None when missing or invalid."""
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def bump(severity):
    return SEVERITIES[min(SEVERITIES.index(severity) + 1, len(SEVERITIES) - 1)]


# ---------------------------------------------------------
# MYSQL STORAGE (reuses your MySQLStorage connection logic)
# ---------------------------------------------------------

ALERTS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS alerts (

    id BIGINT AUTO_INCREMENT PRIMARY KEY,

    dedup_key VARCHAR(150) NOT NULL,

    alert_type VARCHAR(50) NOT NULL,
    category VARCHAR(20) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'OPEN',

    message VARCHAR(500),

    cargo_id VARCHAR(50),
    flight_number VARCHAR(50),
    awb_number VARCHAR(50),
    hazmat_class VARCHAR(20),
    destination_airport VARCHAR(10),

    metric_name VARCHAR(50),
    metric_value DOUBLE,
    threshold_value DOUBLE,

    details JSON,
    source_topic VARCHAR(50),

    triggered_at DATETIME NOT NULL,
    resolved_at DATETIME NULL,
    created_at DATETIME NOT NULL,

    UNIQUE KEY uq_alert_occurrence (dedup_key, triggered_at),

    INDEX idx_alerts_status_severity (status, severity),
    INDEX idx_alerts_type (alert_type),
    INDEX idx_alerts_cargo (cargo_id),
    INDEX idx_alerts_triggered (triggered_at)

)
"""

INSERT_ALERT_SQL = """
INSERT INTO alerts (
    dedup_key, alert_type, category, severity, status, message,
    cargo_id, flight_number, awb_number, hazmat_class, destination_airport,
    metric_name, metric_value, threshold_value,
    details, source_topic,
    triggered_at, created_at
)
VALUES (
    %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s,
    %s, %s, %s,
    %s, %s,
    %s, %s
)
ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)
"""


class AlertsStorage(MySQLStorage):

    def create_tables(self):
        """Called by MySQLStorage.connect(): existing tables + alerts table."""
        super().create_tables()

        cursor = self.connection.cursor()
        cursor.execute(ALERTS_TABLE_DDL)
        self.connection.commit()
        cursor.close()

        logger.info("alerts table ready")

    def _cursor(self, **kwargs):
        # Reconnects if MySQL dropped an idle connection
        self.connection.ping(reconnect=True, attempts=3, delay=1)
        return self.connection.cursor(**kwargs)

    def _rollback(self):
        try:
            self.connection.rollback()
        except Exception:
            pass

    def insert_alert(self, alert):
        """Insert an alert and return its id. Re-inserting the same
        (dedup_key, triggered_at) returns the existing id, so a message that
        is redelivered after a crash does not create a duplicate."""
        cursor = None
        try:
            cursor = self._cursor()
            cursor.execute(INSERT_ALERT_SQL, (
                alert["dedup_key"],
                alert["alert_type"],
                alert["category"],
                alert["severity"],
                alert["status"],
                (alert.get("message") or "")[:500],
                alert.get("cargo_id"),
                alert.get("flight_number"),
                alert.get("awb_number"),
                alert.get("hazmat_class"),
                alert.get("destination_airport"),
                alert.get("metric_name"),
                alert.get("metric_value"),
                alert.get("threshold_value"),
                json.dumps(alert.get("details") or {}, default=str),
                alert.get("source_topic"),
                alert["triggered_at"],
                utcnow(),
            ))

            alert_id = cursor.lastrowid
            if not alert_id:
                cursor.execute(
                    "SELECT id FROM alerts WHERE dedup_key = %s AND triggered_at = %s",
                    (alert["dedup_key"], alert["triggered_at"]),
                )
                row = cursor.fetchone()
                alert_id = row[0] if row else None

            self.connection.commit()
            return alert_id

        except MySQLError:
            self._rollback()
            raise
        finally:
            if cursor:
                cursor.close()

    def resolve_alert(self, alert_id, resolved_at):
        cursor = None
        try:
            cursor = self._cursor()
            cursor.execute(
                "UPDATE alerts SET status = 'RESOLVED', resolved_at = %s "
                "WHERE id = %s AND status = 'OPEN'",
                (resolved_at, alert_id),
            )
            self.connection.commit()
        except MySQLError:
            self._rollback()
            raise
        finally:
            if cursor:
                cursor.close()

    def load_open_state_alerts(self, alert_types):
        """OPEN alerts left over from a previous run, so a restart neither
        duplicates them nor leaves them open forever."""
        cursor = None
        try:
            cursor = self._cursor()
            placeholders = ", ".join(["%s"] * len(alert_types))
            cursor.execute(
                "SELECT id, dedup_key FROM alerts "
                f"WHERE status = 'OPEN' AND alert_type IN ({placeholders})",
                tuple(alert_types),
            )
            return cursor.fetchall()
        finally:
            if cursor:
                cursor.close()

    def fetch_cargo_context(self, cargo_id):
        """Latest manifest for a cargo from cargo_manifests (written by
        consumer_project.py). Used when this consumer has not seen the
        manifest message itself."""
        cursor = None
        try:
            cursor = self._cursor(dictionary=True)
            cursor.execute(
                "SELECT awb_number, hazmat_class, hazmat_code, "
                "insurance_value_usd, status, estimated_delivery, "
                "destination_airport "
                "FROM cargo_manifests WHERE cargo_id = %s "
                "ORDER BY id DESC LIMIT 1",
                (cargo_id,),
            )
            row = cursor.fetchone()
            if not row:
                return None
            row["status"] = (row.get("status") or "").upper()
            return row
        finally:
            if cursor:
                cursor.close()


# ---------------------------------------------------------
# ALERT ENGINE
# ---------------------------------------------------------

class AlertEngine:

    def __init__(self, storage):
        self.storage = storage

        self.cargo_ctx = {}          # cargo_id -> manifest info
        self.ctx_miss = {}           # cargo_id -> monotonic time of last miss
        self.flight_to_cargo = {}    # flight_number -> cargo_id (from sensors)
        self.state = {}              # "RULE|entity" -> breach/clear tracking
        self.last_temp = {}          # cargo_id -> last temperature
        self.last_seen = {}          # cargo_id -> monotonic time of last reading

    # -- startup ------------------------------------------------------

    def restore_open_alerts(self):
        rows = self.storage.load_open_state_alerts(list(RULE_TIMING))
        for alert_id, dedup_key in rows:
            self.state[dedup_key] = {
                "breach_since": utcnow(),
                "clear_since": None,
                "alert_id": alert_id,
            }
        if rows:
            logger.info(f"Restored {len(rows)} open alert(s) from MySQL")

    # -- shared building blocks ---------------------------------------

    def cargo_context(self, cargo_id):
        if not cargo_id:
            return {}

        ctx = self.cargo_ctx.get(cargo_id)
        if ctx:
            return ctx

        missed = self.ctx_miss.get(cargo_id)
        if missed and time.monotonic() - missed < CONTEXT_RETRY_S:
            return {}

        ctx = self.storage.fetch_cargo_context(cargo_id)
        if ctx:
            self.cargo_ctx[cargo_id] = ctx
            self.ctx_miss.pop(cargo_id, None)
            return ctx

        self.ctx_miss[cargo_id] = time.monotonic()
        return {}

    @staticmethod
    def is_high_value(ctx):
        value = num(ctx.get("insurance_value_usd"))
        return value is not None and value >= THRESHOLDS["high_value_usd"]

    def severity(self, base, ctx, escalate=True):
        """High-value or chemical cargo bumps severity one level."""
        if escalate and (self.is_high_value(ctx) or ctx.get("hazmat_class") == "CHEMICAL"):
            return bump(base)
        return base

    def alert(self, alert_type, category, severity, message, topic, *,
              cargo_id=None, flight_number=None, ctx=None,
              metric_name=None, metric_value=None, threshold=None, details=None):
        ctx = ctx or {}
        details = dict(details or {})
        if ctx:
            details.setdefault("insurance_value_usd", num(ctx.get("insurance_value_usd")))
            details.setdefault("high_value", self.is_high_value(ctx))
        return {
            "alert_type": alert_type,
            "category": category,
            "severity": severity,
            "message": message,
            "cargo_id": cargo_id,
            "flight_number": flight_number,
            "awb_number": ctx.get("awb_number"),
            "hazmat_class": ctx.get("hazmat_class"),
            "destination_airport": ctx.get("destination_airport"),
            "metric_name": metric_name,
            "metric_value": metric_value,
            "threshold_value": threshold,
            "details": details,
            "source_topic": topic,
        }

    def emit_event(self, alert, entity, ts):
        """One-shot alert (status EVENT)."""
        alert.update(
            dedup_key=f"{alert['alert_type']}|{entity}",
            status="EVENT",
            triggered_at=ts,
        )
        self.storage.insert_alert(alert)
        logger.warning(f"ALERT EVENT | {alert['severity']} | {alert['message']}")

    def track(self, rule, entity, active, now, build):
        """Threshold rule with a lifecycle: opens once `active` has been true
        for the rule's hold time, resolves once it has been false long enough."""
        hold_s, clear_s = RULE_TIMING[rule]
        key = f"{rule}|{entity}"
        st = self.state.setdefault(
            key, {"breach_since": None, "clear_since": None, "alert_id": None}
        )

        if active:
            st["clear_since"] = None
            if st["breach_since"] is None:
                st["breach_since"] = now

            held = (now - st["breach_since"]).total_seconds()
            if st["alert_id"] is None and held >= hold_s:
                alert = build()
                alert["details"]["breach_started_at"] = st["breach_since"].isoformat()
                alert.update(dedup_key=key, status="OPEN", triggered_at=now)
                st["alert_id"] = self.storage.insert_alert(alert)
                logger.warning(f"ALERT OPEN | {alert['severity']} | {alert['message']}")
        else:
            st["breach_since"] = None
            if st["alert_id"] is not None:
                if st["clear_since"] is None:
                    st["clear_since"] = now
                if (now - st["clear_since"]).total_seconds() >= clear_s:
                    self.storage.resolve_alert(st["alert_id"], now)
                    logger.info(f"ALERT RESOLVED | {key}")
                    st["alert_id"] = None
                    st["clear_since"] = None

    # -- cargo_manifest -----------------------------------------------

    def on_manifest(self, data):
        cargo_id = data.get("cargo_id")
        if not cargo_id:
            logger.warning("Manifest without cargo_id - skipped")
            return

        ctx = {
            "awb_number": data.get("awb_number"),
            "hazmat_class": data.get("hazmat_class"),
            "hazmat_code": data.get("hazmat_code"),
            "insurance_value_usd": num(data.get("insurance_value_usd")),
            "status": (data.get("status") or "").upper(),
            "estimated_delivery": parse_ts(data.get("estimated_delivery")),
            "destination_airport": data.get("destination_airport"),
        }
        self.cargo_ctx[cargo_id] = ctx
        self.ctx_miss.pop(cargo_id, None)

        self.check_stuck_shipment(cargo_id, ctx, utcnow())

    def check_stuck_shipment(self, cargo_id, ctx, now):
        est = ctx.get("estimated_delivery")
        hours = THRESHOLDS["stuck_booked_within_hours"]
        active = (
            ctx.get("status") == "BOOKED"
            and est is not None
            and est <= now + timedelta(hours=hours)
        )

        def build():
            return self.alert(
                "STUCK_SHIPMENT", "CARGO", self.severity("WARNING", ctx, escalate=False),
                f"{cargo_id} (AWB {ctx.get('awb_number')}) is still BOOKED with delivery "
                f"due {est} UTC",
                "cargo_manifest", cargo_id=cargo_id, ctx=ctx,
                metric_name="hours_to_delivery",
                metric_value=round((est - now).total_seconds() / 3600, 1) if est else None,
                threshold=float(hours),
                details={"status": ctx.get("status"), "estimated_delivery": str(est)},
            )

        self.track("STUCK_SHIPMENT", cargo_id, active, now, build)

    # -- sensor_readings ----------------------------------------------

    def on_sensor(self, data):
        cargo_id = data.get("cargo_id")
        if not cargo_id:
            logger.warning("Sensor reading without cargo_id - skipped")
            return

        topic = "sensor_readings"
        flight = data.get("flight_number")
        ts = parse_ts(data.get("timestamp")) or utcnow()
        reading = data.get("sensor_readings") or {}
        location = reading.get("location") or {}
        ctx = self.cargo_context(cargo_id)

        temp = num(reading.get("temperature_celsius"))
        humidity = num(reading.get("humidity_percent"))
        altitude = num(location.get("altitude_meters"))
        quality = num(data.get("data_quality_score"))

        self.last_seen[cargo_id] = time.monotonic()
        if flight:
            self.flight_to_cargo[str(flight)] = cargo_id

        base = {"cargo_id": cargo_id, "flight_number": flight, "ctx": ctx}

        # -- cold chain (only classes listed in TEMP_LIMITS_C) ---------
        limits = TEMP_LIMITS_C.get(ctx.get("hazmat_class"))
        if limits and temp is not None:
            low, high = limits

            def build_cold():
                deviation = max(low - temp, temp - high)
                sev = "CRITICAL" if deviation >= 3 else "WARNING"
                direction = "above" if temp > high else "below"
                return self.alert(
                    "COLD_CHAIN_BREACH", "SENSOR", self.severity(sev, ctx),
                    f"{cargo_id} ({ctx.get('hazmat_class')}) temperature {temp:.1f} C is "
                    f"{direction} the {low:g}-{high:g} C range",
                    topic, metric_name="temperature_celsius", metric_value=temp,
                    threshold=high if temp > high else low,
                    details={"allowed_range_c": [low, high]}, **base)

            self.track("COLD_CHAIN_BREACH", cargo_id, temp < low or temp > high, ts, build_cold)

        # -- humidity --------------------------------------------------
        if humidity is not None:
            limit = THRESHOLDS["humidity_max_pct"]

            def build_humidity():
                return self.alert(
                    "HUMIDITY_HIGH", "SENSOR", self.severity("WARNING", ctx),
                    f"{cargo_id} humidity {humidity:.1f}% is above {limit:g}%",
                    topic, metric_name="humidity_percent", metric_value=humidity,
                    threshold=limit, **base)

            self.track("HUMIDITY_HIGH", cargo_id, humidity > limit, ts, build_humidity)

        # -- data quality ----------------------------------------------
        if quality is not None:
            limit = THRESHOLDS["data_quality_min"]

            def build_quality():
                return self.alert(
                    "DATA_QUALITY_LOW", "SENSOR", "INFO",
                    f"{cargo_id} sensor data quality {quality:.2f} is below {limit:g}",
                    topic, metric_name="data_quality_score", metric_value=quality,
                    threshold=limit, **base)

            self.track("DATA_QUALITY_LOW", cargo_id, quality < limit, ts, build_quality)

        # -- impact (event) --------------------------------------------
        if reading.get("impact_detected"):
            self.emit_event(self.alert(
                "IMPACT_DETECTED", "SENSOR", self.severity("WARNING", ctx),
                f"Impact detected on {cargo_id} ({ctx.get('hazmat_class') or 'unknown class'})",
                topic, metric_name="impact_detected", metric_value=1.0,
                details={"altitude_meters": altitude}, **base), cargo_id, ts)

        # -- door open in flight (event) -------------------------------
        min_alt = THRESHOLDS["door_open_min_altitude_m"]
        if reading.get("door_open") and altitude is not None and altitude > min_alt:
            self.emit_event(self.alert(
                "DOOR_OPEN_IN_FLIGHT", "SENSOR", "CRITICAL",
                f"Door open on {cargo_id} at {altitude:.0f} m altitude",
                topic, metric_name="altitude_meters", metric_value=altitude,
                threshold=min_alt, **base), cargo_id, ts)

        # -- rapid temperature swing (event) ---------------------------
        prev = self.last_temp.get(cargo_id)
        swing = THRESHOLDS["temp_swing_c"]
        if prev is not None and temp is not None and abs(temp - prev) > swing:
            self.emit_event(self.alert(
                "TEMP_SWING", "SENSOR", "WARNING",
                f"{cargo_id} temperature changed {prev:.1f} -> {temp:.1f} C between readings",
                topic, metric_name="temperature_delta_c", metric_value=round(temp - prev, 2),
                threshold=swing, details={"previous_c": prev, "current_c": temp},
                **base), cargo_id, ts)

        # Updated last so a redelivered message is compared against the
        # same previous value
        if temp is not None:
            self.last_temp[cargo_id] = temp

    # -- flight_tracking ----------------------------------------------

    def on_flight(self, data):
        flight = data.get("flight_number")
        if not flight:
            logger.warning("Flight message without flight_number - skipped")
            return
        flight = str(flight)

        topic = "flight_tracking"
        ts = parse_ts(data.get("timestamp")) or utcnow()
        position = data.get("position") or {}
        status = str(data.get("flight_status") or "").strip().lower().replace("_", "-")

        altitude = num(position.get("altitude_ft"))
        speed = num(position.get("speed_knots"))
        scheduled = parse_ts(data.get("scheduled_arrival"))
        estimated = parse_ts(data.get("estimated_arrival"))

        cargo_id = self.flight_to_cargo.get(flight)
        ctx = self.cargo_context(cargo_id)
        base = {"cargo_id": cargo_id, "flight_number": flight, "ctx": ctx}
        route = f"{data.get('origin')}->{data.get('destination')}"

        # -- delay -----------------------------------------------------
        if scheduled and estimated:
            delay = (estimated - scheduled).total_seconds() / 60
            warn = THRESHOLDS["flight_delay_warn_min"]
            crit = THRESHOLDS["flight_delay_critical_min"]

            def build_delay():
                return self.alert(
                    "FLIGHT_DELAY", "FLIGHT", "CRITICAL" if delay >= crit else "WARNING",
                    f"Flight {flight} ({route}) is delayed {delay:.0f} min",
                    topic, metric_name="delay_minutes", metric_value=delay,
                    threshold=float(warn),
                    details={"scheduled_arrival": str(scheduled),
                             "estimated_arrival": str(estimated)}, **base)

            self.track("FLIGHT_DELAY", flight, delay >= warn, ts, build_delay)

        # -- moving while still "scheduled" ----------------------------
        min_speed = THRESHOLDS["flight_scheduled_speed_kt"]
        if speed is not None:
            def build_mismatch():
                return self.alert(
                    "FLIGHT_STATUS_MISMATCH", "FLIGHT", "WARNING",
                    f"Flight {flight} is 'scheduled' but moving at {speed:.0f} kt",
                    topic, metric_name="speed_knots", metric_value=speed,
                    threshold=float(min_speed), **base)

            self.track("FLIGHT_STATUS_MISMATCH", flight,
                       status == "scheduled" and speed > min_speed, ts, build_mismatch)

        # -- low altitude while en-route -------------------------------
        min_alt = THRESHOLDS["flight_low_altitude_ft"]
        if altitude is not None:
            def build_low_alt():
                return self.alert(
                    "FLIGHT_LOW_ALTITUDE", "FLIGHT", "WARNING",
                    f"Flight {flight} is en-route at only {altitude:.0f} ft",
                    topic, metric_name="altitude_ft", metric_value=altitude,
                    threshold=float(min_alt), **base)

            self.track("FLIGHT_LOW_ALTITUDE", flight,
                       status in EN_ROUTE_STATUSES and altitude < min_alt, ts, build_low_alt)

    # -- time-based rules (called every HOUSEKEEPING_INTERVAL_S) --------

    def housekeeping(self):
        now = utcnow()
        limit = THRESHOLDS["sensor_silent_seconds"]

        for cargo_id, seen in list(self.last_seen.items()):
            silent_for = time.monotonic() - seen
            ctx = self.cargo_context(cargo_id)

            def build_silent():
                return self.alert(
                    "SENSOR_SILENT", "SENSOR", self.severity("WARNING", ctx, escalate=False),
                    f"No sensor readings from {cargo_id} for {silent_for / 60:.1f} min",
                    "sensor_readings", cargo_id=cargo_id, ctx=ctx,
                    metric_name="silent_seconds", metric_value=round(silent_for),
                    threshold=float(limit))

            self.track("SENSOR_SILENT", cargo_id, silent_for > limit, now, build_silent)

        for cargo_id, ctx in list(self.cargo_ctx.items()):
            self.check_stuck_shipment(cargo_id, ctx, now)


# ---------------------------------------------------------
# KAFKA CONSUMER
# ---------------------------------------------------------

OK, RETRY, SKIP = "ok", "retry", "skip"


def safe_json(raw):
    """Bad JSON returns None instead of raising inside poll()."""
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception as e:
        logger.error(f"Undecodable message skipped: {e}")
        return None


def create_consumer():
    try:
        consumer = KafkaConsumer(
            *KAFKA_TOPICS,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUP,
            auto_offset_reset=AUTO_OFFSET_RESET,
            enable_auto_commit=False,
            value_deserializer=safe_json,
        )
        logger.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        logger.info(f"Subscribed to topics: {KAFKA_TOPICS} (group {CONSUMER_GROUP})")
        return consumer
    except Exception as e:
        logger.error(f"Failed to create Kafka consumer: {e}")
        return None


def process_message(engine, topic, data):
    if not isinstance(data, dict):
        logger.warning(f"Non-object payload on {topic} - skipped")
        return SKIP

    try:
        if topic == "cargo_manifest":
            engine.on_manifest(data)
        elif topic == "sensor_readings":
            engine.on_sensor(data)
        elif topic == "flight_tracking":
            engine.on_flight(data)
        else:
            logger.warning(f"Unknown Kafka topic: {topic}")
            return SKIP
        return OK

    except MySQLError as e:
        # Database problem: keep the offset so the message is retried
        logger.error(f"MySQL error while processing {topic}: {e}")
        return RETRY

    except Exception:
        # Bad data / bug: log it and move on so one message can't block the topic
        logger.exception(f"Error processing {topic} message - skipped")
        return SKIP


def run_consumer():

    if MYSQL_PASSWORD == "PUT_YOUR_MYSQL_PASSWORD_HERE":
        logger.error("Enter your MySQL password at MYSQL_PASSWORD near the top of this file")
        return

    logger.info("Connecting to MySQL...")
    storage = AlertsStorage(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )
    if not storage.connect():
        logger.error("Could not connect to MySQL")
        return

    engine = AlertEngine(storage)
    try:
        engine.restore_open_alerts()
    except MySQLError as e:
        logger.error(f"Could not restore open alerts: {e}")
        storage.close()
        return

    consumer = create_consumer()
    if consumer is None:
        storage.close()
        return

    logger.info("=" * 70)
    logger.info("AIRLINE CARGO ALERTS CONSUMER STARTED")
    logger.info("=" * 70)

    last_housekeeping = time.monotonic()

    try:
        while True:

            records = consumer.poll(timeout_ms=1000, max_records=200)

            for tp, messages in records.items():
                for message in messages:
                    logger.debug(
                        f"topic={message.topic} partition={message.partition} "
                        f"offset={message.offset}"
                    )

                    outcome = process_message(engine, message.topic, message.value)

                    if outcome == RETRY:
                        # Rewind to this message; the commit below then only
                        # covers what was processed before it
                        consumer.seek(tp, message.offset)
                        time.sleep(RETRY_DELAY_S)
                        break

            if records:
                try:
                    consumer.commit()
                except Exception as e:
                    logger.error(f"Failed to commit offsets: {e}")

            if time.monotonic() - last_housekeeping >= HOUSEKEEPING_INTERVAL_S:
                try:
                    engine.housekeeping()
                except MySQLError as e:
                    logger.error(f"Housekeeping failed: {e}")
                last_housekeeping = time.monotonic()

    except KeyboardInterrupt:
        logger.info("Stopped by user (Ctrl+C)")

    except Exception:
        logger.exception("Consumer error")

    finally:
        logger.info("Shutting down alerts consumer...")
        try:
            consumer.close()
            logger.info("Kafka consumer closed")
        except Exception as e:
            logger.error(f"Error closing Kafka consumer: {e}")
        try:
            storage.close()
        except Exception as e:
            logger.error(f"Error closing MySQL: {e}")
        logger.info("Alerts consumer stopped")


# ---------------------------------------------------------
# ENTRY POINT
# ---------------------------------------------------------

if __name__ == "__main__":
    run_consumer()