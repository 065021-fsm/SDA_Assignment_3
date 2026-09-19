SELECT 
  COUNT(*) as total_shipments,
  SUM(weight_kg) as total_weight_kg,
  COUNT(DISTINCT destination_airport) as destinations,
  COUNT(DISTINCT shipper) as shippers,
  SUM(insurance_value_usd) as total_insured_value_usd
FROM airline_cargo.cargo_manifests;


SELECT 
  status,
  COUNT(*) as count,
  ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM airline_cargo.cargo_manifests), 2) as percentage
FROM airline_cargo.cargo_manifests
GROUP BY status
ORDER BY count DESC;


SELECT 
  ROUND(
    (SUM(CASE WHEN estimated_delivery <= estimated_delivery THEN 1 ELSE 0 END) * 100.0) / 
    COUNT(*), 2
  ) as on_time_percentage,
  ROUND(
    (SUM(CASE WHEN estimated_delivery > estimated_delivery THEN 1 ELSE 0 END) * 100.0) / 
    COUNT(*), 2
  ) as late_percentage
FROM airline_cargo.cargo_manifests
WHERE estimated_delivery IS NOT NULL;


SELECT 
  hazmat_class,
  COUNT(*) as shipment_count,
  SUM(weight_kg) as total_weight_kg,
  ROUND(AVG(insurance_value_usd), 2) as avg_insurance_usd
FROM airline_cargo.cargo_manifests
WHERE hazmat_class IS NOT NULL AND hazmat_class != 'NONE'
GROUP BY hazmat_class
ORDER BY shipment_count DESC;


select count(*) from airline_cargo.alerts where category = "CARGO" or category = "SENSOR";


SELECT 
  DATE_FORMAT(triggered_at, '%Y-%m-%d') as alert_date,
  alert_type,
  severity,
  COUNT(*) as alert_count
FROM airline_cargo.alerts
WHERE triggered_at >= NOW() - INTERVAL 30 DAY
GROUP BY DATE_FORMAT(triggered_at, '%Y-%m-%d'), alert_type, severity
ORDER BY alert_date DESC, alert_count DESC;


SELECT 
  sr.cargo_id,
  sr.flight_number,
  c.destination_airport,
  ROUND(sr.temperature_celsius, 2) as current_temp_c,
  sr.temperature_celsius as temp_raw,
  ROUND(sr.humidity_percent, 2) as current_humidity_pct,
  sr.humidity_percent as humidity_raw,
  ROUND(sr.altitude_meters / 1000, 2) as altitude_km,
  ROUND(sr.pressure_hpa, 2) as pressure_hpa,
  sr.impact_detected,
  sr.door_open,
  ROUND(sr.data_quality_score, 2) as data_quality,
  sr.event_timestamp,
  c.hazmat_class
FROM airline_cargo.sensor_readings sr
LEFT JOIN airline_cargo.cargo_manifests c ON sr.cargo_id = c.cargo_id
WHERE sr.event_timestamp >= NOW() - INTERVAL 6 HOUR
ORDER BY sr.event_timestamp DESC
LIMIT 100;


SELECT 
  alert_type,
  category,
  severity,
  status,
  COUNT(*) as alert_count,
  COUNT(CASE WHEN status = 'RESOLVED' THEN 1 END) as resolved_count,
  COUNT(CASE WHEN status = 'OPEN' THEN 1 END) as open_count,
  COUNT(CASE WHEN status = 'EVENT' THEN 1 END) as event_count
FROM airline_cargo.alerts
WHERE triggered_at >= NOW() - INTERVAL 7 DAY
GROUP BY alert_type, category, severity, status
ORDER BY alert_count DESC;


SELECT 
  alert_type,
  COUNT(*) as total_alerts,
  COUNT(CASE WHEN status = 'RESOLVED' THEN 1 END) as resolved,
  ROUND(
    (COUNT(CASE WHEN status = 'RESOLVED' THEN 1 END) * 100.0) / COUNT(*), 
    2
  ) as resolution_rate_pct,
  ROUND(
    AVG(TIMESTAMPDIFF(MINUTE, triggered_at, resolved_at)), 
    2
  ) as avg_resolution_minutes
FROM airline_cargo.alerts
WHERE triggered_at >= NOW() - INTERVAL 30 DAY AND status = 'RESOLVED'
GROUP BY alert_type
ORDER BY resolution_rate_pct DESC;