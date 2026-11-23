-- Notebook: 06_gold (SQL). Rebuilt every run; small enough that a full rebuild is cheap.

-- Bikes and scooters parked and ready to ride, per H3 cell, per 15 minutes
CREATE OR REPLACE TABLE seattle.gold.vehicle_supply_15min AS
SELECT window(snapshot_ts, '15 minutes').start AS bucket_start,
       h3_r8,
       h3_centeraswkt(h3_r8)                    AS cell_center_wkt,
       COUNT(DISTINCT snapshot_ts)              AS snapshots,
       COUNT(*) / COUNT(DISTINCT snapshot_ts)   AS avg_available
FROM seattle.silver.vehicle_snapshots
WHERE NOT coalesce(is_disabled, false) AND NOT coalesce(is_reserved, false)
GROUP BY ALL;

-- Ship activity per hour
CREATE OR REPLACE TABLE seattle.gold.vessel_activity_hourly AS
SELECT date_trunc('HOUR', event_ts)                     AS hour,
       COUNT(DISTINCT mmsi)                             AS vessels_seen,
       COUNT(DISTINCT CASE WHEN speed_knots > 0.5 THEN mmsi END) AS vessels_moving,
       ROUND(AVG(speed_knots), 2)                       AS avg_speed_knots
FROM seattle.silver.ais_positions
GROUP BY ALL;

-- Air traffic per 15 minutes
CREATE OR REPLACE TABLE seattle.gold.air_traffic_15min AS
SELECT window(snapshot_ts, '15 minutes').start AS bucket_start,
       COUNT(DISTINCT icao24)                                      AS aircraft_seen,
       COUNT(DISTINCT CASE WHEN on_ground THEN icao24 END)         AS on_ground,
       COUNT(DISTINCT CASE WHEN NOT on_ground AND baro_altitude < 3000
                           THEN icao24 END)                        AS low_altitude
FROM seattle.silver.aircraft_states
GROUP BY ALL;

-- One row per hour that ties the whole city together
CREATE OR REPLACE TABLE seattle.gold.city_pulse_hourly AS
WITH bikes AS (
  -- citywide count per snapshot, averaged over the snapshots in each hour
  SELECT date_trunc('HOUR', snapshot_ts) AS hour,
         COUNT(*) / COUNT(DISTINCT snapshot_ts) AS vehicles_available
  FROM seattle.silver.vehicle_snapshots
  WHERE NOT coalesce(is_disabled, false) AND NOT coalesce(is_reserved, false)
  GROUP BY ALL),
air AS (
  SELECT date_trunc('HOUR', bucket_start) AS hour, AVG(aircraft_seen) AS avg_aircraft
  FROM seattle.gold.air_traffic_15min GROUP BY ALL),
wx AS (
  SELECT date_trunc('HOUR', observed_ts) AS hour, AVG(temp_c) AS temp_c,
         MAX(precip_last_hour_mm) AS precip_mm, FIRST(description) AS conditions
  FROM seattle.silver.weather_obs GROUP BY ALL),
wx_ranges AS (
  -- NWS posts about once an hour and runs a little behind, so each hour uses
  -- the most recent observation at or before it ("as-of" join)
  SELECT *, LEAD(hour) OVER (ORDER BY hour) AS next_hour FROM wx),
base AS (
  SELECT coalesce(b.hour, a.hour, v.hour) AS hour,
         ROUND(b.vehicles_available) AS vehicles_available,
         ROUND(a.avg_aircraft, 1) AS avg_aircraft,
         v.vessels_seen, v.vessels_moving
  FROM bikes b
  FULL OUTER JOIN air a ON a.hour = b.hour
  FULL OUTER JOIN seattle.gold.vessel_activity_hourly v ON v.hour = coalesce(b.hour, a.hour))
SELECT base.*, w.temp_c, w.precip_mm, w.conditions
FROM base
LEFT JOIN wx_ranges w
  ON base.hour >= w.hour AND (base.hour < w.next_hour OR w.next_hour IS NULL);

-- Live map for the dashboard: each ship's latest position in the 30 min before the newest
-- data, plus the latest aircraft and bike snapshots. Anchoring on MAX() instead of
-- current_timestamp() keeps the map filled between hourly job runs.
CREATE OR REPLACE VIEW seattle.gold.live_map AS
SELECT 'ship' AS kind, ship_name AS label, lat, lon, event_ts AS ts
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY mmsi ORDER BY event_ts DESC) AS rn
  FROM seattle.silver.ais_positions
  WHERE event_ts > (SELECT MAX(event_ts) FROM seattle.silver.ais_positions) - INTERVAL 30 MINUTES
) latest_ships
WHERE rn = 1
UNION ALL
SELECT 'aircraft', callsign, lat, lon, snapshot_ts
FROM seattle.silver.aircraft_states
WHERE snapshot_ts = (SELECT MAX(snapshot_ts) FROM seattle.silver.aircraft_states)
UNION ALL
SELECT 'vehicle', vehicle_type_id, lat, lon, snapshot_ts
FROM seattle.silver.vehicle_snapshots
WHERE snapshot_ts = (SELECT MAX(snapshot_ts) FROM seattle.silver.vehicle_snapshots);
