-- sql/models.sql
-- Primary Analytical Models for HVFHV Data Platform (Milestone M2)
-- Enforces data contracts from docs/reference/data-contracts.md §2

-- 1. Release Metadata (Singleton: exactly one row per release)
CREATE TABLE IF NOT EXISTS release_metadata (
    release_id VARCHAR PRIMARY KEY,
    derivation_id VARCHAR NOT NULL,
    model_schema_version VARCHAR NOT NULL,
    rules_version VARCHAR NOT NULL,
    metric_definition_version VARCHAR NOT NULL,
    code_bundle_manifest_hash VARCHAR NOT NULL,
    lockfile_hash VARCHAR NOT NULL,
    zone_revision_id VARCHAR NOT NULL,
    first_source_month VARCHAR NOT NULL,
    last_source_month VARCHAR NOT NULL,
    built_at_utc VARCHAR NOT NULL
);

-- 2. Source Contribution (One per contributed monthly object)
CREATE TABLE IF NOT EXISTS source_contribution (
    logical_source_id VARCHAR NOT NULL,
    revision_id VARCHAR NOT NULL,
    content_sha256 VARCHAR NOT NULL,
    source_month VARCHAR NOT NULL,
    source_schema_version VARCHAR NOT NULL,
    cbd_fee_field_present BOOLEAN NOT NULL,
    decoded_row_count BIGINT NOT NULL,
    inserted_row_count BIGINT NOT NULL,
    PRIMARY KEY (logical_source_id, revision_id)
);

-- 3. Zone Reference (TLC Taxi Zone Reference dimension)
CREATE TABLE IF NOT EXISTS zone_reference (
    location_id INTEGER PRIMARY KEY,
    borough VARCHAR,
    zone VARCHAR,
    service_zone VARCHAR,
    reference_revision_id VARCHAR NOT NULL
);

-- 4. Trip Observation (Normalized original physical records with physical locators)
CREATE TABLE IF NOT EXISTS trip_observation (
    revision_id VARCHAR NOT NULL,
    source_row_number BIGINT NOT NULL,
    source_month VARCHAR NOT NULL,
    hvfhs_license_num VARCHAR,
    dispatching_base_num VARCHAR,
    originating_base_num VARCHAR,
    request_datetime TIMESTAMP,
    on_scene_datetime TIMESTAMP,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    pu_location_id INTEGER,
    do_location_id INTEGER,
    trip_miles DOUBLE,
    trip_time BIGINT,
    base_passenger_fare DOUBLE,
    tolls DOUBLE,
    bcf DOUBLE,
    sales_tax DOUBLE,
    congestion_surcharge DOUBLE,
    airport_fee DOUBLE,
    tips DOUBLE,
    driver_pay DOUBLE,
    shared_request_flag VARCHAR,
    shared_match_flag VARCHAR,
    access_a_ride_flag VARCHAR,
    wav_request_flag VARCHAR,
    wav_match_flag VARCHAR,
    cbd_congestion_fee DOUBLE,
    PRIMARY KEY (revision_id, source_row_number)
);

-- 5. Coverage (Release-level coverage across configured window months)
CREATE TABLE IF NOT EXISTS coverage (
    source_month VARCHAR PRIMARY KEY,
    is_expected BOOLEAN NOT NULL,
    is_available BOOLEAN NOT NULL,
    is_included BOOLEAN NOT NULL,
    revision_id VARCHAR,
    row_count BIGINT
);

-- 6. Interface View for Downstream Quality and Quantiles
CREATE OR REPLACE VIEW trip_qualified AS
SELECT
    t.*,
    CASE 
        WHEN t.request_datetime IS NOT NULL AND t.pickup_datetime IS NOT NULL
        THEN (epoch_us(t.pickup_datetime)::BIGINT - epoch_us(t.request_datetime)::BIGINT) / 1000000.0
        ELSE NULL
    END AS recorded_request_to_pickup_seconds,
    CASE
        WHEN t.request_datetime IS NULL OR t.pickup_datetime IS NULL THEN 'missing_timestamp'
        WHEN (epoch_us(t.pickup_datetime)::BIGINT - epoch_us(t.request_datetime)::BIGINT) < 0 THEN 'negative_recorded_interval'
        ELSE 'eligible_nonnegative_recorded_interval'
    END AS interval_class,
    CASE
        WHEN t.request_datetime IS NOT NULL 
         AND t.pickup_datetime IS NOT NULL 
         AND (epoch_us(t.pickup_datetime)::BIGINT - epoch_us(t.request_datetime)::BIGINT) >= 0
        THEN TRUE
        ELSE FALSE
    END AS is_interval_eligible,
    CAST(t.pickup_datetime AS DATE) AS pickup_date,
    STRFTIME(t.pickup_datetime, '%Y-%m') AS pickup_month,
    EXTRACT(HOUR FROM t.pickup_datetime)::INTEGER AS pickup_hour,
    CASE
        WHEN t.pickup_datetime IS NOT NULL AND STRFTIME(t.pickup_datetime, '%Y-%m') != t.source_month THEN true
        ELSE false
    END AS is_off_source_month,
    CASE
        WHEN t.pu_location_id IS NULL THEN 'null_location'
        WHEN t.pu_location_id IN (1, 264, 265) THEN 'matched_sentinel'
        WHEN pz.location_id IS NOT NULL AND t.pu_location_id BETWEEN 2 AND 263 THEN 'matched_ordinary'
        ELSE 'unmatched'
    END AS pu_zone_status,
    CASE
        WHEN t.do_location_id IS NULL THEN 'null_location'
        WHEN t.do_location_id IN (1, 264, 265) THEN 'matched_sentinel'
        WHEN dz.location_id IS NOT NULL AND t.do_location_id BETWEEN 2 AND 263 THEN 'matched_ordinary'
        ELSE 'unmatched'
    END AS do_zone_status,
    CASE
        WHEN t.dropoff_datetime IS NOT NULL AND t.pickup_datetime IS NOT NULL AND t.dropoff_datetime < t.pickup_datetime
        THEN TRUE
        ELSE FALSE
    END AS temporal_dropoff_before_pickup,
    CASE
        WHEN t.on_scene_datetime IS NOT NULL AND t.request_datetime IS NOT NULL AND t.on_scene_datetime < t.request_datetime
        THEN TRUE
        ELSE FALSE
    END AS temporal_on_scene_before_request,
    CASE
        WHEN t.on_scene_datetime IS NOT NULL AND t.pickup_datetime IS NOT NULL AND t.on_scene_datetime > t.pickup_datetime
        THEN TRUE
        ELSE FALSE
    END AS temporal_on_scene_after_pickup,
    CASE
        WHEN t.dropoff_datetime IS NOT NULL AND t.pickup_datetime IS NOT NULL AND t.trip_time IS NOT NULL
        THEN ((epoch_us(t.dropoff_datetime)::BIGINT - epoch_us(t.pickup_datetime)::BIGINT) / 1000000) - t.trip_time
        ELSE NULL
    END AS trip_time_discrepancy_seconds
FROM trip_observation t
LEFT JOIN zone_reference pz ON t.pu_location_id = pz.location_id
LEFT JOIN zone_reference dz ON t.do_location_id = dz.location_id;

-- 7. Materialized Daily Continuous Quantiles Table
CREATE TABLE IF NOT EXISTS interval_daily (
    source_month VARCHAR NOT NULL,
    pickup_date DATE,
    pu_location_id INTEGER,
    hvfhs_license_num VARCHAR,
    total_trips BIGINT NOT NULL,
    eligible_trips BIGINT NOT NULL,
    missing_intervals BIGINT NOT NULL,
    negative_intervals BIGINT NOT NULL,
    median_seconds DOUBLE,
    p95_seconds DOUBLE
);

-- 8. Materialized Monthly Continuous Quantiles Table (Independently Computed)
CREATE TABLE IF NOT EXISTS interval_monthly (
    source_month VARCHAR NOT NULL,
    pickup_month VARCHAR,
    pu_location_id INTEGER,
    hvfhs_license_num VARCHAR,
    total_trips BIGINT NOT NULL,
    eligible_trips BIGINT NOT NULL,
    missing_intervals BIGINT NOT NULL,
    negative_intervals BIGINT NOT NULL,
    median_seconds DOUBLE,
    p95_seconds DOUBLE
);
