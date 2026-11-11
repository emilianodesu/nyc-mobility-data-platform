-- sql/quality_and_profiles.sql
-- Quality Engine and Column Profiles Schema (Milestone M2)
-- Enforces data contracts from docs/reference/data-contracts.md §2 and contracts/rules.json

-- 1. Pre-Seal Quality Results
CREATE TABLE IF NOT EXISTS quality_result (
    rule_id VARCHAR NOT NULL,
    rule_version VARCHAR NOT NULL,
    cohort_key VARCHAR NOT NULL,
    severity VARCHAR NOT NULL CHECK (severity IN ('BLOCK', 'WARN')),
    status VARCHAR NOT NULL CHECK (status IN ('PASS', 'FAIL', 'ERROR', 'NOT_APPLICABLE')),
    method VARCHAR NOT NULL,
    evaluated_count BIGINT NOT NULL,
    exception_count BIGINT NOT NULL,
    denominator BIGINT,
    exception_rate DOUBLE,
    details_json VARCHAR,
    source_provenance VARCHAR,
    evaluated_at_utc VARCHAR NOT NULL,
    PRIMARY KEY (rule_id, cohort_key)
);

-- 2. Full-Scan Column Populations and Profiles
CREATE TABLE IF NOT EXISTS column_profile (
    source_month VARCHAR NOT NULL,
    hvfhs_license_num VARCHAR,
    column_name VARCHAR NOT NULL,
    statistic_name VARCHAR NOT NULL,
    statistic_value_double DOUBLE,
    statistic_value_text VARCHAR,
    method VARCHAR NOT NULL CHECK (method IN ('full_scan', 'footer', 'sample', 'approximate')),
    population_count BIGINT NOT NULL,
    null_count BIGINT NOT NULL,
    nonfinite_count BIGINT NOT NULL DEFAULT 0,
    evaluated_at_utc VARCHAR NOT NULL,
    PRIMARY KEY (source_month, hvfhs_license_num, column_name, statistic_name)
);
