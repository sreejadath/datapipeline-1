-- =============================================================================
-- BRONZE TO SILVER TRANSFORMATION: INSTALLED POWER CAPACITY DATA
-- =============================================================================
-- Purpose: Transform bronze_installed_power to silver_installed_power with 
--          data quality validation, anomaly detection, and enrichment
-- Author: Data Pipeline
-- Created: 2025-06-29
-- =============================================================================

-- Generate unique silver batch ID for this transformation run
SET spark.sql.variable.silver_batch_id = uuid();
SET spark.sql.variable.transformation_timestamp = current_timestamp();
SET spark.sql.variable.created_by = 'bronze_to_silver_installed_power_pipeline';

-- =============================================================================
-- STEP 1: DATA QUALITY AND VALIDATION CHECKS
-- =============================================================================

-- Create temporary view with basic data quality checks
CREATE OR REPLACE TEMPORARY VIEW bronze_installed_power_with_dq AS
SELECT 
    record_id,
    timestamp,
    time_period,
    production_type,
    installed_capacity,
    country,
    data_source,
    source_system,
    ingestion_batch_id,
    ingestion_date,
    
    -- Data Quality Validation Status
    CASE 
        WHEN record_id IS NULL OR record_id = '' THEN 'ERROR'
        WHEN time_period IS NULL OR time_period = '' THEN 'ERROR'
        WHEN production_type IS NULL OR production_type = '' THEN 'ERROR'
        WHEN installed_capacity IS NULL THEN 'ERROR'
        WHEN installed_capacity < 0 THEN 'ERROR'
        WHEN country IS NULL OR country = '' THEN 'ERROR'
        WHEN NOT regexp_like(time_period, '^[0-9]{2}\\.[0-9]{4}$') THEN 'WARNING'
        WHEN installed_capacity = 0 THEN 'WARNING'
        WHEN length(production_type) > 100 THEN 'WARNING'
        ELSE 'VALID'
    END AS dq_check_status,
    
    -- Data Quality Score (1-5 scale)
    CASE 
        WHEN record_id IS NULL OR time_period IS NULL OR production_type IS NULL 
             OR installed_capacity IS NULL OR country IS NULL THEN 1
        WHEN installed_capacity < 0 OR NOT regexp_like(time_period, '^[0-9]{2}\\.[0-9]{4}$') THEN 2
        WHEN installed_capacity = 0 OR length(production_type) > 100 THEN 3
        WHEN record_id != '' AND time_period != '' AND production_type != '' 
             AND installed_capacity > 0 AND country != '' THEN 5
        ELSE 4
    END AS dq_score

FROM bronze_installed_power
WHERE ingestion_date >= current_date() - INTERVAL 30 DAY  -- Process last 30 days
;

-- =============================================================================
-- STEP 2: ANOMALY DETECTION
-- =============================================================================

-- Create temporary view with anomaly detection using statistical methods
CREATE OR REPLACE TEMPORARY VIEW bronze_with_anomaly_detection AS
SELECT 
    *,
    
    -- Calculate percentiles and statistical measures by production_type
    percentile_cont(0.25) OVER (PARTITION BY production_type) AS q1_capacity,
    percentile_cont(0.75) OVER (PARTITION BY production_type) AS q3_capacity,
    avg(installed_capacity) OVER (PARTITION BY production_type) AS avg_capacity,
    stddev(installed_capacity) OVER (PARTITION BY production_type) AS stddev_capacity,
    
    -- Anomaly detection using IQR method and Z-score
    CASE 
        WHEN installed_capacity IS NULL THEN false
        WHEN abs(installed_capacity - avg(installed_capacity) OVER (PARTITION BY production_type)) 
             > 3 * stddev(installed_capacity) OVER (PARTITION BY production_type) THEN true
        WHEN installed_capacity < (percentile_cont(0.25) OVER (PARTITION BY production_type) 
             - 1.5 * (percentile_cont(0.75) OVER (PARTITION BY production_type) 
             - percentile_cont(0.25) OVER (PARTITION BY production_type))) THEN true
        WHEN installed_capacity > (percentile_cont(0.75) OVER (PARTITION BY production_type) 
             + 1.5 * (percentile_cont(0.75) OVER (PARTITION BY production_type) 
             - percentile_cont(0.25) OVER (PARTITION BY production_type))) THEN true
        ELSE false
    END AS is_anomaly

FROM bronze_installed_power_with_dq
;

-- =============================================================================
-- STEP 3: NORMALIZATION AND STANDARDIZATION
-- =============================================================================

-- Create temporary view with normalized and standardized data
CREATE OR REPLACE TEMPORARY VIEW normalized_installed_power AS
SELECT 
    record_id AS bronze_record_id,
    timestamp,
    time_period,
    country,
    data_source,
    source_system,
    ingestion_batch_id,
    ingestion_date,
    dq_check_status,
    dq_score,
    is_anomaly,
    
    -- Normalize and standardize production_type
    CASE 
        WHEN lower(trim(production_type)) LIKE '%solar%' AND lower(trim(production_type)) LIKE '%gross%' THEN 'Solar gross'
        WHEN lower(trim(production_type)) LIKE '%solar%' THEN 'Solar'
        WHEN lower(trim(production_type)) LIKE '%wind%' AND lower(trim(production_type)) LIKE '%onshore%' THEN 'Wind onshore'
        WHEN lower(trim(production_type)) LIKE '%wind%' AND lower(trim(production_type)) LIKE '%offshore%' THEN 'Wind offshore'
        WHEN lower(trim(production_type)) LIKE '%wind%' THEN 'Wind'
        WHEN lower(trim(production_type)) LIKE '%nuclear%' THEN 'Nuclear'
        WHEN lower(trim(production_type)) LIKE '%coal%' AND lower(trim(production_type)) LIKE '%brown%' THEN 'Brown coal'
        WHEN lower(trim(production_type)) LIKE '%coal%' AND lower(trim(production_type)) LIKE '%hard%' THEN 'Hard coal'
        WHEN lower(trim(production_type)) LIKE '%coal%' THEN 'Coal'
        WHEN lower(trim(production_type)) LIKE '%gas%' AND lower(trim(production_type)) LIKE '%fossil%' THEN 'Fossil gas'
        WHEN lower(trim(production_type)) LIKE '%gas%' THEN 'Gas'
        WHEN lower(trim(production_type)) LIKE '%hydro%' AND lower(trim(production_type)) LIKE '%run%' THEN 'Hydro Run-of-River'
        WHEN lower(trim(production_type)) LIKE '%hydro%' AND lower(trim(production_type)) LIKE '%reservoir%' THEN 'Hydro Reservoir'
        WHEN lower(trim(production_type)) LIKE '%hydro%' THEN 'Hydro'
        WHEN lower(trim(production_type)) LIKE '%battery%' THEN 'Battery Storage (Capacity)'
        WHEN lower(trim(production_type)) LIKE '%biomass%' THEN 'Biomass'
        WHEN lower(trim(production_type)) LIKE '%waste%' THEN 'Waste'
        WHEN lower(trim(production_type)) LIKE '%oil%' THEN 'Oil'
        WHEN lower(trim(production_type)) LIKE '%other%' THEN 'Other'
        ELSE trim(production_type)  -- Keep original if no match
    END AS production_type_normalized,
    
    -- Validate and cleanse installed_capacity
    CASE 
        WHEN installed_capacity IS NULL THEN 0.0
        WHEN installed_capacity < 0 THEN 0.0
        ELSE round(installed_capacity, 2)
    END AS installed_capacity_cleansed,
    
    -- Normalize country code to lowercase
    lower(trim(country)) AS country_normalized

FROM bronze_with_anomaly_detection
;

-- =============================================================================
-- STEP 4: BUSINESS KEY GENERATION AND DEDUPLICATION
-- =============================================================================

-- Create temporary view with business keys and deduplication logic
CREATE OR REPLACE TEMPORARY VIEW deduplicated_installed_power AS
SELECT 
    *,
    -- Generate composite business key
    concat(time_period, '_', production_type_normalized, '_', country_normalized) AS business_key,
    
    -- Row number for deduplication (keep most recent record per business key)
    row_number() OVER (
        PARTITION BY time_period, production_type_normalized, country_normalized 
        ORDER BY ingestion_date DESC, bronze_record_id DESC
    ) AS row_num

FROM normalized_installed_power
WHERE dq_check_status IN ('VALID', 'WARNING')  -- Exclude ERROR records
;

-- =============================================================================
-- STEP 5: FINAL SILVER TABLE TRANSFORMATION
-- =============================================================================

-- Main transformation: Create silver_installed_power data
CREATE OR REPLACE TEMPORARY VIEW transformed_silver_installed_power AS
SELECT 
    -- Generate new UUID for silver record
    concat('silver_', row_number() OVER (ORDER BY time_period, production_type_normalized, country_normalized)) AS record_id,
    
    -- Business key for deduplication
    business_key,
    
    -- Timestamp fields
    timestamp,
    time_period,
    
    -- Core business fields
    production_type_normalized AS production_type,
    installed_capacity_cleansed AS installed_capacity,
    country_normalized AS country,
    
    -- Source system fields
    data_source,
    source_system,
    
    -- Data quality fields
    dq_check_status,
    current_timestamp() AS dq_check_date,
    dq_score,
    is_anomaly,
    
    -- Lineage tracking
    bronze_record_id,
    concat('batch_', date_format(current_timestamp(), 'yyyyMMdd_HHmmss')) AS silver_batch_id,
    current_timestamp() AS silver_ingestion_date,
    'bronze_to_silver_installed_power_pipeline' AS created_by,
    current_timestamp() AS last_updated

FROM deduplicated_installed_power
WHERE row_num = 1  -- Keep only the latest record per business key
AND dq_check_status IN ('VALID', 'WARNING')  -- Include valid and warning records
ORDER BY time_period DESC, production_type_normalized, country_normalized
;

-- =============================================================================
-- STEP 6: DATA QUALITY SUMMARY REPORT
-- =============================================================================

-- Generate summary statistics for monitoring
SELECT 
    'BRONZE_TO_SILVER_INSTALLED_POWER_SUMMARY' AS report_type,
    ${spark.sql.variable.silver_batch_id} AS batch_id,
    ${spark.sql.variable.transformation_timestamp} AS run_timestamp,
    
    -- Record counts
    count(*) AS total_bronze_records,
    sum(CASE WHEN dq_check_status = 'VALID' THEN 1 ELSE 0 END) AS valid_records,
    sum(CASE WHEN dq_check_status = 'WARNING' THEN 1 ELSE 0 END) AS warning_records,
    sum(CASE WHEN dq_check_status = 'ERROR' THEN 1 ELSE 0 END) AS error_records,
    sum(CASE WHEN is_anomaly = true THEN 1 ELSE 0 END) AS anomaly_records,
    
    -- Data quality metrics
    round(avg(dq_score), 2) AS avg_dq_score,
    round(avg(installed_capacity), 2) AS avg_installed_capacity,
    round(stddev(installed_capacity), 2) AS stddev_installed_capacity,
    
    -- Coverage metrics
    count(DISTINCT production_type_normalized) AS unique_production_types,
    count(DISTINCT time_period) AS unique_time_periods,
    count(DISTINCT country_normalized) AS unique_countries,
    
    -- Processing details
    min(ingestion_date) AS earliest_bronze_record,
    max(ingestion_date) AS latest_bronze_record

FROM normalized_installed_power
;

-- =============================================================================
-- STEP 7: CLEANUP TEMPORARY VIEWS
-- =============================================================================

DROP VIEW IF EXISTS bronze_installed_power_with_dq;
DROP VIEW IF EXISTS bronze_with_anomaly_detection;
DROP VIEW IF EXISTS normalized_installed_power;
DROP VIEW IF EXISTS deduplicated_installed_power;

-- =============================================================================
-- END OF TRANSFORMATION
-- =============================================================================

-- Optional: Display final silver table statistics
SELECT 
    'SILVER_INSTALLED_POWER_FINAL_STATS' AS summary_type,
    count(*) AS total_silver_records,
    count(DISTINCT business_key) AS unique_business_keys,
    count(DISTINCT production_type) AS unique_production_types,
    min(timestamp) AS earliest_timestamp,
    max(timestamp) AS latest_timestamp,
    round(avg(installed_capacity), 2) AS avg_capacity_mw,
    round(sum(installed_capacity), 2) AS total_capacity_mw
FROM transformed_silver_installed_power
;
    SELECT 
        normalized_production_type,
        country,
        AVG(installed_capacity) as avg_capacity,
        STDDEV(installed_capacity) as stddev_capacity,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY installed_capacity) as q1_capacity,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY installed_capacity) as q3_capacity
    FROM production_type_normalized
    GROUP BY normalized_production_type, country
),

-- Apply data quality checks and anomaly detection
data_quality_enriched AS (
    SELECT 
        b.*,
        s.avg_capacity,
        s.stddev_capacity,
        s.q1_capacity,
        s.q3_capacity,
        
        -- Anomaly detection using IQR method and Z-score
        CASE 
            WHEN b.installed_capacity < (s.q1_capacity - 1.5 * (s.q3_capacity - s.q1_capacity))
                OR b.installed_capacity > (s.q3_capacity + 1.5 * (s.q3_capacity - s.q1_capacity))
                OR (s.stddev_capacity > 0 AND ABS(b.installed_capacity - s.avg_capacity) / s.stddev_capacity > 3)
            THEN TRUE
            ELSE FALSE
        END as is_anomaly,
        
        -- Data quality status determination
        CASE 
            WHEN b.installed_capacity IS NULL OR b.installed_capacity < 0 THEN 'ERROR'
            WHEN b.time_period IS NULL OR b.normalized_production_type IS NULL OR b.country IS NULL THEN 'ERROR'
            WHEN LENGTH(TRIM(b.time_period)) = 0 OR LENGTH(TRIM(b.normalized_production_type)) = 0 THEN 'ERROR'
            WHEN b.installed_capacity = 0 AND b.normalized_production_type NOT LIKE '%Storage%' THEN 'WARNING'
            WHEN b.installed_capacity > 100000 THEN 'WARNING'  -- Extremely high capacity (>100GW)
            WHEN (b.installed_capacity < (s.q1_capacity - 1.5 * (s.q3_capacity - s.q1_capacity))
                OR b.installed_capacity > (s.q3_capacity + 1.5 * (s.q3_capacity - s.q1_capacity))) THEN 'WARNING'
            ELSE 'VALID'
        END as dq_check_status,
        
        -- Data quality score calculation (1-5 scale)
        CASE 
            WHEN b.installed_capacity IS NULL OR b.installed_capacity < 0 THEN 1
            WHEN b.time_period IS NULL OR b.normalized_production_type IS NULL THEN 1
            WHEN b.installed_capacity = 0 AND b.normalized_production_type NOT LIKE '%Storage%' THEN 2
            WHEN b.installed_capacity > 100000 THEN 2
            WHEN (b.installed_capacity < (s.q1_capacity - 1.5 * (s.q3_capacity - s.q1_capacity))
                OR b.installed_capacity > (s.q3_capacity + 1.5 * (s.q3_capacity - s.q1_capacity))) THEN 3
            WHEN s.stddev_capacity > 0 AND ABS(b.installed_capacity - s.avg_capacity) / s.stddev_capacity < 1 THEN 5
            WHEN s.stddev_capacity > 0 AND ABS(b.installed_capacity - s.avg_capacity) / s.stddev_capacity < 2 THEN 4
            ELSE 4
        END as dq_score
        
    FROM production_type_normalized b
    LEFT JOIN capacity_stats s 
        ON b.normalized_production_type = s.normalized_production_type 
        AND b.country = s.country
),

-- Final transformation with all silver columns
silver_transformation AS (
    SELECT 
        -- Generate new UUID for silver record
        CONCAT(
            SUBSTR(MD5(CONCAT(record_id, CURRENT_TIMESTAMP())), 1, 8), '-',
            SUBSTR(MD5(CONCAT(record_id, CURRENT_TIMESTAMP())), 9, 4), '-',
            '4', SUBSTR(MD5(CONCAT(record_id, CURRENT_TIMESTAMP())), 13, 3), '-',
            'a', SUBSTR(MD5(CONCAT(record_id, CURRENT_TIMESTAMP())), 17, 3), '-',
            SUBSTR(MD5(CONCAT(record_id, CURRENT_TIMESTAMP())), 21, 12)
        ) as record_id,
        
        -- Business key: MM.YYYY_production_type_country
        CONCAT(time_period, '_', REPLACE(normalized_production_type, ' ', '_'), '_', country) as business_key,
        
        -- Convert time_period to proper timestamp (first day of the month)
        CAST(CONCAT(
            SUBSTR(time_period, 4, 4), '-',  -- Year
            SUBSTR(time_period, 1, 2), '-',  -- Month
            '01 00:00:00'                    -- First day of month
        ) AS TIMESTAMP) as timestamp,
        
        time_period,
        normalized_production_type as production_type,
        installed_capacity,
        country,
        data_source,
        source_system,
        dq_check_status,
        CURRENT_TIMESTAMP() as dq_check_date,
        dq_score,
        is_anomaly,
        record_id as bronze_record_id,  -- Reference to bronze record
        
        -- Generate silver batch ID
        CONCAT('silver_batch_', DATE_FORMAT(CURRENT_TIMESTAMP(), '%Y%m%d_%H%i%s')) as silver_batch_id,
        
        CURRENT_TIMESTAMP() as silver_ingestion_date,
        'bronze_to_silver_installed_power_pipeline' as created_by,
        NULL as last_updated  -- Will be set on updates
        
    FROM data_quality_enriched
    WHERE dq_check_status != 'ERROR'  -- Filter out error records
)

-- Insert into silver table with deduplication
INSERT INTO silver_installed_power (
    record_id,
    business_key,
    timestamp,
    time_period,
    production_type,
    installed_capacity,
    country,
    data_source,
    source_system,
    dq_check_status,
    dq_check_date,
    dq_score,
    is_anomaly,
    bronze_record_id,
    silver_batch_id,
    silver_ingestion_date,
    created_by,
    last_updated
)
SELECT DISTINCT
    record_id,
    business_key,
    timestamp,
    time_period,
    production_type,
    installed_capacity,
    country,
    data_source,
    source_system,
    dq_check_status,
    dq_check_date,
    dq_score,
    is_anomaly,
    bronze_record_id,
    silver_batch_id,
    silver_ingestion_date,
    created_by,
    last_updated
FROM silver_transformation st
WHERE NOT EXISTS (
    -- Avoid duplicates based on business key
    SELECT 1 
    FROM silver_installed_power sip 
    WHERE sip.business_key = st.business_key
)
ORDER BY timestamp, production_type, country;

-- Performance and validation summary
SELECT 
    COUNT(*) as total_records_processed,
    COUNT(CASE WHEN dq_check_status = 'VALID' THEN 1 END) as valid_records,
    COUNT(CASE WHEN dq_check_status = 'WARNING' THEN 1 END) as warning_records,
    COUNT(CASE WHEN dq_check_status = 'ERROR' THEN 1 END) as error_records,
    COUNT(CASE WHEN is_anomaly = TRUE THEN 1 END) as anomaly_records,
    AVG(dq_score) as average_dq_score,
    MIN(timestamp) as earliest_date,
    MAX(timestamp) as latest_date,
    COUNT(DISTINCT production_type) as distinct_production_types,
    COUNT(DISTINCT country) as distinct_countries
FROM silver_transformation;
