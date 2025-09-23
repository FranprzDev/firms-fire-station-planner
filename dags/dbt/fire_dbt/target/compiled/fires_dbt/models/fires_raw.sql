



-- Modelo Bronze: datos MODIS validados y con metadata
SELECT
    CAST(latitude as FLOAT) as latitude,
    CAST(longitude as FLOAT) as longitude,
    CAST(brightness as FLOAT) as brightness,
    CAST(scan as FLOAT) as scan,
    CAST(track as FLOAT) as track,
    CAST(acq_date as DATE) as acq_date,
    acq_time,
    CAST(satellite as VARCHAR(10)) as satellite,
    CAST(instrument as VARCHAR(10)) as instrument,
    CAST(confidence as INTEGER) as confidence,
    CAST(version as VARCHAR(10)) as version,
    CAST(bright_t31 as FLOAT) as bright_t31,
    CAST(frp as FLOAT) as frp,
    CAST(daynight as VARCHAR(5)) as daynight,

    -- Metadata Bronze
    CURRENT_TIMESTAMP as ingested_at,
    'valid' as validation_status,
    'modis-fire-two-months.csv' as source_file,
    CURRENT_DATE as processing_date,

    -- Data Quality Score basado en confidence
    CASE
        WHEN CAST(confidence as INTEGER) >= 80 THEN 100
        WHEN CAST(confidence as INTEGER) >= 50 THEN 75
        ELSE 50
    END as data_quality_score

FROM "postgres"."public"."fires_raw"

-- Validaciones Bronze aplicadas en WHERE
WHERE CAST(frp as FLOAT) > 0
    AND CAST(confidence as INTEGER) > 30
    AND CAST(latitude as FLOAT) IS NOT NULL
    AND CAST(longitude as FLOAT) IS NOT NULL
    AND CAST(latitude as FLOAT) BETWEEN -55 AND -21
    AND CAST(longitude as FLOAT) BETWEEN -73 AND -53
    AND CAST(acq_date as DATE) IS NOT NULL