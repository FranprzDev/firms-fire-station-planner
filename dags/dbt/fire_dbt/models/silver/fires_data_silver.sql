{{ config(materialized='table', schema='public') }}

-- Transición de Bronze a Silver: agregar provincia
SELECT
    CAST(latitude AS FLOAT) AS latitude,
    CAST(longitude AS FLOAT) AS longitude,
    CAST(brightness AS FLOAT) AS brightness,
    CAST(scan AS FLOAT) AS scan,
    CAST(track AS FLOAT) AS track,
    CAST(acq_date AS DATE) AS acq_date,
    CAST(acq_time AS VARCHAR(10)) AS acq_time,
    CAST(satellite AS VARCHAR(10)) AS satellite,
    CAST(instrument AS VARCHAR(10)) AS instrument,
    CAST(confidence AS INTEGER) AS confidence,
    CAST(version AS VARCHAR(10)) AS version,
    CAST(bright_t31 AS FLOAT) AS bright_t31,
    CAST(frp AS FLOAT) AS frp,
    CAST(daynight AS VARCHAR(5)) AS daynight,
    ingested_at,
    validation_status,
    source_file,
    processing_date,
    CAST(data_quality_score AS INTEGER) AS data_quality_score,
    CASE
        WHEN CAST(latitude AS FLOAT) BETWEEN -41.0 AND -33.5 AND CAST(longitude AS FLOAT) BETWEEN -63.5 AND -56.5 THEN 'Buenos Aires'
        WHEN CAST(latitude AS FLOAT) BETWEEN -35.0 AND -29.5 AND CAST(longitude AS FLOAT) BETWEEN -65.5 AND -61.5 THEN 'Córdoba'
        WHEN CAST(latitude AS FLOAT) BETWEEN -34.5 AND -28.0 AND CAST(longitude AS FLOAT) BETWEEN -62.5 AND -58.5 THEN 'Santa Fe'
        WHEN CAST(latitude AS FLOAT) BETWEEN -34.0 AND -30.0 AND CAST(longitude AS FLOAT) BETWEEN -60.0 AND -57.5 THEN 'Entre Ríos'
        WHEN CAST(latitude AS FLOAT) BETWEEN -30.5 AND -27.0 AND CAST(longitude AS FLOAT) BETWEEN -59.0 AND -56.0 THEN 'Corrientes'
        WHEN CAST(latitude AS FLOAT) BETWEEN -28.0 AND -25.5 AND CAST(longitude AS FLOAT) BETWEEN -56.0 AND -53.5 THEN 'Misiones'
        WHEN CAST(latitude AS FLOAT) BETWEEN -28.0 AND -24.0 AND CAST(longitude AS FLOAT) BETWEEN -63.0 AND -58.5 THEN 'Chaco'
        WHEN CAST(latitude AS FLOAT) BETWEEN -26.5 AND -24.0 AND CAST(longitude AS FLOAT) BETWEEN -60.0 AND -57.5 THEN 'Formosa'
        WHEN CAST(latitude AS FLOAT) BETWEEN -26.0 AND -22.0 AND CAST(longitude AS FLOAT) BETWEEN -66.0 AND -63.5 THEN 'Salta'
        WHEN CAST(latitude AS FLOAT) BETWEEN -24.5 AND -21.5 AND CAST(longitude AS FLOAT) BETWEEN -66.0 AND -64.0 THEN 'Jujuy'
        WHEN CAST(latitude AS FLOAT) BETWEEN -28.5 AND -26.0 AND CAST(longitude AS FLOAT) BETWEEN -66.0 AND -64.0 THEN 'Tucumán'
        WHEN CAST(latitude AS FLOAT) BETWEEN -30.0 AND -24.0 AND CAST(longitude AS FLOAT) BETWEEN -65.0 AND -61.0 THEN 'Santiago del Estero'
        WHEN CAST(latitude AS FLOAT) BETWEEN -29.0 AND -24.0 AND CAST(longitude AS FLOAT) BETWEEN -67.5 AND -64.5 THEN 'Catamarca'
        WHEN CAST(latitude AS FLOAT) BETWEEN -32.0 AND -28.0 AND CAST(longitude AS FLOAT) BETWEEN -68.0 AND -65.5 THEN 'La Rioja'
        WHEN CAST(latitude AS FLOAT) BETWEEN -32.0 AND -28.0 AND CAST(longitude AS FLOAT) BETWEEN -70.0 AND -67.5 THEN 'San Juan'
        WHEN CAST(latitude AS FLOAT) BETWEEN -37.0 AND -32.0 AND CAST(longitude AS FLOAT) BETWEEN -70.0 AND -67.0 THEN 'Mendoza'
        WHEN CAST(latitude AS FLOAT) BETWEEN -35.0 AND -32.0 AND CAST(longitude AS FLOAT) BETWEEN -67.0 AND -64.5 THEN 'San Luis'
        WHEN CAST(latitude AS FLOAT) BETWEEN -39.0 AND -35.0 AND CAST(longitude AS FLOAT) BETWEEN -67.0 AND -62.0 THEN 'La Pampa'
        WHEN CAST(latitude AS FLOAT) BETWEEN -40.0 AND -36.0 AND CAST(longitude AS FLOAT) BETWEEN -71.0 AND -68.0 THEN 'Neuquén'
        WHEN CAST(latitude AS FLOAT) BETWEEN -42.0 AND -38.0 AND CAST(longitude AS FLOAT) BETWEEN -71.0 AND -62.0 THEN 'Río Negro'
        WHEN CAST(latitude AS FLOAT) BETWEEN -46.0 AND -42.0 AND CAST(longitude AS FLOAT) BETWEEN -72.0 AND -64.0 THEN 'Chubut'
        WHEN CAST(latitude AS FLOAT) BETWEEN -52.0 AND -46.0 AND CAST(longitude AS FLOAT) BETWEEN -73.0 AND -65.0 THEN 'Santa Cruz'
        WHEN CAST(latitude AS FLOAT) BETWEEN -55.0 AND -52.0 AND CAST(longitude AS FLOAT) BETWEEN -73.0 AND -65.0 THEN 'Tierra del Fuego'
        WHEN CAST(latitude AS FLOAT) <= -35.0 THEN 'Patagonia'
        WHEN CAST(latitude AS FLOAT) > -28.0 THEN 'Norte'
        ELSE 'Pampa'
    END AS province
FROM {{ source('raw', 'fires_bronze') }}