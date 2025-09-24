{{ config(materialized='table', schema='public') }}

-- Modelo intermedio: datos limpios desde fires_raw
-- Este modelo actúa como puente entre bronze y silver layer
SELECT
    *,
    CASE
        WHEN frp >= 50 THEN 'Alta'
        WHEN frp >= 20 THEN 'Media'
        ELSE 'Baja'
    END as intensity_level,
    
    ROW_NUMBER() OVER (
        PARTITION BY acq_date, latitude, longitude 
        ORDER BY acq_time
    ) as fire_id

FROM {{ ref('fires_raw') }}

WHERE validation_status = 'valid'
    AND confidence > 30
    AND frp > 0