{{ config(materialized='table', schema='public') }}

SELECT
    acq_date,
    satellite,
    province,
    region,

    COUNT(*) as total_fires,
    COUNT(DISTINCT fire_id) as unique_fires,

    ROUND(AVG(frp)::numeric, 2) as avg_frp,
    ROUND(MAX(frp)::numeric, 2) as max_frp,
    ROUND(SUM(frp)::numeric, 2) as total_frp,

    COUNT(CASE WHEN intensity_level = 'Alta' THEN 1 END) as high_intensity_fires,
    COUNT(CASE WHEN intensity_level = 'Media' THEN 1 END) as medium_intensity_fires,
    COUNT(CASE WHEN intensity_level = 'Baja' THEN 1 END) as low_intensity_fires,

    ROUND(AVG(confidence)::numeric, 1) as avg_confidence,
    ROUND(AVG(data_quality_score)::numeric, 1) as avg_quality_score,

    ROUND(AVG(brightness)::numeric, 2) as avg_brightness,
    ROUND(AVG(bright_t31)::numeric, 2) as avg_bright_t31

FROM {{ ref('fires_enriched') }}
GROUP BY acq_date, satellite, province, region
ORDER BY acq_date DESC, total_fires DESC