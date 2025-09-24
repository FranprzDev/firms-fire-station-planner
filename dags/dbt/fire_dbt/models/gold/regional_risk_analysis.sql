{{ config(materialized='table', schema='public') }}

WITH province_metrics AS (
    SELECT
        province,
        COUNT(*) as total_incendios,
        COUNT(*) as area_afectada_km2,
        ROUND(AVG(frp)::numeric, 2) as avg_frp,
        ROUND(AVG(confidence)::numeric, 1) as avg_confidence,
        MAX(frp) as max_frp,
        ROUND((AVG(frp) * COUNT(*))::numeric, 2) as intensidad_ponderada
    FROM {{ ref('fires_enriched') }}
    WHERE province IS NOT NULL
        AND validation_status = 'valid'
        AND frp > 0
    GROUP BY province
    HAVING COUNT(*) > 0
),

province_centroids AS (
    SELECT
        fe.province,
        ROUND(
            (SUM(fe.latitude * fe.frp) / NULLIF(SUM(fe.frp), 0))::numeric,
            6
        ) as centroid_latitude,

        ROUND(
            (SUM(fe.longitude * fe.frp) / NULLIF(SUM(fe.frp), 0))::numeric,
            6
        ) as centroid_longitude

    FROM {{ ref('fires_enriched') }} fe
    WHERE fe.province IS NOT NULL
        AND fe.validation_status = 'valid'
        AND fe.frp > 0
    GROUP BY fe.province
)

SELECT
    pm.province,
    pm.total_incendios,
    pm.area_afectada_km2,
    pm.intensidad_ponderada,
    pm.avg_frp,
    pm.avg_confidence,
    pm.max_frp,

    pc.centroid_latitude,
    pc.centroid_longitude,

    25 as drone_battery_duration_min,
    60 as drone_speed_kmh,
    25 as drone_coverage_radius_km,
    15 as ideal_response_time_min,

    CASE
        WHEN pm.intensidad_ponderada >= 1000.0 THEN 'Alta'
        WHEN pm.intensidad_ponderada >= 500.0 THEN 'Media'
        ELSE 'Baja'
    END as risk_priority,

    CONCAT(
        'POINT(',
        pc.centroid_longitude::text,
        ' ',
        pc.centroid_latitude::text,
        ')'
    ) as station_location,

    pc.centroid_longitude - 25 as coverage_west,
    pc.centroid_longitude + 25 as coverage_east,
    pc.centroid_latitude - 25 as coverage_south,
    pc.centroid_latitude + 25 as coverage_north

FROM province_metrics pm
JOIN province_centroids pc ON pm.province = pc.province
WHERE pm.total_incendios > 0
ORDER BY pm.intensidad_ponderada DESC, pm.area_afectada_km2 DESC
