
  
    

  create  table "postgres"."public_public"."fires_enriched__dbt_tmp"
  
  
    as
  
  (
    

SELECT
    fc.*,

    CAST('Argentina' AS VARCHAR(20)) AS country,

    CASE
        WHEN fc.province IN ('Buenos Aires', 'Córdoba', 'Santa Fe', 'Entre Ríos') THEN 'Pampa'
        WHEN fc.province IN ('Corrientes', 'Misiones', 'Chaco', 'Formosa', 'Salta', 'Jujuy', 'Tucumán', 'Santiago del Estero', 'Catamarca') THEN 'Norte'
        ELSE 'Patagonia'
    END AS region,

    CASE
        WHEN fc.frp >= 50 THEN 'Alta'
        WHEN fc.frp >= 20 THEN 'Media'
        ELSE 'Baja'
    END as intensity_level,

    ROW_NUMBER() OVER (
        PARTITION BY fc.acq_date, fc.latitude, fc.longitude
        ORDER BY fc.acq_time
    ) as fire_id

FROM "postgres"."public_public"."fires_data_silver" fc
  );
  