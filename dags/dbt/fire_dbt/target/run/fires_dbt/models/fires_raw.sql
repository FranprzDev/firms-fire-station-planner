
  
    

  create  table "postgres"."public"."fires_raw__dbt_tmp"
  
  
    as
  
  (
    

select
    cast(latitude as float) as latitude,
    cast(longitude as float) as longitude,
    cast(brightness as float) as brightness,
    cast(scan as float) as scan,
    cast(track as float) as track,
    cast(acq_date as date) as acq_date,
    cast(acq_time as varchar) as acq_time,
    cast(satellite as varchar) as satellite,
    cast(instrument as varchar) as instrument,
    cast(confidence as int) as confidence,
    cast(version as varchar) as version,
    cast(bright_t31 as float) as bright_t31,
    cast(frp as float) as frp,
    cast(daynight as varchar) as daynight
from "postgres"."public"."fires_raw"
  );
  