-- Create fact table for Citi Bike trips

CREATE OR REPLACE TABLE `data-pipeline-project-485214.cyclist_data.fact_citibike_trips` AS

SELECT
  DATE_ADD(DATE(TRI.starttime), INTERVAL 5 YEAR) AS trip_date,
  TRI.usertype AS user_type,

  ZIPSTART.zip_code AS start_zip_code,
  ZIPEND.zip_code AS end_zip_code,

  ROUND(CAST(TRI.tripduration / 60 AS INT64), -1) AS trip_minutes_bucket,

  COUNT(1) AS trip_count,

  AVG(WEA.temp) AS avg_temperature,
  AVG(WEA.wdsp) AS avg_wind_speed,
  SUM(WEA.prcp) AS total_precipitation

FROM `bigquery-public-data.new_york_citibike.citibike_trips` AS TRI

JOIN `bigquery-public-data.geo_us_boundaries.zip_codes` ZIPSTART
ON ST_WITHIN(
  ST_GEOGPOINT(TRI.start_station_longitude, TRI.start_station_latitude),
  ZIPSTART.zip_code_geom
)

JOIN `bigquery-public-data.geo_us_boundaries.zip_codes` ZIPEND
ON ST_WITHIN(
  ST_GEOGPOINT(TRI.end_station_longitude, TRI.end_station_latitude),
  ZIPEND.zip_code_geom
)

JOIN `bigquery-public-data.noaa_gsod.gsod20*` WEA
ON PARSE_DATE("%Y%m%d", CONCAT(WEA.year, WEA.mo, WEA.da)) = DATE(TRI.starttime)

WHERE
  WEA.wban = '94728'
  AND EXTRACT(YEAR FROM DATE(TRI.starttime)) BETWEEN 2014 AND 2015

GROUP BY
  trip_date,
  user_type,
  start_zip_code,
  end_zip_code,
  trip_minutes_bucket;
