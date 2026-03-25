-- Create zipcode dimension table

CREATE OR REPLACE TABLE `data-pipeline-project-485214.cyclist_data.dim_zipcode` AS

SELECT DISTINCT
  CAST(zip AS STRING) AS zip_code,
  borough,
  neighborhood
FROM `data-pipeline-project-485214.cyclist_data.nyc_zipcodes`;
