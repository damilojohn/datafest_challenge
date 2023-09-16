CREATE DATADEVCLEANSED IF NOT EXISTS DATADEVCLEANSED;

-- define fact and dimension tables 

CREATE OR REPLACE TABLE DATADEVCLEANSED.fact_SensorData(
    SENSOR_ID VARCHAR(10),
    TIME_STAMP Timestamp,
    TEMPAERATURE_F DECIMAL(5,2),
    HUMIDITY DECIMAL(5,2),
    SOIL_MOISTURE DECIMAL(5,2),
    LIGHT_INTENSITY DECIMAL(5,2)
    BATTERY_LEVEL NUMBER
);

CREATE OR REPLACE TABLE DATADEVCLEANSED.dim_Location(
    LONGITUDE DECIMAL(8,6)
    LATITUDE DECIMAL(8,6),
    SENSOR_ID VARCHAR(10),
    LOCATION_NAME VARCHAR(30)
    ELEVATION DECIMAL(5,2),
    REGION VARCHAR(20),
);

CREATE OR REPLACE TABLE DATADEVCLEANSED.fact_IrrigationData(
    TIME_STAMP TIMESTAMP,
    Irrigation_Method VARCHAR(20),
    Water_Source VARCHAR(20),   
    IRRIGATION_DURATON_MIN NUMBER,
    SENSOR_ID VARCHAR(10)
)

CREATE OR REPLACE TABLE DATADEVCLEANSED.fact_PestData(
    TIME_STAMP TIMESTAMP,
    PEST_TYPE VARCHAR(20),
    PEST_DESCRIPTION TEXT, 
);

CREATE OR REPLACE TABLE DATADEVCLEANSED.fact_CropData(
    TIME_STAMP TIMESTAMP,
    CROP_TYPE VARCHAR(20),
    CROP_YIELD DECIMAL(8,2)
)
CREATE OR REPLACE TABLE DATADEVCLEANSED.fact_SoilData(
    TIME_STAMP TIMESTAMP,
    SOIL_MOISTURE_PERCENT DECIMALS(5,2),
    SOIL_PH DECIMAL(4,2),
)

CREATE OR REPLACE TABLE DATADEVCLEANSED.fact_WeatherData(
   TIME_STAMP TIMESTAMP,
   PRECIPITATION_INCHES DECIMALS(5,2),
   WIND_SPEED_MPH DECIMALS(5,2)
)