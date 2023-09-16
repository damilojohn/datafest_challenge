import snowflake.connector as connector 
import configparser 

config = configparser.ConfigParser()
config.read('config.ini')

snowflake_config = {
    'account': config['snowflake']['account'],
    'warehouse': config['snowflake']['warehouse'],
    'database': config['snowflake']['database'],
    'schema': config['snowflake']['schema'],
    'role': config['snowflake']['role'],
    'user': config['snowflake']['user'],
    'password': config['snowflake']['password']
}

conn = connector.connect(**snowflake_config)
curr = conn.cursor()

try:
    # Define transformations for fact and dimension tables
    transformation_queries = [
        # Transformation for Sensor Data fact table (example)
        '''
        CREATE OR REPLACE TABLE YourSchema.Fact_SensorData AS
        SELECT
            DateTrunc('DAY', Timestamp) AS Date,
            AVG(SensorValue) AS AverageValue,
            LocationID
        FROM
            Staging_SensorDataRaw
        GROUP BY
            Date,
            LocationID;
        ''',

        # Transformation for Location Dimension table (example)
        '''
        CREATE OR REPLACE TABLE YourSchema.Dim_Location AS
        SELECT
            LocationID,
            LocationName,
            Latitude,
            Longitude
        FROM
            Staging_LocationDataRaw
        QUALIFY ROW_NUMBER() OVER(PARTITION BY LocationID ORDER BY Timestamp DESC) = 1;
        ''',
        # Add more transformations for other tables as needed
    ]
    for query in transformation_queries:
        curr.execute(query)
        conn.commit()
        print(f'Transformation executed successfully.')
except Exception as e:
    print(f'Error as {e}')
finally:
    curr.close()
    conn.close()