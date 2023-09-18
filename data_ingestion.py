import snowflake.connector
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

conn = snowflake.connector.connect(**snowflake_config)
curr = conn.cursor()

try:
    raw_tables = [
        'SensorDataRaw',
        'WeatherDataRaw',
        'SoilDataRaw',
        'CropDataRaw',
        'PestDataRaw',
        'IrrigationDataRaw',
        'LocationDataRaw'
    ]
    for table in raw_tables:
        staging_table_name = f"staging_{table}"
        ingest_query = f''' 
        COPY INTO {staging_table_name} FROM @%{table} 
        FILE_FORMAT_TYPE = 'CSV' );
        '''
        curr.execute(ingest_query)
        conn.commit()
        print(f'data from {table} ingested into {staging_table_name}')
except Exception as e:
    print(f'Error: {e}')
finally:
    curr.close()