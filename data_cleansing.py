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

# create a snowflake connection
conn = connector.connect(**snowflake_config)
curr = conn.cursor 

try:
    staging_tables = [
        'Staging_SensorDataRaw',
        'Staging_WeatherDataRaw',
        'Staging_SoilDataRaw',
        'Staging_CropDataRaw',
        'Staging_PestDataRaw',
        'Staging_IrrigationDataRaw',
        'Staging_LocationDataRaw'
    ]
    for staging_table in staging_tables:
        # get column_names
        column_names_query = f''' 
        SELECT COLUMN NAME FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{staging_table} 
        ''' 
        curr.execute(column_names_query)
        columns = [row[0] for row in curr.fetchall()]
        # construct dynamic sql query to remove all null rows from every table 
        cleansing_query = f'''
        DELETE FROM {staging_table} WHERE {' OR '.join([f"{column} IS NULL" 
        for column in columns])};
        '''
        curr.execute(cleansing_query)
        conn.commit()
        print(f'data in {staging_table} cleansed successfully..')
        
except Exception as e:
    print(f'Error: {e}')
finally:
    curr.close()
    conn.close()
