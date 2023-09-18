import pytest
import snowflake.connector
import configparser

# Load Snowflake connection settings from a config file (config.ini)
config = configparser.ConfigParser()
config.read('config.ini')

# Snowflake connection parameters
snowflake_config = {
    'account': config['snowflake']['account'],
    'warehouse': config['snowflake']['warehouse'],
    'database': config['snowflake']['database'],
    'schema': config['snowflake']['schema'],
    'role': config['snowflake']['role'],
    'user': config['snowflake']['user'],
    'password': config['snowflake']['password']
}

raw_tables = [
        'SensorDataRaw',
        'WeatherDataRaw',
        'SoilDataRaw',
        'CropDataRaw',
        'PestDataRaw',
        'IrrigationDataRaw',
        'LocationDataRaw'
    ]
table_mappings = {k:f'staging_{k}' for k in raw_tables}

@pytest.fixture(scope='module')
def snowflake_connection():
    conn = snowflake.connector.connect(**snowflake_config)
    yield conn
    conn.close()

@pytest.mark.parametrize('raw_table,staging_table',table_mappings.items())
def test_ingestion(snowflake_connection, raw_table, staging_table):
    cur = snowflake_connection.cursor()
    try:
        cur.execut(f'SELECT COUNT(*) FROM {raw_table}')
        raw_count = cur.fetchone()[0]

        cur.execute(f'SELECT COUNT(*) FROM {staging_table}')
        staging_count = cur.fetchone()[0]

        assert raw_count == staging_count 
    finally:
        cur.close()      
