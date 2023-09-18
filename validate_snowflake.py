import snowflake.connector

# Gets the version
ctx = snowflake.connector.connect(
    user='DATADEVS',
    password='oI2rNwH9lK',
    account='wd07046.north-europe.azure'
    )
cs = ctx.cursor()
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
finally:
    cs.close()
ctx.close()