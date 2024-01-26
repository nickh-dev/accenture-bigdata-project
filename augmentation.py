import snowflake.connector as sf
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
from configparser import ConfigParser

# Read the configuration file
config = ConfigParser()
config.read('configuration/config.ini')

# Create a connection to the Snowflake database
conn = sf.connect(
    user=config.get('snowflake', 'user'),
    password=config.get('snowflake', 'password'),
    account=config.get('snowflake', 'account'),
    warehouse=config.get('snowflake', 'warehouse'),
    db=config.get('snowflake', 'db'),
    schema=config.get('snowflake', 'schema')
)

# Create a cursor object
curr = conn.cursor()

# Load the CSV data into a DataFrame
df_covid = pd.read_csv("augmentation_dataset/countryLockdowndates.csv")

# Rename the columns of the DataFrame
df = df_covid.rename(columns={'Country/Region': 'COUNTRY_REGION'})
df = df_covid.rename(columns={'Date': 'LOCKDOWN_DATE'})

# Initialize the SQL command to create a new table
create_table = "CREATE TABLE IF NOT EXISTS BIGDATA_PROJECT.PUBLIC.LOCKDOWN_DATES ("

# Convert the column names to uppercase
df.rename(columns=str.upper, inplace=True)

# Loop through the columns of the DataFrame to generate the SQL command
for col in df.columns:
    
    column_name = col.upper()
    
    # Check the data type of the column and add the appropriate SQL data type to the command
    if df[col].dtype.name == "object":
        create_table = create_table + column_name + " varchar(16777216)"
    elif df[col].dtype.name == "datetime64[ns]":
        create_table = create_table + column_name + " datetime"
    
    # Add a comma after each column except the last one
    if df[col].name != df.columns[-1]:
        create_table = create_table + ",\n"
    else:
        create_table = create_table + ")"

# Execute the SQL command to create the new table
conn.cursor().execute(create_table)

# Truncate the table if it already exists
conn.cursor().execute('TRUNCATE TABLE IF EXISTS BIGDATA_PROJECT.PUBLIC.LOCKDOWN_DATES')    

# Write the DataFrame to the Snowflake database
write_pandas(
            conn=conn,
            df=df,
            table_name="LOCKDOWN_DATES",
            database="BIGDATA_PROJECT",
            schema="PUBLIC"
        )

# Switch to the desired database
conn.cursor().execute("USE DATABASE BIGDATA_PROJECT")

# Create a new table by joining two existing tables
create_new_table = """
CREATE TABLE IF NOT EXISTS BIGDATA_PROJECT.PUBLIC.NEW_TABLE AS 
SELECT t1.*, t2.LOCKDOWN_DATE, t2.TYPE, t2.REFERENCE
FROM BIGDATA_PROJECT.PUBLIC.ECDC_GLOBAL_COPY t1
LEFT JOIN BIGDATA_PROJECT.PUBLIC.LOCKDOWN_DATES t2
ON t1.COUNTRY_REGION = t2.COUNTRY_REGION;
"""

# Execute the SQL command to create the new table
conn.cursor().execute(create_new_table)

# Drop the old table
drop_table_sql = "DROP TABLE IF EXISTS BIGDATA_PROJECT.PUBLIC.ECDC_GLOBAL_COPY;"
conn.cursor().execute(drop_table_sql)

# Rename the new table to the name of the old table
rename_table = "ALTER TABLE BIGDATA_PROJECT.PUBLIC.NEW_TABLE RENAME TO ECDC_GLOBAL_COPY;"
conn.cursor().execute(rename_table)

# Closes the connection and the cursor
conn.close()
curr.close()