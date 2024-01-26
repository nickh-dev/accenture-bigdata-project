from sklearn.metrics import mean_squared_error
from configparser import ConfigParser
from math import sqrt
import pandas as pd
from sqlalchemy import create_engine
from prophet import Prophet

# Read the configuration file
config = ConfigParser()
config.read('configuration/config.ini')

# Create a connection to the Snowflake database
engine = create_engine('snowflake://{user}:{password}@{account}/{database}/{schema}?warehouse={warehouse}'.format(
    user=config.get('snowflake', 'user'),
    password=config.get('snowflake', 'password'),
    account=config.get('snowflake', 'account'),
    database=config.get('snowflake', 'db'),
    schema=config.get('snowflake', 'schema'),
    warehouse=config.get('snowflake', 'warehouse')
))

# Execute SQL query and load the result into a DataFrame
df = pd.read_sql("SELECT COUNTRY_REGION, DATE, CASES FROM COVID19_EPIDEMIOLOGICAL_DATA.PUBLIC.ECDC_GLOBAL WHERE COUNTRY_REGION = 'Latvia';", engine)

# Rename the columns to match the Prophet's requirements
df = df.rename(columns={'date': 'ds', 'cases': 'y'})

# Initialize and fit the Prophet model
prophet = Prophet()
prophet.fit(df)

# Generate future dates
future = prophet.make_future_dataframe(periods=365)

# Predict the future cases
forecast = prophet.predict(future)

# Ensure the predicted values are non-negative
forecast['yhat'] = forecast['yhat'].clip(lower=0)
forecast['yhat_lower'] = forecast['yhat_lower'].clip(lower=0)
forecast['yhat_upper'] = forecast['yhat_upper'].clip(lower=0)

# Calculate the root mean squared error
rmse = sqrt(mean_squared_error(df['y'], forecast['yhat'][:len(df)]))
print(f'Test RMSE: {rmse}')

# Rename the columns for better understanding
forecast = forecast.rename(columns={'ds': 'Date', 'yhat': 'Predicted_Cases', 'yhat_lower': 'Lower_Bound', 'yhat_upper': 'Upper_Bound'})

# Get the maximum date in the original data
max_date = pd.to_datetime(df['ds'].max())

# Filter the forecast data for future dates
future_forecast = forecast[forecast['Date'] > max_date]

# Save the future forecast to a CSV file
future_forecast[['Date', 'Predicted_Cases', 'Lower_Bound', 'Upper_Bound']].to_csv('forecast.csv', index=False)

# Close the engine
engine.dispose()