from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from configparser import ConfigParser
import pandas as pd
from sqlalchemy import create_engine

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

df = pd.read_sql("SELECT COUNTRY_REGION, SUM(CASES) AS TOTAL_CASES, SUM(DEATHS) AS TOTAL_DEATHS FROM COVID19_EPIDEMIOLOGICAL_DATA.PUBLIC.ECDC_GLOBAL GROUP BY COUNTRY_REGION;", engine)

# Initialize the StandardScaler

scaler = StandardScaler()

# Select the features to be scaled

features = df[['total_cases', 'total_deaths']]

# Scale the features

features_scaled = scaler.fit_transform(features)

# Initialize the KMeans model

kmeans = KMeans(n_clusters=5, n_init=10)

# Fit the KMeans model on the scaled features

kmeans.fit(features_scaled)

# Get the cluster labels

labels = kmeans.labels_

# Add the cluster labels to the DataFrame

df['cluster'] = labels

# Sort the DataFrame by the cluster labels

df_sorted = df.sort_values(by='cluster')

# Save the sorted DataFrame to a CSV file

df_sorted.to_csv('csv_files/clustering.csv', index=False)