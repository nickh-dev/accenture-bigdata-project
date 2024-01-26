Student’s name: Ņikita Hramčenko 

Project name: "**COVID-19 Data Integration, Analysis, and Visualization Platform**" 

The "COVID-19 Epidemiological Data" public dataset was used for this project. The analysis revealed many data tables, which can make the process very difficult. Consequently, table "ECDC\_GLOBAL" was selected for the main work, which contains country-specific data on cases, deaths, changes in the number of cases from the previous day, changes in the number of deaths from the previous day, population, date of record, and a few other columns that are less relevant. 

The entire dataset contains 43 tables with various data such as global demographics data, case count data by locality and case type, data on the number of vaccinations with vaccines from different manufacturers, mobility changes according to grocery shopping and pharmacy visits, and much other data. 

**Task 1. Use Snowflake Marketplace and get COVID-19 free dataset.** 

For the project free tear Snowflake account was used with created resource monitoring feature to monitor the usage of spent credits. 

**Task 2. Data Exploration and Enhancement.** 

Second task includes data analyzing to understand structure, patterns, and any gaps in dataset. Kaggle dataset:[ https://www.kaggle.com/datasets/jcyzag/covid19-lockdown-dates-by-country ](https://www.kaggle.com/datasets/jcyzag/covid19-lockdown-dates-by-country)

**Data exploration:** 

- Get the structure of tables in the database one by one. 
- Make a quick look at the data. 
- Count the number of rows in data tables. 
- Find the unique values in a column. 
- Calculate aggregate statistics for a column. 
- Find rows with missing values in column. 
- Count the number of missing values in a column. 

**Pattern identification:** 

- Find the date with the highest number of cases. 
- Find the average number of cases per month. 
- Find the correlation between cases and deaths. 

Query examples for testing can be found in Analyzing data worksheet. 

After data analyzing I’ve proceeded with data augmentation to provide richer insights with lockdown dates, lockdown type, and references. 

I’ve created my own database called BIGDATA\_PROJECT with PUBLIC schema, where I will store data table with write permissions. The query for creating a new table and populating it with data from an existing ECDS\_GLOBAL data table was executed. 

**Global requirements for entire project:** 

- (configparser.ConfigParser) class from from configparser module to work with configuration file, which allows to read database connection details. Fill the config.ini file, which is in configuration folder with your Snowflake credentials. 

**Python script for data augmentation consists of such steps:** 

- Read a configuration file to get the necessary credentials and details for connecting to a Snowflake database. 
- Establish a connection to the Snowflake database. 
- Load a CSV file into a pandas data frame. The CSV file contains data about country lockdown dates, lockdown type, and references. countryLockdowndates.csv file can be accesses from the augmentation\_dataset folder for exploration. 
- Rename the columns of the data frame to match the column names in the Snowflake database or create a new column to prevent the replacement of an existing one. 
- Write a SQL query to create a new data table in the Snowflake database. The 

  query includes the column names and data types based on the data frame. 

- Execute the SQL query to create the new data tabletable. 
- Truncate the table if it already exists. 
- Write the data frame to the newly created data table. 
- Create a new table by joining two existing tables. 
- Drops the old table and the augmented table from the Snowflake database. 
- Renames the new table to the name of the old table. 
- Closes the connection and the cursor to the Snowflake database. 

**Described Python script requires such dependencies:** 

- **(**snowflake.connector**)** library to provide an interface for Python to communicate with Snowflake database. 
- (snowflake.connector.pandas\_tools.write\_pandas) function to write pandas data frame objects to a Snowflake database. 
- (pandas) library to structure data and manipulate on it. 

Snowflake worksheet for this task can be found by the name ‘Create, test, execute’. 

**Task 3. Data modeling in NoSQL.** 

In the third task I was asked to design a schema for NoSQL database. I’ve used DynamoDB database in AWS cloud provider. 

Schema consists of table name, key schema, which specifies the primary key with hash key type. It also includes attribute definitions and their types, and provisioned throughput, which specify the read and write capacity units. 

This schema just defines the attributes for the primary key and optionally for secondary indexes. The other attributes in the items can vary. 

**Demonstration of DynamoDB tables in AWS account:** 

To be able to use DynamoDB I was required to install AWS CLI and configure it with my AWS account credentials and region 

**I have used:** 

- pip install aws cli 
- aws configure 

**Task 4/8. API Development with Python** 

For the fourth task API was developed for querying Snowflake data based on user input, interact with NoSQL DynamoDB database to store and fetch relevant additional data. 

**Python script for API implementation consists of such steps:** 

- Initialize a FastAPI application and mount static file for applying background image for the root endpoint. 
- Read a configuration file to get the necessary credentials for connecting to a Snowflake database. 
- Implement caching for for frequently requested data. 
- Establishes a connection to the Snowflake database. 
- Connects to a DynamoDB database and selects a table. 
- Define a root endpoint that returns a HTML response when accessed. Root page is just a welcome page with short information. 
- Define a data model for items that will be stored in the DynamoDB table. The items have four attributes: data\_point\_id as primary key, user\_comment, annotation, and additional\_source. 
- Define a query endpoint that takes a SQL query as a parameter. First of all, it checks if the query is in the cache. If it is, it returns the cached result. If it’s not, it executes the query on the Snowflake database, stores the result in the DynamoDB table and the cache, and returns the hashed query id, and the query result. 
- Define a metadata endpoint with GET method that takes a data\_point\_id as a parameter. When this endpoint is accessed, it retrieves the item with the given data\_point\_id from the DynamoDB table and returns it. 
- Defines an endpoint to update metadata with POST method. When this endpoint is accessed with a POST request, it updates the item in the DynamoDB table with the data sent in the request and returns the response. 
- Define a cache status endpoint. When this endpoint is accessed, it checks if the cache is empty. If the cache is empty, it returns the message “Cache is empty”. If the cache is not empty, it returns the message “Cache is not empty”. It can be used just for testing purpose. 
- Defines a general exception handler: This is a function that handles all exceptions that are not handled by other exception handlers. When an exception occurs, this function is called with the request that caused the exception and the exception itself as parameters. It returns a JSON response with a status code of 500 (which indicates a server error) and a content that includes a message with the string representation of the exception. 

**Described Python script requires such dependencies:** 

- (FastAPI) web framework for building an API. 
- (Requests) class from FastAPI framework that provides a simple interface for handling HTTP requests. 
- (HTMLResponse, JSONResponse): These are classes from FastAPI that allow to create HTTP responses with HTML or JSON content, respectively. 
- (BaseModel) is a class from the Pydantic library. It’s used to define data models with type annotations. 
- (boto3) is the AWS SDK for Python. It allows to communicate with AWS services. 
- (StaticFiles) is a class from FastAPI that provides a simple way to serve static files. 
- (uuid) module provides immutable UUID objects. 

For user-friendly interaction you can use /docs endpoint to test endpoints with different methods using UI interface. Choose the endpoint you want to test and click on ‘Try it now’ button. 

To test GET and POST methods for metadata endpoints you need to copy unique identifier. which is the value for data\_point\_id key on the top of json response. 

**Task 5. Interactive Visualization with Python** 

Interactive visualization was created with Dash and  Plotly Python libraries, with users possibility to add comments attached to provided user’s username, which are then stored in the DynamoDB database. 

**Python script for implementation interactive visualization consists of such steps:** 

- Read a configuration file to get the necessary credentials for connecting to a Snowflake database and a DynamoDB database. 
- Establishes a connection to the Snowflake database and selects a DynamoDB table. 
- Define SQL queries. 
- Initialize a Dash application and define its layout. 
- Define a callback function that updates the graph based on the selected query from the dropdown menu. The function executes the selected query on the Snowflake database, creates a data frame from the result, creates a bar plot, and returns the plot. 
- Define another callback function that updates the DynamoDB table with a comment when the submit button is clicked. The function generates a comment ID, puts the comment in the DynamoDB table, and clears the input field. 
- Run the Dash application. 

**Described Python script requires such dependencies:** 

- (Dash) is a Python framework for building analytical web applications. It is used to create the Dash application, define its layout, and define the callback functions. 
- (dash.dependencies.Input, Output, State) these are classes in 

  the dash.dependencies module. They are used to define the inputs, outputs, and states of the callback functions. 

- (plotly.express (as px)) is a high-level interface for data visualization. 
- (boto3.dynamodb.conditions.Key) class in the boto3.dynamodb.conditions module is used to define conditions for querying and scanning data in DynamoDB. 

**Task 6. Analytical Features** 

For time series forecasting I choose to predict COVID-19 infection cases for future year starting from the last available date from the Snowflake data table. 

**Python script for time series forecasting consists of such steps:** 

- Read a configuration file to get the necessary credentials for connecting to a Snowflake database. 
- Establish a connection to the Snowflake database. 
- Execute a SQL query on the Snowflake database to fetch data about COVID-19 cases in Latvia and loads the result into a pandasdata frame. 
- Rename the columns of the data frame to match the requirements of the Prophet model. 
- Initialize and fit a Prophet model with thedata frame. 
- Generate future dates for the next year. 
- Predict the future cases of COVID-19 in Latvia using the Prophet model. 
- Ensure that the predicted values are non-negative by clipping them at zero. 
- Calculate the root mean squared error (RMSE) between the actual and predicted cases and print it. 
- Renames the columns of the forecast data frame for better understanding. 
- Filter the forecast data for future dates (dates after the maximum date in the original data). 
- Casts the columns of the future forecast data frame to integers. 
- Save the future forecast to a CSV file. 
- Close the connection to the Snowflake database. 

**Described Python script requires such dependencies:** 

- (sklearn.metrics.mean\_squared\_error) function from the scikit-learn library is used to compute the mean squared error, a risk metric corresponding to the expected value of the squared error or loss. 
- (math.sqrt) function from the math module in Python’s standard library is used to calculate the root mean squared error (RMSE). 
- (sqlalchemy.create\_engine) function from the SQLAlchemy library in Python is used to create a new SQLAlchemy engine, which provides a source of database connectivity. 
- (prophet.Prophet) class from the Prophet library in Python is used to create a new Prophet forecasting model. Prophet is a procedure for forecasting time series data. Prophet was designed by Facebook. 

You can view complete csv file with all predictions for a year by accessing forecast.csv file inside csv\_files folder. 

For the clustering task I’ve segmented regions based on similarities in COVID-19 spread patterns. 

**Python script for clustering consists of such steps:** 

- Read a configuration file to get the necessary credentials for connecting to a Snowflake database. 
- Establish a connection to the Snowflake database. 
- Execute a SQL query on the Snowflake database to fetch data about COVID-19 cases and deaths by country and loads the result into a pandasdata frame. 
- Initialize a StandardScaler from the scikit-learn library, which standardizes features by removing the mean and scaling to unit variance. 
- Select the features to be scaled, which are the total cases and total deaths. 
- Scale the features using the StandardScaler. 
- Initializes a KMeans model with 5 clusters and fits the model on the scaled features. Number of clusters represents the number of groups into which the data points will be divided. 
- Get the cluster labels from the KMeans model. 
- Add the cluster labels to thedata frame as a new column. 
- Sort the data frame by the cluster labels. 
- Save the sorted data frame to a CSV file. 

**Described Python script requires such dependencies:** 

- (sklearn.cluster.KMeans) class from the scikit-learn library is used to perform K- means clustering. 
- (sklearn.preprocessing.StandardScaler) class from the scikit-learn library is used to standardize features by removing the mean and scaling to unit variance. 

You can view complete csv file with all cluster groups by accessing clustering.csv file inside csv\_files folder. 

**Task 7. Performance Optimization** 

Performance optimization on Snowflake processes ensures that the SQL queries on the COVID-19 dataset are optimized for performance. The worksheet with queries can be found by name ‘Performance optimization’. 

**It can consist of such steps:** 

- Create a large warehouse with multiple clusters. 
- Optimize data storage: 
1. Cluster the table by often used column to speed up queries. 
1. Create a new table with the same data ordered according to the clustering key. 
- Use caching. 
  - Snowflake automatically caches query results, so if the same query will be run again, it will return the result from the cache. 
  - Running the same query multiple times will be faster after the first time. 
- Write efficient queries. 

**Conclusion** 

To summarize all the work that has been done, I can say that it has been hard. The project involves a lot of challenging technologies. I had to do a lot of research, inspect GitHub repositories, read articles, do debugging, and have dialogs with AI assistants. Of course, this project needs additional optimization and expansion, as there are many ways to do it, but it is almost impossible to do it in the given timeframe. In the future I plan to continue optimizing and expanding this project, as it turned out to be very interesting. Unfortunately, I was not able to complete the ninth assignment because I am having trouble finding data to use the MATCH\_RECOGNIZE function to look for patterns in the data. I will continue to study this issue and will try to finish it in my free time. 

All the Python scripts and CSV files with outputs can be found in my GitHub repository. 
