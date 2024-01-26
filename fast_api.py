from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
import snowflake.connector as sf
import boto3
from configparser import ConfigParser
from fastapi.staticfiles import StaticFiles
import uuid

# Initialize the FastAPI application
app = FastAPI()

# Mount static files
app.mount("/assets", StaticFiles(directory="assets"), name="assets")

# Read the configuration file
config = ConfigParser()
config.read('configuration/config.ini')

# Initialize a cache
cache = {}

# Create a connection to the Snowflake database
conn = sf.connect(
    user=config.get('snowflake', 'user'),
    password=config.get('snowflake', 'password'),
    account=config.get('snowflake', 'account'),
    warehouse=config.get('snowflake', 'warehouse'),
    db=config.get('snowflake', 'db'),
    schema=config.get('snowflake', 'schema')
)

# Connect to DynamoDB
dynamodb = boto3.resource('dynamodb')

# Select the DynamoDB table
table = dynamodb.Table('SupplementaryData')

# Define the root endpoint
@app.get("/", response_class=HTMLResponse)
def read_root():
    
    # Return a HTML response
    return """
    <html>
        <head>
            <style>
                body {
                    background-image: url('/assets/img.jpg');
                    background-repeat: no-repeat;
                    background-attachment: fixed;
                    background-size: cover;
                }
                .center-screen {
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    height: 100vh;
                    font-size: 30px;
                    color: white;
                    padding-right: 50%;
                }
            </style>
        </head>
        <body>
            <div class="center-screen">
                Hello, welcome to my API for bootcamp project <br>
                For user-friendly testing access '/docs' endpoint<br>
                <br>
                Created by Ņikita Hramčenko
                
            </div>
        </body>
    </html>
    """

# Define the data model for items
class Item(BaseModel):
    data_point_id: str
    user_comment: str = None
    annotation: str = None
    additional_source: str = None

# Define the query endpoint
@app.get("/query")
def query(query: str):
    
    # Check if the query is in the cache
    if query in cache:
        return cache[query]
    
    # Execute the query
    curr = conn.cursor()
    curr.execute(query)
    result = curr.fetchall()
    
    # Generate a data point ID
    data_point_id = str(uuid.uuid4())

    # Put the item in the DynamoDB table
    table.put_item(Item={'data_point_id': data_point_id, 'query': query})
    
    # Prepare the response
    response = {'data_point_id': data_point_id, 'result': result}
    
    # Add the response to the cache
    cache[query] = response

    # Return the response
    return response


# Define the metadata endpoint
@app.get("/metadata/{data_point_id}")
def read_metadata(data_point_id: str):
    
    # Get the item from the DynamoDB table
    response = table.get_item(Key={'data_point_id': data_point_id})
    item = response['Item']

    # Return the item
    return {'item': item}

# Define the endpoint to update metadata
@app.post("/metadata/")
def update_metadata(item: Item):
    # Update the item in the DynamoDB table
    response = table.update_item(
        Key={'data_point_id': item.data_point_id},
        UpdateExpression="set user_comment = :c, annotation=:a, additional_source=:s",
        ExpressionAttributeValues={
            ':c': item.user_comment,
            ':a': item.annotation,
            ':s': item.additional_source
        },
        ReturnValues="UPDATED_NEW"
    )
    
    # Return the response
    return response

# Define the endpoint to check the cache status
@app.get("/cache_status")
def cache_status():
    # Check if the cache is empty
    if not cache:
        return "Cache is empty"
    else:
        return "Cache is not empty"

# Define the exception handler
@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    # Return a JSON response with the exception message
    return JSONResponse(
        status_code=500,
        content={"message": str(exc)}
    )