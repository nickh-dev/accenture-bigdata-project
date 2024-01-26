import snowflake.connector as sf
from configparser import ConfigParser
from dash import *
from dash.dependencies import Input, Output, State
import plotly.express as px
import pandas as pd
import boto3
import uuid
from boto3.dynamodb.conditions import Key

# Read the configuration file
config = ConfigParser()
config.read('configuration/config.ini')

# Connect to DynamoDB
dynamodb = boto3.resource('dynamodb')

# Select the DynamoDB table
table = dynamodb.Table('CommentsOnVisualization')

# Create a connection to the Snowflake database
conn = sf.connect(
    user=config.get('snowflake', 'user'),
    password=config.get('snowflake', 'password'),
    account=config.get('snowflake', 'account'),
    warehouse=config.get('snowflake', 'warehouse'),
    db=config.get('snowflake', 'db'),
    schema=config.get('snowflake', 'schema')
)

# Define the SQL queries
queries = {
    'TOP 10 COUNTRIES PER CASES': """
    SELECT COUNTRY_REGION, SUM(CASES) AS TOTAL_CASES
    FROM COVID19_EPIDEMIOLOGICAL_DATA.PUBLIC.ECDC_GLOBAL
    GROUP BY COUNTRY_REGION
    ORDER BY SUM(CASES) DESC
    LIMIT 10;  
    """,
    'TOP 10 COUNTRIES PER DEATHS': """
    SELECT COUNTRY_REGION, SUM(DEATHS) AS TOTAL_CASES
    FROM COVID19_EPIDEMIOLOGICAL_DATA.PUBLIC.ECDC_GLOBAL
    GROUP BY COUNTRY_REGION
    ORDER BY SUM(DEATHS) DESC
    LIMIT 10;
    """
}

# Initialize the Dash application
app = dash.Dash(__name__)

# Define the layout of the application
app.layout = html.Div([
    dcc.Dropdown(
        id='dropdown',
        options=[{'label': i, 'value': i} for i in queries.keys()],
        value=list(queries.keys())[0]
    ),
    dcc.Graph(id='graph'),
    dcc.Input(id='username', type='text', placeholder='Enter your username'),
    dcc.Input(id='input', type='text', placeholder='Enter a comment'),
    html.Button('Submit', id='submit-button', n_clicks=0),
])

# Define the callback to update the graph
@app.callback(
    Output('graph', 'figure'),
    [Input('dropdown', 'value')]
)

def update_graph(selected_query):
    
    # Execute the selected query
    cur = conn.cursor().execute(queries[selected_query])

    # Fetch the result of the query
    rows = cur.fetchall()

    # Get the column names
    columns = [col[0] for col in cur.description]

    # Create a DataFrame from the result
    df = pd.DataFrame(rows, columns=columns)

    # Create a bar plot from the DataFrame
    fig = px.bar(df, x='COUNTRY_REGION', y='TOTAL_CASES', title=f'{selected_query}')
    fig.update_layout(bargap=0.1)
    
    # Return the figure
    return fig

# Define the callback to update the database
@app.callback(
    Output('input', 'value'),
    [Input('submit-button', 'n_clicks')],
    [State('username', 'value'), State('input', 'value')]
)

def update_db(n_clicks, username, value):
    
    # Check if the submit button has been clicked
    if n_clicks > 0:
        # Generate a comment ID
        comment_id = str(uuid.uuid4())

        # Put the comment in the DynamoDB table
        table.put_item(
            Item={
                'comment_id': comment_id,
                'username': username,
                'comment': value
            }
        )

        # Clear the input field
        return ''
    else:
        # Return the current value of the input field
        return value

# Run the application
if __name__ == '__main__':
    app.run_server(debug=True)