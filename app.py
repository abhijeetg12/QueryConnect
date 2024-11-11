import time
import os 
import json 
from google.cloud import bigquery
from google.oauth2 import service_account
import streamlit as st
from vertexai.generative_models import FunctionDeclaration, GenerativeModel, Part, Tool
import vertexai
from vertexai import init
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from typing import Dict, List, Any
import numpy as np

BIGQUERY_DATASET_ID = "thelook_ecommerce"

credentials_dict = {
    "type": "service_account",
    "project_id": st.secrets["gcp_project_id"],
    "private_key_id": st.secrets["gcp_private_key_id"],
    "private_key": st.secrets["gcp_private_key"],
    "client_email": st.secrets["gcp_client_email"],
    "client_id": st.secrets["gcp_client_id"],
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": st.secrets["client_x509_cert_url"]#f"https://www.googleapis.com/robot/v1/metadata/x509/{st.secrets['gcp_client_email']}"
}
credentials = service_account.Credentials.from_service_account_info(
    credentials_dict
)

project_id = st.secrets["gcp_project_id"]

vertexai.init(
    project=project_id,
    location="us-central1",  # Replace with your preferred location
    credentials=credentials
)

# Initialize BigQuery client
client = bigquery.Client(
    credentials=credentials,
    project=project_id
)


list_datasets_func = FunctionDeclaration(
    name="list_datasets",
    description="Get a list of datasets that will help answer the user's question",
    parameters={
        "type": "object",
        "properties": {},
    },
)

list_tables_func = FunctionDeclaration(
    name="list_tables",
    description="List tables in a dataset that will help answer the user's question",
    parameters={
        "type": "object",
        "properties": {
            "dataset_id": {
                "type": "string",
                "description": "Dataset ID to fetch tables from.",
            }
        },
        "required": [
            "dataset_id",
        ],
    },
)

get_table_func = FunctionDeclaration(
    name="get_table",
    description="Get information about a table, including the description, schema, and number of rows that will help answer the user's question. Always use the fully qualified dataset and table names.",
    parameters={
        "type": "object",
        "properties": {
            "table_id": {
                "type": "string",
                "description": "Fully qualified ID of the table to get information about",
            }
        },
        "required": [
            "table_id",
        ],
    },
)

sql_query_func = FunctionDeclaration(
    name="sql_query",
    description="Get information from data in BigQuery using SQL queries with proper date handling",
    parameters={
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": """SQL query that will help give quantitative answers to the user's question. 
                For date/time queries:
                - Always use DATE() function to convert TIMESTAMP to DATE when comparing dates
                - Use TIMESTAMP_TRUNC() for truncating timestamps to specific granularity
                - Use DATETIME_SUB() or DATE_SUB() for date arithmetic
                - Always use the fully qualified dataset and table names
                Example date handling:
                - For last N months: DATE(created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL N MONTH)
                - For daily aggregation: DATE(created_at) as date
                """,
            }
        },
        "required": ["query"],
    },
)

analyze_table_func = FunctionDeclaration(
    name="analyze_table",
    description="Analyze a table's data distribution, quality issues, and key statistics",
    parameters={
        "type": "object",
        "properties": {
            "table_id": {
                "type": "string",
                "description": "Fully qualified ID of the table to analyze"
            },
            "analysis_type": {
                "type": "string",
                "enum": ["distribution", "quality", "relationships", "all"],
                "description": "Type of analysis to perform"
            }
        },
        "required": ["table_id", "analysis_type"]
    }
)

generate_visualization_func = FunctionDeclaration(
    name="generate_visualization",
    description="Generate appropriate visualization based on query results",
    parameters={
        "type": "object",
        "properties": {
            "query_results": {
                "type": "string",
                "description": "JSON string of query results"
            },
            "viz_type": {
                "type": "string",
                "enum": ["time_series", "distribution", "correlation", "comparison", "geographic"],
                "description": "Type of visualization to generate"
            },
            "title": {
                "type": "string",
                "description": "Title for the visualization"
            }
        },
        "required": ["query_results", "viz_type", "title"]
    }
)

data_quality_check_func = FunctionDeclaration(
    name="check_data_quality",
    description="Run data quality checks on specified columns",
    parameters={
        "type": "object",
        "properties": {
            "table_id": {
                "type": "string",
                "description": "Fully qualified ID of the table to check"
            },
            "columns": {
                "type": "string",
                "description": "Comma-separated list of columns to check"
            }
        },
        "required": ["table_id", "columns"]
    }
)

# Implementation of new functions
def analyze_table(table_id: str, analysis_type: str) -> Dict[str, Any]:
    """
    Implement comprehensive table analysis
    """
    client = bigquery.Client(credentials=credentials)
    
    if analysis_type in ["distribution", "all"]:
        # Get column distributions
        query = f"""
        SELECT column_name, 
               COUNT(*) as total_rows,
               COUNT(DISTINCT {column_name}) as distinct_values,
               COUNT(CASE WHEN {column_name} IS NULL THEN 1 END) as null_count
        FROM `{table_id}`
        GROUP BY column_name
        """
        distribution_results = client.query(query).to_dataframe()
    
    if analysis_type in ["quality", "all"]:
        # Check for data quality issues
        query = f"""
        SELECT COUNT(*) as total_rows,
               COUNT(DISTINCT id) as distinct_ids,
               COUNT(*) - COUNT(DISTINCT id) as duplicate_count
        FROM `{table_id}`
        """
        quality_results = client.query(query).to_dataframe()
    
    return {
        "distribution_analysis": distribution_results.to_dict() if "distribution" in analysis_type else None,
        "quality_analysis": quality_results.to_dict() if "quality" in analysis_type else None
    }

def generate_visualization(query_results: str, viz_type: str, title: str) -> go.Figure:
    """
    Generate appropriate visualizations based on data characteristics
    """
    df = pd.DataFrame(eval(query_results))
    
    if viz_type == "time_series":
        fig = px.line(df, x=df.columns[0], y=df.columns[1], title=title)
    elif viz_type == "distribution":
        fig = px.histogram(df, x=df.columns[0], title=title)
    elif viz_type == "correlation":
        fig = px.scatter(df, x=df.columns[0], y=df.columns[1], title=title)
    elif viz_type == "comparison":
        fig = px.bar(df, x=df.columns[0], y=df.columns[1], title=title)
    elif viz_type == "geographic":
        fig = px.scatter_mapbox(df, lat='latitude', lon='longitude', title=title)
    
    return fig

def check_data_quality(table_id: str, columns: str) -> Dict[str, Any]:
    """
    Perform comprehensive data quality checks
    """
    client = bigquery.Client(credentials=credentials)
    columns_list = [col.strip() for col in columns.split(",")]
    
    quality_checks = {}
    for column in columns_list:
        query = f"""
        SELECT
            COUNT(*) as total_rows,
            COUNT(DISTINCT {column}) as unique_values,
            COUNT(CASE WHEN {column} IS NULL THEN 1 END) as null_count,
            COUNT(CASE WHEN TRIM({column}) = '' THEN 1 END) as empty_string_count
        FROM `{table_id}`
        """
        results = client.query(query).to_dataframe()
        quality_checks[column] = results.to_dict('records')[0]
    
    return quality_checks

# Update the existing sql_query_tool
sql_query_tool = Tool(
    function_declarations=[
        list_datasets_func,
        list_tables_func,
        get_table_func,
        sql_query_func,
        analyze_table_func,
        generate_visualization_func,
        data_quality_check_func
    ]
)

# Helper function to determine appropriate visualization type
def determine_visualization_type(query_results: List[Dict[str, Any]]) -> str:
    df = pd.DataFrame(query_results)
    
    # Check for temporal data
    temporal_columns = df.select_dtypes(include=['datetime64']).columns
    if len(temporal_columns) > 0:
        return "time_series"
    
    # Check for geographic data
    if all(col in df.columns for col in ['latitude', 'longitude']):
        return "geographic"
    
    # Check for categorical vs numerical data
    numerical_columns = df.select_dtypes(include=[np.number]).columns
    if len(numerical_columns) >= 2:
        return "correlation"
    elif len(numerical_columns) == 1:
        return "distribution"
    
    return "comparison"

# sql_query_tool = Tool(
#     function_declarations=[
#         list_datasets_func,
#         list_tables_func,
#         get_table_func,
#         sql_query_func,
#     ],
# )

model = GenerativeModel(
    "gemini-1.5-pro",
    generation_config={"temperature": 0},
    tools=[sql_query_tool],
)


st.set_page_config(
    page_title="Query Connect",
    layout="wide",
)

# Title section
col1, col2 = st.columns([8, 1])
with col1:
    st.title("Query Connect")

# New Dataset Introduction Section
st.markdown("### 📊 TheLook Ecommerce Dataset Explorer")

tab1, tab2, tab3 = st.tabs(["Dataset Overview", "Available Data", "Example Questions"])

with tab1:
    st.markdown("""
        Explore a comprehensive ecommerce dataset containing detailed information about:
        
        | Category | Description |
        |----------|-------------|
        | 🛍️ Products & Orders | Complete order history and product details |
        | 👥 Customer Behavior | Customer demographics and purchase patterns |
        | 📈 Sales Metrics | Revenue, margins, and performance indicators |
        | 🔍 Product Analytics | Categories, attributes, and inventory data |
    """)

with tab2:
    st.markdown("#### Key Tables Available")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
            **Core Business Data**
            - `orders`: Order details, timestamps, and status
            - `order_items`: Individual items in each order
            - `products`: Product catalog with categories and costs
        """)
    
    with col2:
        st.markdown("""
            **Supporting Data**
            - `users`: Customer demographics and details
            - `inventory_items`: Stock levels and distribution
            - `distribution_centers`: Warehouse locations
        """)

with tab3:
    st.markdown("#### Try These Example Questions")
    
    with st.expander("📊 Business Performance", expanded=True):
        st.markdown("""
            - What were our top-selling products last month?
            - How does revenue compare across different product categories?
            - What's our average order value trend over time?
        """)
    
    with st.expander("👥 Customer Insights", expanded=True):
        st.markdown("""
            - What's the average customer lifetime value by region?
            - Which customer segments have the highest repeat purchase rate?
            - How does customer age affect product preferences?
        """)
    
    with st.expander("📦 Operations & Inventory", expanded=True):
        st.markdown("""
            - Which distribution centers are running low on stock?
            - What's our average processing time for orders?
            - Which products have the highest return rates?
        """)

# Add a visual separator
st.markdown("---")

# Chat interface
if "messages" not in st.session_state:
    st.session_state.messages = []

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"].replace("$", r"\$"))  # noqa: W605
        try:
            with st.expander("Function calls, parameters, and responses"):
                st.markdown(message["backend_details"])
        except KeyError:
            pass

if prompt := st.chat_input("Ask me about information in the database..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        full_response = ""
        chat = model.start_chat()
        client = bigquery.Client(credentials=credentials)

        prompt += """
            Please give a concise, high-level summary followed by detail in
            plain language about where the information in your response is
            coming from in the database. Only use information that you learn
            from BigQuery, do not make up information.
            """

        try:
            response = chat.send_message(prompt)
            response = response.candidates[0].content.parts[0]

            print(response)

            api_requests_and_responses = []
            backend_details = ""

            function_calling_in_process = True
            while function_calling_in_process:
                try:
                    params = {}
                    for key, value in response.function_call.args.items():
                        params[key] = value

                    print(response.function_call.name)
                    print(params)

                    if response.function_call.name == "list_datasets":
                        api_response = client.list_datasets()
                        api_response = BIGQUERY_DATASET_ID
                        api_requests_and_responses.append(
                            [response.function_call.name, params, api_response]
                        )

                    if response.function_call.name == "list_tables":
                        api_response = client.list_tables(params["dataset_id"])
                        api_response = str([table.table_id for table in api_response])
                        api_requests_and_responses.append(
                            [response.function_call.name, params, api_response]
                        )

                    if response.function_call.name == "get_table":
                        api_response = client.get_table(params["table_id"])
                        api_response = api_response.to_api_repr()
                        api_requests_and_responses.append(
                            [
                                response.function_call.name,
                                params,
                                [
                                    str(api_response.get("description", "")),
                                    str(
                                        [
                                            column["name"]
                                            for column in api_response["schema"][
                                                "fields"
                                            ]
                                        ]
                                    ),
                                ],
                            ]
                        )
                        api_response = str(api_response)

                    if response.function_call.name == "sql_query":
                        job_config = bigquery.QueryJobConfig(
                            maximum_bytes_billed=100000000
                        )  # Data limit per query job
                        try:
                            cleaned_query = (
                                params["query"]
                                .replace("\\n", " ")
                                .replace("\n", "")
                                .replace("\\", "")
                            )
                            query_job = client.query(
                                cleaned_query, job_config=job_config
                            )
                            api_response = query_job.result()
                            api_response = str([dict(row) for row in api_response])
                            api_response = api_response.replace("\\", "").replace(
                                "\n", ""
                            )
                            api_requests_and_responses.append(
                                [response.function_call.name, params, api_response]
                            )
                        except Exception as e:
                            error_message = f"""
                            We're having trouble running this SQL query. This
                            could be due to an invalid query or the structure of
                            the data. Try rephrasing your question to help the
                            model generate a valid query. Details:

                            {str(e)}"""
                            st.error(error_message)
                            api_response = error_message
                            api_requests_and_responses.append(
                                [response.function_call.name, params, api_response]
                            )
                            st.session_state.messages.append(
                                {
                                    "role": "assistant",
                                    "content": error_message,
                                }
                            )

                    print(api_response)

                    response = chat.send_message(
                        Part.from_function_response(
                            name=response.function_call.name,
                            response={
                                "content": api_response,
                            },
                        ),
                    )
                    response = response.candidates[0].content.parts[0]

                    backend_details += "- Function call:\n"
                    backend_details += (
                        "   - Function name: ```"
                        + str(api_requests_and_responses[-1][0])
                        + "```"
                    )
                    backend_details += "\n\n"
                    backend_details += (
                        "   - Function parameters: ```"
                        + str(api_requests_and_responses[-1][1])
                        + "```"
                    )
                    backend_details += "\n\n"
                    backend_details += (
                        "   - API response: ```"
                        + str(api_requests_and_responses[-1][2])
                        + "```"
                    )
                    backend_details += "\n\n"
                    with message_placeholder.container():
                        st.markdown(backend_details)

                except AttributeError:
                    function_calling_in_process = False

            time.sleep(3)

            full_response = response.text
            with message_placeholder.container():
                st.markdown(full_response.replace("$", r"\$"))  # noqa: W605
                with st.expander("Function calls, parameters, and responses:"):
                    st.markdown(backend_details)

            st.session_state.messages.append(
                {
                    "role": "assistant",
                    "content": full_response,
                    "backend_details": backend_details,
                }
            )
        except Exception as e:
            print(e)
            error_message = f"""
                Something went wrong! We encountered an unexpected error while
                trying to process your request. Please try rephrasing your
                question. Details:

                {str(e)}"""
            st.error(error_message)
            st.session_state.messages.append(
                {
                    "role": "assistant",
                    "content": error_message,
                }
            )
