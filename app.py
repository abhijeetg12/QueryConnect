import time
import os 
import json 
from google.cloud import bigquery
import streamlit as st
from vertexai.generative_models import FunctionDeclaration, GenerativeModel, Part, Tool
import vertexai
from vertexai import init

# GOOGLE_APPLICATION_CREDENTIALS = """{type = "service_account", project_id  = "formal-airway-439707-t6", private_key_id = "0f8f4c30769cee4b05632336e47397457fa75976",private_key = "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDsd4tm9F8SYZD/\n8BMdzshnNd95dwCxM8EAqWZUdPmvhk0iCY6hUKkEwyi4B+f6dDS+ec0VymjlBPa/\nfZH4qbktAYJnRZxOla8uz9WeGVWRqRUE1MbPbUBn5F2KhHVLuhzpKdv0hRkHctQ+\naXEdsj+HHALDCP8tT5FYnJLqWhCqD6sZJI/wL+1SOUiB4eOsHStBIFNthd7JPAYN\nIQpC2zZB7k9oqaZRemrhsbebqC/fY8TTnd5FY++qYZjqB2oGNdeWeGvCgUrdZjRT\neuQ9eXZtxkmQanwsCXZ6fyXcGa0OUydv0zqMzK9zPz4pijd7hy2rO3xIfS4xlYzw\nSYoDNg6BAgMBAAECggEAYgaso2FqiBQqJ+89/X1bVm3e1luezdbGi5+t7BUR7NGf\n6BxOJFFrv62nk6KzaAAEXXHgssfV9Bq6r2c+u/af7ShTBry0r18d4CoIRCH8dwXA\n0N/kCtkfefIRVPrUJTBiC8ZuiE8ksRHJKpZLbiQWccwK2Q9BuWbiufkubjgn8FcC\neNt3zD7zHmKkGB/n4IOo3jTdiT3YuItTgmMAZ4YR2yUJNSy6kesy/bao0LkXz3XJ\nrJ7YihebQrj9cLgEhHJM92jfXciKdMY4qOh56WsENgh7r4DJc900EEENGR1ZP2Jo\nha6y8ahc1sClXiMyu++H1WynyramhSjEjfLOa+MigQKBgQD2Ds3wCZCMXn3Hkuts\nvh+fqhz/jZE6ymsT3uJUajc5WGj3yFpKezbAlx4nshfJxysi7DHs5fxqbghugSN8\n0qA3teO/seX4hB3LF57ZgXDuKYXPxJw4omWMcDnym3l4+wFT5pDxQTJAjwIxKNku\nrr8zI24qxEWBZ9tJXZ6Xl9sXWwKBgQD2BYiFmE/HzKE/P7xE3xtO9uvZK2HGwxMr\nV6TnMrzGoAh3pKBSgJM5IhSOVcu4VQCcwVkzBbKNIQUJc4sTh38lqmaf8N8Wq77J\nR5vKCcgtfu5K1S+5ixXRfUGbVe+id3gL2KT03PxLUbfPgHXGeKHzPz3Q2A7OTCWE\nfVcnzX00UwKBgD1QhRral02TQk6YGthXLDQyRNWdpmH7DOG/ubCFY0uD72xHXdCP\nkZ4+SgJkS685VVN3fh1lVhgDYVCAF6LELa6UQbOEFiVubqosMaZLriN672BNwwwN\n07ZCRP5ipcty6OrKWrXzpB0YRdiQMEaEvxp0KsC3dgaAJdHLZXirG6pfAoGAcMxY\nq+gJrDHGPJmcWHdyreHIgOnDCr6mK1kj4l0A8JCvUSvJ1gnddnSJuDjeDsXqYfTE\nUQBrvQlGpe6z9WBKi5p1Mx/dKSfVcbPDWf7iKPnph6X3G7sJZeNoETB1jqf4AnZy\nK1Km2mG9RQZA+Z6VolF8piRppWyERxqwuy8cGMECgYAERmStdsvedSFpWzgaphEP\nuLo1shgzqAT1jLjSoO4M1wJzud586fcAxb5Ui3rOle0RHKcI5LhgYZ/F9qxKfA4R\nL2aoPWSyJ3C9nt5hHC8Bjb7Zb0Sq29bjv1I8RDW3ap+uL1kEZiTegIQpxAXFunQI\n0vnTTbXPGJUcWcaOPu/JdA==\n-----END PRIVATE KEY-----\n",client_email = "streamlit-access@formal-airway-439707-t6.iam.gserviceaccount.com",client_id = "102146647707152988018",auth_uri = "https://accounts.google.com/o/oauth2/auth",token_uri = "https://oauth2.googleapis.com/token",auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs",client_x509_cert_url= "https://www.googleapis.com/robot/v1/metadata/x509/streamlit-access%40formal-airway-439707-t6.iam.gserviceaccount.com",universe_domain = "googleapis.com"}"""

GOOGLE_APPLICATION_CREDENTIALS =json.loads(st.secrets["GOOGLE_APPLICATION_CREDENTIALS"]) # "bq-key.json"#
# GCP_PROJECT_ID = st.secrets["project-id"]
BIGQUERY_DATASET_ID = "thelook_ecommerce"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS


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
    description="Get information from data in BigQuery using SQL queries",
    parameters={
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "SQL query on a single line that will help give quantitative answers to the user's question when run on a BigQuery dataset and table. In the SQL query, always use the fully qualified dataset and table names.",
            }
        },
        "required": [
            "query",
        ],
    },
)

sql_query_tool = Tool(
    function_declarations=[
        list_datasets_func,
        list_tables_func,
        get_table_func,
        sql_query_func,
    ],
)

model = GenerativeModel(
    "gemini-1.5-pro",
    generation_config={"temperature": 0},
    tools=[sql_query_tool],
)

st.set_page_config(
    page_title="Query Connect",
    layout="wide",
)

col1, col2 = st.columns([8, 1])
with col1:
    st.title("Query Connect")

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
        client = bigquery.Client()

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