
import json
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
import _snowflake
import time
import requests
import matplotlib.pyplot as plt
import plotly.express as px
from datetime import datetime
from typing import Any, Dict, List, Optional
import yaml
import re
import tempfile
import streamlit.components.v1 as components


DATABASE = "CORTEX_DB"
SCHEMA = "CORTEX_SCHEMA"
STAGE = "CORTEX_ANALYST_STAGE"

def send_message(prompt: str, yaml_file: str) -> dict:
    """Calls the REST API with the selected YAML file and returns the response."""
    request_body = {
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": prompt
                    }
                ]
            }
        ],
        "semantic_model_file": f"@{DATABASE}.{SCHEMA}.{STAGE}/{yaml_file}",
    }
    resp = _snowflake.send_snow_api_request(
        "POST",
        f"/api/v2/cortex/analyst/message",
        {},
        {},
        request_body,
        {},
        30000,
    )
    if resp["status"] < 400:
        return json.loads(resp["content"])
    else:
        raise Exception(
            f"Failed request with status {resp['status']}: {resp}"
        )



def check_mixed_types(df: pd.DataFrame) -> bool:
    """Check for mixed types in the DataFrame columns and return True if any are found."""
    for col in df.columns:
        if df[col].apply(type).nunique() > 1:
            return True
    return False

# Store the last chart type for each question globally
last_suggested_charts = {}

def suggest_chart_type(df: pd.DataFrame, question: str) -> Dict[str, str]:
    """Suggest chart types based on the data types available in the DataFrame."""
    chart_options = {}
    
    # Determine column types
    numeric_columns = df.select_dtypes(include=['number']).columns.tolist()
    categorical_columns = df.select_dtypes(include=['object']).columns.tolist()
    date_columns = df.select_dtypes(include=['datetime64']).columns.tolist()

    # Create a unique key for the question and numeric columns presence
    key = f"{question}_{'numeric' if numeric_columns else 'categorical' if categorical_columns else 'others'}"
    
    # Initialize the last suggested chart for the question if not present
    if key not in last_suggested_charts:
        last_suggested_charts[key] = -1  # Start from -1 to get the first chart

    last_index = last_suggested_charts[key]

    # Define chart options based on column types
    if len(df.columns) == 1:
        chart_options = {"Table View 📋": "Table"}
    elif len(df.columns) == 3 and "START_DATE" in df.columns and "END_DATE" in df.columns:
        chart_options = {"Bar Chart 📊": "bar"}
    else:
        if numeric_columns and categorical_columns:
            chart_options = {
                "Bar Chart 📊": "bar",
                "Line Chart 📈": "line",
                "Area Chart 🌐": "area",
                "Pie Chart 🥧": "pie",
                "Box Plot 📦": "box",
                "Scatter Plot 🔵": "scatter",
                "Violin Plot 🎻": "violin",
                "Density Heatmap 🌡️": "density_heatmap",
                "Funnel Plot 🚥": "funnel",
            }
        elif numeric_columns:
            chart_options = {
                "Line Chart 📈": "line",
                "Area Chart 🌐": "area",
                "Histogram 📊": "histogram",
                "Box Plot 📦": "box",
                "Violin Plot 🎻": "violin",
            }
        elif categorical_columns or date_columns:
            chart_options = {
                "Pie Chart 🥧": "pie",
                "Bar Chart 📊": "bar"
            }
        else:
            chart_options = {"Table View 📋": "table"}

    # Rotate through the available charts
    available_charts = list(chart_options.values())
    
    if available_charts:
        # Increment index for the next suggestion
        last_index = (last_index + 1) % len(available_charts)
        next_chart = available_charts[last_index]
        
        # Suggest the next chart
        suggested_chart = {next_chart.title(): next_chart}  # Capitalize the chart name for display
        
        # Update the last suggested chart for this question
        last_suggested_charts[key] = last_index
        
        return suggested_chart
    else:
        return chart_options


def rearrange_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rearrange START_DATE and END_DATE columns and ensure no commas in year display."""
    date_columns = ["START_DATE", "END_DATE"]
    
    # Convert the date columns to integers or strings to remove commas
    for col in date_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)  # Ensure it's treated as a string, preventing formatting with commas
    
    other_columns = [col for col in df.columns if col not in date_columns]
    rearranged_columns = other_columns + [col for col in date_columns if col in df.columns]
    return df[rearranged_columns]

def render_charts_based_on_selection(df: pd.DataFrame, selected_chart: str, chart_options: dict, chart_key: str) -> None:
    """Render the appropriate chart based on the user's selection."""
    numeric_columns = df.select_dtypes(include=['number']).columns.tolist()
    categorical_columns = df.select_dtypes(include=['object']).columns.tolist()

    if numeric_columns and categorical_columns:
        x_column = categorical_columns[0]
        y_column = numeric_columns[0]
        if chart_options[selected_chart] == "bar":
            st.bar_chart(df.set_index(x_column)[y_column])  # Removed 'key'
        elif chart_options[selected_chart] == "line":
            st.line_chart(df.set_index(x_column)[y_column])  # Removed 'key'
        elif chart_options[selected_chart] == "area":
            st.area_chart(df.set_index(x_column)[y_column])  # Removed 'key'
        elif chart_options[selected_chart] == "pie":
            fig = px.pie(df, names=x_column, values=y_column)
            st.plotly_chart(fig, key=f"{chart_key}_pie")
        elif chart_options[selected_chart] == "box":
            fig = px.box(df, x=x_column, y=y_column)
            st.plotly_chart(fig, key=f"{chart_key}_box")
        elif chart_options[selected_chart] == "scatter":
            fig = px.scatter(df, x=x_column, y=y_column)
            st.plotly_chart(fig, key=f"{chart_key}_scatter")
        elif chart_options[selected_chart] == "violin":
            fig = px.violin(df, x=x_column, y=y_column)
            st.plotly_chart(fig, key=f"{chart_key}_violin")
        elif chart_options[selected_chart] == "density_heatmap":
            fig = px.density_heatmap(df, x=x_column, y=y_column)
            st.plotly_chart(fig, key=f"{chart_key}_density_heatmap")
        else:
            st.warning(f"{selected_chart} not suitable for this data. Please choose another chart type.")
    elif numeric_columns:
        y_column = numeric_columns[0]
        if chart_options[selected_chart] == "line":
            st.line_chart(df[y_column])  # Removed 'key'
        elif chart_options[selected_chart] == "area":
            st.area_chart(df[y_column])  # Removed 'key'
        elif chart_options[selected_chart] == "box":
            fig = px.box(df, y=y_column)
            st.plotly_chart(fig, key=f"{chart_key}_box_numeric")
        elif chart_options[selected_chart] == "scatter":
            fig = px.scatter(df, y=y_column)
            st.plotly_chart(fig, key=f"{chart_key}_scatter_numeric")
        elif chart_options[selected_chart] == "violin":
            fig = px.violin(df, y=y_column)
            st.plotly_chart(fig, key=f"{chart_key}_violin_numeric")
        elif chart_options[selected_chart] == "density_heatmap":
            fig = px.density_heatmap(df, y=y_column)
            st.plotly_chart(fig, key=f"{chart_key}_density_heatmap_numeric")
        else:
            st.warning(f"{selected_chart} not suitable for this data. Please choose another chart type.")
    elif categorical_columns:
        x_column = categorical_columns[0]
        if chart_options[selected_chart] == "bar":
            st.bar_chart(df[x_column])  # Removed 'key'
        elif chart_options[selected_chart] == "pie":
            fig = px.pie(df, names=x_column)
            st.plotly_chart(fig, key=f"{chart_key}_pie_categorical")
        else:
            st.warning(f"{selected_chart} charts require hierarchical data with more than one categorical column. Please choose another chart type.")
    else:
        st.warning("No suitable data available for charting.")


def format_message(role: str, content: str) -> str:
    """Format message with a timestamp for better chat experience."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    formatted_message = f"**{role.capitalize()} [{timestamp}]:** {content}"
    return formatted_message



def load_yaml_files_from_stage() -> Dict[str, Dict]:
    """
    Load and parse all YAML files from the Snowflake stage.
    Returns a dictionary where keys are file names and values are parsed YAML content.
    """
    session = get_active_session()
    query = f"LIST @{DATABASE}.{SCHEMA}.{STAGE}"
    result = session.sql(query).collect()

    # List YAML files in the stage
    yaml_files = [row['name'] for row in result if row['name'].endswith('.yaml')]
    # st.write("YAML Files in Stage:", yaml_files)

    parsed_yaml = {}

    # Use a temporary directory for downloading files
    with tempfile.TemporaryDirectory() as temp_dir:
        for file_path in yaml_files:
            file_name = file_path.split('/')[-1]  # Extract the file name
            #st.write(f"Processing: {file_name}")
            try:
                # Download the YAML file to the temporary directory
                downloaded_files = session.file.get(file_path, target_directory=temp_dir)

                # Construct the local file path
                local_path = f"{temp_dir}/{file_name}"

                # Read and parse the YAML file
                with open(local_path, 'r') as yaml_file:
                    parsed_yaml[file_name] = yaml.safe_load(yaml_file)
            except Exception as e:
                st.error(f"Error parsing {file_name}: {e}")

    return parsed_yaml


def process_message(prompt: str, yaml_file_options: List[str]) -> None:
    if "temp_selected_yaml" not in st.session_state:
        st.session_state.temp_selected_yaml = yaml_file_options[0]  # Default to the first YAML file

    # Process the YAML query and retrieve results
    yaml_query_result = process_yaml_query(
        question=prompt,
        yaml_file_options=yaml_file_options
    )

    
    # Add user message to chat
    st.session_state.messages.append({"role": "user", "content": [{"type": "text", "text": prompt}]})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Dynamically suggest a YAML file based on the question
    if yaml_query_result:
        suggested_yaml = yaml_query_result[0]  # Pick the first suggestion

        # Display the popup if the suggested YAML differs from the current one
        if suggested_yaml != st.session_state.temp_selected_yaml:
            suggested_yaml_display = suggested_yaml
            st.markdown(
                f"""
                <div style="padding: 20px; background-color: #FFFBEA; border: 1px solid #F5C518; border-radius: 5px; margin-bottom: 15px;">
                    <p style="margin: 0; font-size: 16px; font-weight: bold;">
                        Suggested YAML for this query: <span style="color: #3366CC;">{suggested_yaml_display}</span>.
                    </p>
                    <p style="margin: 5px 0 15px; font-size: 14px;">
                        The current file <span style="color: #FF6347;">{st.session_state.temp_selected_yaml}</span> may not fully support your query.
                    </p>
                </div>
                """,
                unsafe_allow_html=True,
            )

            # Update temp_selected_yaml to reflect the new YAML file
            st.session_state.temp_selected_yaml = suggested_yaml

            # Notify the user about the YAML change
            st.toast(
                f"YAML file automatically updated based on the suggestion! Changed to: {suggested_yaml}",
                icon="✅"
            )


    # Display assistant's response
    with st.chat_message("assistant"):
        with st.spinner("Generating response..."):
            try:
                response = send_message(prompt=prompt, yaml_file=st.session_state.temp_selected_yaml)
                content = response["message"]["content"]
                display_content(content=content)
                st.session_state.messages.append({"role": "assistant", "content": content})
            except Exception as e:
                st.error(f"Error processing the message: {str(e)}")


def display_content(content: list, message_index: int = None, user_question: str = "") -> None:
    """Displays a content item for a message."""
    message_index = message_index or len(st.session_state.messages)
    for item in content:
        if item["type"] == "text":
            st.markdown(item["text"])
        elif item["type"] == "suggestions":
            with st.expander("Suggestions", expanded=True):
                for suggestion_index, suggestion in enumerate(item["suggestions"]):
                    if st.button(suggestion, key=f"{message_index}_{suggestion_index}"):
                        st.session_state.active_suggestion = suggestion
        elif item["type"] == "sql":
            with st.expander("SQL Query", expanded=False):
                st.code(item["statement"], language="sql")
            with st.expander("Results", expanded=True):          
                with st.spinner("Running SQL..."):
                    session = get_active_session()
                    df = session.sql(item["statement"]).to_pandas()

                    if not df.empty:
                        # Rearrange columns for better display
                        df = rearrange_columns(df)

                        # Generate and display the summary using Cortex
                        with st.spinner("Generating summary..."):
                            summary = generate_summary_using_cortex(
                                query_result_df=df,
                                user_question=user_question,  # Pass dynamically
                                sql_query=item["statement"]  # The SQL query from the content
                            )
                            # st.markdown(summary)
                            # st.markdown("Brief Summary")
                            st.markdown(
                            f"""
                            <div style="background-color: #f9f9f9; padding: 10px; border-radius: 8px; border: 1px solid #ddd;">
                                <p style="font-size: 18px; line-height: 1.8; color: #333;">
                                    {summary.replace("highest", "<b>highest</b>").replace("lowest", "<b>lowest</b>").replace("trend", "<b>trend</b>")}
                                </p>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )
                
                        # Check for valid columns for charting
                        numeric_columns = df.select_dtypes(include=['number']).columns.tolist()
                        categorical_columns = df.select_dtypes(include=['object']).columns.tolist()
                        valid_for_chart = bool(numeric_columns and categorical_columns)
    
                        # Dynamically adjust view options based on data suitability
                        view_options = ["Table View"]
                        if valid_for_chart:
                            view_options.append("Chart View")
    
                        # Set default index for the view
                        default_index = 1 if valid_for_chart else 0  # Default to Chart View if available, else Table View
    
                        # User selects view type
                        view_type = st.selectbox(
                            "Select View Type",
                            options=view_options,
                            index=default_index,  # Default to Chart View
                            key=f"view_type_{message_index}"
                        )
    
                        # Display content based on view type
                        if view_type == "Table View":
                            st.dataframe(df)
                        elif view_type == "Chart View":
                            question = "Show chart options for the SQL results."
                            chart_options = suggest_chart_type(df, question)

                            # Handle the chart type selection and rendering logic
                            selected_chart = st.selectbox(
                                "Choose chart type:",
                                list(chart_options.keys()),
                                key=f"chart_select_{message_index}"
                            )
    
                            # Centralized chart rendering logic
                            render_charts_based_on_selection(
                                df,
                                selected_chart,
                                chart_options,
                                chart_key=f"chart_{message_index}"
                            )
                    else:
                        st.warning("The query returned an empty result set.")



def generate_summary_using_cortex(query_result_df, user_question: str, sql_query: str):
    """
    Dynamically generates a summary based on the user's question and SQL query.
    """
    try:
        session = get_active_session()

        # Convert DataFrame to JSON for prompt
        json_data = query_result_df.to_json(orient="records")

        # Create the dynamic prompt
        final_prompt = f"""
        You are a healthcare domain expert and business analyst. Analyze the provided dataset and deliver concise, actionable insights tailored for C-level executives. 
        Focus on quantifiable results and key highlights only. Avoid unnecessary explanations or context—they have access to the dataset. 
        This dataset originates from a large claims data warehouse, filtered by an SQL query executed based on the LLM prompt: '{user_question}'
        SQL Query:
        {sql_query}


        
        Data: {json_data}
        
        Provide a single-paragraph summary with measurable outcomes.
        """

        # Execute Cortex Complete with the prompt
        cortex_query = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            'snowflake-arctic',
            '{final_prompt.replace("'", "''")}'
        ) AS summary;
        """
        result = session.sql(cortex_query).collect()
        summary = result[0]["SUMMARY"] if result else "No summary generated."
        return summary

    except Exception as e:
        return f"Error generating summary: {e}"


def process_yaml_query(question: str, yaml_file_options: List[str]) -> List[str]:
    session = get_active_session()
    try:
        # Format YAML options as a comma-separated string
        yaml_options = ", ".join(yaml_file_options)

        # Create the prompt
        prompt = f'Based on the question "{question}", choose the best YAML file from the following options: {yaml_options}.'
        
        # SQL query to execute SNOWFLAKE.CORTEX.COMPLETE
        sql_query = f"""
        WITH response_data AS (
            SELECT SNOWFLAKE.CORTEX.COMPLETE(
                'snowflake-arctic',
                '{prompt.replace("'", "''")}'
            ) AS response
        ),
        extracted_text AS (
            SELECT response::string AS full_response
            FROM response_data
        ),
        yaml_file_extracted AS (
            SELECT
                REGEXP_SUBSTR(full_response, '\\\\b\\\\w+\\\\.yaml\\\\b') AS yaml_file
            FROM extracted_text
        )
        SELECT yaml_file
        FROM yaml_file_extracted;
        """

        # Execute the query and fetch the result
        result = session.sql(sql_query).collect()
        yaml_files = [row["YAML_FILE"] for row in result if row["YAML_FILE"]]


        return yaml_files

    except Exception as e:
        st.error(f"Error processing YAML query: {e}")
        return []


    
def list_yaml_files_from_stage() -> list:
    """List all available YAML files from the Snowflake stage."""
    session = get_active_session()
    query = f"LIST @{DATABASE}.{SCHEMA}.{STAGE}"
    result = session.sql(query).collect()
    
    # Extract YAML file names from the result
    yaml_files = [row['name'].split('/')[-1] for row in result if row['name'].endswith('.yaml')]
    return yaml_files


def main():
    st.markdown(
        """
        <div style="background-color:#004080;padding:10px;border-radius:4px;">
            <h1 style="color:white;text-align:center;">Cortex Analyst Chat</h1>
        </div>
        """,
        unsafe_allow_html=True
    )

    # Initialize session state variables
    if "messages" not in st.session_state:
        st.session_state.messages = []
        st.session_state.suggestions = []
        st.session_state.active_suggestion = None

    if "temp_selected_yaml" not in st.session_state:
        st.session_state.temp_selected_yaml = None

    # Load YAML files from stage
    yaml_files = list_yaml_files_from_stage()
    if not yaml_files:
        st.error("No YAML files found in the stage. Please upload YAML files.")
        return

    # Ensure temp_selected_yaml is valid
    if st.session_state.temp_selected_yaml not in yaml_files or st.session_state.temp_selected_yaml is None:
        st.session_state.temp_selected_yaml = yaml_files[0]  # Default to the first YAML file

    # Sidebar: YAML File Selection
    with st.sidebar:
        st.title("⚙️ Settings")
        st.markdown("Configure your YAML file and application preferences below.")

        # Reflect temp_selected_yaml in the dropdown
        selected_yaml = st.selectbox(
            "Select YAML File",
            options=yaml_files,
            index=yaml_files.index(st.session_state.temp_selected_yaml),  # Reflect temp_selected_yaml dynamically
            key="selected_yaml_file"
        )

        # If the user manually changes the selection
        if selected_yaml != st.session_state.temp_selected_yaml:
            st.session_state.temp_selected_yaml = selected_yaml

        st.markdown(f"**Using YAML File:** `{st.session_state.temp_selected_yaml}`")

    # Display chat messages
    for message_index, message in enumerate(st.session_state.messages):
        with st.chat_message(message["role"]):
            display_content(content=message["content"], message_index=message_index)

    # Handle user input
    if user_input := st.chat_input("What is your question?"):
        process_message(prompt=user_input, yaml_file_options=yaml_files)

    # Handle suggestions if active
    if st.session_state.active_suggestion:
        process_message(prompt=st.session_state.active_suggestion, yaml_file_options=yaml_files)
        st.session_state.active_suggestion = None

if __name__ == "__main__":
    main()
