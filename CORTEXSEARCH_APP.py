import json
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session
import _snowflake
import time
import requests
from datetime import datetime
from typing import Any, Dict, List, Optional

# Read configuration from Streamlit secrets
DATABASE = "CORTEX_DB"
SCHEMA = "CORTEX_SCHEMA"
service_name = "PDF_CHUNK_SERVICE"
table_name = "CHUNK_CORTEX_PDF"


def get_cortex_search_attributes(session, service_name):
    services_query = f"SHOW CORTEX SEARCH SERVICES LIKE '{service_name}';"
    services_result = session.sql(services_query).collect()
    services_df = pd.DataFrame(services_result)

    if not services_df.empty and 'name' in services_df.columns and 'attribute_columns' in services_df.columns:
        service_row = services_df[services_df['name'] == service_name]
        if not service_row.empty:
            attribute_str = service_row.iloc[0]['attribute_columns']
            attributes = [attr.strip() for attr in attribute_str.split(',')]
            
            # Exclude 'CHUNK' from the attributes list
            attributes = [attr for attr in attributes if attr.upper() != 'CHUNK']
            return attributes
    return []


# Function to get available columns from the Snowflake table
def get_available_columns(session, table_name):
    column_query = f"""
    SELECT COLUMN_NAME
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_NAME = '{table_name}'
    ORDER BY ORDINAL_POSITION;
    """
    columns_df = session.sql(column_query).to_pandas()
    return columns_df['COLUMN_NAME'].tolist()

def run_cortex_search(session, database_name, schema_name, service_name, query, columns, filter_condition, limit):
    # If no filter_condition is provided, exclude the filter from the query
    if filter_condition:
        filter_str = f'"filter": {filter_condition},'
    else:
        filter_str = ""

    search_query = f"""
    SELECT PARSE_JSON(
        SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            '{database_name}.{schema_name}.{service_name}',
            '{{ 
               "query": "{query}",
               "columns": {columns},
               {filter_str}
               "limit": {limit}
            }}'
        )
    )['results'] as results;
    """

    result_df = session.sql(search_query).to_pandas()
    if not result_df.empty:
        results_json = json.loads(result_df.iloc[0]['RESULTS'])
        structured_results = pd.DataFrame(results_json)
    else:
        structured_results = pd.DataFrame(columns=json.loads(columns))
    return structured_results

def display_cortex_search(session):
    # Get available columns and attributes dynamically
    available_columns = get_available_columns(session, table_name)
    attributes = get_cortex_search_attributes(session, service_name)

    # Sidebar for inputs
    st.sidebar.header("🔍 Search Settings")
    query = st.sidebar.text_input("Enter your search query:")
    selected_columns = st.sidebar.multiselect("Select columns to retrieve:", available_columns, default=[])
    columns = json.dumps(selected_columns)

    # Optionally, select filter column and value
    use_filter = st.sidebar.checkbox("Apply Filter?")
    filter_condition = None
    if use_filter:
        filter_column = st.sidebar.selectbox("Select a filter column:", attributes)
        filter_value = st.sidebar.text_input(f"Enter filter value for {filter_column}:")
        if filter_column and filter_value:
            filter_condition = json.dumps({"@eq": {filter_column: filter_value}})
    
    limit = st.sidebar.number_input("Enter the limit for search results:", min_value=1, value=10)

    # Button to trigger the Cortex Search
    if st.sidebar.button("🚀 Run Search"):
        # Validate required fields: query and selected columns
        if not query:
            st.warning("⚠️ Please enter a search query.")
        elif not selected_columns:
            st.warning("⚠️ Please select at least one column to retrieve.")
        # Validate the filter fields if the filter is applied
        elif use_filter and (not filter_column or not filter_value):
            st.warning("⚠️ Please select a filter column and provide a filter value.")
        else:
            with st.spinner("Running Cortex search..."):
                results = run_cortex_search(session, DATABASE, SCHEMA, service_name, query, columns, filter_condition, limit)
                if not results.empty:
                    st.success(f"✅ Search completed successfully! Found {len(results)} results.")
                    
                    # Display results in list format
                    st.markdown("### Search Results:")

                    for index, row in results.iterrows():
                        for column in selected_columns:
                            # Dynamically display the column name and its value
                            st.markdown(f"- **{column}**: {row.get(column, 'N/A')}")
                        st.markdown("---")  # Add a separator between results for better readability

                else:
                    st.warning("⚠️ No results found.")

    # Collapsible section for showing the raw SQL query
    with st.expander("🛠️ Show SQL Query"):
        filter_str = f'"filter": {filter_condition},' if filter_condition else ""
        st.code(f"""
        SELECT PARSE_JSON(
            SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                '{DATABASE}.{SCHEMA}.{service_name}',
                '{{ 
                   "query": "{query}",
                   "columns": {columns},
                   {filter_str}
                   "limit": {limit}
                }}'
            )
        )['results'] as results;
        """, language="sql")



# Main function to handle Cortex Search
def main():
    # Start the Snowflake session
    session = get_active_session()

    # Display Cortex Search interface
    st.title("Cortex Search")
    display_cortex_search(session)

# Run the main function
if __name__ == "__main__":
    main()



