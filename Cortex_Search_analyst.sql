----------------------User Role Configuration for Secure Access----------------

-- Granting a role to a user
USE ROLE accountadmin;
select current_user;
GRANT ROLE accountadmin TO USER CORTEXSEARCHANALYST2025;

---------------------------Database and Schema Setup----------------------------

-- Creating a new database and schema
CREATE OR REPLACE DATABASE CORTEX_DB;
CREATE OR REPLACE SCHEMA CORTEX_SCHEMA;

--------------------------Warehouse Provisioning and Configuration--------------

-- Creating a virtual warehouse with specific configurations
CREATE OR REPLACE WAREHOUSE COMPUTE_WH 
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

------------------------------CORTEX SEARCH------------------------------------
---------------------------Stage Creation for Data Loading-----------------------

-- Creating an internal stage with encryption and directory support enabled for cortex_search
CREATE OR REPLACE STAGE CORTEX_SEARCH_STAGE
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

CREATE OR REPLACE STAGE CORTEX_ANALYST_STAGE
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');  

--------------Granting Privileges for Database ,Schema ,Warehouse ,Stage and Cortex Search Serivce Operations----------
-- Granting privileges to create databases and warehouses
GRANT CREATE DATABASE ON ACCOUNT TO ROLE accountadmin;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE accountadmin;

-- Grant Usage on Database
GRANT USAGE ON DATABASE CORTEX_DB TO ROLE ACCOUNTADMIN;

-- Grant Usage on Schema
GRANT USAGE ON SCHEMA CORTEX_DB.CORTEX_SCHEMA TO ROLE ACCOUNTADMIN;

-- Grant Usage on Warehouse
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ACCOUNTADMIN;

-- Grant Usage and Read Access on the Stage
GRANT READ ON STAGE CORTEX_SEARCH_STAGE TO ROLE ACCOUNTADMIN;

GRANT READ ON STAGE CORTEX_ANALYST_STAGE TO ROLE ACCOUNTADMIN;

-- Grant Usage on the Cortex Search Service (if needed)
GRANT USAGE ON CORTEX SEARCH SERVICE PDF_CHUNK_SERVICE TO ROLE ACCOUNTADMIN;


------------------------------CORTEX ANALTICS---------------------------

GRANT USAGE ON SCHEMA CORTEX_DB.CORTEX_SCHEMA  TO ROLE accountadmin;
GRANT USAGE ON DATABASE CORTEX_DB TO ROLE accountadmin;
GRANT SELECT ON ALL TABLES IN SCHEMA CORTEX_DB.CORTEX_SCHEMA  TO ROLE accountadmin;

----------------------------------------------------------------------------

-----------------------------------------------------------------------------------------
**********************************CORTEX SEARCH******************************************
-----------------------------------------------------------------------------------------

USE ROLE accountadmin;
USE WAREHOUSE compute_wh;
USE DATABASE CORTEX_DB;
USE SCHEMA CORTEX_SCHEMA;

----------------------------------------------------------------------------
-- Use Stage CORTEX_SEARCHSTAGE for Cortex Search

List @CORTEX_SEARCH_STAGE;
-----------------------------Create Funcation---------------------------------

CREATE OR REPLACE FUNCTION pdf_text_chunker(file_url STRING)
RETURNS TABLE (chunk VARCHAR)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
HANDLER = 'pdf_text_chunker'
PACKAGES = ('snowflake-snowpark-python', 'PyPDF2', 'langchain')
AS
$$
from snowflake.snowpark import types as T
from langchain.text_splitter import RecursiveCharacterTextSplitter
from snowflake.snowpark.files import SnowflakeFile
import PyPDF2, io
import logging

# Define the PDF text chunker class
class pdf_text_chunker:
    
    def read_pdf(self, file_url: str) -> str:
        logger = logging.getLogger("udf_logger")
        logger.info(f"Opening file {file_url}")
        
        # Open and read the PDF from the Snowflake stage
        with SnowflakeFile.open(file_url, 'rb') as f:
            buffer = io.BytesIO(f.read())
        
        # Use PyPDF2 to extract text from the PDF
        reader = PyPDF2.PdfReader(buffer)
        text = ""
        for page in reader.pages:
            try:
                text += page.extract_text().replace('\n', ' ').replace('\0', ' ')
            except:
                text = "Unable to Extract"
                logger.warning(f"Unable to extract from file {file_url}, page {page}")
        
        return text

    def process(self, file_url: str):
        # Extract text from the PDF file
        text = self.read_pdf(file_url)
        
        # Split the text into chunks using RecursiveCharacterTextSplitter
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=4000,  # Adjust chunk size as needed
            chunk_overlap=400,  # Some overlap for better context
            length_function=len
        )
        
        # Split the extracted text into chunks
        chunks = text_splitter.split_text(text)
        
        # Yield the chunks one by one
        for chunk in chunks:
            yield (chunk,)

$$;

---------------------Create PdfChunk Table------------------------------------------------

CREATE OR REPLACE TABLE CHUNK_CORTEX_PDF (
    AREA VARCHAR(16777216),
    RELATIVE_PATH VARCHAR(16777216), 
    SIZE NUMBER(38, 0), 
    FILE_URL VARCHAR(16777216), 
    CHUNK VARCHAR(16777216)
);

select * from CHUNK_CORTEX_PDF;

---------------------Insert PdfChunk Table------------------------------------------------

CREATE OR REPLACE PROCEDURE PROCESS_NEW_PDF_FILES()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'process_new_pdf_files'
AS
$$
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import json

def process_new_pdf_files(session: Session) -> str:
    # Query to find new files not already processed
    new_files_query = """
        SELECT dir.relative_path, dir.size, dir.file_url
        FROM DIRECTORY(@CORTEX_SEARCH_STAGE) AS dir
        LEFT JOIN CHUNK_CORTEX_PDF cp
        ON dir.relative_path = cp.relative_path
        WHERE cp.relative_path IS NULL
    """
    
    # Get new files as a dataframe
    new_files_df = session.sql(new_files_query)
    new_files = new_files_df.collect()
    
    for file in new_files:
        relative_path = file['RELATIVE_PATH']
        size = file['SIZE']
        file_url = file['FILE_URL']

        # Extract the name before the underscore
        area_name = relative_path.split('_')[0] if '_' in relative_path else 'Unknown'

        # Generate chunks and insert into CHUNK_CORTEX_PDF
        chunk_insert_query = f"""
            INSERT INTO CHUNK_CORTEX_PDF (relative_path, size, file_url, chunk, area)
            SELECT 
                '{relative_path}', 
                {size}, 
                '{file_url}', 
                func.chunk, 
                '{area_name}'
            FROM 
                TABLE(pdf_text_chunker(build_scoped_file_url(@CORTEX_SEARCH_STAGE, '{relative_path}'))) AS func
        """
        session.sql(chunk_insert_query).collect()
    
    return 'Processing completed!'
$$;


CALL PROCESS_NEW_PDF_FILES();

SELECT * FROM CHUNK_CORTEX_PDF;

------------------------Create Cortex Search Service -------------------------


CREATE OR REPLACE CORTEX SEARCH SERVICE CORTEX_DB.CORTEX_SCHEMA.PDF_CHUNK_SERVICE
  ON CHUNK
  ATTRIBUTES RELATIVE_PATH, FILE_URL , AREA , SIZE
  WAREHOUSE = compute_wh
  TARGET_LAG = '1 hour'
  EMBEDDING_MODEL = 'snowflake-arctic-embed-l-v2.0'
  AS (
    SELECT * FROM CHUNK_CORTEX_PDF
  );


SHOW CORTEX SEARCH SERVICES;

DESCRIBE CORTEX SEARCH SERVICE PDF_CHUNK_SERVICE;

--------------------------------------------------------------------------------------



----------------------------WITH FLITER TESTING QUERY---------------------------------

SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'CORTEX_DB.CORTEX_SCHEMA.PDF_CHUNK_SERVICE',
        '{
           "query": "explain cardiology",
           "columns": ["CHUNK", "AREA"],
           "filter": {"@eq": {"RELATIVE_PATH": "Cardiology_Medical_Insights_and_Overview.pdf"}},
           "limit": 5
        }'
    )
)['results'] as results;


---------------------------------WITHOUT FLITER TESTING QUERY----------------------------


SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'CORTEX_DB.CORTEX_SCHEMA.PDF_CHUNK_SERVICE',
        '{ 
           "query": "Explain Cardiology",
           "columns": ["CHUNK", "RELATIVE_PATH"],
           "limit": 7
        }'
    )
)['results'] as results;



-----------------------------------------------------------------------------------------
-- **********************************CORTEX ANALYST***************************************
-------------------------------------------------------------------------------------------

list @CORTEX_ANALYST_STAGE;

--------------------------------Create Table---------------------------------------------

USE ROLE accountadmin;
USE WAREHOUSE compute_wh;
USE DATABASE CORTEX_DB;
USE SCHEMA CORTEX_SCHEMA;

CREATE OR REPLACE TABLE CORTEX_DB.CORTEX_SCHEMA.CustomerDim (
    Customerid INT NOT NULL,
    Firstname VARCHAR(100),
    Lastname VARCHAR(100),
    Email VARCHAR(255),
    Phone VARCHAR(50),
    FOREIGN KEY (Customerid) REFERENCES CustomerFact(Customerid)
);


CREATE OR REPLACE TABLE CustomerFact (
    Customerid INT NOT NULL PRIMARY KEY,
    Signupdate VARCHAR(50)
);


CREATE OR REPLACE TABLE OrderFact (
    customerid INT NOT NULL,
    OrderCount FLOAT,
    OrderDate VARCHAR(50),
    PRIMARY KEY (customerid)
);

CREATE OR REPLACE TABLE RevenueFact (
    Customerid INT NOT NULL,
    Amount FLOAT,
    SaleDate VARCHAR(50),
    PRIMARY KEY (Customerid)
);


COPY INTO CORTEX_DB.CORTEX_SCHEMA.CustomerDim
FROM @CORTEX_ANALYST_STAGE/CustomerDim.csv
FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    FIELD_DELIMITER = ',',
    NULL_IF = ('')
)
ON_ERROR = CONTINUE;



COPY INTO CORTEX_DB.CORTEX_SCHEMA.CustomerFact
FROM @CORTEX_ANALYST_STAGE/CustomerFact.csv
FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    FIELD_DELIMITER = ',',
    NULL_IF = ('')
)
ON_ERROR = CONTINUE;

COPY INTO CORTEX_DB.CORTEX_SCHEMA.OrderFact
FROM @CORTEX_ANALYST_STAGE/OrderFact.csv
FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    FIELD_DELIMITER = ',',
    NULL_IF = ('')
)
ON_ERROR = CONTINUE;

COPY INTO CORTEX_DB.CORTEX_SCHEMA.RevenueFact
FROM @CORTEX_ANALYST_STAGE/RevenueFact.csv
FILE_FORMAT = (
    TYPE = 'CSV',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"',
    SKIP_HEADER = 1,
    FIELD_DELIMITER = ',',
    NULL_IF = ('')
)
ON_ERROR = CONTINUE;



SELECT * FROM CustomerDim;
SELECT * FROM CustomerFact;
SELECT * FROM OrderFact;
SELECT * FROM RevenueFact;
--------------------------------------------------------------------------------------


-----------------------------CORTEX.COMPLETE---------------------------------------------------------

SHOW MODELS IN SCHEMA CORTEX_DB.CORTEX_SCHEMA;

SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'snowflake-arctic',
    'Based on the question "Retrieve all transactions with an amount of 300.0?", choose the best YAML file from the following options: Customer.yaml, Revenue.yaml, Orders.yaml.'
) AS response;

----------------------------------------------------------------

SELECT 
    response
FROM (
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        'llama2-70b-chat',
        PARSE_JSON('[
            {"role": "system", "content": "You are a helpful AI assistant. Based on the question, choose the best YAML file."},
            {"role": "user", "content": "Based on the question \'Retrieve all transactions with an amount of 300.0?\', choose the best YAML file from the following options: Customer.yaml, Revenue.yaml, Orders.yaml."}
        ]'),
        PARSE_JSON('{}')
    ) AS response
);

-------------------------------------------------------

SELECT SNOWFLAKE.CORTEX.COMPLETE(
    'snowflake-arctic',
    PARSE_JSON('[
        {"role": "system", "content": "You are a helpful AI assistant. Based on the question, choose the best YAML file."},
        {"role": "user", "content": "Based on the question \'Retrieve all transactions with an amount of 300.0?\', choose the best YAML file from the following options: Customer.yaml, Revenue.yaml, Orders.yaml."}
    ]'),
    PARSE_JSON('{"max_tokens": 100}')
) AS response;




