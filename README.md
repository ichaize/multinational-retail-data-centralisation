# Data Cleaning and Centralization Project

This project simulated the data cleaning and centralization process for a large retail chain. The first step was to extract the data from various sources and formats. Next the data was cleaned before being uploaded to a local SQL database. After some more data processing using SQL (ensuring correct types, adding columns based on conditionals), a database schema was created and used to run SQL queries.

Technologies used: Python, pandas, PostgreSQL, PGAdmin, AWS (s3), boto3, sqlalchemy

## Project Components

1.  A DatabaseConnector class stored in database_utils.py. This uses sqlalchemy to access the RDS database where most of the raw data is stored, and the local database where the cleaned data will be collected. 

2. A DataExtractor class stored in data_extraction.py. This contains methods that extract data from different formats: sql table, pdf, json via url, or s3 bucket and return a pandas dataframe that can be cleaned. 

3. A DataCleaning class stored in data_cleaning.py. This applies the data cleaning and processing to the data.

4. A Transformer class stored in transforms.py. This contains the data cleaning and processing methods used by the DataCleaning class. It imports mapping dictionaries from mappings.py.

5. The connections, data extraction and cleaning, and uploading to the local database are all performed in main.py.

6. The SQL queries performed on the database in PGAdmin are recorded in milestone_three_queries.sql and milestone_four_queries.sql.

7. All libraries used in the project are listed in requirements.txt, which was created using pipreqs.

