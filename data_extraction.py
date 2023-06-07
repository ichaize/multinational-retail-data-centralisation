from database_utils import DatabaseConnector
import pandas as pd
import tabula
import requests


class DataExtractor:

    def read_rds_table(self, connector, table_name):
        engine = connector.init_db_engine()
        table = pd.read_sql_table(table_name, engine)
        return table
    
    def retrieve_pdf_data(self, link):
        dfs = tabula.read_pdf(link, pages="all")
        df = pd.concat(dfs)
        return df
    
    def list_number_of_stores(self, endpoint, header):
        response = requests.get(endpoint, header)
        return response.text
        

DE = DataExtractor()
DC = DatabaseConnector()
user_data = DE.read_rds_table(DC, "legacy_users")

pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
card_data = DE.retrieve_pdf_data(pdf_link)

header_dict = {"x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
number_of_stores = DE.list_number_of_stores("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores", header=header_dict)
print(number_of_stores)