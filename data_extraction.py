from database_utils import DatabaseConnector
import pandas as pd
import tabula
import requests
import yaml
import boto3


class DataExtractor:

    def read_db_creds(self):
        with open("db_creds.yaml") as stream:
            db_creds = yaml.safe_load(stream)
        return db_creds

    def read_rds_table(self, connector, table_name):
        engine = connector.init_db_engine()
        table = pd.read_sql_table(table_name, engine)
        return table
    
    def retrieve_pdf_data(self, link):
        dfs = tabula.read_pdf(link, pages="all")
        df = pd.concat(dfs)
        return df
    
    def list_number_of_stores(self, endpoint, header):
        response = requests.get(endpoint, headers=header)
        return response.text
    
    def retrieve_stores_data(self, store_endpoint, header):
        frames = []
        for store_number in range(0, 451):
            response = requests.get(f"{store_endpoint}/{store_number}", headers=header)
            json_res = response.json()
            store = pd.json_normalize(json_res)
            frames.append(store)
        stores_df = pd.concat(frames)
        return stores_df
    
    def extract_from_s3(self, address):
        creds = self.read_db_creds()
        aws_access_key_id = creds["AWS_ACCESS_KEY_ID"]
        aws_secret_access_key = creds["AWS_SECRET_ACCESS_KEY"]
        region = creds["REGION_NAME"]
        bucket, key = address.split("/",2)[-1].split("/",1) # splits the address into data-handling-public and products.csv
        S3 = boto3.client("s3", region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        if address.endswith("csv"):    
            response = S3.get_object(Bucket=bucket, Key=key)
            data = pd.read_csv(response.get("Body"))
        elif address.endswith("json"):
            response = S3.get_object(Bucket="data-handling-public", Key="date_details.json")
            data = pd.read_json(response.get("Body"))
        return data
        

        

DE = DataExtractor()
DC = DatabaseConnector()
user_data = DE.read_rds_table(DC, "legacy_users")

# pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
# card_data = DE.retrieve_pdf_data(pdf_link)

header_dict = {"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
# number_of_stores = DE.list_number_of_stores("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores", header_dict)
store_data = DE.retrieve_stores_data(f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details", header_dict)
# product_data = DE.extract_from_s3("s3://data-handling-public/products.csv")
order_data = DE.read_rds_table(DC, "orders_table")
date_data = DE.extract_from_s3("https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json")