import pandas as pd
import tabula
import requests
import yaml
import boto3

class DataExtractor:

    def read_db_creds(self):
        ''' gets the credentials for accessing the RDS database from a yaml file'''
        with open("db_creds.yaml") as stream:
            db_creds = yaml.safe_load(stream)
        return db_creds

    def read_rds_table(self, connector, table_name):
        ''' takes in a table name and a connector to return a dataframe from the RDS database'''
        engine = connector.init_db_engine()
        table = pd.read_sql_table(table_name, engine)
        return table
    
    def retrieve_pdf_data(self, link):
        ''' reads a pdf from a link and returns a dataframe containing the data from all pages'''
        dfs = tabula.read_pdf(link, pages="all")
        df = pd.concat(dfs)
        return df
    
    def list_number_of_stores(self, endpoint, header):
        ''' returns the number of stores in the RDS database'''
        response = requests.get(endpoint, headers=header)
        return response.text
    
    def retrieve_stores_data(self, store_endpoint, header):
        ''' retrieves the data from each store endpoint and combines them into one large dataframe'''
        frames = []
        for store_number in range(0, 451):
            response = requests.get(f"{store_endpoint}/{store_number}", headers=header)
            json_res = response.json()
            store = pd.json_normalize(json_res)
            frames.append(store)
        stores_df = pd.concat(frames)
        return stores_df
    
    def extract_from_s3(self, address):
        '''retrieves data stored in an S3 bucket and returns a dataframe'''
        creds = self.read_db_creds()
        aws_access_key_id = creds["AWS_ACCESS_KEY_ID"]
        aws_secret_access_key = creds["AWS_SECRET_ACCESS_KEY"]
        region = creds["REGION_NAME"]
        bucket, key = address.split("/",2)[-1].split("/",1) # splits the address into data-handling-public and products.csv
        S3 = boto3.client("s3", region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        if address.endswith("csv"): # used for extracting the product data   
            response = S3.get_object(Bucket=bucket, Key=key)
            data = pd.read_csv(response.get("Body"))
        elif address.endswith("json"): # used for extracting the date data
            response = S3.get_object(Bucket="data-handling-public", Key="date_details.json")
            data = pd.read_json(response.get("Body"))
        return data