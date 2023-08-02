import pandas as pd
import tabula
import requests
import yaml
import boto3

class DataExtractor:

    def __init__(self, credentials):
        '''The DataExtractor extracts data from different sources and returns pandas Dataframes
        
           Args:
                credentials (str): the yaml dictionary containing the access credentials
        '''
        self.credentials = self.read_db_creds(credentials)

    def read_db_creds(self, file):
        ''' Gets the credentials for accessing the database from a yaml file
        
            Args: 
                file (str): the yaml file containing the access credentials
                
            Returns:
                yaml dictionary
        '''
    
        with open(file) as stream:
            db_creds = yaml.safe_load(stream)
        return db_creds

    def read_rds_table(self, connector, table_name):
        ''' Extracts data from an RDS database
        
            Args:
                connector (instance): an instance of the DatabaseConnector class
                table_name (str): the name of the RDS table to be extracted
                
            Returns:
                pandas.Dataframe
        '''  
        engine = connector.init_db_engine()
        table = pd.read_sql_table(table_name, engine)
        return table
    
    def retrieve_pdf_data(self, link):
        ''' Extracts data from a pdf accessed via a link
        
            Args:
                link (str): the link to access the pdf
                
            Returns:
                pandas.Dataframe
        '''     
        dfs = tabula.read_pdf(link, pages="all")
        df = pd.concat(dfs)
        return df
    
    def list_number_of_stores(self, endpoint, header):
        ''' Lists the number of stores in the RDS database
        
            Args:
                endpoint (str): the link where the data is stored
                header (dict): dictionary containing the API header

            Returns:
                content of the Response object in unicode
        ''' 
        response = requests.get(endpoint, headers=header)
        return response.text
    
    def retrieve_stores_data(self, store_endpoint, header):
        ''' Retrieves the data from each store endpoint and combines them into one large dataframe
        
            Args:
                store_endpoint (str): the endpoint of each store
                header (dict): dictionary containing the API header
                
            Returns:
                pandas.Dataframe
        '''
        frames = []
        for store_number in range(0, 451):
            response = requests.get(f"{store_endpoint}/{store_number}", headers=header)
            json_res = response.json()
            store = pd.json_normalize(json_res)
            frames.append(store)
        stores_df = pd.concat(frames)
        return stores_df
    
    def extract_from_s3(self, address):
        '''Extracts data from an S3 bucket
        
           Args:
                address (str): the url of the s3 bucket
            
            Returns: 
                pandas.Dataframe
        '''
        aws_access_key_id = self.credentials["AWS_ACCESS_KEY_ID"]
        aws_secret_access_key = self.credentials["AWS_SECRET_ACCESS_KEY"]
        region = self.credentials["REGION_NAME"]
        bucket, key = address.split("/",2)[-1].split("/",1) # splits the address into data-handling-public and products.csv
        S3 = boto3.client("s3", region_name=region, aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        if address.endswith("csv"): # used for extracting the product data   
            response = S3.get_object(Bucket=bucket, Key=key)
            data = pd.read_csv(response.get("Body"))
        elif address.endswith("json"): # used for extracting the date data
            response = S3.get_object(Bucket="data-handling-public", Key="date_details.json")
            data = pd.read_json(response.get("Body"))
        return data