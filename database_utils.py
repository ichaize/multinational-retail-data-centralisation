import yaml
import pandas as pd
from sqlalchemy import create_engine, text, inspect

class DatabaseConnector:

    def read_db_creds(self):
        with open("db_creds.yaml") as stream:
            db_creds = yaml.safe_load(stream)
        return db_creds
    
    def read_local_db_creds(self):
        with open("read_local_db_creds") as stream:
            local_creds = yaml.safe_load(stream)
        return local_creds
    
    def init_db_engine(self):
        creds = self.read_db_creds()
        user = creds["RDS_USER"]
        password = creds["RDS_PASSWORD"]
        host = creds["RDS_HOST"]
        port = creds["RDS_PORT"]
        database = creds["RDS_DATABASE"]
        engine = create_engine(url="postgresql://{0}:{1}@{2}:{3}/{4}".format(
                                    user, password, host, port, database))
        return engine
    
    def list_db_tables(self):
        engine = self.init_db_engine()
        engine.connect()
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        return table_names
    
    def upload_to_db(self, df, table):
        creds = self.read_local_db_creds()
        DATABASE_TYPE = creds["DATABASE_TYPE"]
        DBAPI = creds["DBAPI"]
        HOST = creds["HOST"]
        USER = creds["USER"]
        PASSWORD = creds["PASSWORD"]
        DATABASE = creds["DATABASE"]
        PORT = creds["PORT"]
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}", echo=True)
        engine.connect()
        df.to_sql(table, engine, if_exists="replace")

    
connector = DatabaseConnector()
if __name__ == "__main__":
    # from data_cleaning import cleaned_user_data
    # connector.upload_to_db(cleaned_user_data, "dim_users")


    # from data_cleaning import cleaned_card_data
    # connector.upload_to_db(cleaned_card_data, "dim_user_details")

    # from data_cleaning import cleaned_store_data
    # connector.upload_to_db(cleaned_store_data, "dim_store_details")

    # from data_cleaning import cleaned_product_data
    # connector.upload_to_db(cleaned_product_data, "dim_products")

    from data_cleaning import cleaned_order_data
    connector.upload_to_db(cleaned_order_data, "orders_table")
    
    # from data_cleaning import cleaned_date_data
    # connector.upload_to_db(cleaned_date_data, "dim_date_times")