import yaml
import pandas as pd
from sqlalchemy import create_engine, text, inspect

class DatabaseConnector:

    def read_db_creds(self):
        with open("db_creds.yaml") as stream:
            db_creds = yaml.safe_load(stream)
        return db_creds
    
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
        DATABASE_TYPE = "postgresql"
        DBAPI = "psycopg2"
        HOST = "localhost"
        USER = "postgres"
        PASSWORD = "temple"
        DATABASE = "sales_data"
        PORT = 5432
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}", echo=True)
        engine.connect()
        df.to_sql(table, engine)

    
connector = DatabaseConnector()
if __name__ == "__main__":
    from data_cleaning import cleaned_user_data
    connector.upload_to_db(cleaned_user_data, "dim_users")


