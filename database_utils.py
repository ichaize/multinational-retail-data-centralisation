import yaml
from sqlalchemy import create_engine, inspect

class DatabaseConnector:

    def __init__(self, credentials):
        self.credentials = self.read_db_creds(credentials)

    def read_db_creds(self, file):
        ''' gets the credentials for accessing the database from a yaml file'''
        with open(file) as stream:
            db_creds = yaml.safe_load(stream)
        return db_creds
    
    def init_db_engine(self):
        '''iniiates an engine to connect to the RDS or local database'''
        database_type = self.credentials["DB_DATABASE_TYPE"]
        user = self.credentials["DB_USER"]
        password = self.credentials["DB_PASSWORD"]
        host = self.credentials["DB_HOST"]
        port = self.credentials["DB_PORT"]
        database = self.credentials["DB_DATABASE"]
        dbapi = self.credentials["DB_DBAPI"]
        if dbapi != None:
            engine = create_engine(f"{database_type}+{dbapi}://{user}:{password}@{host}:{port}/{database}", echo=True)
        else: 
            engine = create_engine(f"{database_type}+{dbapi}://{user}:{password}@{host}:{port}/{database}", echo=True)
        return engine
    
    def list_db_tables(self):
        ''' lists the tables contained in the RDS database'''
        engine = self.init_db_engine()
        engine.connect()
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        return table_names
    
    def upload_to_db(self, df, table):
        '''uploads the cleaned data to the local SQL database'''
        engine = self.init_db_engine()
        df.to_sql(table, engine, if_exists="replace")
    

rds_db = DatabaseConnector("local_db_creds.yaml")
print(rds_db.credentials)
