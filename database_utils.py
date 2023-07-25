import yaml
from sqlalchemy import create_engine, inspect

class DatabaseConnector:

    def read_db_creds(self):
        ''' gets the credentials for accessing the RDS database from a yaml file'''
        with open("db_creds.yaml") as stream:
            db_creds = yaml.safe_load(stream)
        return db_creds
    
    def init_db_engine(self):
        '''accesses the RDS database using the credentials from read_db_creds'''
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
        ''' lists the tables contained in the RDS database'''
        engine = self.init_db_engine()
        engine.connect()
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        return table_names
    
    def upload_to_db(self, df, table):
        '''uploads the cleaned data to the local SQL database'''
        creds = self.read_db_creds()
        database_type = creds["LOCAL_DATABASE_TYPE"]
        dbapi = creds["LOCAL_DBAPI"]
        host = creds["LOCAL_HOST"]
        user = creds["LOCAL_USER"]
        password = creds["LOCAL_PASSWORD"]
        database = creds["LOCAL_DATABASE"]
        port = creds["LOCAL_PORT"]
        engine = create_engine(f"{database_type}+{dbapi}://{user}:{password}@{host}:{port}/{database}", echo=True)
        engine.connect()
        df.to_sql(table, engine, if_exists="replace")