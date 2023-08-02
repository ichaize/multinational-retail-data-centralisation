import yaml
from sqlalchemy import create_engine, inspect

class DatabaseConnector:

    def __init__(self, credentials): 
        '''The DatabaseConnector establishes connections to the RDS and local databases
        
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
    
    def init_db_engine(self):
        '''Iniiates an engine to connect to the RDS or local database
        
           Returns:
                sqlalchemy.engine
        '''
        database_type = self.credentials["DB_DATABASE_TYPE"]
        user = self.credentials["DB_USER"]
        password = self.credentials["DB_PASSWORD"]
        host = self.credentials["DB_HOST"]
        port = self.credentials["DB_PORT"]
        database = self.credentials["DB_DATABASE"]
        dbapi = self.credentials["DB_DBAPI"]
        if dbapi != "None": # the url for the rds database does not include a dbapi
            engine = create_engine(f"{database_type}+{dbapi}://{user}:{password}@{host}:{port}/{database}", echo=True)
        else: 
            engine = create_engine(f"{database_type}://{user}:{password}@{host}:{port}/{database}", echo=True)
        return engine
    
    def list_db_tables(self):
        '''Lists the tables contained in the RDS database
        
           Returns: 
                list
        '''
        engine = self.init_db_engine()
        engine.connect()
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        return table_names
    
    def upload_to_db(self, df, table):
        '''Uploads the cleaned data to the local SQL database
        
           Args:
                df (pandas.Dataframe): the dataframe to be uploaded
                table (str): the name of the table to create in the SQL database
        '''
        engine = self.init_db_engine()
        df.to_sql(table, engine, if_exists="replace")
    


