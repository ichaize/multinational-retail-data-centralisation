import yaml
from sqlalchemy import create_engine

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
    
DC = DatabaseConnector()
engine = DC.init_db_engine()
engine.connect()

