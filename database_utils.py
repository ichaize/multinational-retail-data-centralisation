import yaml

class DatabaseConnector:

    def read_db_creds(self):
        with open("db_creds.yaml") as stream:
            db_creds = yaml.safe_load(stream)
        return db_creds
    
DC = DatabaseConnector()
DC.read_db_creds()

