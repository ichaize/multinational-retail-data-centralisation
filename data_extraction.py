from database_utils import DatabaseConnector
import pandas as pd

class DataExtractor:

    def read_rds_table(self, connector, table_name):
        engine = connector.init_db_engine()
        table = pd.read_sql_table(table_name, engine)
        return table

DE = DataExtractor()
DC = DatabaseConnector()
# tables = DC.list_db_tables()
user_data = DE.read_rds_table(DC, "legacy_users")


 
# def read_rds_table(self, connector):
    #     tables = connector.list_db_tables()
    #     engine = connector.init_db_engine()
    #     for item in tables:
    #         table = pd.read_sql_table(item, engine)
    #         return table

# DE = DataExtractor()
# DE.read_rds_table(DatabaseConnector())