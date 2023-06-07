from database_utils import DatabaseConnector
import pandas as pd
import tabula

class DataExtractor:

    def read_rds_table(self, connector, table_name):
        engine = connector.init_db_engine()
        table = pd.read_sql_table(table_name, engine)
        return table
    
    def retrieve_pdf_data(self, link):
        dfs = tabula.read_pdf(link, pages="all")
        df = pd.concat(dfs)
        return df
        

DE = DataExtractor()
DC = DatabaseConnector()
# tables = DC.list_db_tables()
user_data = DE.read_rds_table(DC, "legacy_users")
pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
card_data = DE.retrieve_pdf_data(pdf_link)
print(card_data.shape[0])