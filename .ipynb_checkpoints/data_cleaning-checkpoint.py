
from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import pandas as pd
DE = DataExtractor()
DC = DatabaseConnector()
user_data = DE.read_rds_table(DC, "legacy_users")
# user_data = user_data.convert_dtypes()
print(user_data.at[752, "date_of_birth"])
# user_data["date_of_birth"] = pd.to_datetime(user_data["date_of_birth"], format="mixed", dayfirst=True)
# print(user_data.isnull().sum())
# print(user_data.dtypes)

# class DataCleaning:
    
#     def clean_user_data(self, table):




# cleaned_user_data = DataCleaning.clean_user_data(user_data)