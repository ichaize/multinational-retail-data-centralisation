
#%%
from data_extraction import DataExtractor
from database_utils import DatabaseConnector
import pandas as pd
from pyisemail import is_email
# from data_extraction import user_data

class DataCleaning:
    
    def __init__(self, table):
        self.table = table       
    
    def clean_user_data(self):
        self.table = self.convert_types()
        self.table = self.remove_null_values()
        self.table = self.clean_emails()
        self.table = self.remove_nonsense_values()
        self.table = self.clean_dates()
        self.table = self.clean_country_codes()
        self.table = self.clean_phone_numbers()
        self.table = self.clean_addresses()
        self.table = self.reset_idx()
        return self.table

    def convert_types(self):
        self.table["email_address"] = self.table["email_address"].astype("string")
        self.table["address"] = self.table["address"].astype("string")
        self.table["phone_number"] = self.table["phone_number"].astype("string")
        return self.table

    def remove_null_values(self):
        self.table = self.table[self.table["email_address"] != "NULL"]
        return self.table
    
    def clean_emails(self):
        self.table["checked_emails"] = self.table["email_address"].apply(lambda x: is_email(x))
        invalid_emails = self.table[self.table["checked_emails"] == False]
        invalid_emails["email_address"] = invalid_emails["email_address"].apply(lambda x: x.replace("@@", "@"))
        self.table.update(invalid_emails)
        self.table.drop(["checked_emails"], axis=1, inplace=True)
        return self.table
    
    def remove_nonsense_values(self):
        self.table["nonsense"] = self.table["email_address"].apply(lambda x: "@" not in x)
        self.table = self.table[~self.table["nonsense"]]
        self.table.drop(["nonsense"], axis=1, inplace=True)
        return self.table
    
    def clean_dates(self):
        self.table["date_of_birth"] = pd.to_datetime(self.table["date_of_birth"], format="mixed").dt.date
        self.table["join_date"] = pd.to_datetime(self.table["join_date"], format="mixed").dt.date
        return self.table

    def clean_country_codes(self):
        self.table["country_code"].replace("GGB", "GB", inplace=True)
        return self.table
    
    def clean_phone_numbers(self):
        germans = self.table[self.table["country"] == "Germany"]
        germans["phone_number"] = germans["phone_number"].apply(lambda x: x.replace(" ", "").replace("(", "").replace(")", "").replace("0", "", 1))
        germans["phone_number"] = germans["phone_number"].apply(lambda x: (x := f"+49{x}") if x[0] != "+" else x)
        self.table.update(germans)
        uk = self.table[self.table["country"] == "United Kingdom"]
        uk["phone_number"] = uk["phone_number"].apply(lambda x: x.replace(" ", "").replace("(", "").replace(")", "").replace("0", "", 1))
        uk["phone_number"] = uk["phone_number"].apply(lambda x: (x := f"+44{x}") if x[0] != "+" else x)
        self.table.update(uk)
        americans = self.table[self.table["country"] == "United States"]
        americans["phone_number"] = americans["phone_number"].apply(lambda x: x.replace(" ", "").replace("(", "").replace(")", "").replace("-", "").replace(".", ""))
        americans["phone_number"] = americans["phone_number"].apply(lambda x: x.replace("001", "", 1) if x.startswith("001") else x)
        americans["phone_number"] = americans["phone_number"].apply(lambda x: (x := f"+1{x}") if x[0] != "+" else x)
        self.table.update(americans)
        return self.table

    def clean_addresses(self):
        self.table["address"] = self.table["address"].apply(lambda x: x.replace("\n", ", "))
        self.table["address"] = self.table["address"].apply(lambda x: (x := " ".join([word[0].upper() + word[1:] for word in x.split()])))
        return self.table

    def reset_idx(self):
        self.table.drop(["index"], axis=1, inplace=True)
        self.table.reset_index()
        return self.table
        
        
    


DE = DataExtractor()
DC = DatabaseConnector()
user_data = DE.read_rds_table(DC, "legacy_users")
data_to_clean = DataCleaning(user_data)
cleaned_user_data = data_to_clean.clean_user_data()
# user_data = data_to_clean.convert_types()
# user_data = data_to_clean.remove_null_values()
# # print(user_data.loc[11761])
# user_data = data_to_clean.clean_emails()
# # print(user_data.loc[205])
# user_data = data_to_clean.remove_nonsense_values()
# # print(user_data.loc[752])
# user_data = data_to_clean.clean_dates()
# # print(user_data["join_date"].head())
# # print(user_data["country"].value_counts())
# user_data = data_to_clean.clean_country_codes()
# # print(user_data["country_code"].value_counts()) 
# # pd.set_option('display.max_columns', None)
# user_data = data_to_clean.clean_phone_numbers()
# # print(user_data.head(20))
# user_data = data_to_clean.clean_dates()
# user_data = data_to_clean.clean_addresses()
# user_data = data_to_clean.reset_index()
# print(user_data.shape[0])
