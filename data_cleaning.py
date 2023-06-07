
#%%
from data_extraction import DataExtractor, card_data, user_data
from database_utils import DatabaseConnector
import pandas as pd
from pyisemail import is_email
# DE = DataExtractor()
# DC = DatabaseConnector()
# user_data = DE.read_rds_table(DC, "legacy_users")

# print(card_data["card_provider"].value_counts())



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
    
    def clean_card_data(self):
        self.table = self.convert_card_types()
        self.table = self.remove_nonsense_and_nulls()
        self.table = self.clean_card_numbers()
        return self.table

    def convert_card_types(self):
        self.table["expiry_date"] = self.table["expiry_date"].astype("string")
        self.table["card_number"] = self.table["card_number"].astype("string")
        return self.table

    def remove_nonsense_and_nulls(self):
        self.table["nonsense"] = self.table["expiry_date"].apply(lambda x: len(x) != 5) # gets rid of NULL and nonsense (10 char string) values
        self.table = self.table[~self.table["nonsense"]]
        self.table.drop(["nonsense"], axis=1, inplace=True)
        return self.table
    
    def clean_card_numbers(self):
        self.table["card_number"] = self.table["card_number"].apply(lambda x: x.replace("?", ""))
        self.table["card_number"] = self.table["card_number"].apply(lambda x: (x := f"0{x}") if len(x) < 12 else x) # all 11 digit numbers are maestro and missing a 0 on the front to make them valid
        self.table = self.table[self.table["card_number"] != "0604691111"] # drop the only remaining number under 12 digits which must be a typo
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
        
        
    



# user_data_to_clean = DataCleaning(user_data)
# cleaned_user_data = user_data_to_clean.clean_user_data()
card_data_to_clean = DataCleaning(card_data)
cleaned_card_data = card_data_to_clean.clean_card_data()
print(cleaned_card_data.shape[0])

# # print(user_data["country"].value_counts())
# # pd.set_option('display.max_columns', None)
# print(user_data.shape[0])

# finding errors in card number (some start with series of ???, some are too short): 

# self.table["incorrect_card_length"] = self.table["card_number"].apply(lambda x: len(x) < 12 or len(x) > 19)
# incorrect_cards = self.table[self.table["incorrect_card_length"] == True]
# self.table["wrong_card_length"] = self.table["card_number"].apply(lambda x: len(x) < 12 or len(x) > 19)
# wrong_cards = self.table[self.table["wrong_card_length"] == True]
# incorrect_cards["card_number"] = incorrect_cards["card_number"].apply(lambda x: (x := f"0{x}"))
