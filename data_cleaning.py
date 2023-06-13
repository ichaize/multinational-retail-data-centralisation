
#%%
from data_extraction import store_data, card_data, user_data
import pandas as pd
from pyisemail import is_email

class DataCleaning:     
    
    def clean_user_data(self, table):
        table = self.convert_types(table)
        table = self.remove_null_values(table)
        table = self.clean_emails(table)
        table = self.remove_nonsense_user_values(table)
        table = self.clean_dates(table)
        table = self.clean_country_codes(table)
        table = self.clean_phone_numbers(table)
        table = self.clean_addresses(table)
        # table = self.reset_idx(table)
        return table
    
    def clean_card_data(self, table):
        table = self.convert_types(table)
        table = self.remove_nonsense_and_nulls(table)
        table = self.clean_card_numbers(table)
        table = self.clean_dates(table)
       #  table = self.reset_idx(table)
        return table
    
    def clean_store_data(self, table):
        table = self.convert_types(table)
        table = self.remove_null_values(table)
        table = self.remove_nonsense_store_values(table)
        table = self.clean_addresses(table)
        table = self.clean_dates(table)
        table = self.clean_staff_numbers(table)
        table.replace({"continent": {"eeEurope": "Europe", "eeAmerica": "America"}}, inplace=True)
        table.drop(["lat"], axis=1, inplace=True)
        # table = self.reset_idx(table)
        return table
    
    def convert_types(self, table):
        to_convert = ["email_address", "address", "phone_number", "expiry_date", "card_number", "country_code"]
        for col in to_convert:
            if col in table.columns:
                table[col] = table[col].astype("string")
        return table
    
    def remove_null_values(self, table):
        table = table[table["address"] != "NULL"]
        return table
    
    def remove_nonsense_user_values(self, table):
        table["nonsense"] = table["email_address"].apply(lambda x: "@" not in x)
        table = table[~table["nonsense"]]
        table.drop(["nonsense"], axis=1, inplace=True)
        return table
    
    def remove_nonsense_store_values(self, table):
        table["nonsense"] = table["country_code"].apply(lambda x: len(x) != 2)
        table = table[~table["nonsense"]]
        table.drop(["nonsense"], axis=1, inplace=True)
        return table
    
    def remove_nonsense_and_nulls(self, table):
        table["expiry_date"] = table["expiry_date"].astype("string")
        table["nonsense"] = table["expiry_date"].apply(lambda x: len(x) != 5) # gets rid of NULL and nonsense (10 char string) values
        table = table[~table["nonsense"]]
        table.drop(["nonsense"], axis=1, inplace=True)
        return table
    
    def clean_dates(self, table):
        date_columns = ["date_of_birth", "join_date", "date_payment_confirmed", "opening_date"]
        for col in date_columns:
            if col in table.columns:
                table[col] = pd.to_datetime(table[col], format="mixed").dt.date
        return table
    
    def clean_addresses(self, table):
        table["address"] = table["address"].apply(lambda x: x.replace("\n", ", "))
        table["address"] = table["address"].apply(lambda x: (x := " ".join([word[0].upper() + word[1:] for word in x.split()])))
        return table
    
    def clean_card_numbers(self, table):
        table["card_number"] = table["card_number"].apply(lambda x: x.replace("?", "")) # some numbers begin with a series of ???
        table["card_number"] = table["card_number"].apply(lambda x: (x := f"0{x}") if len(x) < 12 else x) # all 11 digit numbers are maestro and missing a 0 on the front to make them valid
        table = table[table["card_number"] != "0604691111"] # drop the only remaining number under 12 digits which must be a typo (all credit card numbers are between 12 and 19)
        return table
    
    def clean_staff_numbers(self, table):
        table["staff_numbers"] = table["staff_numbers"].astype("string")
        table["staff_numbers"] = table["staff_numbers"].str.replace(r"\D+", "", regex=True)
        return table
    
    def clean_phone_numbers(self, table):
        germans = table[table["country"] == "Germany"]
        germans["phone_number"] = germans["phone_number"].apply(lambda x: x.replace(" ", "").replace("(", "").replace(")", "").replace("0", "", 1))
        germans["phone_number"] = germans["phone_number"].apply(lambda x: (x := f"+49{x}") if x[0] != "+" else x)
        table.update(germans)
        uk = table[table["country"] == "United Kingdom"]
        uk["phone_number"] = uk["phone_number"].apply(lambda x: x.replace(" ", "").replace("(", "").replace(")", "").replace("0", "", 1))
        uk["phone_number"] = uk["phone_number"].apply(lambda x: (x := f"+44{x}") if x[0] != "+" else x)
        table.update(uk)
        americans = table[table["country"] == "United States"]
        americans["phone_number"] = americans["phone_number"].apply(lambda x: x.replace(" ", "").replace("(", "").replace(")", "").replace("-", "").replace(".", ""))
        americans["phone_number"] = americans["phone_number"].apply(lambda x: x.replace("001", "", 1) if x.startswith("001") else x)
        americans["phone_number"] = americans["phone_number"].apply(lambda x: (x := f"+1{x}") if x[0] != "+" else x)
        table.update(americans)
        return table
    
    def clean_emails(self, table):
        table["checked_emails"] = table["email_address"].apply(lambda x: is_email(x))
        invalid_emails = table[table["checked_emails"] == False]
        invalid_emails["email_address"] = invalid_emails["email_address"].apply(lambda x: x.replace("@@", "@"))
        table.update(invalid_emails)
        table.drop(["checked_emails"], axis=1, inplace=True)
        return table

    def clean_country_codes(self, table):
        table["country_code"].replace("GGB", "GB", inplace=True)
        return table

    def reset_idx(self, table):
        if "index" in table.columns:
            table.drop(["index"], axis=1, inplace=True)
        table.reset_index(drop=True, inplace=True)
        return table
    

data_cleaner = DataCleaning()
cleaned_user_data = data_cleaner.clean_user_data(user_data)
cleaned_card_data = data_cleaner.clean_card_data(card_data)
cleaned_store_data = data_cleaner.clean_store_data(store_data)
