
#%%
from data_extraction import store_data
# from data_extraction import store_data, card_data, user_data, product_data, order_data
import pandas as pd
from pyisemail import is_email
import numpy as np

print(store_data["store_type"].head())

class DataCleaning:     
    
    def clean_user_data(self, table):
        table = self.convert_types(table)
        table = self.remove_nonsense_values(table)
        table = self.remove_null_values(table)
        table = self.clean_emails(table)
        table = self.clean_dates(table)
        table = self.clean_country_codes(table)
        table = self.clean_phone_numbers(table)
        table = self.clean_addresses(table)
        # table = self.reset_idx(table)
        return table
    
    def clean_card_data(self, table):
        table = self.convert_types(table)
        table = self.remove_null_values(table)
        table = self.remove_nonsense_values(table)
        table = self.clean_card_numbers(table)
        table = self.clean_dates(table)
       #  table = self.reset_idx(table)
        return table
    
    def clean_store_data(self, table):
        table = self.convert_types(table)
        table = self.remove_null_values(table)
        table = self.remove_nonsense_values(table)
        table = self.clean_addresses(table)
        table = self.clean_dates(table)
        table = self.clean_staff_numbers(table)
        table.replace({"continent": {"eeEurope": "Europe", "eeAmerica": "America"}}, inplace=True)
        table.drop(["lat"], axis=1, inplace=True)
        # table = self.reset_idx(table)
        return table
    
    def clean_product_data(self, table):
        table.dropna(inplace=True)
        table = self.convert_types(table)
        table = self.remove_nonsense_values(table)
        table = self.convert_product_weights(table)
        table = self.reset_idx(table)
        return table
    
    def clean_order_data(self, table):
        table = self.convert_types(table)
        table.drop(labels=["first_name", "last_name", "1"], axis=1, inplace=True)
        table = self.clean_card_numbers(table)
        table = self.reset_idx(table)
        return table
    
    def clean_date_data(self, table):
        table = self.convert_types(table)
        table = self.remove_null_values(table)
        table = self.remove_nonsense_values(table)
        return table

    def convert_types(self, table):
        to_convert = ["email_address", "address", "phone_number", "expiry_date", "card_number", "country_code", "weight", "removed", "year"]
        for col in to_convert:
            if col in table.columns:
                table[col] = table[col].astype("string")
        return table
    
    def remove_null_values(self, table):
        table = table[~table.isin(["NULL"]).any(axis=1)]
        return table
    
    def remove_nonsense_values(self, table):
        to_check = ["country_code", "removed", "expiry_date", "year"]
        for col in to_check:
            if col in table.columns:
                table["nonsense"] = table[col].apply(lambda x: len(x) == 10)
        table = table[~table["nonsense"]]
        table.drop(["nonsense"], axis=1, inplace=True)
        return table
    
    def clean_dates(self, table):
        date_columns = ["date_of_birth", "join_date", "date_payment_confirmed", "opening_date", "date_uuid"]
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
    
    def convert_product_weights(self, table):
        table["weight"] = table["weight"].apply(lambda x: x.replace("ml", "g"))
        grams = table[~table["weight"].str.contains("k")] 
        grams["weight"] = grams["weight"].apply(lambda x: x.replace("g", "").replace(".", ""))
        x = grams[grams["weight"].str.contains("x")]
        x["weight"] = x["weight"].apply(lambda x: x.replace("x", ""))
        x["weight"] = x["weight"].apply(self.weight_cleaner)
        grams.update(x)
        grams["weight"] = grams["weight"].astype("string")
        ounces = grams[grams["weight"].str.contains("oz", na=False)]
        ounces["weight"] = ounces["weight"].apply(lambda x: x.replace("oz", ""))
        ounces["weight"] = ounces["weight"].astype("float")
        ounces["weight"] = ounces["weight"].apply(lambda x: x/35.274)
        grams["weight"] = grams["weight"].apply(lambda x: x.replace("oz", ""))
        grams["weight"] = grams["weight"].astype("float")
        grams["weight"] = grams["weight"].apply(lambda x: x/1000)
        grams.update(ounces)
        table.update(grams)
        table["weight"] = table["weight"].astype("string")
        table["weight"] = table["weight"].apply(lambda x: x.replace("kg", ""))
        table["weight"] = table["weight"].astype("float")
        table["weight"] = table["weight"].apply(lambda x: (x := "{0:g}".format(x))) 
        return table
    
    def weight_cleaner(self, x):
        first_num = int(x.split()[0])
        second_num = int(x.split()[1])
        x = first_num * second_num
        return x
        
    def reset_idx(self, table):
        if "index" in table.columns:
            table.drop(["index"], axis=1, inplace=True)
        if "Unnamed: 0" in table.columns:
            table.drop(["Unnamed: 0"], axis=1, inplace=True)
        table.reset_index(drop=True, inplace=True)
        return table
    

data_cleaner = DataCleaning()
# cleaned_date_data = data_cleaner.clean_date_data(date_data)
# cleaned_user_data = data_cleaner.clean_user_data(user_data)
# cleaned_card_data = data_cleaner.clean_card_data(card_data)
# cleaned_store_data = data_cleaner.clean_store_data(store_data)
# cleaned_product_data = data_cleaner.clean_product_data(product_data) 
# cleaned_order_data = data_cleaner.clean_order_data(order_data)


