from pyisemail import is_email
import pandas as pd 
from mappings import WEIGHT_REPLACEMENTS, REPLACEMENTS, PHONE_REPLACEMENTS

class Transformer:

    def make_replacements(self, table, column, key, count=0):
        '''uses a dictionary from mappings.py to make string replacements, default count is 0 (all)'''
        table[column] = table[column].apply(lambda x: x.replace(key, REPLACEMENTS[key], count))
        return table
    
    def convert_types(self, table):
        '''prepares the data for cleaning by converting the columns to be cleaned to strings where necessary'''
        to_convert = ["email_address", "address", "phone_number", "expiry_date", "card_number", "country_code", "weight", "removed", "year"]
        for col in to_convert:
            if col in table.columns:
                table[col] = table[col].astype("string")
        return table
    
    def remove_null_values(self, table):
        '''removes any null values, which in this database are a string "NULL"'''
        table = table[~table.isin(["NULL"]).any(axis=1)]
        return table
    
    def remove_nonsense_values(self, table):
        '''removes rows where all columns are a random string of 10 alphanumeric characters by checking the length against columns where the correct length is never 10'''
        to_check = ["country_code", "removed", "expiry_date", "year"]
        for col in to_check:
            if col in table.columns:
                table["nonsense"] = table[col].apply(lambda x: len(x) == 10)
        table = table[~table["nonsense"]]
        table.drop(["nonsense"], axis=1, inplace=True)
        return table
    
    def clean_dates(self, table):
        '''converts all dates to datetime and presents only date information'''
        date_columns = ["date_of_birth", "join_date", "date_payment_confirmed", "opening_date", "date_uuid"]
        for col in date_columns:
            if col in table.columns:
                table[col] = pd.to_datetime(table[col], format="mixed").dt.date
        return table
    
    def clean_addresses(self, table):
        '''replaces newlines with a comma, then capitalizes the first letter of every word'''
        self.make_replacements(table, "address", "\n")
        table["address"] = table["address"].apply(lambda x: (x := " ".join([word[0].upper() + word[1:] for word in x.split()])))
        return table
    
    def clean_card_numbers(self, table):
        '''removes non-numeric characters, then ensures all numbers are the correct length (12-19 digits) and format'''
        self.make_replacements(table, "card_number", "?") # some numbers begin with a series of ???
        table["card_number"] = table["card_number"].apply(lambda x: (x := f"0{x}") if len(x) < 12 else x) # all 11 digit numbers are maestro and missing a 0 on the front to make them valid
        table = table[table["card_number"] != "0604691111"] # drop the only remaining number under 12 digits which must be a typo (all credit card numbers are between 12 and 19)
        return table
    
    def clean_staff_numbers(self, table):
        '''removes any non-numeric characters using regex'''
        table["staff_numbers"] = table["staff_numbers"].astype("string")
        table["staff_numbers"] = table["staff_numbers"].str.replace(r"\D+", "", regex=True)
        return table
    
    def clean_phone_numbers(self, table):
        '''splits the table by country and ensures all phone numbers are in the appropriate format and adds country codes'''
        germans = table[table["country"] == "Germany"]
        self.make_phone_replacements(germans)
        self.make_replacements(germans, "phone_number", "0", 1)
        germans["phone_number"] = germans["phone_number"].apply(lambda x: (x := f"+49{x}") if x[0] != "+" else x)
        table.update(germans)
        uk = table[table["country"] == "United Kingdom"]
        self.make_phone_replacements(uk)
        self.make_replacements(uk, "phone_number", "0", 1)
        uk["phone_number"] = uk["phone_number"].apply(lambda x: (x := f"+44{x}") if x[0] != "+" else x)
        table.update(uk)
        americans = table[table["country"] == "United States"]
        self.make_phone_replacements(americans)
        americans["phone_number"] = americans["phone_number"].apply(lambda x: x.replace("001", "", 1) if x.startswith("001") else x)
        americans["phone_number"] = americans["phone_number"].apply(lambda x: (x := f"+1{x}") if x[0] != "+" else x)
        table.update(americans)
        return table
    
    def make_phone_replacements(self, table):
        '''uses a dictionary from mappings.py to make string replacements'''
        for key, value in PHONE_REPLACEMENTS.items():
            table["phone_number"] = table["phone_number"].apply(lambda x: x.replace(key, value))
        return table
    
    def clean_emails(self, table):
        '''checks whether emails are in valid email format; if not removes incorrectly added @'''
        table["checked_emails"] = table["email_address"].apply(lambda x: is_email(x))
        invalid_emails = table[table["checked_emails"] == False]
        invalid_emails["email_address"] = invalid_emails["email_address"].apply(lambda x: x.replace("@@", "@"))
        table.update(invalid_emails)
        table.drop(["checked_emails"], axis=1, inplace=True)
        return table

    def clean_country_codes(self, table):
        '''corrects a typo where GB is written GGB in several records'''
        table["country_code"].replace("GGB", "GB", inplace=True)
        return table
    
    def convert_product_weights(self, table):
        '''converts all weights into kg represented as a float with no unit of measurement'''
        self.make_weight_replacements(table, "ml") # converts all ml to grams
        not_kg = table[~table["weight"].str.contains("k")] # filters out weights already in kg
        self.make_weight_replacements(not_kg, "g")
        self.make_weight_replacements(not_kg, ".")
        x = not_kg[not_kg["weight"].str.contains("x")] # deals with products in multipacks where the weight is given in the format "weight x items_in_pack"
        self.make_weight_replacements(x, "x")
        x["weight"] = x["weight"].apply(self.weight_cleaner)
        not_kg.update(x)
        not_kg["weight"] = not_kg["weight"].astype("string")
        ounces = not_kg[not_kg["weight"].str.contains("oz", na=False)] # this section converts oz to g using standard formula
        self.make_weight_replacements(ounces, "oz")
        ounces["weight"] = ounces["weight"].astype("float")
        ounces["weight"] = ounces["weight"].apply(lambda x: x/35.274) # converts ounces to kg
        self.make_weight_replacements(not_kg, "oz")
        not_kg["weight"] = not_kg["weight"].astype("float")
        not_kg["weight"] = not_kg["weight"].apply(lambda x: x/1000) # converts g to kg
        not_kg.update(ounces)
        table.update(not_kg)
        table["weight"] = table["weight"].astype("string")
        self.make_weight_replacements(table, "kg") # removes the remaining units of measurement
        table["weight"] = table["weight"].astype("float")
        table["weight"] = table["weight"].apply(lambda x: (x := "{0:g}".format(x))) # removes trailing zeros after the decimal point
        return table

    def make_weight_replacements(self, table, key):
        '''uses a dictionary from mappings.py to make string replacements'''
        table["weight"] = table["weight"].apply(lambda x: x.replace(key, WEIGHT_REPLACEMENTS[key]))
        return table
    
    def weight_cleaner(self, x):
        '''splits the weight data of multipack products into numbers representing weight and number of items, then multiplies them together'''
        first_num = int(x.split()[0])
        second_num = int(x.split()[1])
        x = first_num * second_num
        return x
        
    def reset_idx(self, table):
        '''resets the table index'''
        if "index" in table.columns:
            table.drop(["index"], axis=1, inplace=True)
        if "Unnamed: 0" in table.columns:
            table.drop(["Unnamed: 0"], axis=1, inplace=True)
        if "level_0" in table.columns:
            table.drop(["level_0"], axis=1, inplace=True)
        table.reset_index(drop=True, inplace=True)
        return table