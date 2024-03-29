from transforms import Transformer
transformer = Transformer()

class DataCleaning:     
    
    def clean_user_data(self, table):
        '''Collects all methods needed to proces the user data
           
           Args:
                table (pandas.Dataframe): The table containing user data to be cleaned
        
           Returns:
                pandas.Dataframe 
        '''     
        table = transformer.convert_types(table)
        table = transformer.remove_nonsense_values(table)
        table = transformer.remove_null_values(table)
        table = transformer.clean_emails(table)
        table = transformer.clean_dates(table)
        table = transformer.clean_country_codes(table)
        table = transformer.clean_phone_numbers(table)
        table = transformer.clean_addresses(table)
        table = transformer.reset_idx(table)
        return table
    
    def clean_card_data(self, table):
        '''Collects all methods needed to process the card data

        Args:
                table (pandas.Dataframe): The table containing card data to be cleaned
        
           Returns:
                pandas.Dataframe 
        '''
        table = transformer.convert_types(table)
        table = transformer.remove_null_values(table)
        table = transformer.remove_nonsense_values(table)
        table = transformer.clean_card_numbers(table)
        table = transformer.clean_dates(table)
        table = transformer.reset_idx(table)
        return table
    
    def clean_store_data(self, table):
        '''collects all methods needed to process the store data

        Args:
                table (pandas.Dataframe): The table containing store data to be cleaned
        
           Returns:
                pandas.Dataframe 
        '''
        table = transformer.convert_types(table)
        table = transformer.remove_null_values(table)
        table = transformer.remove_nonsense_values(table)
        table = transformer.clean_addresses(table)
        table = transformer.clean_dates(table)
        table = transformer.clean_staff_numbers(table)
        # the next two methods deal with problems specific to the store details dataframe
        table.replace({"continent": {"eeEurope": "Europe", "eeAmerica": "America"}}, inplace=True)
        table.drop(["lat"], axis=1, inplace=True)
        table = transformer.reset_idx(table)
        return table
    
    def clean_product_data(self, table):
        '''collects all methods needed to process the product data

        Args:
                table (pandas.Dataframe): The table containing product data to be cleaned
        
           Returns:
                pandas.Dataframe 
        '''
        table.dropna(inplace=True)
        table = transformer.convert_types(table)
        table = transformer.remove_nonsense_values(table)
        table = transformer.convert_product_weights(table)
        table = transformer.reset_idx(table)
        return table
    
    def clean_order_data(self, table):
        '''collects all methods needed to process the order data

        Args:
                table (pandas.Dataframe): The table containing order data to be cleaned
        
           Returns:
                pandas.Dataframe 
        '''
        table = transformer.convert_types(table)
        table.drop(labels=["first_name", "last_name", "1"], axis=1, inplace=True)
        table = transformer.clean_card_numbers(table)
        table = transformer.reset_idx(table)
        return table
    
    def clean_date_data(self, table):
        '''collects all methods needed to process the date data

        Args:
                table (pandas.Dataframe): The table containing date data to be cleaned
        
           Returns:
                pandas.Dataframe 
        '''
        table = transformer.convert_types(table)
        table = transformer.remove_null_values(table)
        table = transformer.remove_nonsense_values(table)
        table = transformer.reset_idx(table)
        return table
