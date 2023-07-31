from data_cleaning import DataCleaning
from database_utils import DatabaseConnector
from data_extraction import DataExtractor

rds_extractor = DataExtractor("rds_db_creds.yaml")
cleaner = DataCleaning()
rds_connector = DatabaseConnector("rds_db_creds.yaml")
local_connector = DatabaseConnector("local_db_creds.yaml")

header_dict = {"x-api-key":"yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"

# user_data = rds_extractor.read_rds_table(rds_connector, "legacy_users")
# card_data = rds_extractor.retrieve_pdf_data(pdf_link)
# store_data = rds_extractor.retrieve_stores_data(f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details", header_dict)
# product_data = rds_extractor.extract_from_s3("s3://data-handling-public/products.csv")
# order_data = rds_extractor.read_rds_table(rds_connector, "orders_table")
# date_data = rds_extractor.extract_from_s3("https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json")

# number_of_stores = extractor.list_number_of_stores("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores", header_dict)

# cleaned_user_data = cleaner.clean_user_data(user_data)
# cleaned_card_data = cleaner.clean_card_data(card_data)
# cleaned_store_data = cleaner.clean_store_data(store_data)
# cleaned_product_data = cleaner.clean_product_data(product_data) 
# cleaned_order_data = cleaner.clean_order_data(order_data)
# cleaned_date_data = cleaner.clean_date_data(date_data)

# local_connector.upload_to_db(cleaned_user_data, "dim_users")
# local_connector.upload_to_db(cleaned_card_data, "dim_user_details")
# local_connector.upload_to_db(cleaned_store_data, "dim_store_details")
# local_connector.upload_to_db(cleaned_product_data, "dim_products")
# local_connector.upload_to_db(cleaned_order_data, "orders_table")
# local_connector.upload_to_db(cleaned_date_data, "dim_date_times")