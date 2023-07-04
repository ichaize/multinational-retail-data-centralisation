ALTER TABLE orders_table
	ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid

-- do the above for all columns to be converted to UUID

SELECT MAX(CHAR_LENGTH(store_code)) 
FROM orders_table

ALTER TABLE orders_table
	ALTER COLUMN store_code TYPE VARCHAR(12)

-- follow the aboe procedure for all columns to be converted to VARCHAR(?)

ALTER TABLE dim_store_details
	ALTER COLUMN longitude TYPE FLOAT USING longitude::double precision;

-- do the above for all columns to be converted to FLOAT

ALTER TABLE dim_store_details
	ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::smallint;

-- do the above for all columns to be converted to SMALLINT

ALTER TABLE dim_store_details
	ALTER COLUMN opening_date TYPE DATE;

-- do the above for all columns to be converted to DATE

UPDATE dim_products
    SET product_price = REPLACE(product_price, '£', '') 

ALTER TABLE dim_products
    ADD weight_class TEXT

UPDATE dim_products 
	SET weight_class = CASE
		WHEN weight < 2 THEN 'Light'
		WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
		WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
		ELSE 'Truck_Required'
		END

UPDATE dim_products 
	SET still_available = CASE
		WHEN still_available = 'Still_avaliable' THEN 'true'
		ELSE 'false'
		END		

ALTER TABLE dim_store_details
    ADD PRIMARY KEY (store_code)

-- follow the above procedure for all primary keys

ALTER TABLE orders_table    
    ADD CONSTRAINT orders_stores FOREIGN KEY (store_code) REFERENCES dim_store_details (store_code)

-- follow the above procedure for all foreign keys