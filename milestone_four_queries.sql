-- find number of stores per country
SELECT country_code,
	   COUNT(DISTINCT(store_code)) AS num_stores
FROM dim_store_details
GROUP BY country_code
ORDER BY num_stores DESC

-- find localities with 10 or more stores:
SELECT locality, num_stores
FROM (
	  SELECT locality,
	  COUNT(DISTINCT(store_code)) AS num_stores
	  FROM dim_store_details
	  GROUP BY locality) AS number_of_stores
WHERE num_stores > 9
ORDER BY num_stores DESC

-- find months with highest total sale amount:
WITH sale_amount AS (
		SELECT ROUND(CAST(product_price * product_quantity AS numeric), 2) AS order_amount,
		       date_uuid
		FROM dim_products
		JOIN orders_table 
		ON dim_products.product_code = orders_table.product_code
		)
SELECT SUM(order_amount) AS sales_per_month,
	   month
FROM sale_amount
JOIN dim_date_times
ON dim_date_times.date_uuid = sale_amount.date_uuid
GROUP BY month
ORDER BY sales_per_month DESC

-- find number of sales and products sold per store:
WITH store_sales AS (
	SELECT COUNT(DISTINCT(date_uuid)) AS number_of_store_sales,
		   SUM(product_quantity) AS store_total_products_sold,
		   store_code
	FROM orders_table
	GROUP BY store_code)
SELECT SUM(number_of_store_sales) AS number_of_sales, 
	   SUM(store_total_products_sold) AS product_quantity_count, 
	   CASE WHEN store_type = 'Web Portal' THEN 'Web'
	   	    ELSE 'Offline'
	   		END AS location
FROM store_sales
JOIN dim_store_details ON store_sales.store_code = dim_store_details.store_code
GROUP BY location
ORDER BY number_of_sales 

-- find total sales per store as percentage of total sales
-- two methods to find percentage: cross join and sub-select
-- CROSS JOIN:
WITH sale_amount AS (
		SELECT ROUND(CAST(product_price * product_quantity AS numeric), 2) AS order_price,
		       store_code
		FROM dim_products
		JOIN orders_table 
		ON dim_products.product_code = orders_table.product_code
		),
	 sales_per_store AS (
		SELECT SUM(order_price) AS total_sales,
	    	   store_type
		FROM sale_amount
		JOIN dim_store_details
		ON sale_amount.store_code = dim_store_details.store_code
		GROUP BY store_type
		)
SELECT store_type, total_sales, total_sales*100 / t.s AS percentage_total
FROM sales_per_store
CROSS JOIN (SELECT SUM(total_sales) AS s FROM sales_per_store) AS t
ORDER BY percentage_total DESC

-- SUB-SELECT:
WITH sale_amount AS (
		SELECT ROUND(CAST(product_price * product_quantity AS numeric), 2) AS order_price,
		       store_code
		FROM dim_products
		JOIN orders_table 
		ON dim_products.product_code = orders_table.product_code
		),
	 sales_per_store AS (
		SELECT SUM(order_price) AS total_sales,
	    	   store_type
		FROM sale_amount
		JOIN dim_store_details
		ON sale_amount.store_code = dim_store_details.store_code
		GROUP BY store_type
		)
SELECT store_type, total_sales, total_sales*100 / (SELECT SUM(total_sales) FROM sales_per_store) AS percentage_total
FROM sales_per_store
ORDER BY percentage_total DESC

-- find 10 most profitable months historically:
WITH sale_amount AS (
		SELECT ROUND(CAST(product_price * product_quantity AS numeric), 2) AS order_amount,
		       date_uuid
		FROM dim_products
		JOIN orders_table 
		ON dim_products.product_code = orders_table.product_code
		)
SELECT SUM(order_amount) AS total_sales,
	   year,
	   month
FROM sale_amount
JOIN dim_date_times
ON dim_date_times.date_uuid = sale_amount.date_uuid
GROUP BY year, month
ORDER BY total_sales DESC
LIMIT 10

-- find staff numbers per country
SELECT SUM(staff_numbers) AS total_staff_numbers,
	   country_code
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC

-- find most profitable store type in Germany:
WITH german_stores AS (
		SELECT * FROM dim_store_details
		WHERE country_code = 'DE'),
	 sale_amount AS (
		SELECT ROUND(CAST(product_price * product_quantity AS numeric), 2) AS order_price,
		       store_code
		FROM dim_products
		JOIN orders_table 
		ON dim_products.product_code = orders_table.product_code
		)
SELECT SUM(order_price) AS total_sales,
	    store_type,
		country_code
FROM sale_amount
JOIN german_stores
ON sale_amount.store_code = german_stores.store_code
GROUP BY store_type, country_code
ORDER BY total_sales 

-- alternative to see most profitable store type in Germany:
SELECT SUM(ROUND(CAST(product_price * product_quantity AS numeric), 2)) AS order_price,
	   dim_store_details.country_code,
	   dim_store_details.store_type
FROM dim_products
JOIN orders_table 
ON dim_products.product_code = orders_table.product_code
JOIN dim_store_details
ON dim_store_details.store_code = orders_table.store_code
WHERE dim_store_details.country_code = 'DE'
GROUP BY dim_store_details.store_type, dim_store_details.country_code

-- get average interval between sales grouped by year
WITH full_dates AS (
			SELECT CONCAT(year, '-', month, '-', day, ' ', timestamp) AS full_date,
				   date_uuid
			FROM dim_date_times),
	 next_sales AS (
			SELECT year,
	   			   full_dates.full_date,
	   			   LEAD(full_dates.full_date, 1) OVER (
 	   			   ORDER BY full_dates.full_date) AS next_sale
			FROM dim_date_times
			JOIN full_dates ON dim_date_times.date_uuid = full_dates.date_uuid
			ORDER BY full_date)
SELECT year,
	   AVG(TO_TIMESTAMP(next_sale, 'YYYY-MM-DD HH24:MI:SS') - TO_TIMESTAMP(full_date, 'YYYY-MM-DD HH24:MI:SS')) as avg_interval
FROM next_sales
GROUP BY year
ORDER BY avg_interval DESC



