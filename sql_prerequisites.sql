-- get weekday from timestamp: 
SELECT TO_CHAR(timestamp_column, 'Day')
FROM table

-- count number of titles that have each combination of special features 
-- SELECT special_features, COUNT(title) as num_titles
-- FROM film
-- GROUP BY special_features

-- FIND MOST PROFITABLE DAYS
SELECT TO_CHAR(payment_date, 'Day') AS payment_day, 
	   SUM(amount) AS total_amount
FROM payment
GROUP BY payment_day
ORDER BY total_amount DESC

-- find total number of rentals and (from different table) total sale amount per day from timestamp
SELECT DATE(rental.rental_date) AS rental_day,
	   COUNT(rental.rental_id) AS rentals_per_day,
	   SUM(payment.amount) AS total_sales
FROM rental
JOIN payment ON payment.rental_id = rental.rental_id
GROUP BY rental_day
ORDER BY rentals_per_day DESC

-- GET CUSTOMER IDS FOR ALL CUSTOMERS WHO'VE SPENT OVER $100 DURING MEMBERSHIP
SELECT customer_id,
	   SUM(amount) AS total
FROM payment
GROUP BY customer_id
HAVING SUM(amount) > 100
ORDER BY total DESC

-- find average amount spent per film rating, connecting via multiple tables
SELECT film.rating,
	   AVG(payment.amount) AS average_amount
FROM film
JOIN inventory ON film.film_id = inventory.film_id
JOIN rental ON inventory.inventory_id = rental.inventory_id
JOIN payment ON  payment.rental_id = rental.rental_id
GROUP BY rating
ORDER BY average_amount

-- FIND FILMS THAT HAVE NOT BEEN RETURNED AND WHO RENTED THEM
SELECT return_date, title, customer.customer_id
FROM rental
JOIN customer ON customer.customer_id = rental.customer_id
JOIN inventory ON inventory.inventory_id = rental.inventory_id
JOIN film ON film.film_id = inventory.film_id
WHERE return_date IS NULL

-- FIND NUMBER OF COPIES OF SPECIFIC FILM IN INVENTORY SYSTEM
-- SELECT film.film_id, title, COUNT(inventory.inventory_id) AS num_copies
-- FROM film
-- JOIN inventory ON film.film_id = inventory.film_id
-- GROUP BY film.film_id
-- HAVING title = 'HUNCHBACK IMPOSSIBLE'

-- find total income per store (in other words the sum of the amounts associated with each address/manager name group combo)
SELECT address, first_name, last_name, SUM(amount) AS total_income
FROM address
JOIN store ON store.address_id = address.address_id
JOIN staff ON staff.staff_id = store.manager_staff_id
JOIN inventory ON inventory.store_id = store.store_id
JOIN rental ON inventory.inventory_id = rental.inventory_id
JOIN payment ON rental.rental_id = payment.rental_id
GROUP BY address, first_name, last_name

-- find all cities containing a resident who has spent over $150
SELECT city, SUM(amount) AS total_amount
FROM city
JOIN address ON city.city_id = address.city_id
JOIN customer ON address.address_id = customer.address_id
JOIN payment ON customer.customer_id = payment.customer_id
GROUP BY city
HAVING SUM(amount) > 150
ORDER BY city

-- count number of customers making low, medium, or high value transactions (displays num_cust and transaction_value)
SELECT COUNT(customer.customer_id) AS num_cust,
CASE
	WHEN payment.amount < 3 THEN 'low'
	WHEN payment.amount > 7 THEN 'high'
	ELSE 'medium'
	END AS transaction_value
FROM payment
JOIN customer ON payment.customer_id = customer.customer_id
GROUP BY transaction_value

-- find number of customers per store
SELECT store.store_id, COUNT(customer_id) AS total_customers
FROM store
JOIN customer ON store.store_id = customer.store_id
GROUP BY store.store_id

-- find earliest transaction for each customer including names and rental rates 
SELECT DISTINCT ON (customer.customer_id) customer.customer_id, first_name, last_name, rental_rate, DATE(rental_date)
FROM rental
JOIN customer ON rental.customer_id = customer.customer_id
JOIN inventory ON rental.inventory_id = inventory.inventory_id
JOIN film ON inventory.film_id = film.film_id
ORDER BY customer.customer_id, DATE(rental_date)
-- distinct on effectively groups by customer.customer_id without needing a GROUP BY clause, and so bypasses the problem
-- of wanting to display rental_rate but not wanting to include it in the GROUP BY clause (including it would give earliest
-- transaction per customer per rate, rather than earliest transaction per customer regardless of rate).
-- Effective solution for when you need to GROUP BY but want a bare column i.e. know the id of the largest value, etc

-- SUBQUERIES - remove the need for lots of join clauses
-- THIS QUERY:
SELECT actor.actor_id, first_name, last_name, actor.last_update
FROM actor
JOIN film_actor ON actor.actor_id = film_actor.actor_id
JOIN film ON film.film_id = film_actor.film_id
WHERE film.film_id = 2

-- DOES THE SAME THING AS THIS QUERY:
SELECT *
FROM actor
WHERE actor_id IN (
	SELECT actor_id
	FROM film_actor
	WHERE film_id = 2 -- i.e. a list of actors in film 2
	)

-- AND ALSO THIS QUERY:
SELECT *
FROM actor
WHERE actor_id IN (
	SELECT actor_id
	FROM film_actor
	WHERE film_id = (
		SELECT film_id FROM film WHERE title = 'ACE GOLDFINGER' -- i.e. where film_id = 2
	)
)

-- find average total amount spent by customer
SELECT AVG(a) FROM
(
SELECT customer_id, SUM(amount) AS a
FROM payment
GROUP BY customer_id
) AS totals;

-- alternatively, more readable/efficient form:
WITH total_amounts AS (
	(SELECT customer_id, SUM(amount) AS a
	FROM payment
	GROUP BY customer_id)
)
SELECT AVG(a)
FROM total_amounts

-- select all items that have the max value (can't use MAX in WHERE):
SELECT title,
       rental_duration,
       rental_rate
FROM 
    film
WHERE
    (rental_duration, rental_rate) IN
    (
        SELECT MAX(rental_duration),
               MAX(rental_rate)
        FROM
            film
    );

-- find all animations:
-- SELECT film_id,
-- 	   title,
-- 	   release_year
-- FROM film 
-- WHERE film_id IN (
-- 	SELECT film_id
-- 	FROM film_category
-- 	WHERE category_id IN (
-- 		SELECT category_id 
-- 		FROM category
-- 		WHERE name = 'Animation')
-- 	)

-- find all customers in canada:
SELECT first_name, last_name, email
FROM customer
WHERE address_id IN (
	SELECT address_id 
	FROM address
	WHERE city_id IN (
		SELECT city_id 
		FROM city
		WHERE country_id = (
			SELECT country_id 
			FROM country
			WHERE country = 'Canada')
	)
)

-- find average sales per day for each staff member:
SELECT AVG(average_amount) AS average_sale,
	   staff_id
FROM (SELECT AVG(amount) AS average_amount,
	  DATE(payment_date), 
	  staff_id
	  FROM payment
	  GROUP BY DATE(payment_date), staff_id)
	  AS average_payment_on_each_date
GROUP BY staff_id

-- UPDATE REPLACEMENT COST OF ALL FILMS MATCHING CERTAIN CRITERIA
UPDATE film
SET replacement_cost = film.replacement_cost - 1
FROM (
	SELECT
	   rating,
	   rental_duration,
	   rental_rate, 
   	   replacement_cost
	FROM film
	WHERE rating = 'R'
	AND rental_duration = 3
	AND rental_rate = 2.99 
	) AS films_to_update