-- BAKEHOUSE
-- 1. List all distinct products sold, along with their total quantity sold.
SELECT product, SUM(quantity) total_quantity_sold FROM bakehouse.sales_transactions bst
GROUP BY product
ORDER BY total_quantity_sold DESC

-- 2. Find the top 5 franchises by total revenue.
SELECT bsf.name franchise, SUM(bst.totalPrice) total_revenue FROM bakehouse.sales_transactions bst JOIN bakehouse.sales_franchises bsf ON bst.franchiseID = bsf.franchiseID 
GROUP BY bst.franchiseID, bsf.name
ORDER BY total_revenue DESC
LIMIT 5

-- 3. Count the number of customers per city.
SELECT bsc.city city, count(bsc.customerID) total_customers from bakehouse.sales_customers bsc
GROUP BY bsc.city
ORDER BY total_customers DESC, city ASC

--4 Find the average transaction amount per product.
SELECT product, ROUND(AVG(bst.totalPrice),2) avg_price FROM bakehouse.sales_transactions bst
GROUP BY product
ORDER BY avg_price DESC

--5. Find month-over-month total sales trend (use date_trunc on the transaction date).
SELECT date_trunc('month', bst.dateTime) month, SUM(bst.totalPrice) total_sales 
FROM bakehouse.sales_transactions bst
GROUP BY date_trunc('month', bst.dateTime)
ORDER BY month

--6. Join sales_transactions with sales_returns to find the return rate (returns/total sales) per product.
--! No sales_returns table in the bakehouse database, so this query cannot be completed.


--7. Identify the top 3 best-selling products per franchise using a window function (RANK() or ROW_NUMBER())
WITH product_sales AS (
  SELECT 
    franchiseID,
    product,
    SUM(totalPrice) AS total_sales,
    SUM(quantity) AS total_quantity
  FROM bakehouse.sales_transactions
  GROUP BY franchiseID, product
),
ranked_products AS (
  SELECT 
    franchiseID,
    product,
    total_sales,
    total_quantity,
    ROW_NUMBER() OVER (PARTITION BY franchiseID ORDER BY total_sales DESC) AS sales_rank
  FROM product_sales
)
SELECT 
  franchiseID,
  product,
  total_sales,
  total_quantity,
  sales_rank
FROM ranked_products
WHERE sales_rank <= 3
ORDER BY franchiseID, sales_rank;

--8. Find customers who made purchases but never left a review (anti-join between sales_customers and media_customer_reviews).
--! No customer_id in media_customer_reviews table in the bakehouse database, so this query cannot be completed.

--9. Calculate the average review rating per product and compare it against sales volume — is there a correlation?
--! No review ratings or product information on the in the bakehouse database, so this query cannot be completed.  

--10. Compute a running (cumulative) total of sales per franchise ordered by date, using a window function.
SELECT 
  franchiseID,
  dateTime,
  totalPrice,
  SUM(totalPrice) OVER (
    PARTITION BY franchiseID 
    ORDER BY dateTime 
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS running_total_sales
FROM bakehouse.sales_transactions
ORDER BY franchiseID, dateTime;

--11. Segment customers into spend tiers (e.g., High/Medium/Low) using NTILE() or CASE WHEN on total lifetime spend.
WITH customer_lifetime_spend AS (
  SELECT 
    customerID,
    SUM(totalPrice) AS total_lifetime_spend,
    COUNT(*) AS transaction_count
  FROM bakehouse.sales_transactions
  GROUP BY customerID
),
customer_tiers AS (
  SELECT 
    customerID,
    total_lifetime_spend,
    transaction_count,
    NTILE(3) OVER (ORDER BY total_lifetime_spend DESC) AS spend_tier_number
  FROM customer_lifetime_spend
)
SELECT 
  customerID,
  total_lifetime_spend,
  transaction_count,
  CASE 
    WHEN spend_tier_number = 1 THEN 'High'
    WHEN spend_tier_number = 2 THEN 'Medium'
    WHEN spend_tier_number = 3 THEN 'Low'
  END AS spend_tier
FROM customer_tiers
ORDER BY total_lifetime_spend DESC;


-- 12. Find each franchise's month with the highest sales ("best month") using QUALIFY + ROW_NUMBER().
WITH monthly_sales AS (
  SELECT 
    franchiseID,
    DATE_TRUNC('month', dateTime) AS month,
    SUM(totalPrice) AS monthly_sales,
    COUNT(*) AS transaction_count
  FROM bakehouse.sales_transactions
  GROUP BY franchiseID, DATE_TRUNC('month', dateTime)
)
SELECT 
  franchiseID,
  month,
  monthly_sales,
  transaction_count
FROM monthly_sales
QUALIFY ROW_NUMBER() OVER (PARTITION BY franchiseID ORDER BY monthly_sales DESC) = 1
ORDER BY monthly_sales DESC;


--13. Detect suppliers whose products have above-average return rates compared to the overall average.
--! "Unfortunately, the samples.bakehouse schema doesn't have a sales_returns table, and the sales_transactions table doesn't contain return information. 
--  Additionally, there's no direct link between suppliers and products in the current schema structure."


--NYC TAXI
--1. Find the average fare_amount and trip_distance overall.
SELECT AVG(nyt.trip_distance), AVG(nyt.fare) FROM samples.nyctaxi.trips nyt

--2. Count total trips per day.
SELECT DATE_TRUNC('day', nyt.tpep_pickup_datetime) AS day, COUNT(DATE_TRUNC('day', nyt.tpep_pickup_datetime)) AS count
FROM samples.nyctaxi.trips nyt
GROUP BY day
ORDER BY day

--3 Find the top 10 pickup zip codes by number of trips.
SELECT pickup_zip, COUNT(*) count
FROM samples.nyctaxi.trips nyt
GROUP BY pickup_zip
ORDER BY count DESC
LIMIT 10

--4. Find the longest and shortest trips by distance.
SELECT MAX(trip_distance) AS longest_trip, MIN(trip_distance) AS shortest_trip
FROM samples.nyctaxi.trips nyt

--5. Calculate average fare by hour of day (extract hour from tpep_pickup_datetime) to find peak pricing times.
SELECT EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour, AVG(fare_amount) AS avg_fare
FROM samples.nyctaxi.trips nyt
GROUP BY hour
ORDER BY avg_fare DESC 

--6. Compute trip duration (dropoff - pickup) and find its correlation with fare_amount.
WITH trip_durations AS (
  SELECT 
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    fare_amount,
    trip_distance,
    (UNIX_TIMESTAMP(tpep_dropoff_datetime) - UNIX_TIMESTAMP(tpep_pickup_datetime)) / 60.0 AS duration_minutes
  FROM samples.nyctaxi.trips
  WHERE tpep_dropoff_datetime > tpep_pickup_datetime
    AND fare_amount > 0
)
SELECT 
  CORR(duration_minutes, fare_amount) AS correlation_duration_fare,
  COUNT(*) AS total_trips,
  AVG(duration_minutes) AS avg_duration_minutes,
  AVG(fare_amount) AS avg_fare
FROM trip_durations;

--7. Find the busiest pickup zip → dropoff zip pairs (top routes by trip count)
SELECT 
  pickup_zip,
  dropoff_zip,
  COUNT(*) AS trip_count
FROM samples.nyctaxi.trips
WHERE pickup_zip IS NOT NULL 
  AND dropoff_zip IS NOT NULL
GROUP BY pickup_zip, dropoff_zip
ORDER BY trip_count DESC

--8. Flag anomalies: trips with trip_distance = 0 but fare_amount > 0, or very high fare-per-mile
WITH trip_metrics AS (
  SELECT 
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    trip_distance,
    fare_amount,
    pickup_zip,
    dropoff_zip,
    CASE 
      WHEN trip_distance > 0 THEN fare_amount / trip_distance 
      ELSE NULL 
    END AS fare_per_mile
  FROM samples.nyctaxi.trips
  WHERE fare_amount > 0
),
anomalies AS (
  SELECT 
    *,
    CASE 
      WHEN trip_distance = 0 AND fare_amount > 0 THEN 'Zero distance with positive fare'
      WHEN fare_per_mile > 50 THEN 'Very high fare-per-mile (>$50)'
      ELSE NULL
    END AS anomaly_type
  FROM trip_metrics
)
SELECT 
  anomaly_type,
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  trip_distance,
  fare_amount,
  fare_per_mile,
  pickup_zip,
  dropoff_zip
FROM anomalies
WHERE anomaly_type IS NOT NULL
ORDER BY 
  CASE WHEN anomaly_type = 'Zero distance with positive fare' THEN 1 ELSE 2 END,
  fare_per_mile DESC NULLS LAST
LIMIT 100;


--9. Compare weekday vs weekend average trip volume and fares.
WITH trip_day_classification AS (
  SELECT 
    tpep_pickup_datetime,
    fare_amount,
    trip_distance,
    DAYOFWEEK(tpep_pickup_datetime) AS day_of_week,
    CASE 
      WHEN DAYOFWEEK(tpep_pickup_datetime) IN (1, 7) THEN 'Weekend'  -- 1=Sunday, 7=Saturday
      ELSE 'Weekday'
    END AS day_type
  FROM samples.nyctaxi.trips
  WHERE fare_amount > 0
)
SELECT 
  day_type,
  COUNT(*) AS total_trips,
  AVG(fare_amount) AS avg_fare,
  AVG(trip_distance) AS avg_distance,
  SUM(fare_amount) AS total_revenue,
  MIN(fare_amount) AS min_fare,
  MAX(fare_amount) AS max_fare
FROM trip_day_classification
GROUP BY day_type
ORDER BY day_type;


--10. Use a window function to rank zip codes by daily trip count and find each day's top pickup zone.
WITH daily_zip_counts AS (
  SELECT 
    DATE(tpep_pickup_datetime) AS pickup_date,
    pickup_zip,
    COUNT(*) AS daily_trip_count,
    AVG(fare_amount) AS avg_fare,
    AVG(trip_distance) AS avg_distance
  FROM samples.nyctaxi.trips
  WHERE pickup_zip IS NOT NULL
  GROUP BY DATE(tpep_pickup_datetime), pickup_zip
),
ranked_zones AS (
  SELECT 
    pickup_date,
    pickup_zip,
    daily_trip_count,
    avg_fare,
    avg_distance,
    RANK() OVER (PARTITION BY pickup_date ORDER BY daily_trip_count DESC) AS zip_rank
  FROM daily_zip_counts
)
SELECT 
  pickup_date,
  pickup_zip AS top_pickup_zip,
  daily_trip_count,
  avg_fare,
  avg_distance
FROM ranked_zones
WHERE zip_rank = 1
ORDER BY pickup_date;

--11. Build an hourly heatmap query: trips grouped by hour and day_of_week (useful later for a chart).
SELECT 
  DAYOFWEEK(tpep_pickup_datetime) AS day_of_week,
  CASE DAYOFWEEK(tpep_pickup_datetime)
    WHEN 1 THEN 'Sunday'
    WHEN 2 THEN 'Monday'
    WHEN 3 THEN 'Tuesday'
    WHEN 4 THEN 'Wednesday'
    WHEN 5 THEN 'Thursday'
    WHEN 6 THEN 'Friday'
    WHEN 7 THEN 'Saturday'
  END AS day_name,
  HOUR(tpep_pickup_datetime) AS hour_of_day,
  COUNT(*) AS trip_count,
  AVG(fare_amount) AS avg_fare,
  AVG(trip_distance) AS avg_distance
FROM samples.nyctaxi.trips
GROUP BY DAYOFWEEK(tpep_pickup_datetime), HOUR(tpep_pickup_datetime)
ORDER BY day_of_week, hour_of_day;

--12. Calculate a rolling 7-day average of daily fare revenue using AVG() OVER (ORDER BY ... ROWS BETWEEN 6 PRECEDING AND CURRENT ROW).
WITH daily_revenue AS (
  SELECT 
    DATE(tpep_pickup_datetime) AS trip_date,
    SUM(fare_amount) AS daily_fare_revenue,
    COUNT(*) AS daily_trip_count,
    AVG(fare_amount) AS avg_fare
  FROM samples.nyctaxi.trips
  WHERE fare_amount > 0
  GROUP BY DATE(tpep_pickup_datetime)
)
SELECT 
  trip_date,
  daily_fare_revenue,
  daily_trip_count,
  avg_fare,
  AVG(daily_fare_revenue) OVER (
    ORDER BY trip_date 
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS rolling_7day_avg_revenue,
  COUNT(*) OVER (
    ORDER BY trip_date 
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS days_in_window
FROM daily_revenue
ORDER BY trip_date;

--13. Identify outlier trips using standard deviation (e.g., fares more than 3 std devs from the mean).
WITH fare_statistics AS (
  SELECT 
    AVG(fare_amount) AS mean_fare,
    STDDEV(fare_amount) AS stddev_fare
  FROM samples.nyctaxi.trips
  WHERE fare_amount > 0
),
trip_with_stats AS (
  SELECT 
    t.tpep_pickup_datetime,
    t.tpep_dropoff_datetime,
    t.trip_distance,
    t.fare_amount,
    t.pickup_zip,
    t.dropoff_zip,
    s.mean_fare,
    s.stddev_fare,
    (t.fare_amount - s.mean_fare) / s.stddev_fare AS z_score,
    ABS((t.fare_amount - s.mean_fare) / s.stddev_fare) AS abs_z_score
  FROM samples.nyctaxi.trips t
  CROSS JOIN fare_statistics s
  WHERE t.fare_amount > 0
)
SELECT 
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  trip_distance,
  fare_amount,
  pickup_zip,
  dropoff_zip,
  ROUND(mean_fare, 2) AS mean_fare,
  ROUND(stddev_fare, 2) AS stddev_fare,
  ROUND(z_score, 2) AS z_score,
  CASE 
    WHEN z_score > 3 THEN 'High outlier (>3σ)'
    WHEN z_score < -3 THEN 'Low outlier (<-3σ)'
  END AS outlier_type
FROM trip_with_stats
WHERE abs_z_score > 3
ORDER BY abs_z_score DESC
LIMIT 50;