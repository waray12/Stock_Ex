-- Table created to hold data from stock_exchange_data.csv (Public data accessed from Kaggle for project use). --
create table                                     
stock_exchange_data(
    exchange_id INT PRIMARY KEY AUTO_INCREMENT,
    ticker_index VARCHAR(10),
    observation_date DATE,
    opening_price DECIMAL(10, 2),
    highest_price DECIMAL(10, 2),
    lowest_price DECIMAL(10, 2),
    closing_price DECIMAL(10, 2),
    adjusted_closing_price DECIMAL(10, 2),
    volume_of_shares INT 
);

CREATE TABLE


SELECT *
FROM stock_exchange_data;

describe stock_exchange_data; 
-------------------------------------------------------------------------------------------------------------------
-- METHOD OF DATA TRANSFER (VIA TERMINAL): --
SET GLOBAL local_infile = 1;

USE course;

LOAD DATA LOCAL INFILE '/Users/----ray/Desktop/stock_exchange_data.csv'
INTO TABLE stock_exchange_data
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(ticker_index, observation_date, opening_price, highest_price, lowest_price, closing_price, adjusted_closing_price, volume_of_shares);

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--BASIC DATA RETRIEVAL, FILTERING & CONDITIONS, AGGREGATION & GROUPING, SORTING & LIMITING, JOINING & SUBQUERIES, HANDLING INVALID DATA, DATE HANDLING, 
--FUNCTIONS, DATA CLEANUP/PREPARATION

-- Select first 10 rows only --
SELECT * 
from stock_exchange_data
LIMIT 10;

-- Retrieve all rows where observation date was in 2005 --
SELECT *
from stock_exchange_data
where obs_date like '2005%';

-- show how many exchanges where the volume of shares was more than 1,000,000 --
SELECT COUNT(exchange_id)
from stock_exchange_data
where vol_of_shares > (1000000);

-- Find all records where the opening price is between $50 and $150 (inclusive and in descending order) --
select * 
from stock_exchange_data
where open_price
BETWEEN 50 AND 150
ORDER BY open_price DESC;

-- List all records where the ticker is either "GDAXI", "SSMI", or "N225" --
SELECT * 
from stock_exchange_data
where ticker_index IN(
'GDAXI', 'SSMI', 'N225'
) 
ORDER BY adj_close_price DESC;
    
-- show all unique ticker indexes by name --
SELECT DISTINCT ticker_index
from  stock_exchange_data;

--Highest closing price for each ticker --
SELECT MAX(close_price) as highest_closing_price
from stock_exchange_data
GROUP BY ticker_index;

-- The total volume of shares traded for each ticker --
SELECT SUM(vol_of_shares)
from stock_exchange_data
GROUP BY ticker_index;

-- Calculate the average highest price and lowest price for each ticker symbol --
SELECT ticker_index,
    AVG(high_price) AS avg_highest_price,
    AVG(low_price) AS avg_lowest_price
FROM stock_exchange_data
GROUP BY ticker_index;

-- Find the ticker with the highest adjusted closing price over the entire dataset --
SELECT exchange_id, ticker_index, MAX(adj_close_price) AS max_adj_close_price
FROM stock_exchange_data
GROUP BY exchange_id, ticker_index
ORDER BY ticker_index DESC
LIMIT 1;

-- find ticker index with highest average closing price over the whole dataset --
SELECT ticker_index, AVG(close_price) AS avg_close_price
FROM stock_exchange_data
group by ticker_index
order by avg_close_price DESC
LIMIT 1;

-- Order the data by observation date in descending order --
SELECT *
FROM stock_exchange_data
ORDER BY obs_date DESC; 

-- Get the 5 records with the lowest opening price, ordered by ascending --
SELECT *
FROM stock_exchange_data
ORDER BY open_price ASC
LIMIT 5; 

-- Find the stock with the highest closing price --
SELECT exchange_id, ticker_index, close_price AS highest_closing_price
FROM stock_exchange_data
ORDER BY close_price DESC
LIMIT 1;

-- List the ticker, observation_date, and closing_price for all records where the opening price is greater than the closing price -- 
SELECT ticker_index, obs_date, close_price, open_price
FROM stock_exchange_data
WHERE open_price > close_price;

-- Write a subquery to find the ticker with the lowest closing price (>0) in the dataset and return its exchange id, ticker index, observation date, closing price, and volume of shares -- 
SELECT exchange_id, ticker_index, obs_date, close_price, vol_of_shares
FROM stock_exchange_data
WHERE close_price = (
    select MIN(close_price)
    from stock_exchange_data
    where close_price > 0
);

-- The standard deviation of the closing prices --
SELECT STDDEV(close_price) AS closing_price_stddev
FROM stock_exchange_data;

-- Find records where the opening price is greater than 10% of the closing price --
SELECT *
FROM stock_exchange_data
WHERE open_price > close_price * 1.1
ORDER BY open_price DESC;

-- Update the volume of shares column to set any NULL values to 0 --
UPDATE stock_exchange_data 
SET vol_of_shares = 0 
WHERE vol_of_shares IS NULL;

-- Find the percentage change between opening price and closing price for each record, and list the 10 biggest positive changes -- 
SELECT 
exchange_id,
ticker_index,
obs_date,
open_price,
close_price,
ROUND(((close_price - open_price) / open_price) * 100, 2) AS percent_change 
FROM stock_exchange_data
WHERE open_price > 0 
ORDER BY percent_change DESC
LIMIT 10; 

-- For each ticker, calculate the total volume of shares traded over time and rank them from highest to lowest -- 
SELECT SUM(vol_of_shares), exchange_id, ticker_index
FROM stock_exchange_data
GROUP BY ticker_index, exchange_id
ORDER BY vol_of_shares DESC;

-------------------------------------------------------------------------------------------------------------------





