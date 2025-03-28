---New SP
CREATE OR REPLACE PROCEDURE dwh.generate_sales()
LANGUAGE plpgsql
AS $procedure$
BEGIN
    -- Step 1: Truncate and Insert into Staging (stg)
    -- Description: Clear the staging table and populate it with data from the source table.
        -- Step 1: Truncate and Insert into stg
    -- Description: Clear the staging table and populate it with data from the source table.
    CREATE TABLE IF NOT EXISTS stg.stg_sales_transaction AS SELECT *, CURRENT_TIMESTAMP AS last_update FROM public.sales_transaction;
    TRUNCATE TABLE stg.stg_sales_transaction;
    INSERT INTO stg.stg_sales_transaction 
    SELECT *, CURRENT_TIMESTAMP AS last_update FROM public.sales_transaction;

    -- Step 2: Insert or Update into Dim_Product
    -- Description: Insert new product data into the product dimension if they don't already exist.
    --              Update existing product data if they already exist.
    CREATE TABLE IF NOT EXISTS dwh.dim_product (
        product_id INT PRIMARY KEY,
        product_name VARCHAR(255),
        product_category VARCHAR(50),
        last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    TRUNCATE TABLE dwh.dim_product;

    INSERT INTO dwh.dim_product (product_id, product_name, product_category, last_update)
    SELECT DISTINCT src.product_id, src.product_name, src.product_category, CURRENT_TIMESTAMP AS last_update
    FROM stg.stg_sales_transaction AS src;

    -- Step 3: Insert or Update into Dim_Store
    -- Description: Insert new store data into the store dimension if they don't already exist.
    --              Update existing store data if they already exist.
    CREATE TABLE IF NOT EXISTS dwh.dim_store (
        store_id INT PRIMARY KEY,
        store_name VARCHAR(255),
        store_phone VARCHAR(50),
        store_city VARCHAR(50),
        last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    TRUNCATE TABLE dwh.dim_store;

    INSERT INTO dwh.dim_store (store_id, store_name, store_phone, store_city, last_update)
    SELECT DISTINCT src.store_id, src.store_name, src.store_phone, src.store_city, CURRENT_TIMESTAMP AS last_update
    FROM stg.stg_sales_transaction AS src;

    -- Step 4: Insert or Update into Dim_User
    -- Description: Insert new user data into the user dimension if they don't already exist.
    --              Update existing user data if they already exist.
    CREATE TABLE IF NOT EXISTS dwh.dim_user (
        user_id INT PRIMARY KEY,
        user_name VARCHAR(255),
        user_email VARCHAR(255),
        user_phone VARCHAR(50),
        user_gender VARCHAR(50),
        user_age INT,
        user_city VARCHAR(50),
        last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    TRUNCATE TABLE dwh.dim_user;

    INSERT INTO dwh.dim_user (user_id, user_name, user_email, user_phone, user_gender, user_age, user_city, last_update)
    SELECT DISTINCT src.user_id, src.user_name, src.user_email, src.user_phone, src.user_gender, src.user_age, src.user_city, CURRENT_TIMESTAMP AS last_update
    FROM stg.stg_sales_transaction AS src;

    -- Step 5: Insert or Update into Dim_Time (using transaction_time)
    -- Description: Insert new time data into the time dimension if they don't already exist.
    --              Update existing time data if they already exist.
    CREATE TABLE IF NOT EXISTS dwh.dim_time (
        time_id INT PRIMARY KEY,
        date DATE,
        day_of_week VARCHAR(20),
        month INT,
        year INT,
        last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    TRUNCATE TABLE dwh.dim_time;

    INSERT INTO dwh.dim_time (time_id, date, day_of_week, month, year, last_update)
    SELECT DISTINCT 
        EXTRACT(EPOCH FROM transaction_time)::INT AS time_id,
        transaction_time::DATE AS date,
        TO_CHAR(transaction_time, 'Day') AS day_of_week,
        EXTRACT(MONTH FROM transaction_time) AS month,
        EXTRACT(YEAR FROM transaction_time) AS year,
        CURRENT_TIMESTAMP AS last_update
    FROM stg.stg_sales_transaction AS src;

    -- Step 6: Insert or Update into Fact_Sales
    -- Description: Insert new sales data into the fact table if it doesn't already exist.
    --              Use foreign keys to reference dimension tables.
    CREATE TABLE IF NOT EXISTS dwh.fact_sales (
        sale_id INT PRIMARY KEY,
        store_id INT,
        user_id INT,
        time_id INT,
        product_id INT,
        quantity INT,
        total_price FLOAT,
        payment_method VARCHAR(50),
        transaction_status VARCHAR(50),
        shipping_method VARCHAR(50),
        last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (store_id) REFERENCES dwh.dim_store(store_id),
        FOREIGN KEY (user_id) REFERENCES dwh.dim_user(user_id),
        FOREIGN KEY (time_id) REFERENCES dwh.dim_time(time_id),
        FOREIGN KEY (product_id) REFERENCES dwh.dim_product(product_id)
    );

    INSERT INTO dwh.fact_sales (sale_id, store_id, user_id, time_id, product_id, quantity, total_price, payment_method, transaction_status, shipping_method, last_update)
    SELECT
        src.transaction_id AS sale_id,
        src.store_id,
        src.user_id,
        (SELECT time_id FROM dwh.dim_time WHERE date = src.transaction_time::DATE) AS time_id,
        src.product_id,
        src.quantity,
        src.total_price,
        src.payment_method,
        src.transaction_status,
        src.shipping_method,
        CURRENT_TIMESTAMP AS last_update
    FROM stg.stg_sales_transaction AS src
    WHERE NOT EXISTS (
        SELECT 1
        FROM dwh.fact_sales AS fact
        WHERE
            fact.sale_id = src.transaction_id AND
            fact.store_id = src.store_id AND
            fact.user_id = src.user_id AND
            fact.time_id = (SELECT time_id FROM dwh.dim_time WHERE date = src.transaction_time::DATE) AND
            fact.product_id = src.product_id
    );

    -- Step 7: Truncate and Insert into Data Mart (dm)
    -- Description: Populate the data mart with the latest sales transactions.
    CREATE OR REPLACE VIEW dm.vw_dm_sales_transaction AS 
    SELECT 
        fs.sale_id,
        fs.store_id,
        ds.store_name,
        ds.store_city,
        fs.user_id,
        du.user_name,
        du.user_city,
        fs.time_id,
        dt.date,
        dt.day_of_week,
        dt.month,
        dt.year,
        fs.product_id,
        dp.product_name,
        dp.product_category,
        fs.quantity,
        fs.total_price,
        fs.payment_method,
        fs.transaction_status,
        fs.shipping_method,
        fs.last_update
    FROM 
        dwh.fact_sales fs
    LEFT JOIN 
        dwh.dim_store ds ON fs.store_id = ds.store_id
    LEFT JOIN 
        dwh.dim_user du ON fs.user_id = du.user_id
    LEFT JOIN 
        dwh.dim_time dt ON fs.time_id = dt.time_id
    LEFT JOIN 
        dwh.dim_product dp ON fs.product_id = dp.product_id;

    -- Step 8: Create Data Mart Table (dm_sales_transaction)
    CREATE TABLE IF NOT EXISTS dm.dm_sales_transaction AS
    SELECT * 
    FROM (
        SELECT *, 
               ROW_NUMBER() OVER (PARTITION BY sale_id ORDER BY last_update DESC) AS flag_unique
        FROM dm.vw_dm_sales_transaction
    ) AS ranked_data
    WHERE flag_unique = 1;

    -- Step 9: Truncate Data Mart Table and Insert Latest Data
    TRUNCATE TABLE dm.dm_sales_transaction;

    INSERT INTO dm.dm_sales_transaction 
    SELECT * 
    FROM (
        SELECT *, 
               ROW_NUMBER() OVER (PARTITION BY sale_id ORDER BY last_update DESC) AS flag_unique
        FROM dm.vw_dm_sales_transaction
    ) AS ranked_data
    WHERE flag_unique = 1;

END;
$procedure$;
