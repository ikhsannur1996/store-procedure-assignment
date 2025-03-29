CREATE OR REPLACE PROCEDURE dwh.generate_ecommerce_transaction()
LANGUAGE plpgsql
AS $procedure$
BEGIN
    -- Step 1: Load Data into Staging (stg)
    CREATE TABLE IF NOT EXISTS stg.stg_ecommerce_transaction AS 
    SELECT *, CURRENT_TIMESTAMP AS last_update FROM public.ecommerce_transction;
    TRUNCATE TABLE stg.stg_ecommerce_transaction;
    INSERT INTO stg.stg_ecommerce_transaction 
    SELECT *, CURRENT_TIMESTAMP AS last_update FROM public.ecommerce_transction;

    -- Step 2: Load Data into Dim_Product
    CREATE TABLE IF NOT EXISTS dwh.dim_ecommerce_product (
        product_id INT PRIMARY KEY,
        product_name VARCHAR(255),
        product_category VARCHAR(50),
        product_price FLOAT4,
        last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    TRUNCATE TABLE dwh.dim_ecommerce_product;
    INSERT INTO dwh.dim_ecommerce_product (product_id, product_name, product_category, product_price, last_update)
    SELECT DISTINCT src.product_id, src.product_name, src.product_category, src.product_price, CURRENT_TIMESTAMP
    FROM stg.stg_ecommerce_transaction AS src;

    -- Step 3: Load Data into Dim_Store
    CREATE TABLE IF NOT EXISTS dwh.dim_ecommerce_store (
        store_id INT PRIMARY KEY,
        store_name VARCHAR(255),
        store_phone VARCHAR(50),
        store_city VARCHAR(50),
        last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    TRUNCATE TABLE dwh.dim_ecommerce_store;
    INSERT INTO dwh.dim_ecommerce_store (store_id, store_name, store_phone, store_city, last_update)
    SELECT DISTINCT src.store_id, src.store_name, src.store_phone, src.store_city, CURRENT_TIMESTAMP
    FROM stg.stg_ecommerce_transaction AS src;

    -- Step 4: Load Data into Dim_User
    CREATE TABLE IF NOT EXISTS dwh.dim_ecommerce_user (
        user_id INT PRIMARY KEY,
        user_name VARCHAR(255),
        user_email VARCHAR(255),
        user_phone VARCHAR(50),
        user_gender VARCHAR(50),
        user_age INT,
        user_city VARCHAR(50),
        last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    TRUNCATE TABLE dwh.dim_ecommerce_user;
    INSERT INTO dwh.dim_ecommerce_user (user_id, user_name, user_email, user_phone, user_gender, user_age, user_city, last_update)
    SELECT DISTINCT src.user_id, src.user_name, src.user_email, src.user_phone, src.user_gender, src.user_age, src.user_city, CURRENT_TIMESTAMP
    FROM stg.stg_ecommerce_transaction AS src;

    -- Step 5: Load Data into Fact_Sales
    CREATE TABLE IF NOT EXISTS dwh.fact_ecommerce_transaction (
        sale_id INT PRIMARY KEY,
        transaction_time TIMESTAMP,
        store_id INT,
        user_id INT,
        product_id INT,
        quantity INT,
        total_price FLOAT,
        payment_method VARCHAR(50),
        transaction_status VARCHAR(50),
        shipping_method VARCHAR(50),
        last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    INSERT INTO dwh.fact_ecommerce_transaction (sale_id, transaction_time, store_id, user_id, product_id, quantity, total_price, payment_method, transaction_status, shipping_method, last_update)
    SELECT
        src.transaction_id AS sale_id,
        src.transaction_time,
        src.store_id,
        src.user_id,
        src.product_id,
        src.quantity,
        src.total_price,
        src.payment_method,
        src.transaction_status,
        src.shipping_method,
        CURRENT_TIMESTAMP AS last_update
    FROM stg.stg_ecommerce_transaction AS src
    WHERE NOT EXISTS (
        SELECT 1 FROM dwh.fact_ecommerce_transaction AS fact
        WHERE fact.sale_id = src.transaction_id
    );

    -- Step 6: Create Data Mart View (vw_dm_cube_ecommerce_transaction)
    CREATE OR REPLACE VIEW dm.vw_dm_cube_ecommerce_transaction AS 
    SELECT 
        fs.sale_id,
        fs.transaction_time,
        fs.store_id,
        ds.store_name,
        ds.store_city,
        fs.user_id,
        du.user_name,
        du.user_city,
        fs.product_id,
        dp.product_name,
        dp.product_category,
        dp.product_price,
        fs.quantity,
        fs.total_price,
        fs.payment_method,
        fs.transaction_status,
        fs.shipping_method,
        fs.last_update
    FROM 
        dwh.fact_ecommerce_transaction fs
    LEFT JOIN 
        dwh.dim_ecommerce_store ds ON fs.store_id = ds.store_id
    LEFT JOIN 
        dwh.dim_ecommerce_user du ON fs.user_id = du.user_id
    LEFT JOIN 
        dwh.dim_ecommerce_product dp ON fs.product_id = dp.product_id;

    -- Step 7: Ccreate Data Mart Cube Table
    CREATE TABLE IF NOT EXISTS dm.dm_cube_ecommerce_transaction AS
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY sale_id ORDER BY last_update DESC) AS flag_unique
        FROM dm.vw_dm_cube_ecommerce_transaction
    ) AS ranked_data
    WHERE flag_unique = 1;

    -- Step 8: Refresh Data in Data Mart Cube Table
    -- Description: Remove old data and insert the latest transactions.
    TRUNCATE TABLE dm.dm_cube_ecommerce_transaction;
    INSERT INTO dm.dm_cube_ecommerce_transaction 
    SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY sale_id ORDER BY last_update DESC) AS flag_unique
        FROM dm.vw_dm_cube_ecommerce_transaction
    ) AS ranked_data
    WHERE flag_unique = 1;

    -- Step 9: Create Data Mart for Most Transactions by Date
    CREATE TABLE IF NOT EXISTS dm.dm_most_transaction_date AS 
    SELECT transaction_time::DATE AS transaction_date, COUNT(*) AS total_transactions
    FROM dm.dm_cube_ecommerce_transaction
    GROUP BY transaction_time::DATE
    ORDER BY total_transactions DESC;
    TRUNCATE TABLE dm.dm_most_transaction_date;
    INSERT INTO dm.dm_most_transaction_date 
    SELECT transaction_time::DATE, COUNT(*)
    FROM dm.dm_cube_ecommerce_transaction
    GROUP BY transaction_time::DATE
    ORDER BY COUNT(*) DESC;

    -- Step 10: ----
    -- Step 11: ----
    -- Step 12: ----

END;
$procedure$;
