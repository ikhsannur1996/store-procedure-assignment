CREATE OR REPLACE PROCEDURE dwh.generate_sales()
LANGUAGE plpgsql
AS $procedure$
BEGIN
    -- Step 1: Truncate and Insert into stg
    -- Description: Clear the staging table and populate it with data from the source table.
    CREATE TABLE IF NOT EXISTS stg.stg_sales_transaction AS SELECT *, CURRENT_TIMESTAMP AS last_update FROM public.sales_transaction;
    TRUNCATE TABLE stg.stg_sales_transaction;
    INSERT INTO stg.stg_sales_transaction 
    SELECT *, CURRENT_TIMESTAMP AS last_update FROM public.sales_transaction;
 
    -- Step 2: Insert or update into dim_product
    -- Description: Insert new product data into the product dimension if they don't already exist.
    --              Update existing product data if they already exist.
    -- Answer: 
    
    -- Step 3: Insert or update into dim_store
    -- Description: Insert new store data into the store dimension if they don't already exist.
    --              Update existing store data if they already exist.
    -- Answer: 

    
    -- Step 4: Insert or update into dim_time
    -- Description: Insert new time data into the time dimension if they don't already exist.
    --              Update existing time data if they already exist.
    -- Answer: 

    
    -- Step 5: Insert or update into dim_sales_name
    -- Description: Insert new sales name data into the sales name dimension if they don't already exist.
    --              Update existing sales name data if they already exist.
    -- Answer: 

    
    -- Step 6: Insert into fact_sales
    -- Description: Insert new sales data into the fact table if they don't already exist.
   -- Answer: 

    -- Step 7: Truncate and Insert into dm_sales_transaction
    -- Description: Populate the data mart with the latest sales transactions.
    CREATE OR REPLACE VIEW dm.vw_dm_sales_transaction AS SELECT 
        fs.sale_id,
        fs.store_id,
        ds.store_name,
        ds.city,
        ds.state,
        ds.country,
        fs.sales_name_id,
        dns.sales_name,
        dns.sales_age,
        dns.sales_gender,
        fs.time_id,
        dt.date,
        dt.day_of_week,
        dt.month,
        dt.year,
        fs.product_id,
        dp.product_name,
        dp.category,
        fs.quantity,
        fs.price,
        fs.last_update
    FROM 
        dwh.fact_sales fs
    LEFT JOIN 
        dwh.dim_store ds ON fs.store_id = ds.store_id
    LEFT JOIN 
        dwh.dim_sales_name dns ON fs.sales_name_id = dns.sales_name_id
    LEFT JOIN 
        dwh.dim_time dt ON fs.time_id = dt.time_id
    LEFT JOIN 
        dwh.dim_product dp ON fs.product_id = dp.product_id;
    
    CREATE TABLE IF NOT EXISTS dm.dm_sales_transaction AS
    SELECT * 
    FROM (
        SELECT *, 
               ROW_NUMBER() OVER (PARTITION BY sale_id ORDER BY last_update DESC) AS flag_unique
        FROM dm.vw_dm_sales_transaction
    ) AS ranked_data
    WHERE flag_unique = 1;
    
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
