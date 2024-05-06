# ETL Assignment: Stored Procedure

## Objective:
Design a stored procedure to transfer data from a transactional database to a staging area, then load it into a data warehouse (DWH), distinguishing between fact and dimension tables.

## Requirements:
- Basic understanding of SQL
- Access to a database management system (DBMS) such as MySQL, PostgreSQL, SQL Server, etc.

## Task:
Design a stored procedure named `TransferDataToDWH` to perform the following tasks:
1. Transfer data from the transactional database tables to corresponding staging tables.
2. Load data from staging tables into appropriate dimension tables in the data warehouse.
3. Load data from staging tables into appropriate fact tables in the data warehouse.
4. Implement basic error handling and transaction management.
5. Provide clear documentation within the stored procedure.

## Steps to Follow:
1. **Analysis**: Analyze the structure of the transactional database and identify the tables to be transferred.
2. **Design**: Design the structure of staging, dimension, and fact tables in the data warehouse.
3. **Development**: Develop the stored procedure based on the provided template and your design.
4. **Testing**: Test the stored procedure with sample data to ensure its functionality and reliability.
5. **Documentation**: Document the stored procedure explaining its purpose, parameters, and steps involved.
6. **Submission**: Submit the SQL script containing the stored procedure along with documentation.

## Example SQL Script:
```sql
-- Create the stored procedure
CREATE PROCEDURE TransferDataToDWH ()
BEGIN
    -- Step 1: Transfer data from transactional database to staging area
    -- INSERT INTO staging_table SELECT * FROM transactional_table;

    -- Step 2: Load data into dimension tables in the data warehouse
    -- INSERT INTO dwh.dim_product (product_id, product_name, ...) SELECT product_id, product_name, ... FROM staging_table;

    -- Step 3: Load data into fact tables in the data warehouse
    -- INSERT INTO dwh.fact_sales (transaction_id, product_id, quantity, amount, ...) SELECT transaction_id, product_id, quantity, amount, ... FROM staging_table;

    -- Step 4: Data Management (Optional)
    -- Perform any data management tasks like data cleaning, transformation, or aggregation
    
    -- Step 5: Clean-up
    -- Optionally, clean up staging data after loading to DWH to save space
    -- DELETE FROM staging_table;

    -- Step 6: Logging and Error Handling (Optional)
    -- Log successful execution or handle errors gracefully
    
    -- Step 7: Commit transaction
    -- COMMIT;
    
    -- Step 8: Error Handling (Optional)
    -- Rollback transaction if an error occurs
    -- BEGIN
    --    -- Handle errors or exceptions
    --    ROLLBACK;
    -- END;
END;
```

## Submission Guidelines:
- Submit the SQL script containing the stored procedure.
- Include documentation explaining the purpose, parameters, and steps of the stored procedure.
- Ensure clarity and completeness in your submission.

## Evaluation Criteria:
- Correctness and completeness of the stored procedure.
- Documentation quality and clarity.
- Adherence to best practices in SQL development.
