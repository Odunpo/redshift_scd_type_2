CREATE SCHEMA IF NOT EXISTS scd;



-- Creating a new table customers
DROP TABLE IF EXISTS scd.customers;
CREATE TABLE scd.customers (
    c_pk INT,
    c_nk INT,
    name TEXT,
    rec_start_date TIMESTAMP,
    rec_end_date TIMESTAMP,
    PRIMARY KEY(c_pk)
);



-- Inserting some initial data
INSERT INTO scd.customers VALUES
    (1, 1, 'John0', SYSDATE - INTERVAL '3 hours', SYSDATE - INTERVAL '2 hours'),
    (2, 1, 'John1', SYSDATE - INTERVAL '2 hours', TO_TIMESTAMP('2100-01-01', 'YYYY-MM-DD'));

-- current view of customers table:

-- c_pk  c_nk   name       rec_start_date         rec_end_date
--    1     1  John0  2023-10-25 01:00:00  2023-10-25 02:00:00
--    2     1  John1  2023-10-25 02:00:00  2100-01-01 00:00:00



-- Creating a staging customers table - the table for loading data from the source
DROP TABLE IF EXISTS scd.stg_customers;
CREATE TABLE scd.stg_customers (
    id INT,
    name TEXT,
    updated_at TIMESTAMP
);



-- Inserting some data from the source
INSERT INTO scd.stg_customers VALUES
    (1, 'John2', SYSDATE - INTERVAL '1 hour'),
    (1, 'John3', SYSDATE);

-- current view of stg_customers table:

-- id   name           updated_at
--  1  John2  2023-10-25 03:00:00
--  1  John3  2023-10-25 04:00:00



BEGIN TRANSACTION;

-- Creating a temporary table for customers transformations
DROP TABLE IF EXISTS scd.temp_customers;
CREATE TABLE scd.temp_customers (LIKE scd.customers, ver_id INT);


INSERT INTO scd.temp_customers (



    -- Adding change version to stg_customers to be able to reflect each change
    WITH ranked AS (
        SELECT
            id,
            name,
            updated_at,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at) AS ver_id
        FROM scd.stg_customers
    ),
    -- current view of ranked:

    -- id   name           updated_at  ver_id
    --  1  John2  2023-10-25 03:00:00       1
    --  1  John3  2023-10-25 04:00:00       2



    -- Adding latest version id for each customer
    versioned AS (
        SELECT
            id,
            name,
            updated_at,
            ver_id,
            MAX(ver_id) OVER (PARTITION BY id) AS last_ver_id
        FROM ranked
    ),
    -- current view of versioned:

    -- id   name           updated_at  ver_id  last_ver_id
    --  1  John2  2023-10-25 03:00:00       1            2
    --  1  John3  2023-10-25 04:00:00       2            2



    -- Joining versioned with customers table
    joined AS (
        SELECT
            cus.c_nk,
            ver.name,
            cus.rec_start_date,
            cus.rec_end_date,
            ver.updated_at,
            ver.ver_id,
            ver.last_ver_id
        FROM scd.customers AS cus
        INNER JOIN versioned AS ver ON cus.c_nk = ver.id
        WHERE cus.rec_end_date > SYSDATE
    ),
    -- current view of joined:

    -- c_nk   name       rec_start_date         rec_end_date           updated_at  ver_id  last_ver_id
    --    1  John2  2023-10-25 02:00:00  2100-01-01 00:00:00  2023-10-25 03:00:00       1            2
    --    1  John3  2023-10-25 02:00:00  2100-01-01 00:00:00  2023-10-25 04:00:00       2            2



    -- Transforming only the latest version of customers
    latest_ver AS (
        SELECT
            c_nk,
            name,
            updated_at AS rec_start_date,
            TO_TIMESTAMP('2100-01-01', 'YYYY-MM-DD') AS rec_end_date,
            ver_id
        FROM joined
        WHERE ver_id = last_ver_id
    ),
    -- current view of latest_ver:

    -- c_nk   name       rec_start_date         rec_end_date  ver_id
    --    1  John3  2023-10-25 04:00:00  2100-01-01 00:00:00       2



    -- Transforming intermediate versions of customers
    old_ver AS (
        SELECT
            j1.c_nk,
            j1.name,
            j1.updated_at as rec_start_date,
            j2.updated_at as rec_end_date,
            j1.ver_id
        FROM joined AS j1
        INNER JOIN joined AS j2 ON j1.c_nk = j2.c_nk AND j1.ver_id = j2.ver_id - 1
        WHERE j1.ver_id <> j1.last_ver_id
    ),
    -- current view of old_ver:

    -- c_nk   name       rec_start_date         rec_end_date  ver_id
    --    1  John2  2023-10-25 03:00:00  2023-10-25 04:00:00       1



    -- Concatenating latest_ver and old_ver
    final AS (
        SELECT * FROM old_ver
        UNION ALL
        SELECT * FROM latest_ver
    ),
    -- current view of final:

    -- c_nk   name       rec_start_date         rec_end_date  ver_id
    --    1  John2  2023-10-25 03:00:00  2023-10-25 04:00:00       1
    --    1  John3  2023-10-25 04:00:00  2100-01-01 00:00:00       2



    -- Extracting last customer primary key to generate new ones before inserting into temp_customers
    last_customer_pk AS (
        SELECT
            MAX(c_pk) AS last_pk
        FROM scd.customers
    )
    -- last pk is 2



    -- Inserting into temp_customers
    SELECT
        ROW_NUMBER() OVER (ORDER BY f.c_nk) + l.last_pk AS c_pk,
        f.c_nk,
        f.name,
        f.rec_start_date,
        f.rec_end_date,
        f.ver_id
    FROM final AS f, last_customer_pk AS l
    -- current view of temp_customers:

    -- c_pk  c_nk   name       rec_start_date         rec_end_date  ver_id
    --    3     1  John2  2023-10-25 03:00:00  2023-10-25 04:00:00       1
    --    4     1  John3  2023-10-25 04:00:00  2100-01-01 00:00:00       2
);



-- Updating rec_end_date field in customers table for current customers with 2100-01-01 end date
UPDATE scd.customers AS cus
    SET rec_end_date = temp.rec_start_date
FROM scd.temp_customers AS temp
    WHERE cus.c_nk = temp.c_nk
    AND temp.ver_id = 1
    AND cus.rec_end_date > SYSDATE;
-- current view of customers:

-- c_pk  c_nk   name       rec_start_date         rec_end_date
--    1     1  John0  2023-10-25 01:00:00  2023-10-25 02:00:00
--    2     1  John1  2023-10-25 02:00:00  2023-10-25 03:00:00



-- Inserting data from temp_customers into customers
INSERT INTO scd.customers (
    SELECT
        c_pk,
        c_nk,
        name,
        rec_start_date,
        rec_end_date
    FROM scd.temp_customers
);
-- currrent view of customers:

-- c_pk  c_nk   name       rec_start_date         rec_end_date
--    1     1  John0  2023-10-25 01:00:00  2023-10-25 02:00:00
--    2     1  John1  2023-10-25 02:00:00  2023-10-25 03:00:00
--    3     1  John2  2023-10-25 03:00:00  2023-10-25 04:00:00
--    4     1  John3  2023-10-25 04:00:00  2100-01-01 00:00:00



-- Dropping temporary table
DROP TABLE scd.temp_customers;

-- Truncating staging table
TRUNCATE TABLE scd.stg_customers;

COMMIT;
END TRANSACTION;
