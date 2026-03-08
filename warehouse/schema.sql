
DROP TABLE IF EXISTS fact_sales CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_product CASCADE;
DROP TABLE IF EXISTS dim_customer CASCADE;
DROP TABLE IF EXISTS dim_store CASCADE;


-- DIMENSION TABLES


-- Date Dimension
CREATE TABLE dim_date (
    date_key SERIAL PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    week_of_year INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    fiscal_year INTEGER NOT NULL
);

-- Product Dimension
CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL UNIQUE,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    unit_cost DECIMAL(10, 2) NOT NULL,
    supplier VARCHAR(100) NOT NULL,
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

-- Customer Dimension
CREATE TABLE dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL UNIQUE,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    phone VARCHAR(20),
    city VARCHAR(100) NOT NULL,
    state VARCHAR(10) NOT NULL,
    registration_date DATE NOT NULL,
    customer_segment VARCHAR(50) DEFAULT 'Standard'
);

-- Store Dimension
CREATE TABLE dim_store (
    store_key SERIAL PRIMARY KEY,
    store_id INTEGER NOT NULL UNIQUE,
    store_name VARCHAR(100) NOT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(10) NOT NULL,
    region VARCHAR(50) NOT NULL,
    manager_name VARCHAR(100),
    opening_date DATE NOT NULL
);


-- FACT TABLE


CREATE TABLE fact_sales (
    sale_id SERIAL PRIMARY KEY,
    product_key INTEGER NOT NULL REFERENCES dim_product(product_key),
    customer_key INTEGER NOT NULL REFERENCES dim_customer(customer_key),
    store_key INTEGER NOT NULL REFERENCES dim_store(store_key),
    date_key INTEGER NOT NULL REFERENCES dim_date(date_key),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    revenue DECIMAL(10, 2) NOT NULL,
    cost DECIMAL(10, 2) NOT NULL,
    profit DECIMAL(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints
    CONSTRAINT chk_quantity_positive CHECK (quantity > 0),
    CONSTRAINT chk_price_positive CHECK (unit_price >= 0),
    CONSTRAINT chk_revenue_positive CHECK (revenue >= 0)
);


--  PERFORMANCE INDEX


CREATE INDEX idx_fact_sales_date ON fact_sales(date_key);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_store ON fact_sales(store_key);
CREATE INDEX idx_dim_customer_city ON dim_customer(city);
CREATE INDEX idx_dim_product_category ON dim_product(category);


-- VIEWS


-- Sales Summary View
CREATE VIEW vw_sales_summary AS
SELECT 
    d.full_date,
    d.month_name,
    d.year,
    p.product_name,
    p.category,
    c.city as customer_city,
    s.store_name,
    s.region,
    fs.quantity,
    fs.revenue,
    fs.profit
FROM fact_sales fs
JOIN dim_date d ON fs.date_key = d.date_key
JOIN dim_product p ON fs.product_key = p.product_key
JOIN dim_customer c ON fs.customer_key = c.customer_key
JOIN dim_store s ON fs.store_key = s.store_key;

-- Monthly Revenue View
CREATE VIEW vw_monthly_revenue AS
SELECT 
    d.year,
    d.month,
    d.month_name,
    COUNT(*) as total_transactions,
    SUM(fs.quantity) as total_quantity,
    SUM(fs.revenue) as total_revenue,
    SUM(fs.profit) as total_profit,
    ROUND(AVG(fs.revenue), 2) as avg_revenue_per_transaction
FROM fact_sales fs
JOIN dim_date d ON fs.date_key = d.date_key
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year DESC, d.month DESC;

-- =====================================================
-- STORED PROCEDURE: Populate Date Dimension
-- =====================================================

CREATE OR REPLACE FUNCTION populate_date_dimension(start_date DATE, end_date DATE)
RETURNS VOID AS $$
DECLARE
    curr_date DATE := start_date;
BEGIN
    WHILE curr_date <= end_date LOOP
        INSERT INTO dim_date (
            full_date, year, quarter, month, month_name, day,
            day_of_week, day_name, week_of_year, is_weekend, fiscal_year
        ) VALUES (
            curr_date,
            EXTRACT(YEAR FROM curr_date),
            EXTRACT(QUARTER FROM curr_date),
            EXTRACT(MONTH FROM curr_date),
            TO_CHAR(curr_date, 'Month'),
            EXTRACT(DAY FROM curr_date),
            EXTRACT(DOW FROM curr_date),
            TO_CHAR(curr_date, 'Day'),
            EXTRACT(WEEK FROM curr_date),
            EXTRACT(DOW FROM curr_date) IN (0, 6),
            EXTRACT(YEAR FROM curr_date)
        )
        ON CONFLICT (full_date) DO NOTHING;
        
        curr_date := curr_date + INTERVAL '1 day';
    END LOOP;
END;
$$ LANGUAGE plpgsql;
