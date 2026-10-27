CREATE TABLE IF NOT EXISTS gold_orders_aggregated (
    city VARCHAR(100),
    category VARCHAR(100),
    window_start TIMESTAMPTZ,
    window_end TIMESTAMPTZ,
    num_events INT,
    avg_amount FLOAT,
    total_amount FLOAT,
    unique_users INT,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (city, category, window_start, window_end)
);