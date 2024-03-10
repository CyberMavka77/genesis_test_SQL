CREATE SCHEMA IF NOT EXISTS gen_test;

CREATE TABLE gen_test.data (
    "id" INTEGER NOT NULL,
    user_id INT,
    product_id VARCHAR(255),
    trial BOOLEAN,
    refunded BOOLEAN,
    purchase_date DATE,
    country_code VARCHAR(255),
    media_source VARCHAR(255),
    PRIMARY KEY ("id")
);