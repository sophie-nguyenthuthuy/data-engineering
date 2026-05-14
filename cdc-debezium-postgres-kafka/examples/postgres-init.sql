-- Initial schema for the local Debezium demo.
CREATE TABLE IF NOT EXISTS orders (
    id          serial PRIMARY KEY,
    customer    text   NOT NULL,
    email       text,
    amount      numeric(10, 2) NOT NULL,
    created_at  timestamptz DEFAULT now()
);

-- Make sure REPLICA IDENTITY captures the full row so the Debezium
-- envelope's `before` is populated on updates and deletes.
ALTER TABLE orders REPLICA IDENTITY FULL;

INSERT INTO orders (customer, email, amount) VALUES
    ('alice', 'alice@example.com', 19.99),
    ('bob',   'bob@example.com',   42.00);
