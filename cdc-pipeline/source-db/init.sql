-- Source transactional database schema
-- Logical replication is enabled via docker-compose postgres flags

CREATE TABLE users (
    id           SERIAL PRIMARY KEY,
    email        VARCHAR(255) NOT NULL UNIQUE,
    username     VARCHAR(100) NOT NULL,
    status       VARCHAR(50)  NOT NULL DEFAULT 'active',
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE orders (
    id           SERIAL PRIMARY KEY,
    user_id      INT          NOT NULL REFERENCES users(id),
    status       VARCHAR(50)  NOT NULL DEFAULT 'pending',
    total_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
    currency     CHAR(3)      NOT NULL DEFAULT 'USD',
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE order_items (
    id           SERIAL PRIMARY KEY,
    order_id     INT          NOT NULL REFERENCES orders(id),
    sku          VARCHAR(100) NOT NULL,
    quantity     INT          NOT NULL CHECK (quantity > 0),
    unit_price   NUMERIC(10,2) NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

-- Update trigger for updated_at
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

CREATE TRIGGER users_updated_at  BEFORE UPDATE ON users       FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER orders_updated_at BEFORE UPDATE ON orders      FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Publication for Debezium (pgoutput plugin)
CREATE PUBLICATION cdc_publication FOR TABLE users, orders, order_items;

-- Seed data
INSERT INTO users (email, username, status) VALUES
    ('alice@example.com',   'alice',   'active'),
    ('bob@example.com',     'bob',     'active'),
    ('charlie@example.com', 'charlie', 'inactive'),
    ('diana@example.com',   'diana',   'active'),
    ('eve@example.com',     'eve',     'active');

INSERT INTO orders (user_id, status, total_amount, currency) VALUES
    (1, 'completed', 149.99, 'USD'),
    (1, 'pending',    49.99, 'USD'),
    (2, 'completed', 299.00, 'USD'),
    (3, 'cancelled',  19.99, 'USD'),
    (4, 'processing', 89.50, 'USD');

INSERT INTO order_items (order_id, sku, quantity, unit_price) VALUES
    (1, 'WIDGET-001', 2,  49.99),
    (1, 'GADGET-002', 1,  50.01),
    (2, 'WIDGET-001', 1,  49.99),
    (3, 'PREMIUM-001', 1, 299.00),
    (5, 'GADGET-002', 2,  44.75);
