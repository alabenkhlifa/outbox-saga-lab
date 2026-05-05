-- Seed a few sample SKUs so the saga has something to reserve in dev.
-- Quantities are intentionally small for some items so that "out of stock"
-- failures are reachable in manual testing.
INSERT INTO stock (sku, available_qty) VALUES
    ('pizza-margherita', 100),
    ('burger-deluxe',     50),
    ('salad-caesar',     200),
    ('soda-cola',          5);  -- intentionally low to test StockReservationFailed
