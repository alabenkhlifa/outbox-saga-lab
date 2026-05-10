-- Seeded fixed cross-rates. Approximate real-world rates as of early 2026.
INSERT INTO fx_rate (base_currency, quote_currency, rate) VALUES
    ('EUR', 'USD', 1.08000000),
    ('USD', 'EUR', 0.92590000),
    ('EUR', 'GBP', 0.85000000),
    ('GBP', 'EUR', 1.17640000),
    ('USD', 'GBP', 0.78700000),
    ('GBP', 'USD', 1.27060000);
