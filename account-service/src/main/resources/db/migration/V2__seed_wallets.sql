-- Seed wallets for the three demo users in three currencies.
INSERT INTO wallet (id, user_id, currency, balance) VALUES
    ('11111111-1111-1111-1111-aaaaaaaaaaaa', 'alice',  'EUR', 1000.0000),
    ('11111111-1111-1111-1111-bbbbbbbbbbbb', 'alice',  'USD',  500.0000),
    ('11111111-1111-1111-1111-cccccccccccc', 'alice',  'GBP',  300.0000),
    ('22222222-2222-2222-2222-aaaaaaaaaaaa', 'bob',    'EUR',  200.0000),
    ('22222222-2222-2222-2222-bbbbbbbbbbbb', 'bob',    'USD',  800.0000),
    ('22222222-2222-2222-2222-cccccccccccc', 'bob',    'GBP',  100.0000),
    ('33333333-3333-3333-3333-aaaaaaaaaaaa', 'carol',  'EUR',  500.0000),
    ('33333333-3333-3333-3333-bbbbbbbbbbbb', 'carol',  'USD',  500.0000);
