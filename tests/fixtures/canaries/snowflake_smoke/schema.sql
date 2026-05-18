CREATE TABLE observations (
    id NUMBER(38,0) NOT NULL,
    label VARCHAR(32),
    observed_on DATE,
    captured_at TIMESTAMP_NTZ(6)
);

INSERT INTO observations (id, label, observed_on, captured_at) VALUES
    (1, 'alpha', 19723, 1700000000000000),
    (2, 'beta', 19724, 1700000001000000),
    (3, 'gamma', 19725, 1700000002000000),
    (4, 'delta', 19726, 1700000003000000),
    (5, 'epsilon', 19727, 1700000004000000);

CREATE TABLE account_events (
    event_id NUMBER(38,0) NOT NULL,
    amount NUMBER(10,2),
    active BOOLEAN,
    payload VARIANT
);

INSERT INTO account_events (event_id, amount, active, payload) VALUES
    (10, 12.34, true, '{"kind":"signup","n":1}'),
    (11, 23.45, false, '{"kind":"upgrade","n":2}'),
    (12, 34.56, true, '{"kind":"renewal","n":3}'),
    (13, 45.67, true, '{"kind":"usage","n":4}'),
    (14, 56.78, false, '{"kind":"cancel","n":5}');
