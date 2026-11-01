CREATE TABLE public.deals (
    deal_id TEXT PRIMARY KEY,
    deal_name TEXT NOT NULL,
    amount NUMERIC CHECK (amount >= 0)
);
