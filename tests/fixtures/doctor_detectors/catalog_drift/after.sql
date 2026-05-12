CREATE TABLE public.deals (
    deal_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    deal_name TEXT NOT NULL,
    status TEXT
);
