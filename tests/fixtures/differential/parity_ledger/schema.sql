CREATE TABLE public.tenants (
    tenant_id TEXT PRIMARY KEY,
    tenant_name TEXT NOT NULL
);

CREATE TABLE public.deals (
    deal_id TEXT PRIMARY KEY,
    tenant_id TEXT REFERENCES public.tenants (tenant_id),
    external_key TEXT UNIQUE,
    deal_name TEXT NOT NULL,
    status TEXT CHECK (status IN ('open', 'closed')),
    amount INTEGER CHECK (amount >= 0)
);
