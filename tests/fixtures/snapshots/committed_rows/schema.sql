CREATE TABLE public.tenants (
    tenant_id TEXT PRIMARY KEY,
    tenant_name TEXT NOT NULL
);

CREATE TABLE public.deals (
    deal_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES public.tenants (tenant_id),
    deal_name TEXT NOT NULL,
    amount INTEGER CHECK (amount >= 0)
);
