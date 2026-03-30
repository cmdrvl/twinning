CREATE TABLE public.tenants (
    tenant_id TEXT PRIMARY KEY,
    tenant_name TEXT NOT NULL UNIQUE
);

CREATE TABLE public.deals (
    deal_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES public.tenants (tenant_id),
    deal_name TEXT NOT NULL,
    amount NUMERIC CHECK (amount >= 0)
);

CREATE INDEX idx_deals_tenant_id ON public.deals (tenant_id);
