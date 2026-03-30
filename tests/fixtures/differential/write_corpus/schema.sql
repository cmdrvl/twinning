CREATE TABLE public.tenants (
    tenant_id TEXT PRIMARY KEY,
    tenant_name TEXT NOT NULL
);

CREATE TABLE public.deals (
    deal_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    external_key TEXT UNIQUE,
    deal_name TEXT NOT NULL,
    CONSTRAINT deals_tenant_fk
        FOREIGN KEY (tenant_id) REFERENCES public.tenants (tenant_id)
);
