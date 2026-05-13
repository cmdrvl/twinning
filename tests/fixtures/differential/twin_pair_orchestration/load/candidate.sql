INSERT INTO public.tenants (tenant_id, tenant_name) VALUES
  ('tenant-a', 'Tenant A'),
  ('tenant-b', 'Tenant B');

INSERT INTO public.deals (deal_id, tenant_id, deal_name, amount) VALUES
  ('deal-001', 'tenant-a', 'Alpha', 100),
  ('deal-002', 'tenant-b', 'Beta', 120);
