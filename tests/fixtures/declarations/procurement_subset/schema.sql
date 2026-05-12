CREATE TABLE public.procurement_outcomes (
    outcome_id TEXT PRIMARY KEY,
    outcome_name TEXT NOT NULL
);

CREATE TABLE public.procurement_sources (
    source_id TEXT PRIMARY KEY,
    source_name TEXT NOT NULL
);

CREATE TABLE public.procurement_evidence (
    evidence_id TEXT PRIMARY KEY,
    outcome_id TEXT NOT NULL REFERENCES public.procurement_outcomes (outcome_id),
    source_id TEXT NOT NULL REFERENCES public.procurement_sources (source_id),
    evidence_text TEXT NOT NULL
);
