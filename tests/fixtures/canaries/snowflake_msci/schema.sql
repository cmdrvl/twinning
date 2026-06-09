-- MSCI Real Assets Transactions - derived from listing metadata (GZ1M6ZQE25B).
-- Sample tables: London, Dallas, Singapore. Schema is identical across regions.

CREATE TABLE rca_sample_transactions (
    propertykey_id               TEXT,
    deal_id                      TEXT,
    property_id                  TEXT,
    propertyname                 TEXT,
    address_tx                   TEXT,
    city_tx                      TEXT,
    price                        NUMERIC,
    sqmeters_nb                  NUMERIC,
    status_dt                    DATE,
    transtype_tx                 TEXT,
    maintype                     TEXT,
    buyer1_principal_entity_tx   TEXT,
    seller1_principal_entity_tx  TEXT,
    deal_update_dt               TIMESTAMP,
    property_update_dt           TIMESTAMP
);
