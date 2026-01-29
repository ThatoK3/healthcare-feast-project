-- Create schema for Feast offline store tables
CREATE SCHEMA IF NOT EXISTS feast_offline;

-- Create schema for Feast registry (tables auto-created by Feast)
CREATE SCHEMA IF NOT EXISTS feast_registry;

-- Verify connection
\dn
