-- Runs once, as the postgres superuser, when the data volume is initialised;
-- `make db-init` runs it again on an existing volume, so it is idempotent.
-- The application_svc role owns application_db; the password comes from
-- APPLICATION_DB_PASSWORD (set from .env by docker-compose.infra.yml).
\getenv application_password APPLICATION_DB_PASSWORD
\if :{?application_password}
\else
  \warn 'APPLICATION_DB_PASSWORD is not set'
  \quit 1
\endif

SELECT format('CREATE ROLE application_svc LOGIN PASSWORD %L', :'application_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'application_svc') \gexec
SELECT 'CREATE DATABASE application_db OWNER application_svc'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'application_db') \gexec
REVOKE ALL ON DATABASE application_db FROM PUBLIC;
