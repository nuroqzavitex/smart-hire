-- Runs once, as the postgres superuser, when the data volume is initialised;
-- `make db-init` runs it again on an existing volume, so it is idempotent.
-- The notification_svc role owns notification_db; the password comes from
-- NOTIFICATION_DB_PASSWORD (set from .env by docker-compose.infra.yml).
\getenv notification_password NOTIFICATION_DB_PASSWORD
\if :{?notification_password}
\else
  \warn 'NOTIFICATION_DB_PASSWORD is not set'
  \quit 1
\endif

SELECT format('CREATE ROLE notification_svc LOGIN PASSWORD %L', :'notification_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'notification_svc') \gexec
SELECT 'CREATE DATABASE notification_db OWNER notification_svc'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'notification_db') \gexec
REVOKE ALL ON DATABASE notification_db FROM PUBLIC;
