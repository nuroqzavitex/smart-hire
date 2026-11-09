-- Runs once, as the postgres superuser, when the data volume is initialised.
-- Each service gets its own role that owns its database (spec.md §4.2 level 1);
-- no service connects as postgres. The password comes from AUTH_DB_PASSWORD
-- (set from .env by docker-compose.infra.yml).
\getenv auth_password AUTH_DB_PASSWORD
\if :{?auth_password}
\else
  \warn 'AUTH_DB_PASSWORD is not set'
  \quit 1
\endif

CREATE ROLE auth_svc LOGIN PASSWORD :'auth_password';
CREATE DATABASE auth_db OWNER auth_svc;
REVOKE ALL ON DATABASE auth_db FROM PUBLIC;
