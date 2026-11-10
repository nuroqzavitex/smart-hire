-- Runs once, as the postgres superuser, when the data volume is initialised.
-- The candidate_svc role owns candidate_db; the password comes from
-- CANDIDATE_DB_PASSWORD (set from .env by docker-compose.infra.yml).
\getenv candidate_password CANDIDATE_DB_PASSWORD
\if :{?candidate_password}
\else
  \warn 'CANDIDATE_DB_PASSWORD is not set'
  \quit 1
\endif

CREATE ROLE candidate_svc LOGIN PASSWORD :'candidate_password';
CREATE DATABASE candidate_db OWNER candidate_svc;
REVOKE ALL ON DATABASE candidate_db FROM PUBLIC;
