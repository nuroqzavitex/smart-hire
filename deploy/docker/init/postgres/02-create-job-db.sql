-- Runs once, as the postgres superuser, when the data volume is initialised.
-- The job_svc role owns job_db; the password comes from JOB_DB_PASSWORD
-- (set from .env by docker-compose.infra.yml).
\getenv job_password JOB_DB_PASSWORD
\if :{?job_password}
\else
  \warn 'JOB_DB_PASSWORD is not set'
  \quit 1
\endif

CREATE ROLE job_svc LOGIN PASSWORD :'job_password';
CREATE DATABASE job_db OWNER job_svc;
REVOKE ALL ON DATABASE job_db FROM PUBLIC;
