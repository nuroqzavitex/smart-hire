-- Runs once, as the postgres superuser, when the data volume is initialised.
-- The ai_svc role owns ai_db; the password comes from AI_DB_PASSWORD
-- (set from .env by docker-compose.infra.yml).
\getenv ai_password AI_DB_PASSWORD
\if :{?ai_password}
\else
  \warn 'AI_DB_PASSWORD is not set'
  \quit 1
\endif

CREATE ROLE ai_svc LOGIN PASSWORD :'ai_password';
CREATE DATABASE ai_db OWNER ai_svc;
REVOKE ALL ON DATABASE ai_db FROM PUBLIC;
