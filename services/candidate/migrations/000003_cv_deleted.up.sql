-- Account deletion (ADR 0014): a purged CV stays as a DELETED tombstone
-- without file name or parse data, so late parse results stay stale.
ALTER TABLE cvs DROP CONSTRAINT cvs_status_check;
ALTER TABLE cvs ADD CONSTRAINT cvs_status_check
    CHECK (status IN ('UPLOADING','UPLOADED','PARSED','FAILED','DELETED'));
