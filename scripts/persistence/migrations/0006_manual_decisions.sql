ALTER TABLE exceptions_decisions ADD COLUMN request_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE exceptions_decisions ADD COLUMN exception_version INTEGER NOT NULL DEFAULT 1;
CREATE TRIGGER exceptions_decisions_no_update BEFORE UPDATE ON exceptions_decisions
BEGIN
    SELECT RAISE(ABORT, 'Las decisiones aplicadas son inmutables');
END;
CREATE TRIGGER exceptions_decisions_no_delete BEFORE DELETE ON exceptions_decisions
BEGIN
    SELECT RAISE(ABORT, 'Las decisiones aplicadas son inmutables');
END;
