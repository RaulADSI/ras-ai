ALTER TABLE allocations ADD COLUMN property_code TEXT NOT NULL DEFAULT '';
ALTER TABLE allocations ADD COLUMN vendor_name TEXT NOT NULL DEFAULT '';
ALTER TABLE allocations ADD COLUMN gl_account TEXT NOT NULL DEFAULT '';
ALTER TABLE allocations ADD COLUMN cash_account TEXT NOT NULL DEFAULT '';
ALTER TABLE allocations ADD COLUMN description TEXT NOT NULL DEFAULT '';
ALTER TABLE export_items ADD COLUMN snapshot_json TEXT;
ALTER TABLE export_batches ADD COLUMN file_path TEXT;
CREATE TRIGGER export_snapshot_no_update BEFORE UPDATE OF snapshot_json ON export_items
WHEN OLD.snapshot_json IS NOT NULL
BEGIN
    SELECT RAISE(ABORT, 'La instantánea reservada es inmutable');
END;
