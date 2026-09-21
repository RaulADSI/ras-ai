ALTER TABLE invoice_matches ADD COLUMN match_details_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE exceptions ADD COLUMN reason TEXT NOT NULL DEFAULT '';
ALTER TABLE exceptions ADD COLUMN metadata_json TEXT NOT NULL DEFAULT '{}';
