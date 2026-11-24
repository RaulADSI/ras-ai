CREATE TABLE review_pilot_form_links (
    override_id TEXT PRIMARY KEY REFERENCES review_pilot_delivery_overrides(override_id),
    form_url TEXT NOT NULL,
    form_url_sha256 TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TRIGGER pilot_form_link_no_update BEFORE UPDATE ON review_pilot_form_links
BEGIN SELECT RAISE(ABORT, 'Pilot form link is immutable'); END;
CREATE TRIGGER pilot_form_link_no_delete BEFORE DELETE ON review_pilot_form_links
BEGIN SELECT RAISE(ABORT, 'Pilot form link is append-only'); END;
