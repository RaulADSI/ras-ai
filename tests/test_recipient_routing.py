import json

import pytest

from scripts.review.recipient_routing import load_mapping, resolve_recipient, validate_mapping


@pytest.fixture
def mapping_v1():
    return {
        "version": 1,
        "recipients": {
            "Richard Libutti": "ricky@rentify.live",
            "Cory Reiter": "coryreiter@gmail.com",
            "Lindsay Reiter": "lindsay@rentify.live",
        },
    }


def test_authorized_v1_mapping_routes_with_versioned_evidence(mapping_v1):
    decision = resolve_recipient("Richard Libutti", mapping_v1)
    assert decision.status == "PASS"
    assert decision.recipient == "ricky@rentify.live"
    assert decision.mapping_version == 1
    assert len(decision.mapping_sha256) == 64
    assert decision.reason is None


def test_same_input_and_mapping_are_deterministic(mapping_v1):
    assert resolve_recipient("Cory Reiter", mapping_v1) == resolve_recipient("Cory Reiter", mapping_v1)


def test_mapping_change_produces_new_evidence(mapping_v1):
    changed = dict(mapping_v1, version=2, recipients=dict(mapping_v1["recipients"], **{"Cory Reiter": "cory@rentify.live"}))
    first, second = resolve_recipient("Cory Reiter", mapping_v1), resolve_recipient("Cory Reiter", changed)
    assert (first.mapping_version, first.mapping_sha256, first.recipient) != (second.mapping_version, second.mapping_sha256, second.recipient)


def test_unknown_responsible_is_closed(mapping_v1):
    decision = resolve_recipient("Unmapped Person", mapping_v1)
    assert (decision.status, decision.recipient, decision.reason) == ("REVIEW_REQUIRED", None, "unknown_responsible")


@pytest.mark.parametrize(("recipients", "reason"), [
    ({"Richard Libutti": ""}, "missing_recipient"),
    ({"Richard Libutti": ["one@example.test", "two@example.test"]}, "multiple_recipients"),
])
def test_missing_or_ambiguous_recipient_is_closed(recipients, reason):
    decision = resolve_recipient("Richard Libutti", {"version": 1, "recipients": recipients})
    assert (decision.status, decision.recipient, decision.reason) == ("REVIEW_REQUIRED", None, reason)


@pytest.mark.parametrize("mapping", [
    {},
    {"version": "1", "recipients": {"Richard Libutti": "ricky@rentify.live"}},
    {"version": 1, "recipients": {"Richard Libutti": "not-an-email"}},
])
def test_malformed_mapping_is_closed(mapping):
    decision = resolve_recipient("Richard Libutti", mapping)
    assert (decision.status, decision.recipient, decision.reason) == ("REVIEW_REQUIRED", None, "invalid_mapping")


def test_duplicate_responsible_in_json_is_rejected(tmp_path):
    path = tmp_path / "mapping.json"
    path.write_text('{"version":1,"recipients":{"Richard Libutti":"one@example.test","Richard Libutti":"two@example.test"}}', encoding="utf-8")
    with pytest.raises(ValueError, match="multiple recipients"):
        load_mapping(path)


def test_mapping_fixture_is_json_only_and_no_provider_import_is_needed(mapping_v1, tmp_path):
    path = tmp_path / "mapping.json"
    path.write_text(json.dumps(mapping_v1), encoding="utf-8")
    assert load_mapping(path) == mapping_v1
