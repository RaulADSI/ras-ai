import sys

from scripts.review.delivery_dry_run import deliver_dry_run


MAPPING_V1 = {
    "version": 1,
    "recipients": {
        "Richard Libutti": "ricky@rentify.live",
        "Cory Reiter": "coryreiter@gmail.com",
        "Lindsay Reiter": "lindsay@rentify.live",
    },
}


def test_dry_run_composes_message_and_injected_adapter_receives_authorized_recipient():
    captured = []

    result = deliver_dry_run(
        "Richard Libutti", sender="review@rentify.live",
        review_url="https://review.rentify.test/review/dry-run",
        company="RAS", transaction_count=2, total_cents=12345,
        mapping=MAPPING_V1, emitter=lambda message: captured.append(message) or "captured-1",
    )

    assert result.status == "DRY_RUN_PASS"
    assert result.provider_reference == "captured-1"
    assert result.routing.recipient == "ricky@rentify.live"
    assert result.routing.mapping_version == 1
    assert len(captured) == 1
    assert captured[0]["To"] == "ricky@rentify.live"
    assert captured[0]["From"] == "review@rentify.live"
    assert captured[0]["Subject"] == "AMEX — Property assignment required"
    assert "Please review 2 AMEX transactions." in captured[0].get_content()
    assert "This is a delivery dry-run" in captured[0].get_content()
    assert "smtplib" not in sys.modules
    assert "scripts.review.gmail" not in sys.modules


def test_failed_routing_never_invokes_the_dry_run_adapter():
    captured = []
    result = deliver_dry_run(
        "Unknown", sender="review@rentify.live",
        review_url="https://review.rentify.test/review/dry-run",
        company="RAS", transaction_count=1, total_cents=1,
        mapping=MAPPING_V1, emitter=captured.append,
    )
    assert result.status == "REVIEW_REQUIRED"
    assert result.routing.reason == "unknown_responsible"
    assert captured == []
