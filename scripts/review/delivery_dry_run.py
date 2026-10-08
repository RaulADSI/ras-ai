"""Provider-free delivery rehearsal for authorised recipient routing.

This is deliberately separate from ``delivery.deliver``: it has no database,
OAuth, SMTP, or Gmail dependency and cannot change a review request's state.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from email.message import EmailMessage
from typing import Any, Callable
from urllib.parse import urlsplit

from scripts.review.recipient_routing import RoutingDecision, resolve_recipient


@dataclass(frozen=True)
class DryRunResult:
    status: str
    routing: RoutingDecision
    provider_reference: str | None

    def evidence(self) -> dict[str, Any]:
        result = asdict(self)
        result["routing"] = self.routing.evidence()
        return result


def _valid_address(value: Any) -> bool:
    return isinstance(value, str) and "@" in value and "\n" not in value and "\r" not in value


def compose_message(*, recipient: str, sender: str, review_url: str, company: str,
                    transaction_count: int, total_cents: int, currency: str = "USD") -> EmailMessage:
    """Compose the exact review request message without dispatching it."""
    if not _valid_address(sender) or not _valid_address(recipient):
        raise ValueError("Valid sender and recipient required")
    origin = urlsplit(review_url)
    if origin.scheme != "https" or not origin.netloc:
        raise ValueError("HTTPS review URL required")
    if not isinstance(company, str) or not company.strip() or type(transaction_count) is not int or transaction_count < 1:
        raise ValueError("Valid review details required")
    if type(total_cents) is not int or not isinstance(currency, str) or not currency.strip():
        raise ValueError("Valid review amount required")

    message = EmailMessage()
    message["Subject"] = "AMEX — Property assignment required"
    message["From"], message["To"] = sender, recipient
    message["Message-ID"] = f"<rentify-review-dry-run@{origin.hostname}>"
    message.set_content(
        f"Please review {transaction_count} AMEX transactions.\n"
        f"Company: {company}\n"
        f"Total: {currency} {'-' if total_cents < 0 else ''}{abs(total_cents)//100}.{abs(total_cents)%100:02d}\n\n"
        f"Review transactions: {review_url}\n\n"
        "This is a delivery dry-run. No email has been sent and no batch has been authorised.\n"
    )
    return message


def deliver_dry_run(responsible: Any, *, sender: str, review_url: str, company: str,
                    transaction_count: int, total_cents: int,
                    emitter: Callable[[EmailMessage], Any], mapping: Any = None) -> DryRunResult:
    """Route and rehearse delivery through an injected in-memory emitter only."""
    routing = resolve_recipient(responsible, mapping)
    if routing.status != "PASS":
        return DryRunResult("REVIEW_REQUIRED", routing, None)
    if not callable(emitter):
        raise ValueError("Injected dry-run emitter required")
    message = compose_message(recipient=routing.recipient, sender=sender, review_url=review_url,
                              company=company, transaction_count=transaction_count,
                              total_cents=total_cents)
    reference = emitter(message)
    return DryRunResult("DRY_RUN_PASS", routing, str(reference or message["Message-ID"]))
