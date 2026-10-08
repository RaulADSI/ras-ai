"""Closed, auditable responsible-to-recipient routing.

This module intentionally has no provider dependency. Resolving a recipient is
only preparation for a later, explicitly invoked dry-run or delivery action.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
import json
from pathlib import Path
import re
from typing import Any


DEFAULT_MAPPING_PATH = Path(__file__).with_name("recipient_mapping.v1.json")
EMAIL = re.compile(r"[^\s@<>]+@[^\s@<>]+\.[^\s@<>]+\Z")


@dataclass(frozen=True)
class RoutingDecision:
    responsible: str | None
    status: str
    recipient: str | None
    reason: str | None
    mapping_version: int | None
    mapping_sha256: str | None

    def evidence(self) -> dict[str, Any]:
        return asdict(self)


class MappingError(ValueError):
    def __init__(self, reason: str):
        super().__init__(reason.replace("_", " "))
        self.reason = reason


def _pairs_no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise MappingError("multiple_recipients" if key != "recipients" else "invalid_mapping")
        result[key] = value
    return result


def _canonical_sha256(mapping: dict[str, Any]) -> str:
    payload = json.dumps(mapping, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return sha256(payload.encode("utf-8")).hexdigest()


def load_mapping(path: str | Path = DEFAULT_MAPPING_PATH) -> dict[str, Any]:
    """Load the sole authorised mapping and validate its closed schema."""
    try:
        raw = Path(path).read_text(encoding="utf-8")
        mapping = json.loads(raw, object_pairs_hook=_pairs_no_duplicates)
    except (OSError, json.JSONDecodeError, MappingError) as exc:
        if isinstance(exc, MappingError):
            raise
        raise MappingError("invalid_mapping") from exc
    return validate_mapping(mapping)


def validate_mapping(mapping: Any) -> dict[str, Any]:
    if not isinstance(mapping, dict) or set(mapping) != {"version", "recipients"}:
        raise MappingError("invalid_mapping")
    version, recipients = mapping["version"], mapping["recipients"]
    if type(version) is not int or version < 1 or not isinstance(recipients, dict) or not recipients:
        raise MappingError("invalid_mapping")

    normalized: dict[str, str] = {}
    used_recipients: set[str] = set()
    for responsible, recipient in recipients.items():
        if not isinstance(responsible, str) or not responsible.strip():
            raise MappingError("invalid_mapping")
        if isinstance(recipient, (list, tuple, set)):
            raise MappingError("multiple_recipients")
        if recipient is None or (isinstance(recipient, str) and not recipient.strip()):
            raise MappingError("missing_recipient")
        if not isinstance(recipient, str) or not EMAIL.fullmatch(recipient):
            raise MappingError("invalid_mapping")
        responsible = responsible.strip()
        recipient = recipient.strip()
        if responsible in normalized or recipient.casefold() in used_recipients:
            raise MappingError("multiple_recipients")
        normalized[responsible] = recipient
        used_recipients.add(recipient.casefold())
    return {"version": version, "recipients": normalized}


def resolve_recipient(responsible: Any, mapping: Any = None) -> RoutingDecision:
    """Return PASS only for one exact, authorised recipient; otherwise close routing."""
    name = responsible.strip() if isinstance(responsible, str) and responsible.strip() else None
    try:
        configured = load_mapping() if mapping is None else validate_mapping(mapping)
    except MappingError as exc:
        return RoutingDecision(name, "REVIEW_REQUIRED", None, exc.reason, None, None)

    evidence_hash = _canonical_sha256(configured)
    recipient = configured["recipients"].get(name) if name else None
    if recipient is None:
        return RoutingDecision(name, "REVIEW_REQUIRED", None, "unknown_responsible",
                               configured["version"], evidence_hash)
    return RoutingDecision(name, "PASS", recipient, None, configured["version"], evidence_hash)
