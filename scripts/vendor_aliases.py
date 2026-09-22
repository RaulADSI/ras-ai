"""Explicit accounting aliases; never infer an entity from the token ACE."""
from dataclasses import dataclass


@dataclass(frozen=True)
class MerchantIdentity:
    merchant_raw: str
    merchant_normalized: str
    vendor_name: str
    merchant_variant: str
    location: str


_ALIASES = {}
for variant, location, names in (
    ('Sykes Ace Hardware', 'Miami, FL', (
        'SYKES ACE HARDWARE', 'SYKES ACE HARDWARE CO',
        'SYKES ACE HARDWARE 0MIAMI FL', 'SYKES ACE HARDWARE MIAMI FL')),
    ('Ace Hardware of Opa Locka', 'Opa Locka, FL', (
        'ACE HDWE OF OPA LOCKA', 'ACE HARDWARE OF OPA LOCKA',
        'ACE HDWE OF OPA LOCKOPA LOCKA FL',
        'ACE HDWE OF OPA LOCKA OPA LOCKA FL',
        'ACE HARDWARE OF OPA LOCKA OPA LOCKA FL')),
):
    for name in names:
        _ALIASES[name] = (variant, location)


def lookup_vendor_alias(value):
    if not isinstance(value, str):
        return None
    normalized = ' '.join(value.upper().split())
    match = _ALIASES.get(normalized)
    if match is None:
        return None
    variant, location = match
    return MerchantIdentity(value, normalized, 'Hardware, ACE', variant, location)
