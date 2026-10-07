"""Auditable email delivery with explicit handling of ambiguous provider outcomes."""
import smtplib
import ssl
from email.message import EmailMessage
from urllib.parse import urlsplit

from scripts.review.security import public_origin
from scripts.review.pilot_delivery import begin_dispatch, finish_dispatch


def deliver(service, access, request_id, *, origin=None, sender, send, retry_uncertain=False,
            pilot_override_id=None):
    if pilot_override_id is None:
        origin = public_origin(origin)
    if not isinstance(sender, str) or '@' not in sender or '\n' in sender or '\r' in sender:
        raise ValueError('Configured sender required')
    pilot = None
    with service.ledger._atomic():
        request = service.active(request_id)
        delivery = service.conn.execute('SELECT status FROM review_deliveries WHERE request_id=?', (request_id,)).fetchone()
        if delivery == ('SENT',):
            return 'SENT'
        # A crash while SENDING may have delivered mail. Never retry silently.
        if delivery and delivery[0] in ('SENDING', 'UNCERTAIN') and not retry_uncertain:
            raise ValueError('Delivery outcome uncertain; check provider before explicit retry')
        if pilot_override_id is not None:
            if retry_uncertain:
                raise ValueError('One-time pilot delivery overrides cannot be retried')
            pilot = begin_dispatch(service, request_id=request_id, override_id=pilot_override_id)
        service.conn.execute("UPDATE review_deliveries SET status='SENDING',attempts=attempts+1,updated_at=? WHERE request_id=?", (service.now(), request_id))
    message = EmailMessage()
    message['Subject'] = 'AMEX — Piloto controlado: asignación de propiedad' if pilot else 'AMEX — Property assignment required'
    recipient = pilot['pilot_recipient'] if pilot else request['recipient']
    message['From'], message['To'] = sender, recipient
    message['Message-ID'] = f'<rentify-review-{request_id}@{urlsplit(origin).hostname}>'
    amount = request['total_cents']
    if pilot:
        message.set_content(f"Por favor revisa únicamente la transacción AMEX del piloto (ID {pilot['transaction_id']}).\n"
                            f"Empresa: {request['company']}\nTotal de la solicitud: {request['currency']} {'-' if amount < 0 else ''}{abs(amount)//100}.{abs(amount)%100:02d}\n\n"
                            f"Formulario existente: {pilot['form_url']}\n\n"
                            "Deja los demás ítems en estado PENDING. Este es un piloto controlado: no autoriza ningún batch, CSV ni importación a AppFolio.\n"
                            f"El enlace vence el {request['expires_at']}. No lo reenvíes.\n")
    else:
        message.set_content(f"Please review {request['transaction_count']} AMEX transactions.\n"
                            f"Company: {request['company']}\nTotal: {request['currency']} {'-' if amount < 0 else ''}{abs(amount)//100}.{abs(amount)%100:02d}\n\n"
                            f"Review transactions: {origin}/review/{request_id}?token={access.token(request_id)}\n\n"
                            "Submitted assignments will be saved and validated before processing. "
                            "Pending transactions remain excluded from the bulk.\n"
                            f"This link expires at {request['expires_at']}. Do not forward it.\n")
    try:
        reference = send(message)
    except Exception as exc:
        with service.ledger._atomic():
            service.conn.execute("UPDATE review_deliveries SET status='UNCERTAIN',last_error=?,updated_at=? WHERE request_id=?", (type(exc).__name__, service.now(), request_id))
            if pilot:
                finish_dispatch(service, request_id=request_id, override_id=pilot_override_id, outcome='UNCERTAIN')
            service.ledger._audit('REVIEW_DELIVERY_UNCERTAIN', dict(request_id=request_id, error_type=type(exc).__name__))
        raise RuntimeError('Email delivery outcome uncertain; inspect provider before retry') from None
    with service.ledger._atomic():
        service.conn.execute("UPDATE review_deliveries SET status='SENT',last_error=NULL,provider_reference=?,updated_at=? WHERE request_id=?", (str(reference or message['Message-ID']), service.now(), request_id))
        if pilot:
            finish_dispatch(service, request_id=request_id, override_id=pilot_override_id, outcome='CONSUMED')
        service.conn.execute("UPDATE review_requests SET status=CASE WHEN status='DRAFT' THEN 'SENT' ELSE status END,sent_at=? WHERE request_id=?", (service.now(), request_id))
        service.ledger._audit('REVIEW_EMAIL_SENT', dict(request_id=request_id, recipient=recipient,
                                                        pilot_override_id=pilot_override_id))
    return 'SENT'


def smtp_sender(*, host, port, username, password):
    """Implicit TLS; never enable SMTP protocol debugging (it exposes tokens)."""
    if not host or not username or not password:
        raise ValueError('SMTP host and credentials required')
    def send(message):
        with smtplib.SMTP_SSL(host, port, timeout=30, context=ssl.create_default_context()) as smtp:
            smtp.login(username, password)
            if smtp.send_message(message):
                raise RuntimeError('Recipient refused')
        return message['Message-ID']
    return send
