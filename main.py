import json
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from fastapi import Depends, FastAPI, Header, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from config import settings
from database import SessionLocal
from models import Payment, Subscription, User, WebhookEvent

app = FastAPI(title="License Validation API", version="1.1.0")


def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def require_api_key(x_api_key: str = Header(default="", alias="X-API-Key")) -> None:
    if x_api_key != settings.license_api_key:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key",
        )


class CheckLicenseRequest(BaseModel):
    userId: str = Field(..., min_length=1)


class CheckLicenseResponse(BaseModel):
    userId: str
    isActive: bool
    expiresAt: datetime | None = None


class WebhookResponse(BaseModel):
    message: str
    eventId: str
    topic: str
    processed: bool
    userId: str | None = None
    isActive: bool | None = None
    expiresAt: datetime | None = None


def get_or_create_subscription(db: Session, user_id: str) -> Subscription:
    user = db.get(User, user_id)
    if user is None:
        user = User(id=user_id)
        db.add(user)
        db.flush()

    subscription = db.execute(
        select(Subscription).where(Subscription.user_id == user_id)
    ).scalar_one_or_none()
    if subscription is None:
        subscription = Subscription(user_id=user_id, status="inactive")
        db.add(subscription)
        db.flush()
    return subscription


def apply_subscription_event(
    subscription: Subscription,
    payment_event: str,
    duration_days: int,
) -> tuple[str, bool]:
    now = datetime.now(timezone.utc)
    if payment_event == "payment_approved":
        current_expiration = subscription.expires_at
        if current_expiration and current_expiration.tzinfo is None:
            current_expiration = current_expiration.replace(tzinfo=timezone.utc)
        base_date = (
            current_expiration
            if current_expiration and current_expiration > now
            else now
        )
        subscription.expires_at = base_date + timedelta(days=duration_days)
        subscription.status = "active"
        return "Subscription activated/extended", True
    if payment_event == "payment_failed":
        subscription.status = "inactive"
        return "Subscription marked as inactive", True
    return "Payment status does not change subscription", False


def fetch_mp_payment(payment_id: str) -> dict[str, Any] | None:
    if not settings.mp_access_token:
        return None
    url = f"https://api.mercadopago.com/v1/payments/{payment_id}"
    headers = {"Authorization": f"Bearer {settings.mp_access_token}"}
    try:
        response = httpx.get(url, headers=headers, timeout=15)
    except httpx.HTTPError:
        return None
    if response.status_code != 200:
        return None
    body = response.json()
    return body if isinstance(body, dict) else None


def extract_duration_days(payment_data: dict[str, Any]) -> int:
    metadata = payment_data.get("metadata")
    if isinstance(metadata, dict):
        raw = metadata.get("duration_days")
        try:
            parsed = int(raw)
            if parsed > 0:
                return parsed
        except (TypeError, ValueError):
            pass
    return settings.default_subscription_days


@app.get("/")
def healthcheck() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/check_license", response_model=CheckLicenseResponse)
def check_license(
    payload: CheckLicenseRequest,
    _: None = Depends(require_api_key),
    db: Session = Depends(get_db),
) -> CheckLicenseResponse:
    subscription = db.execute(
        select(Subscription).where(Subscription.user_id == payload.userId)
    ).scalar_one_or_none()
    if not subscription:
        raise HTTPException(status_code=404, detail="User not found")

    now = datetime.now(timezone.utc)
    expires_at = subscription.expires_at
    if expires_at and expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)

    is_active = bool(subscription.status == "active" and expires_at and expires_at > now)
    if subscription.status != ("active" if is_active else "inactive"):
        subscription.status = "active" if is_active else "inactive"
        db.commit()

    return CheckLicenseResponse(
        userId=payload.userId,
        isActive=is_active,
        expiresAt=expires_at,
    )


@app.post("/webhook", response_model=WebhookResponse)
def webhook(
    payload: dict[str, Any],
    x_mp_secret: str = Header(default="", alias="X-MP-Secret"),
    db: Session = Depends(get_db),
) -> WebhookResponse:
    if (
        settings.mp_webhook_secret not in {"", "change-me", "troque-este-segredo"}
        and x_mp_secret
        and x_mp_secret != settings.mp_webhook_secret
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid webhook secret",
        )

    event_id = str(payload.get("eventId") or payload.get("id") or "")
    if not event_id:
        raise HTTPException(status_code=422, detail="Missing event id")
    topic = str(payload.get("topic") or payload.get("type") or "unknown")

    event = WebhookEvent(
        event_id=event_id,
        topic=topic,
        payload_json=json.dumps(payload, ensure_ascii=True),
        status="received",
    )
    db.add(event)
    try:
        db.flush()
    except IntegrityError:
        db.rollback()
        return WebhookResponse(
            message="Duplicate event ignored",
            eventId=event_id,
            topic=topic,
            processed=False,
        )

    user_id = payload.get("userId")
    payment_event = payload.get("event")
    if user_id and payment_event in {"payment_approved", "payment_failed"}:
        duration_days = int(payload.get("durationDays", settings.default_subscription_days))
        subscription = get_or_create_subscription(db, str(user_id))
        message, changed = apply_subscription_event(subscription, str(payment_event), duration_days)
        event.status = "processed" if changed else "ignored"
        event.processed_at = datetime.now(timezone.utc)
        db.commit()
        db.refresh(subscription)
        return WebhookResponse(
            message=message,
            eventId=event_id,
            topic=topic,
            processed=changed,
            userId=str(user_id),
            isActive=bool(subscription.status == "active"),
            expiresAt=subscription.expires_at,
        )

    if topic != "payment":
        event.status = "ignored"
        event.processed_at = datetime.now(timezone.utc)
        db.commit()
        return WebhookResponse(
            message="Event received and ignored (unsupported topic)",
            eventId=event_id,
            topic=topic,
            processed=False,
        )

    raw_data = payload.get("data")
    payment_id = str(raw_data.get("id")) if isinstance(raw_data, dict) and raw_data.get("id") else ""
    if not payment_id:
        event.status = "invalid_payload"
        event.processed_at = datetime.now(timezone.utc)
        db.commit()
        return WebhookResponse(
            message="Notification received with invalid payment payload",
            eventId=event_id,
            topic=topic,
            processed=False,
        )

    payment_data = fetch_mp_payment(payment_id)
    if not payment_data:
        event.status = "lookup_failed"
        event.processed_at = datetime.now(timezone.utc)
        db.commit()
        return WebhookResponse(
            message="Notification received; payment lookup failed",
            eventId=event_id,
            topic=topic,
            processed=False,
        )

    external_reference = payment_data.get("external_reference")
    if not external_reference:
        event.status = "missing_reference"
        event.processed_at = datetime.now(timezone.utc)
        db.commit()
        return WebhookResponse(
            message="Payment found, but missing external_reference",
            eventId=event_id,
            topic=topic,
            processed=False,
        )

    payment_status = str(payment_data.get("status", "")).lower()
    event_mapping = {
        "approved": "payment_approved",
        "rejected": "payment_failed",
        "cancelled": "payment_failed",
        "refunded": "payment_failed",
        "charged_back": "payment_failed",
    }
    internal_event = event_mapping.get(payment_status, "")
    duration_days = extract_duration_days(payment_data)
    user_id = str(external_reference)

    subscription = get_or_create_subscription(db, user_id)
    payment_row = db.execute(
        select(Payment).where(Payment.mp_payment_id == payment_id)
    ).scalar_one_or_none()
    if payment_row is None:
        payment_row = Payment(
            user_id=user_id,
            mp_payment_id=payment_id,
            status=payment_status or "unknown",
            amount=str(payment_data.get("transaction_amount", "")),
            raw_json=json.dumps(payment_data, ensure_ascii=True),
        )
        db.add(payment_row)
    else:
        payment_row.user_id = user_id
        payment_row.status = payment_status or "unknown"
        payment_row.amount = str(payment_data.get("transaction_amount", ""))
        payment_row.raw_json = json.dumps(payment_data, ensure_ascii=True)

    message, changed = apply_subscription_event(subscription, internal_event, duration_days)
    event.status = "processed" if changed else "ignored"
    event.processed_at = datetime.now(timezone.utc)
    db.commit()
    db.refresh(subscription)

    return WebhookResponse(
        message=message if changed else "Payment synced; no subscription change",
        eventId=event_id,
        topic=topic,
        processed=changed,
        userId=user_id,
        isActive=bool(subscription.status == "active"),
        expiresAt=subscription.expires_at,
    )
