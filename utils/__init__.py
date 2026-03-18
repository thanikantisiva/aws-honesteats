"""Utilities package"""


def normalize_phone(phone: str | None) -> str | None:
    """Normalize phone to +91XXXXXXXXXX format. Handles +91..., 91..., or bare 10-digit input."""
    if not phone:
        return phone
    phone = phone.strip()
    if phone.startswith("+91") and len(phone) == 13:
        return phone
    if phone.startswith("91") and len(phone) == 12:
        return f"+{phone}"
    if len(phone) == 10:
        return f"+91{phone}"
    return phone

