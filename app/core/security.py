from hmac import new, compare_digest
from hashlib import sha256
from typing import Optional


def verify_webhook_signature(
        payload: bytes,
        signature: Optional[str],
        secret: str
) -> bool:
    """
    Verify webhook signature using HMAC-SHA256

    Args:
        payload: Raw request body
        signature: Signature from header (format: "sha256=...")
        secret: Webhook secret key

    Returns:
        True if signature is valid
    """
    if not signature:
        return False

    try:
        method, provided_hash = signature.split("=", 1)
        if method != "sha256":
            return False
    except ValueError:
        return False

    expected_hash = new(
        secret.encode(),
        payload,
        sha256
    ).hexdigest()

    return compare_digest(expected_hash, provided_hash)


async def verify_telegram_webapp_init_data(init_data: str, bot_token: str) -> bool:
    """
    Verify Telegram Mini App initData

    Args:
        init_data: Data from Telegram.WebApp.initData
        bot_token: Bot token

    Returns:
        True if data is valid
    """
    try:
        from urllib.parse import parse_qsl

        parsed = dict(parse_qsl(init_data))
        hash_value = parsed.pop("hash", None)

        if not hash_value:
            return False

        data_check_arr = [f"{k}={v}" for k, v in sorted(parsed.items())]
        data_check_string = "\n".join(data_check_arr)

        secret_key = new(
            "WebAppData".encode(),
            bot_token.encode(),
            sha256
        ).digest()

        calculated_hash = new(
            secret_key,
            data_check_string.encode(),
            sha256
        ).hexdigest()

        return compare_digest(calculated_hash, hash_value)

    except Exception:
        return False
