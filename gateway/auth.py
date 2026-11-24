"""
Per-device token authentication.

Every device gets its own bearer token: HMAC-SHA256(master secret, device_id). The gateway holds the master secret
(GATEWAY_API_TOKEN) and recomputes the token for whichever device_id the message claims, so:
  - a token stolen from one device only works for that device_id, never for another one;
  - no token list has to be stored; provisioning a new device is `python provision_token.py <device-id>`.

The master secret must be set, must not be a known default, and must be at least 16 characters, otherwise the
gateway refuses to start (set GATEWAY_ALLOW_INSECURE_DEV=1 only for throwaway local experiments).
"""
import hashlib
import hmac
import json
import os

from fastapi import Header, HTTPException, Request, status

import limits

_KNOWN_BAD = {"", "dev-shared-secret-change-me", "changeme", "secret"}


def _load_master() -> str:
    secret = os.getenv("GATEWAY_API_TOKEN", "")
    weak = secret in _KNOWN_BAD or secret.startswith("change-me") or len(secret) < 16
    if weak and os.getenv("GATEWAY_ALLOW_INSECURE_DEV") != "1":
        raise RuntimeError(
            "GATEWAY_API_TOKEN is missing, a known default, or shorter than 16 characters. Set a long random value "
            "in .env, e.g.  python -c \"import secrets; print(secrets.token_urlsafe(32))\"")
    return secret


MASTER_SECRET = _load_master()


def device_token(device_id: str) -> str:
    return hmac.new(MASTER_SECRET.encode(), device_id.encode(), hashlib.sha256).hexdigest()


def _deny(code: int, detail: str) -> HTTPException:
    return HTTPException(status_code=code, detail=detail)


async def verify_token(request: Request, authorization: str = Header(default=None)) -> bool:
    """Checks the bearer token against the device_id inside the (size-limited) request body."""
    client = request.client.host if request.client else "unknown"
    if limits.auth_failures.blocked(client):
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Too many failed attempts; try again later",
                            headers={"Retry-After": "60"})
    scheme, _, token = (authorization or "").partition(" ")
    device_id = None
    if scheme.lower() == "bearer" and token:
        try:
            body = json.loads(await request.body())
            device_id = body.get("device_id") if isinstance(body, dict) else None
        except ValueError:
            device_id = None
    if not isinstance(device_id, str) or not hmac.compare_digest(token.encode(), device_token(device_id).encode()):
        limits.auth_failures.fail(client)
        raise _deny(status.HTTP_401_UNAUTHORIZED, "Missing or invalid credentials for this device")
    request.state.authenticated = True
    return True