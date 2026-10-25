"""
Mock authentication, device access rules and the audit log.

This is a deliberate stand-in for a real identity provider: accounts are
hard-coded and passwords are plain text. The shape of the API (login ->
bearer token -> role and device checks on every request) is what a real
implementation would keep; only USERS and login() would change.
"""
import fnmatch
import secrets
import time
from collections import deque

from fastapi import Header, HTTPException

# "devices" are glob patterns matched against device_id. "*" means every device.
# Edit these to match the device IDs your simulator sends.
USERS = {
    "admin": {
        "password": "admin123", "name": "Dana Reyes", "role": "admin",
        "devices": ["*"],
    },
    "nurse.amara": {
        "password": "nurse123", "name": "Amara Okafor, RN", "role": "nurse",
        "devices": ["infusion-pump-*", "pulse-ox*"],
    },
    "nurse.ben": {
        "password": "nurse123", "name": "Ben Tran, RN", "role": "nurse",
        "devices": ["cardiac-monitor-*"],
    },
}

sessions: dict[str, str] = {}        # bearer token -> username
audit: deque = deque(maxlen=1000)    # newest first
_audit_seq = 0


def public(username: str) -> dict:
    u = USERS[username]
    return {"username": username, "name": u["name"], "role": u["role"], "devices": u["devices"]}


def check_password(username: str, password: str) -> bool:
    u = USERS.get(username)
    # compare even for unknown users so timing does not reveal which names exist
    expected = u["password"] if u else secrets.token_hex(8)
    ok = secrets.compare_digest(password.encode(), expected.encode())
    return bool(u) and ok


def new_session(username: str) -> str:
    token = secrets.token_urlsafe(24)
    sessions[token] = username
    return token


def user_for_token(token: str | None) -> dict | None:
    username = sessions.get(token or "")
    return public(username) if username else None


def can_see(user: dict, device_id: str | None) -> bool:
    if user["role"] == "admin":
        return True
    return any(fnmatch.fnmatch(device_id or "", p) for p in user["devices"])


def nurses_for(device_id: str) -> list[str]:
    """Usernames of the nurses who have access to this device."""
    return [n for n, u in USERS.items()
            if u["role"] == "nurse" and any(fnmatch.fnmatch(device_id or "", p) for p in u["devices"])]


def record(user: str, role: str, action: str, target: str = "", detail: str = "") -> dict:
    """Append one audit entry and return it (the caller broadcasts it)."""
    global _audit_seq
    _audit_seq += 1
    entry = {
        "id": _audit_seq,
        "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "user": user, "role": role, "action": action, "target": target, "detail": detail,
    }
    audit.appendleft(entry)
    return entry


def current_user(authorization: str = Header(default="")) -> dict:
    token = authorization.removeprefix("Bearer ").strip()
    user = user_for_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Not signed in")
    return {**user, "token": token}


def require_admin(authorization: str = Header(default="")) -> dict:
    user = current_user(authorization)
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    return user