"""
Mock authentication, device access rules and the audit log.

This is a deliberate stand-in for a real identity provider: accounts are
hard-coded and passwords are plain text. The shape of the API (login ->
bearer token -> role and device checks on every request) is what a real
implementation would keep; only USERS and login() would change.
"""
import fnmatch
import os
import secrets
import time
from collections import defaultdict, deque

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

SESSION_TTL_S = int(os.getenv("SESSION_TTL_S", str(8 * 3600)))   # one shift; after this the user must sign in again
sessions: dict[str, dict] = {}       # bearer token -> {"user": username, "expires": epoch seconds}
audit: deque = deque(maxlen=1000)    # newest first
_audit_seq = 0


def restore_audit(entries: list[dict], max_id: int) -> None:
    """Reload saved audit entries (newest first) after a restart and continue the numbering."""
    global _audit_seq
    audit.clear()
    audit.extend(entries)
    _audit_seq = max(_audit_seq, max_id)


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
    now = time.time()
    for t in [t for t, s in sessions.items() if s["expires"] <= now]:   # drop expired sessions so the table cannot grow forever
        del sessions[t]
    token = secrets.token_urlsafe(24)
    sessions[token] = {"user": username, "expires": now + SESSION_TTL_S}
    return token


def user_for_token(token: str | None) -> dict | None:
    session = sessions.get(token or "")
    if session and session["expires"] <= time.time():
        sessions.pop(token, None)
        session = None
    return public(session["user"]) if session else None


# ---- login throttling: slows password guessing without letting an attacker flood the audit log ----
LOGIN_WINDOW_S = int(os.getenv("LOGIN_WINDOW_S", "300"))
MAX_FAILS_PER_USER = int(os.getenv("LOGIN_MAX_FAILS_PER_USER", "5"))     # per (client, username)
MAX_FAILS_PER_CLIENT = int(os.getenv("LOGIN_MAX_FAILS_PER_CLIENT", "20"))  # per client, across usernames
_fails: dict[tuple, deque] = defaultdict(deque)


def _recent(key: tuple) -> deque:
    q, now = _fails[key], time.time()
    while q and now - q[0] > LOGIN_WINDOW_S:
        q.popleft()
    if not q:
        _fails.pop(key, None)
    return q


def login_blocked(client: str, username: str) -> bool:
    return len(_recent((client, username))) >= MAX_FAILS_PER_USER or len(_recent((client, "*"))) >= MAX_FAILS_PER_CLIENT


def login_failed(client: str, username: str) -> bool:
    """Count a failed login. Returns True when this failure is the one that starts a lockout."""
    was = login_blocked(client, username)
    now = time.time()
    _fails[(client, username)].append(now)
    _fails[(client, "*")].append(now)
    if len(_fails) > 20_000:
        _fails.pop(next(iter(_fails)))
    return not was and login_blocked(client, username)


def login_succeeded(client: str, username: str) -> None:
    _fails.pop((client, username), None)


def reset_throttle() -> None:
    _fails.clear()


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