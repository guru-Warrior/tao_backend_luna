"""
App-level password gate: set APP_ACCESS_PASSWORD in .env (non-empty).
When unset, all routes stay open (backward compatible).
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import time

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, RedirectResponse

COOKIE_NAME = "app_access"
MAX_AGE_SEC = 60 * 60 * 24 * 7  # 7 days
_PEPPER = b"::tao-trading-app-auth-v1"


def auth_enabled() -> bool:
    return bool(os.environ.get("APP_ACCESS_PASSWORD", "").strip())


def _signing_key() -> bytes:
    p = os.environ.get("APP_ACCESS_PASSWORD", "").strip().encode("utf-8")
    return hashlib.sha256(p + _PEPPER).digest()


def create_session_token() -> str:
    payload = {"exp": int(time.time()) + MAX_AGE_SEC, "v": 1}
    raw = json.dumps(payload, separators=(",", ":")).encode()
    body = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
    sig = hmac.new(_signing_key(), body.encode("ascii"), hashlib.sha256).hexdigest()
    return f"{body}.{sig}"


def verify_session_token(token: str) -> bool:
    if not token or "." not in token:
        return False
    try:
        body, sig = token.rsplit(".", 1)
        expected = hmac.new(_signing_key(), body.encode("ascii"), hashlib.sha256).hexdigest()
        if not secrets.compare_digest(expected, sig):
            return False
        pad = (-len(body)) % 4
        if pad:
            body += "=" * pad
        data = json.loads(base64.urlsafe_b64decode(body.encode("ascii")))
        exp = int(data.get("exp", 0))
        return int(time.time()) <= exp
    except Exception:
        return False


def password_matches(given: str) -> bool:
    expected = os.environ.get("APP_ACCESS_PASSWORD", "").strip()
    if not expected:
        return False
    try:
        return secrets.compare_digest(given.encode("utf-8"), expected.encode("utf-8"))
    except Exception:
        return False


def is_authenticated_request(cookies: dict[str, str]) -> bool:
    if not auth_enabled():
        return True
    token = cookies.get(COOKIE_NAME)
    return bool(token and verify_session_token(token))


def websocket_authenticated(cookies: dict[str, str]) -> bool:
    return is_authenticated_request(cookies)


def exempt_path(path: str) -> bool:
    if path == "/login":
        return True
    if path == "/api/health":
        return True
    if path == "/api/auth/login" or path == "/api/auth/status" or path == "/api/auth/logout":
        return True
    if path == "/favicon.ico":
        return True
    return False


class AppAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if request.method == "OPTIONS":
            return await call_next(request)
        if not auth_enabled():
            return await call_next(request)
        path = request.url.path
        if exempt_path(path):
            return await call_next(request)
        if is_authenticated_request(request.cookies):
            return await call_next(request)
        if path.startswith("/api/"):
            return JSONResponse({"detail": "Not authenticated"}, status_code=401)
        if request.method == "GET" and path.startswith("/assets/"):
            return RedirectResponse(url="/login", status_code=302)
        accepts = request.headers.get("accept") or ""
        if request.method == "GET" and "text/html" in accepts:
            return RedirectResponse(url="/login", status_code=302)
        return JSONResponse({"detail": "Not authenticated"}, status_code=401)


LOGIN_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Sign in</title>
  <style>
    * { box-sizing: border-box; }
    body { font-family: system-ui, sans-serif; background: #0f1115; color: #e6e8ec;
      margin: 0; min-height: 100vh; display: flex; align-items: center; justify-content: center; }
    form { width: 100%; max-width: 20rem; padding: 1.5rem; border-radius: 8px;
      background: #161a22; border: 1px solid #2a3140; }
    label { display: block; font-size: 0.75rem; text-transform: uppercase; letter-spacing: 0.08em;
      color: #8b929e; margin-bottom: 0.35rem; }
    input { width: 100%; padding: 0.5rem 0.65rem; border-radius: 6px; border: 1px solid #2a3140;
      background: #0f1115; color: #e6e8ec; margin-bottom: 1rem; }
    button { width: 100%; padding: 0.55rem; border: none; border-radius: 6px;
      background: #3b82f6; color: #fff; font-weight: 600; cursor: pointer; }
    button:hover { background: #2563eb; }
    .err { color: #f87171; font-size: 0.85rem; margin-bottom: 0.75rem; min-height: 1.2em; }
  </style>
</head>
<body>
  <form method="post" action="/api/auth/login" id="f">
    <label for="p">Password</label>
    <input type="password" id="p" name="password" autocomplete="current-password" required/>
    <div class="err" id="e"></div>
    <button type="submit">Continue</button>
  </form>
  <script>
    document.getElementById('f').addEventListener('submit', async function(ev) {
      ev.preventDefault();
      var p = document.getElementById('p').value;
      var e = document.getElementById('e');
      e.textContent = '';
      try {
        var r = await fetch('/api/auth/login', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          credentials: 'same-origin',
          body: JSON.stringify({ password: p })
        });
        var j = await r.json().catch(function() { return {}; });
        if (r.ok && j.status === 'ok') { window.location.href = '/'; return; }
        e.textContent = j.message || 'Invalid password';
      } catch (x) {
        e.textContent = 'Request failed';
      }
    });
  </script>
</body>
</html>
"""
