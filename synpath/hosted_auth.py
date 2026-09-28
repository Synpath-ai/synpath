"""Browser-assisted login and API-key management for Synpath's hosted API."""
from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import re
import secrets
import sys
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, urlencode, urlparse
from urllib.request import Request, urlopen

DEFAULT_BASE_URL = "https://api2.synpath.dev"


def credentials_path() -> Path:
    override = os.environ.get("SYNPATH_CREDENTIALS_FILE")
    if override:
        return Path(override).expanduser()
    root = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
    return root / "synpath" / "credentials.json"


def load_credentials() -> dict:
    path = credentials_path()
    if not path.exists():
        return {}
    if os.name != "nt" and path.stat().st_mode & 0o077:
        raise RuntimeError(f"Credential file must be private (chmod 600): {path}")
    return json.loads(path.read_text())


def save_credentials(credentials: dict) -> None:
    path = credentials_path()
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(path.name + ".tmp." + secrets.token_hex(6))
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as stream:
            json.dump(credentials, stream, indent=2)
            stream.write("\n")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def stored_api_key() -> str | None:
    return load_credentials().get("api_key")


def _random() -> str:
    return base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode()


def _challenge(verifier: str) -> str:
    return base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()


def _request(base: str, path: str, *, method: str = "GET", token: str | None = None,
             body: dict | None = None) -> dict:
    headers = {"Accept": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    data = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(body).encode()
    request = Request(base.rstrip("/") + path, data=data, headers=headers, method=method)
    try:
        with urlopen(request, timeout=15) as response:
            return json.load(response) if response.status != 204 else {}
    except HTTPError as exc:
        try:
            detail = json.load(exc).get("error", {}).get("code", "request_failed")
        except (ValueError, AttributeError):
            detail = "request_failed"
        raise RuntimeError(f"Synpath API returned {exc.code}: {detail}") from None
    except URLError as exc:
        raise RuntimeError(f"Could not reach Synpath API: {exc.reason}") from None


def _base_url(explicit: str | None = None) -> str:
    base = explicit or os.environ.get("SYNPATH_HISTORY_URL") or DEFAULT_BASE_URL
    parsed = urlparse(base)
    if parsed.scheme != "https" and not (parsed.scheme == "http" and parsed.hostname in ("localhost", "127.0.0.1")):
        raise RuntimeError("Hosted API URL must use HTTPS")
    if parsed.path not in ("", "/") or parsed.query or parsed.fragment:
        raise RuntimeError("Hosted API URL must be an origin without a path")
    return base.rstrip("/")


def login(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="synpath login")
    parser.add_argument("--base-url")
    parser.add_argument("--no-browser", action="store_true")
    args = parser.parse_args(argv)
    base = _base_url(args.base_url)
    state = _random()
    verifier = _random()
    result: dict[str, str] = {}

    class Callback(BaseHTTPRequestHandler):
        def do_GET(self):
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            code = query.get("code", [""])[0]
            incoming_state = query.get("state", [""])[0]
            valid = (parsed.path == "/callback" and hmac.compare_digest(incoming_state, state)
                     and re.fullmatch(r"[A-Za-z0-9_-]{43}", code))
            if valid:
                result["code"] = code
            self.send_response(200 if valid else 400)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Referrer-Policy", "no-referrer")
            self.end_headers()
            self.wfile.write(b"<h1>Synpath sign-in complete</h1><p>Return to your terminal.</p>"
                             if valid else b"<h1>Invalid sign-in response</h1>")

        def log_message(self, _format, *_args):
            pass  # The URL contains a one-time authorization code.

    with HTTPServer(("127.0.0.1", 0), Callback) as listener:
        port = listener.server_port
        query = urlencode({"cli_port": port, "cli_state": state, "cli_challenge": _challenge(verifier)})
        url = f"{base}/oauth/google/start?{query}"
        print("Open this URL to sign in with Google:", url, flush=True)
        if not args.no_browser:
            webbrowser.open(url)
        deadline = time.monotonic() + 300
        listener.timeout = 1
        while "code" not in result and time.monotonic() < deadline:
            listener.handle_request()
    if "code" not in result:
        raise RuntimeError("Google sign-in timed out")
    data = _request(base, "/v1/auth/cli/exchange", method="POST",
                    body={"code": result["code"], "code_verifier": verifier})
    credentials = load_credentials()
    credentials.update({"base_url": base, "session_token": data["session_token"],
                        "session_expires_at": int(time.time()) + data["expires_in"]})
    save_credentials(credentials)
    print("Signed in. Management session saved with restricted permissions.")
    return 0


def keys(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="synpath keys")
    sub = parser.add_subparsers(dest="action", required=True)
    create = sub.add_parser("create")
    create.add_argument("label")
    create.add_argument("--no-print-key", action="store_true")
    sub.add_parser("list")
    revoke = sub.add_parser("revoke")
    revoke.add_argument("id")
    args = parser.parse_args(argv)
    credentials = load_credentials()
    token = credentials.get("session_token")
    if not token or time.time() >= credentials.get("session_expires_at", 0):
        raise RuntimeError("Sign in first with `synpath login`")
    base = _base_url(credentials.get("base_url"))
    if args.action == "create":
        data = _request(base, "/v1/auth/keys", method="POST", token=token, body={"label": args.label})
        credentials["api_key"] = data["secret"]
        save_credentials(credentials)
        print(f"Created key {data['id']} and saved it to {credentials_path()}")
        if not args.no_print_key:
            print(f"API key (shown once): {data['secret']}")
    elif args.action == "list":
        data = _request(base, "/v1/auth/keys", token=token)
        for key in data["keys"]:
            print(f"{key['id']}\t{key['label']}\t{'revoked' if key['revoked_at'] else 'active'}")
    else:
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", args.id):
            raise RuntimeError("Invalid key ID")
        _request(base, "/v1/auth/keys/" + args.id, method="DELETE", token=token)
        if credentials.get("api_key", "").startswith(f"spk_{args.id}."):
            credentials.pop("api_key", None)
            save_credentials(credentials)
        print(f"Revoked key {args.id}")
    return 0


def logout(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="synpath logout")
    parser.parse_args(argv)
    credentials = load_credentials()
    token = credentials.pop("session_token", None)
    credentials.pop("session_expires_at", None)
    if token:
        _request(_base_url(credentials.get("base_url")), "/v1/auth/logout", method="POST", token=token)
    save_credentials(credentials)
    print("Signed out of key management. Existing API keys remain valid.")
    return 0
