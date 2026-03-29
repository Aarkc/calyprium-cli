#!/usr/bin/env python3
"""Calyprium CLI -- autonomous web scraping from the command line."""
from __future__ import annotations

import argparse
import hashlib
import html.parser
import http.server
import json
import os
import re
import secrets
import sys
import textwrap
import threading
import time
import webbrowser
import base64
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlencode, urlparse, parse_qs

import httpx

# Fix Windows console encoding for unicode output
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# ---------------------------------------------------------------------------
# ANSI Colors (respects NO_COLOR and pipe detection)
# ---------------------------------------------------------------------------

if os.getenv("NO_COLOR") or not sys.stdout.isatty():
    DIM = BOLD = CYAN = GREEN = RED = YELLOW = RESET = ""
else:
    DIM = "\033[2m"
    BOLD = "\033[1m"
    CYAN = "\033[36m"
    GREEN = "\033[32m"
    RED = "\033[31m"
    YELLOW = "\033[33m"
    RESET = "\033[0m"

# ---------------------------------------------------------------------------
# Spinner
# ---------------------------------------------------------------------------

class Spinner:
    """Minimal terminal spinner."""
    _FRAMES = "\u280b\u2819\u2839\u2838\u283c\u2834\u2826\u2827\u2807\u280f"

    def __init__(self, text=""):
        self.text = text
        self._stop = threading.Event()
        self._thread = None

    def __enter__(self):
        if sys.stderr.isatty():
            self._thread = threading.Thread(target=self._spin, daemon=True)
            self._thread.start()
        return self

    def __exit__(self, *_):
        self._stop.set()
        if self._thread:
            self._thread.join()
            sys.stderr.write(f"\r\033[K")
            sys.stderr.flush()

    def _spin(self):
        i = 0
        while not self._stop.wait(0.08):
            frame = self._FRAMES[i % len(self._FRAMES)]
            sys.stderr.write(f"\r  {frame} {self.text}")
            sys.stderr.flush()
            i += 1

    def update(self, text):
        self.text = text


class StageTracker:
    """Manages pipeline stage display with inline spinner on the active stage.

    Prints completed stages with a ✓/✗ indicator and shows a spinner on the
    currently running stage. Call `update()` with each new stage message and
    `finish()` when the pipeline completes.
    """
    _FRAMES = Spinner._FRAMES

    def __init__(self):
        self._active_stage: str | None = None
        self._active_text: str = ""
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._is_tty = sys.stderr.isatty()

    def update(self, stage: str, text: str, status: str = "running"):
        """Set the current stage. Finalizes the previous stage first."""
        with self._lock:
            # Finalize the previous active stage as "done"
            if self._active_stage and self._active_stage != stage:
                self._finalize_line("done")

            self._active_stage = stage
            self._active_text = text

            if status in ("done", "fail"):
                self._finalize_line(status)
                return

            # Start spinner if not already running
            if self._is_tty and not self._thread:
                self._stop.clear()
                self._thread = threading.Thread(target=self._spin, daemon=True)
                self._thread.start()
            elif not self._is_tty:
                # Non-tty: just print the line
                label = STAGE_NAMES.get(stage, stage)
                sys.stderr.write(f"  * {label:<14s} {text}\n")
                sys.stderr.flush()

    def finish(self, status: str = "done"):
        """Finalize the last active stage."""
        with self._lock:
            if self._active_stage:
                self._finalize_line(status)

    def _finalize_line(self, status: str):
        """Clear spinner and print the completed stage line."""
        self._stop_spinner()
        stage = self._active_stage or ""
        text = self._active_text
        label = STAGE_NAMES.get(stage, stage)

        if status == "fail":
            indicator = f"{RED}✗{RESET}"
        else:
            indicator = f"{GREEN}✓{RESET}"

        # Clear current line and print final
        if self._is_tty:
            sys.stderr.write(f"\r\033[K")
        sys.stderr.write(f"  {indicator} {BOLD}{label:<14s}{RESET} {DIM}{text}{RESET}\n")
        sys.stderr.flush()
        self._active_stage = None
        self._active_text = ""

    def _stop_spinner(self):
        if self._thread:
            self._stop.set()
            self._thread.join()
            self._thread = None

    def _spin(self):
        i = 0
        while not self._stop.wait(0.08):
            with self._lock:
                stage = self._active_stage or ""
                text = self._active_text
            label = STAGE_NAMES.get(stage, stage)
            frame = self._FRAMES[i % len(self._FRAMES)]
            sys.stderr.write(f"\r\033[K  {frame} {BOLD}{label:<14s}{RESET} {DIM}{text}{RESET}")
            sys.stderr.flush()
            i += 1


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ENVIRONMENTS = {
    "prod": {
        "agent_url": "https://strata.calyprium.com",
        "mimic_url": "https://mimic.calyprium.com",
        "prism_url": "https://prism.calyprium.com",
        "forge_url": "https://forge.calyprium.com",
        "keycloak_url": "https://auth.calyprium.com",
        "realm": "calyprium",
        "client_id": "calyprium-backend",
    },
}
DEFAULT_ENV = "prod"

DEFAULT_AGENT_URL = ENVIRONMENTS[DEFAULT_ENV]["agent_url"]
DEFAULT_MIMIC_URL = ENVIRONMENTS[DEFAULT_ENV]["mimic_url"]
DEFAULT_KEYCLOAK_URL = ENVIRONMENTS[DEFAULT_ENV]["keycloak_url"]
DEFAULT_REALM = ENVIRONMENTS[DEFAULT_ENV]["realm"]
DEFAULT_CLIENT_ID = ENVIRONMENTS[DEFAULT_ENV]["client_id"]
DEFAULT_CLIENT_SECRET = "change-me-in-production"


def _load_env():
    """Load .env file from project root if python-dotenv is available."""
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        try:
            from dotenv import load_dotenv
            load_dotenv(env_path)
        except ImportError:
            with open(env_path) as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" in line:
                        key, _, value = line.partition("=")
                        key = key.strip()
                        value = value.strip()
                        if not os.environ.get(key):
                            os.environ[key] = value


_load_env()


def get_config(env: str = DEFAULT_ENV) -> dict:
    """Build config by layering: environment preset < env vars < CLI flags."""
    base = ENVIRONMENTS.get(env, ENVIRONMENTS[DEFAULT_ENV])
    return {
        "agent_url": os.getenv("CALYPRIUM_URL", base["agent_url"]).rstrip("/"),
        "mimic_url": os.getenv("MIMIC_URL", base["mimic_url"]).rstrip("/"),
        "prism_url": os.getenv("PRISM_URL", base["prism_url"]).rstrip("/"),
        "forge_url": os.getenv("FORGE_URL", base["forge_url"]).rstrip("/"),
        "keycloak_url": os.getenv("KEYCLOAK_URL", base["keycloak_url"]).rstrip("/"),
        "realm": os.getenv("KEYCLOAK_REALM", base["realm"]),
        "client_id": os.getenv("KEYCLOAK_CLIENT_ID", base["client_id"]),
        "client_secret": os.getenv("KEYCLOAK_CLIENT_SECRET", DEFAULT_CLIENT_SECRET),
    }


# ---------------------------------------------------------------------------
# Auth — Token storage
# ---------------------------------------------------------------------------

TOKEN_DIR = Path.home() / ".calyprium"
TOKEN_FILE = TOKEN_DIR / "tokens.json"
LOGIN_CLIENT_ID = "calyprium-ui"  # Public client with PKCE
LOGIN_REDIRECT_PORT = 11899
LOGIN_REDIRECT_URI = f"http://localhost:{LOGIN_REDIRECT_PORT}/callback"

_token_cache: dict = {}


def _save_tokens(data: dict):
    """Persist tokens to disk."""
    TOKEN_DIR.mkdir(parents=True, exist_ok=True)
    TOKEN_FILE.write_text(json.dumps(data, indent=2))


def _load_tokens() -> dict:
    """Load tokens from disk."""
    if TOKEN_FILE.exists():
        try:
            return json.loads(TOKEN_FILE.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def _refresh_access_token(cfg: dict, refresh_token: str) -> dict | None:
    """Use a refresh token to get a new access token."""
    token_url = (
        f"{cfg['keycloak_url']}/realms/{cfg['realm']}"
        f"/protocol/openid-connect/token"
    )
    try:
        resp = httpx.post(
            token_url,
            data={
                "client_id": LOGIN_CLIENT_ID,
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
            },
            timeout=10,
        )
        if resp.status_code == 200:
            return resp.json()
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# Auth — Login (browser PKCE flow)
# ---------------------------------------------------------------------------

def cmd_login(args, cfg: dict):
    """Open browser for Keycloak login (PKCE authorization code flow)."""
    _header("login")

    # Generate PKCE challenge
    code_verifier = secrets.token_urlsafe(64)
    code_challenge = (
        base64.urlsafe_b64encode(hashlib.sha256(code_verifier.encode()).digest())
        .rstrip(b"=")
        .decode()
    )
    state = secrets.token_urlsafe(32)

    auth_url = (
        f"{cfg['keycloak_url']}/realms/{cfg['realm']}"
        f"/protocol/openid-connect/auth?"
        + urlencode({
            "client_id": LOGIN_CLIENT_ID,
            "response_type": "code",
            "redirect_uri": LOGIN_REDIRECT_URI,
            "scope": "openid profile email",
            "state": state,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        })
    )

    # One-shot HTTP server to catch the callback
    auth_code = None
    server_error = None

    class _CallbackHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            nonlocal auth_code, server_error
            qs = parse_qs(urlparse(self.path).query)
            if qs.get("state", [None])[0] != state:
                server_error = "State mismatch"
                self._respond("Login failed: state mismatch. Please try again.")
                return
            if "error" in qs:
                server_error = qs["error"][0]
                self._respond(f"Login failed: {server_error}")
                return
            auth_code = qs.get("code", [None])[0]
            self._respond("Login successful! You can close this tab.")

        def _respond(self, msg):
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(
                f"<html><body style='font-family:system-ui;padding:2em;text-align:center'>"
                f"<h2>{msg}</h2>"
                f"<p style='color:#666'>Return to your terminal.</p>"
                f"</body></html>".encode()
            )

        def log_message(self, *_):
            pass  # Suppress request logs

    server = http.server.HTTPServer(("127.0.0.1", LOGIN_REDIRECT_PORT), _CallbackHandler)
    server.timeout = 120

    sys.stderr.write(f"  Opening browser for login...\n")
    sys.stderr.write(f"  {DIM}(waiting for callback on port {LOGIN_REDIRECT_PORT}){RESET}\n\n")
    webbrowser.open(auth_url)

    # Wait for the callback
    server.handle_request()
    server.server_close()

    if server_error or not auth_code:
        _die(f"Login failed: {server_error or 'no authorization code received'}")

    # Exchange code for tokens
    token_url = (
        f"{cfg['keycloak_url']}/realms/{cfg['realm']}"
        f"/protocol/openid-connect/token"
    )
    try:
        resp = httpx.post(
            token_url,
            data={
                "client_id": LOGIN_CLIENT_ID,
                "grant_type": "authorization_code",
                "code": auth_code,
                "redirect_uri": LOGIN_REDIRECT_URI,
                "code_verifier": code_verifier,
            },
            timeout=10,
        )
        resp.raise_for_status()
    except httpx.HTTPStatusError as e:
        _die(f"Token exchange failed: {e.response.status_code}\n  {e.response.text[:200]}")

    token_data = resp.json()
    stored = {
        "access_token": token_data["access_token"],
        "refresh_token": token_data.get("refresh_token"),
        "expires_at": time.time() + token_data.get("expires_in", 300) - 30,
        "env": cfg.get("_env_name", "prod"),
    }
    _save_tokens(stored)

    # Decode token for user info
    try:
        payload_b64 = token_data["access_token"].split(".")[1]
        payload_b64 += "=" * (4 - len(payload_b64) % 4)  # pad
        payload = json.loads(base64.urlsafe_b64decode(payload_b64))
        user = payload.get("preferred_username") or payload.get("email") or payload.get("sub", "unknown")
    except Exception:
        user = "authenticated"

    sys.stderr.write(f"  {GREEN}Logged in as {BOLD}{user}{RESET}\n")
    sys.stderr.write(f"  {DIM}Tokens saved to {TOKEN_FILE}{RESET}\n\n")


def cmd_logout(args, cfg: dict):
    """Remove stored tokens."""
    _header("logout")
    if TOKEN_FILE.exists():
        TOKEN_FILE.unlink()
        sys.stderr.write(f"  Tokens removed.\n\n")
    else:
        sys.stderr.write(f"  No stored tokens found.\n\n")


# ---------------------------------------------------------------------------
# Auth — Token resolution
# ---------------------------------------------------------------------------

def get_token(cfg: dict) -> str:
    """Get an access token. Tries: API key > memory cache > stored tokens > client credentials."""
    # 0. API key from environment (no expiry, returned directly)
    api_key = os.getenv("CALYPRIUM_API_KEY")
    if api_key:
        return api_key

    # 1. Memory cache
    cached = _token_cache.get("token")
    if cached and _token_cache.get("expires_at", 0) > time.time():
        return cached

    # 2. Stored tokens from `calyprium login`
    stored = _load_tokens()
    if stored.get("access_token"):
        if stored.get("expires_at", 0) > time.time():
            _token_cache["token"] = stored["access_token"]
            _token_cache["expires_at"] = stored["expires_at"]
            return stored["access_token"]

        # Try refresh
        if stored.get("refresh_token"):
            refreshed = _refresh_access_token(cfg, stored["refresh_token"])
            if refreshed:
                token = refreshed["access_token"]
                stored["access_token"] = token
                stored["refresh_token"] = refreshed.get("refresh_token", stored["refresh_token"])
                stored["expires_at"] = time.time() + refreshed.get("expires_in", 300) - 30
                _save_tokens(stored)
                _token_cache["token"] = token
                _token_cache["expires_at"] = stored["expires_at"]
                return token

    # 3. Client credentials (service account)
    client_secret = cfg.get("client_secret", "")
    if client_secret and client_secret != DEFAULT_CLIENT_SECRET:
        token_url = (
            f"{cfg['keycloak_url']}/realms/{cfg['realm']}"
            f"/protocol/openid-connect/token"
        )
        try:
            resp = httpx.post(
                token_url,
                data={
                    "client_id": cfg["client_id"],
                    "client_secret": client_secret,
                    "grant_type": "client_credentials",
                },
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            token = data["access_token"]
            _token_cache["token"] = token
            _token_cache["expires_at"] = time.time() + data.get("expires_in", 300) - 30
            return token
        except Exception:
            pass

    # 4. Nothing worked
    _die(f"Not authenticated. Run {BOLD}calyprium login{RESET} to sign in.")


def api_headers(cfg: dict) -> dict:
    return {
        "Authorization": f"Bearer {get_token(cfg)}",
        "Content-Type": "application/json",
    }


# ---------------------------------------------------------------------------
# API Client
# ---------------------------------------------------------------------------

def api_get(cfg: dict, path: str) -> dict:
    url = f"{cfg['agent_url']}{path}"
    resp = httpx.get(url, headers=api_headers(cfg), timeout=30)
    resp.raise_for_status()
    return resp.json()


def api_post(cfg: dict, path: str, body: dict) -> dict:
    url = f"{cfg['agent_url']}{path}"
    resp = httpx.post(url, headers=api_headers(cfg), json=body, timeout=30)
    resp.raise_for_status()
    return resp.json()


def api_stream(cfg: dict, path: str, body: dict):
    """Stream SSE events from the API. Yields (event_type, data) tuples."""
    url = f"{cfg['agent_url']}{path}"
    with httpx.stream(
        "POST",
        url,
        headers=api_headers(cfg),
        json=body,
        timeout=httpx.Timeout(connect=10, read=300, write=10, pool=10),
    ) as resp:
        resp.raise_for_status()
        event_type = None
        data_lines = []

        for line in resp.iter_lines():
            if line.startswith("event:"):
                event_type = line[6:].strip()
            elif line.startswith("data:"):
                data_lines.append(line[5:].strip())
            elif line == "" and event_type:
                raw = "\n".join(data_lines)
                try:
                    parsed = json.loads(raw)
                except (json.JSONDecodeError, ValueError):
                    parsed = raw
                yield event_type, parsed
                event_type = None
                data_lines = []


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def _die(msg: str):
    """Print error and exit."""
    print(f"\n  {RED}error{RESET}  {msg}", file=sys.stderr)
    sys.exit(1)


def _kv(key: str, value: str, indent: int = 2):
    """Print a key-value pair with aligned formatting."""
    print(f"{' ' * indent}{DIM}{key:<8s}{RESET} {value}")


def _header(title: str):
    """Print a command header line."""
    print(f"\n  {DIM}calyprium {title}{RESET}\n")


def _relative_time(iso_str: str) -> str:
    """Convert ISO timestamp to relative time string like '2h ago'."""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        delta = now - dt
        seconds = int(delta.total_seconds())
        if seconds < 60:
            return f"{seconds}s ago"
        elif seconds < 3600:
            return f"{seconds // 60}m ago"
        elif seconds < 86400:
            return f"{seconds // 3600}h ago"
        elif seconds < 604800:
            return f"{seconds // 86400}d ago"
        else:
            return dt.strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return iso_str or ""


STAGE_NAMES = {
    "Recon": "Recon",
    "API Discovery": "API Discovery",
    "Strategy": "Strategy",
    "Selector Mapping": "Selectors",
    "Generate": "Generate",
    "Validate": "Validate",
    "Iterate": "Iterate",
    "Report": "Report",
}


def _stage_line(stage: str, text: str, status: str = "running"):
    """Print a pipeline stage line with status indicator."""
    if status == "done":
        indicator = f"{GREEN}*{RESET}"
    elif status == "fail":
        indicator = f"{RED}x{RESET}"
    else:
        indicator = f"{YELLOW}*{RESET}"
    label = STAGE_NAMES.get(stage, stage)
    print(f"  {indicator} {BOLD}{label:<14s}{RESET} {DIM}{text}{RESET}")


def _parse_stage_message(text: str) -> tuple[str | None, str]:
    """Extract (stage_name, clean_text) from a **[Stage]** message."""
    for stage in STAGE_NAMES:
        if text.startswith(f"**[{stage}]**"):
            clean = text.replace(f"**[{stage}]**", "").strip()
            return stage, clean
    clean = text.replace("**", "").replace("[", "").replace("]", "")
    return None, clean


def _print_stage(text: str, tracker: StageTracker | None = None):
    """Parse and print a stage progress message."""
    stage, clean = _parse_stage_message(text)
    if stage and tracker:
        tracker.update(stage, clean)
    elif stage:
        _stage_line(stage, clean)
    else:
        print(f"  {DIM}{clean}{RESET}")


def _print_message(msg: dict):
    """Print a message from thread history."""
    msg_type = msg.get("type", "unknown")
    content = msg.get("content", "")

    if msg_type == "human":
        print(f"\n  {BOLD}> {content}{RESET}")
    elif msg_type == "ai":
        if isinstance(content, str) and content.strip():
            if content.startswith("**["):
                _print_stage(content)
            else:
                lines = content.split("\n")
                print()
                if len(lines) > 30:
                    for line in lines[:30]:
                        print(f"  {line}")
                    print(f"  {DIM}... ({len(lines) - 30} more lines){RESET}")
                else:
                    for line in lines:
                        print(f"  {line}")
    elif msg_type == "tool":
        name = msg.get("name", "unknown")
        if isinstance(content, str) and len(content) > 200:
            content = content[:200] + "..."
        print(f"  {DIM}[{name}]{RESET} {content}")


# ---------------------------------------------------------------------------
# Commands: fetch
# ---------------------------------------------------------------------------

def cmd_fetch(args, cfg: dict):
    """Fetch a page via the Mimic browser service."""
    url = args.url
    if not url.startswith(("http://", "https://")):
        url = f"https://{url}"
    mimic_url = cfg["mimic_url"]
    output_format = args.format or "markdown"

    _header("fetch")

    # Build fetch request
    fetch_body: dict = {
        "url": url,
        "headless": True,
        "use_proxy": not args.no_proxy,
        "browser_engine": args.engine or "auto",
    }
    if args.stealth:
        fetch_body["stealth_level"] = args.stealth
    if args.proxy_type:
        fetch_body["proxy_type"] = args.proxy_type
    if args.proxy_country:
        fetch_body["proxy_country"] = args.proxy_country
    if hasattr(args, 'proxy_profile') and args.proxy_profile:
        fetch_body["proxy_profile"] = args.proxy_profile
    if hasattr(args, 'proxy_template') and args.proxy_template:
        fetch_body["proxy_template"] = args.proxy_template
    if args.timeout:
        fetch_body["timeout"] = args.timeout
    if args.wait:
        fetch_body["wait_after_load"] = args.wait
    if args.wait_until:
        fetch_body["wait_until"] = args.wait_until
    if args.screenshot:
        fetch_body["take_screenshot"] = True
    if args.network:
        fetch_body["capture_network"] = True
    if args.console:
        fetch_body["capture_console"] = True
    if args.track_api:
        fetch_body["track_api_calls"] = True
        fetch_body["capture_scripts"] = True

    # Spectre fingerprint settings
    if args.no_spectre:
        fetch_body["use_spectre"] = False
    if args.spectre_profile:
        fetch_body["spectre_profile_id"] = args.spectre_profile
    if args.spectre_session:
        fetch_body["spectre_session_id"] = args.spectre_session

    # Auth header
    fetch_headers = {"Content-Type": "application/json"}
    token = get_token(cfg)
    fetch_headers["Authorization"] = f"Bearer {token}"

    # Make the request
    t0 = time.monotonic()
    with Spinner(f"Fetching {url}"):
        try:
            resp = httpx.post(
                f"{mimic_url}/api/fetch",
                json=fetch_body,
                headers=fetch_headers,
                timeout=httpx.Timeout(connect=10, read=120, write=10, pool=10),
            )
            resp.raise_for_status()
        except httpx.ConnectError:
            _die(f"Cannot connect to Mimic at {mimic_url}\n"
                 "  Check your MIMIC_URL or run `calyprium login` to authenticate.")
        except httpx.HTTPStatusError as e:
            try:
                body = e.response.json()
                detail = body.get("detail", body.get("message", e.response.text))
            except Exception:
                detail = e.response.text[:500]
            _die(f"{e.response.status_code} {detail}")

    elapsed = time.monotonic() - t0
    data = resp.json()
    html_content = data.get("html", "")
    status_code = data.get("status_code", 0)
    final_url = data.get("final_url", url)
    engine = data.get("browser_engine", "unknown")

    # Status line
    status_color = GREEN if 200 <= status_code < 400 else RED
    _kv("url", url)
    _kv("engine", f"{engine} {DIM}->{RESET} {status_color}{status_code}{RESET} in {elapsed:.1f}s")
    _kv("size", f"{len(html_content):,} bytes")
    if final_url != url:
        _kv("redirect", final_url)
    print()

    # Save screenshot if requested
    if args.screenshot and data.get("screenshot"):
        import base64
        screenshot_path = args.screenshot if isinstance(args.screenshot, str) and args.screenshot != "True" else "screenshot.png"
        if screenshot_path is True or screenshot_path == "True":
            screenshot_path = "screenshot.png"
        with open(screenshot_path, "wb") as f:
            f.write(base64.b64decode(data["screenshot"]))
        print(f"  {GREEN}*{RESET} Screenshot saved: {screenshot_path}")

    # Process output based on format/extraction mode
    output = ""

    if args.selector:
        results = _extract_with_selectors(html_content, args.selector)
        print(f"  {DIM}Matched {len(results)} elements with: {args.selector}{RESET}\n")
        output = json.dumps(results, indent=2, ensure_ascii=False)

    elif args.extract:
        print(f"  {DIM}Extracting: {args.extract}{RESET}")
        with Spinner("Querying LLM"):
            output = _extract_with_llm(html_content, args.extract, cfg)
        print()

    elif output_format == "html":
        output = html_content

    elif output_format == "text":
        if args.raw:
            extractor = _HTMLTextExtractor()
            extractor.feed(html_content)
            output = extractor.get_text()
        else:
            output = _html_to_text(html_content)

    elif output_format == "markdown":
        if args.raw:
            try:
                import html2text
                h = html2text.HTML2Text()
                h.body_width = 0
                output = h.handle(html_content).strip()
            except ImportError:
                output = _html_to_text(html_content)
        else:
            output = _html_to_markdown(html_content)

    elif output_format == "json":
        output = json.dumps(data, indent=2, ensure_ascii=False)

    # Output
    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(output)
        print(f"  {GREEN}*{RESET} Output saved: {args.output} ({len(output):,} chars)")
    else:
        print(output)

    # Network requests
    if args.network and data.get("network_requests"):
        requests_list = data["network_requests"]
        print(f"\n  {BOLD}Network requests{RESET} {DIM}({len(requests_list)}){RESET}")
        for req in requests_list[:20]:
            status = req.get("status", "?")
            method = req.get("method", "?")
            rtype = req.get("resource_type", "")
            rurl = req.get("url", "")
            if len(rurl) > 80:
                rurl = rurl[:77] + "..."
            s_color = GREEN if isinstance(status, int) and 200 <= status < 400 else DIM
            print(f"    {s_color}{status}{RESET} {method:<6s} {DIM}{rtype:<12s}{RESET} {rurl}")
        if len(requests_list) > 20:
            print(f"    {DIM}... and {len(requests_list) - 20} more{RESET}")

    # Console messages
    if args.console and data.get("console_messages"):
        messages = data["console_messages"]
        print(f"\n  {BOLD}Console messages{RESET} {DIM}({len(messages)}){RESET}")
        for msg in messages[:30]:
            mtype = msg.get("type", "log")
            text = msg.get("text", "")
            if len(text) > 120:
                text = text[:117] + "..."
            type_color = YELLOW if mtype == "warning" else RED if mtype == "error" else DIM
            print(f"    {type_color}[{mtype}]{RESET} {text}")
        if len(messages) > 30:
            print(f"    {DIM}... and {len(messages) - 30} more{RESET}")

    # API tracking
    if args.track_api and data.get("api_tracking"):
        tracking = data["api_tracking"]
        total_calls = tracking.get("total_api_calls", 0)
        total_scripts = tracking.get("total_scripts", 0)
        print(f"\n  {BOLD}API tracking{RESET} {DIM}{total_calls} calls from {total_scripts} scripts{RESET}")
        for script_url, calls in list(tracking.get("api_calls", {}).items())[:10]:
            short_url = script_url if len(script_url) <= 60 else script_url[:57] + "..."
            print(f"    {DIM}{short_url}:{RESET} {', '.join(calls[:5])}")
            if len(calls) > 5:
                print(f"      {DIM}... and {len(calls) - 5} more calls{RESET}")

    print()


# ---------------------------------------------------------------------------
# Commands: scrape
# ---------------------------------------------------------------------------

def _print_final_state(state: dict, agent: str):
    """Print relevant final state."""
    values = state.get("values", state)

    if agent == "auto_spider":
        artifact = values.get("artifact")
        if artifact and isinstance(artifact, dict):
            spider_slug = artifact.get("spider_slug", "N/A")
            validation = artifact.get("validation", {})
            items = validation.get("items_scraped", 0)
            success = validation.get("success", False)

            if success:
                _stage_line("Validate", f"{items} items, 0 errors", "done")
                _stage_line("Report", f"Spider deployed as {BOLD}{spider_slug}{RESET}", "done")
            else:
                errors = validation.get("error_count", 0)
                _stage_line("Validate", f"{items} items, {errors} errors", "fail")

            report = artifact.get("report", "")
            if report:
                print(f"\n{report}")
        else:
            error = values.get("error")
            if error:
                print(f"\n  {RED}error{RESET}  {error}")
            else:
                spider_slug = values.get("spider_slug")
                if spider_slug:
                    _kv("spider", spider_slug)
                stage = values.get("current_stage", "unknown")
                _kv("stage", stage)
    else:
        messages = values.get("messages", [])
        ai_messages = [m for m in messages if isinstance(m, dict) and m.get("type") == "ai"]
        if ai_messages:
            last = ai_messages[-1]
            content = last.get("content", "")
            if isinstance(content, str) and content.strip():
                print()
                for line in content.split("\n")[:40]:
                    print(f"  {line}")


def cmd_scrape(args, cfg: dict):
    """Run the autonomous spider pipeline."""
    agent = args.agent or "auto_spider"
    url = args.url
    if not url.startswith(("http://", "https://")):
        url = f"https://{url}"
    description = args.description

    _header("scrape")
    _kv("target", url)
    _kv("data", description)

    limits = []
    if args.max_items:
        limits.append(f"max {args.max_items} items")
    if args.max_pages:
        limits.append(f"max {args.max_pages} pages")
    if limits:
        _kv("limits", ", ".join(limits))

    # Create thread
    try:
        with Spinner("Creating thread"):
            thread = api_post(cfg, "/threads", {"metadata": {}})
    except httpx.ConnectError:
        _die(f"Cannot connect to agent at {cfg['agent_url']}\n"
             "  Check your connection settings or run `calyprium login` to authenticate.")

    thread_id = thread["thread_id"]
    _kv("thread", thread_id)
    print()

    # Build input
    run_input = {
        "messages": [
            {"type": "human", "content": f"{url} {description}"}
        ],
    }

    if agent == "auto_spider":
        run_input["target_url"] = url
        run_input["data_description"] = description
        if args.max_items:
            run_input["max_items"] = args.max_items
        if args.max_pages:
            run_input["max_pages"] = args.max_pages

    run_body = {
        "assistant_id": agent,
        "input": run_input,
        "stream_mode": ["updates"],
    }

    if args.no_stream:
        with Spinner("Running pipeline"):
            try:
                result = api_post(cfg, f"/threads/{thread_id}/runs", run_body)
            except httpx.HTTPStatusError as e:
                _die(f"{e.response.status_code} -- {e.response.text}")

        _kv("run", result.get("run_id", "N/A"))
        _kv("status", result.get("status", "N/A"))
        print()

        state = api_get(cfg, f"/threads/{thread_id}/state")
        _print_final_state(state, agent)
    else:
        tracker = StageTracker()
        try:
            seen_msg_count = 0
            last_validation = None
            report_content = None
            for event_type, data in api_stream(
                cfg, f"/threads/{thread_id}/runs/stream", run_body
            ):
                if event_type == "updates" and isinstance(data, dict):
                    for node_name, node_data in data.items():
                        if not isinstance(node_data, dict):
                            continue

                        messages = node_data.get("messages", [])
                        new_messages = messages[seen_msg_count:]
                        seen_msg_count = len(messages)

                        for msg in new_messages:
                            if isinstance(msg, dict):
                                content = msg.get("content", "")
                                msg_type = msg.get("type", "")
                                if msg_type == "ai" and isinstance(content, str):
                                    if content.startswith("**["):
                                        _print_stage(content, tracker)
                                    elif content.strip() and len(content) > 200:
                                        report_content = content

                        validation = node_data.get("validation")
                        if validation and isinstance(validation, dict) and validation != last_validation:
                            last_validation = validation
                            success = validation.get("success", False)
                            items = validation.get("items_scraped", 0)
                            errors = validation.get("error_count", 0)
                            status = "done" if success else "fail"
                            tracker.update("Validate", f"{items} items, {errors} errors", status)

                elif event_type == "error":
                    tracker.finish("fail")
                    print(f"\n  {RED}error{RESET}  {data}", file=sys.stderr)

                elif event_type == "end":
                    break

            # Finalize last spinner
            tracker.finish()

            # Print report if captured
            if report_content:
                print()
                for line in report_content.split("\n"):
                    print(f"  {line}")

            # Final state (only if no report was streamed)
            if not report_content:
                print()
                state = api_get(cfg, f"/threads/{thread_id}/state")
                _print_final_state(state, agent)

        except httpx.HTTPStatusError as e:
            tracker.finish("fail")
            _die(f"{e.response.status_code} -- {e.response.text}")
        except KeyboardInterrupt:
            tracker.finish()
            print(f"\n\n  {YELLOW}Interrupted.{RESET} Thread preserved: {thread_id}")
            sys.exit(0)

    print(f"\n  {DIM}Resume: calyprium data {thread_id}{RESET}\n")


# ---------------------------------------------------------------------------
# Commands: data
# ---------------------------------------------------------------------------

def cmd_data(args, cfg: dict):
    """List threads or show thread details."""
    thread_id = getattr(args, "thread_id", None)

    if thread_id:
        _cmd_data_detail(thread_id, cfg)
    else:
        _cmd_data_list(args, cfg)


def _cmd_data_list(args, cfg: dict):
    """List recent threads in a table."""
    limit = args.limit or 10

    _header("data")

    with Spinner("Loading threads"):
        try:
            threads = api_post(cfg, "/threads/search", {"limit": limit})
        except httpx.ConnectError:
            _die(f"Cannot connect to agent at {cfg['agent_url']}")
        except httpx.HTTPStatusError as e:
            _die(f"Agent returned {e.response.status_code}")

    if not threads:
        print(f"  {DIM}No threads found.{RESET}\n")
        return

    # Table header
    print(f"  {DIM}{'ID':<12s} {'TARGET':<36s} {'STATUS':<12s} {'AGE'}{RESET}")

    for t in threads:
        tid = t.get("thread_id", "?")
        short_id = tid[:8] + ".." if len(tid) > 10 else tid
        meta = t.get("metadata", {})
        status_raw = t.get("status", meta.get("status", ""))
        created = t.get("created_at", "")

        # Try to extract a target from metadata
        target = meta.get("target_url", meta.get("thread_name", ""))
        if not target:
            # Fall back to first message content or "Untitled"
            target = "Untitled"
        # Shorten target for display
        target = target.replace("https://", "").replace("http://", "").replace("www.", "")
        if len(target) > 34:
            target = target[:31] + "..."

        # Format status
        if status_raw in ("done", "completed", "success"):
            status_str = f"{GREEN}* done{RESET}"
        elif status_raw in ("running", "pending", "busy"):
            status_str = f"{YELLOW}* running{RESET}"
        elif status_raw in ("failed", "error"):
            status_str = f"{RED}x failed{RESET}"
        else:
            status_str = f"{DIM}  --{RESET}"

        age = _relative_time(created) if created else ""

        print(f"  {short_id:<12s} {target:<36s} {status_str:<22s} {DIM}{age}{RESET}")

    print()


def _cmd_data_detail(thread_id: str, cfg: dict):
    """Show thread state summary and message history."""
    _header(f"data {thread_id[:12]}")

    with Spinner("Loading thread"):
        try:
            state = api_get(cfg, f"/threads/{thread_id}/state")
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                _die(f"Thread not found: {thread_id}")
            else:
                _die(f"Agent returned {e.response.status_code}")

    values = state.get("values", state)

    # State summary
    _kv("thread", thread_id)
    stage = values.get("current_stage")
    if stage:
        _kv("stage", stage)
    for key in ("target_url", "data_description", "spider_slug", "spider_name"):
        val = values.get(key)
        if val is not None:
            label = key.replace("_", " ").replace("target url", "target").replace("data description", "data")
            _kv(label[:7], str(val))

    iteration = values.get("iteration_count")
    if iteration is not None:
        _kv("iters", str(iteration))

    error = values.get("error")
    if error:
        _kv("error", f"{RED}{error}{RESET}")

    # Validation
    validation = values.get("validation")
    if validation and isinstance(validation, dict):
        success = validation.get("success")
        items = validation.get("items_scraped", 0)
        errors = validation.get("error_count", 0)
        v_status = f"{GREEN}passed{RESET}" if success else f"{RED}failed{RESET}"
        _kv("valid", f"{v_status} -- {items} items, {errors} errors")

    # Strategy
    strategy = values.get("strategy")
    if strategy and isinstance(strategy, dict):
        parts = []
        if strategy.get("approach"):
            parts.append(strategy["approach"])
        if strategy.get("rendering"):
            parts.append(strategy["rendering"])
        if strategy.get("stealth_level"):
            parts.append(f"stealth:{strategy['stealth_level']}")
        if parts:
            _kv("strat", ", ".join(parts))

    # Message history
    messages = values.get("messages", [])
    if messages:
        print(f"\n  {DIM}{'Messages':} ({len(messages)}){RESET}")
        print(f"  {DIM}{'─' * 40}{RESET}")

        for msg in messages:
            if isinstance(msg, dict):
                _print_message(msg)

    print()


# ---------------------------------------------------------------------------
# Commands: chat
# ---------------------------------------------------------------------------

def cmd_chat(args, cfg: dict):
    """Start or continue a conversation with the agent."""
    thread_id = getattr(args, "resume", None)
    message = getattr(args, "message", None)
    agent = getattr(args, "agent", None) or "chat"

    # Determine mode
    if thread_id and message:
        # Single message on existing thread
        _chat_send(cfg, thread_id, message, agent)
    elif thread_id and not message:
        # Interactive REPL on existing thread
        _chat_repl(cfg, thread_id, agent)
    elif message and not thread_id:
        # Single message on new thread
        thread_id = _chat_create_thread(cfg)
        _chat_send(cfg, thread_id, message, agent)
        print(f"\n  {DIM}Resume: calyprium chat --resume {thread_id}{RESET}")
    else:
        # Interactive REPL on new thread
        thread_id = _chat_create_thread(cfg)
        _chat_repl(cfg, thread_id, agent)


def _chat_create_thread(cfg: dict) -> str:
    """Create a new thread and return its ID."""
    with Spinner("Creating thread"):
        try:
            thread = api_post(cfg, "/threads", {"metadata": {}})
        except httpx.ConnectError:
            _die(f"Cannot connect to agent at {cfg['agent_url']}")
    return thread["thread_id"]


def _chat_send(cfg: dict, thread_id: str, message: str, agent: str):
    """Send a single message and stream the response."""
    _header("chat")
    _kv("thread", thread_id)
    print(f"\n  {BOLD}> {message}{RESET}\n")

    run_body = {
        "assistant_id": agent,
        "input": {
            "messages": [{"type": "human", "content": message}],
        },
        "stream_mode": ["updates"],
    }

    _stream_chat_response(cfg, thread_id, run_body)


def _chat_repl(cfg: dict, thread_id: str, agent: str):
    """Interactive chat REPL."""
    _header("chat")
    _kv("thread", thread_id)
    print(f"  {DIM}{'─' * 40}{RESET}")
    print()

    while True:
        try:
            user_input = input(f"  {BOLD}>{RESET} ")
        except (EOFError, KeyboardInterrupt):
            print(f"\n\n  {DIM}Thread: {thread_id}{RESET}")
            print(f"  {DIM}Resume: calyprium chat --resume {thread_id}{RESET}\n")
            return

        user_input = user_input.strip()
        if not user_input:
            continue
        if user_input.lower() in ("/quit", "/exit", "/q"):
            print(f"\n  {DIM}Thread: {thread_id}{RESET}")
            print(f"  {DIM}Resume: calyprium chat --resume {thread_id}{RESET}\n")
            return

        print()

        run_body = {
            "assistant_id": agent,
            "input": {
                "messages": [{"type": "human", "content": user_input}],
            },
            "stream_mode": ["updates"],
        }

        _stream_chat_response(cfg, thread_id, run_body)
        print()


def _stream_chat_response(cfg: dict, thread_id: str, run_body: dict):
    """Stream an agent response, printing tokens incrementally."""
    try:
        buffer = ""
        in_response = False
        for event_type, data in api_stream(
            cfg, f"/threads/{thread_id}/runs/stream", run_body
        ):
            if event_type == "updates" and isinstance(data, dict):
                for node_name, node_data in data.items():
                    if not isinstance(node_data, dict):
                        continue
                    messages = node_data.get("messages", [])
                    for msg in messages:
                        if not isinstance(msg, dict):
                            continue
                        content = msg.get("content", "")
                        msg_type = msg.get("type", "")

                        if msg_type == "ai" and isinstance(content, str) and content.strip():
                            if content.startswith("**["):
                                _print_stage(content)
                            else:
                                # Print response content
                                new_text = content[len(buffer):] if content.startswith(buffer) else content
                                if new_text:
                                    if not in_response:
                                        sys.stdout.write("  ")
                                        in_response = True
                                    sys.stdout.write(new_text)
                                    sys.stdout.flush()
                                    buffer = content

                        elif msg_type == "tool":
                            name = msg.get("name", "unknown")
                            print(f"  {DIM}[{name}]{RESET}", end="")
                            if isinstance(content, str) and len(content) > 100:
                                print(f" {DIM}{content[:100]}...{RESET}")
                            elif content:
                                print(f" {DIM}{content}{RESET}")
                            else:
                                print()

            elif event_type == "error":
                print(f"\n  {RED}error{RESET}  {data}", file=sys.stderr)

            elif event_type == "end":
                break

        if in_response:
            print()  # Final newline after streamed content

    except httpx.HTTPStatusError as e:
        print(f"\n  {RED}error{RESET}  {e.response.status_code} -- {e.response.text}", file=sys.stderr)
    except httpx.ConnectError:
        _die(f"Cannot connect to agent at {cfg['agent_url']}")


# ---------------------------------------------------------------------------
# HTML Processing
# ---------------------------------------------------------------------------

class _HTMLTextExtractor(html.parser.HTMLParser):
    """Strip HTML tags, keeping only visible text."""

    _SKIP_TAGS = {"script", "style", "head", "noscript"}

    def __init__(self):
        super().__init__()
        self._parts: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag, attrs):
        if tag.lower() in self._SKIP_TAGS:
            self._skip_depth += 1
        elif tag.lower() in ("br", "hr"):
            self._parts.append("\n")
        elif tag.lower() in ("p", "div", "li", "tr", "h1", "h2", "h3", "h4", "h5", "h6"):
            self._parts.append("\n")

    def handle_endtag(self, tag):
        if tag.lower() in self._SKIP_TAGS:
            self._skip_depth = max(0, self._skip_depth - 1)
        elif tag.lower() in ("p", "div", "li", "tr", "h1", "h2", "h3", "h4", "h5", "h6"):
            self._parts.append("\n")

    def handle_data(self, data):
        if self._skip_depth == 0:
            self._parts.append(data)

    def get_text(self) -> str:
        text = "".join(self._parts)
        lines = text.split("\n")
        lines = [" ".join(line.split()) for line in lines]
        result = []
        prev_blank = False
        for line in lines:
            if not line:
                if not prev_blank:
                    result.append("")
                    prev_blank = True
            else:
                result.append(line)
                prev_blank = False
        return "\n".join(result).strip()


def _html_to_text(html_content: str) -> str:
    """Convert HTML to plain text by stripping tags. Cleans boilerplate first."""
    cleaned = _clean_html(html_content)
    if not cleaned.strip().startswith("<"):
        return cleaned
    extractor = _HTMLTextExtractor()
    extractor.feed(cleaned)
    return extractor.get_text()


def _clean_html(html_content: str) -> str:
    """Extract main content from HTML, removing boilerplate.

    Uses trafilatura if available (best quality), otherwise falls back
    to BeautifulSoup-based cleanup that strips nav, footer, ads, scripts.
    Returns cleaned HTML suitable for markdown conversion.
    """
    try:
        import trafilatura
        result = trafilatura.extract(
            html_content,
            include_links=True,
            include_images=True,
            include_tables=True,
            include_formatting=True,
            output_format="txt",
            favor_precision=False,
            favor_recall=True,
        )
        if result and len(result) > 100:
            return result
    except ImportError:
        pass

    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html_content, "html.parser")

        for tag in soup.find_all([
            "script", "style", "noscript", "iframe", "svg",
            "nav", "footer", "header",
        ]):
            tag.decompose()

        for tag in soup.find_all(attrs={"style": re.compile(r"display\s*:\s*none")}):
            tag.decompose()
        for tag in soup.find_all(attrs={"aria-hidden": "true"}):
            tag.decompose()

        main = (
            soup.find("main")
            or soup.find("article")
            or soup.find(id=re.compile(r"content|main|product|detail", re.I))
            or soup.find(class_=re.compile(r"content|main|product|detail", re.I))
        )

        if main and len(main.get_text(strip=True)) > 200:
            return str(main)

        body = soup.find("body")
        return str(body) if body else html_content
    except ImportError:
        return html_content


def _html_to_markdown(html_content: str) -> str:
    """Convert HTML to markdown. Cleans boilerplate first, then converts."""
    cleaned = _clean_html(html_content)

    if not cleaned.strip().startswith("<"):
        return cleaned

    try:
        import html2text
        h = html2text.HTML2Text()
        h.body_width = 0
        h.ignore_links = False
        h.ignore_images = False
        h.ignore_emphasis = False
        return h.handle(cleaned).strip()
    except ImportError:
        text = html_content
        for i in range(1, 7):
            text = re.sub(
                rf"<h{i}[^>]*>(.*?)</h{i}>",
                lambda m, n=i: f"\n{'#' * n} {m.group(1).strip()}\n",
                text, flags=re.DOTALL | re.IGNORECASE,
            )
        text = re.sub(r"<(strong|b)>(.*?)</\1>", r"**\2**", text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"<(em|i)>(.*?)</\1>", r"*\2*", text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(
            r'<a[^>]+href="([^"]*)"[^>]*>(.*?)</a>',
            r"[\2](\1)", text, flags=re.DOTALL | re.IGNORECASE,
        )
        text = re.sub(r"<li[^>]*>(.*?)</li>", r"- \1\n", text, flags=re.DOTALL | re.IGNORECASE)
        return _html_to_text(text)


def _extract_with_selectors(html_content: str, selector: str) -> list[dict]:
    """Extract elements matching a CSS selector. Requires bs4."""
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        _die("CSS selector extraction requires beautifulsoup4\n"
             "  Install it: pip install beautifulsoup4")

    soup = BeautifulSoup(html_content, "html.parser")
    elements = soup.select(selector)

    results = []
    for el in elements:
        item = {
            "tag": el.name,
            "text": el.get_text(strip=True),
        }
        if el.get("href"):
            item["href"] = el["href"]
        if el.get("src"):
            item["src"] = el["src"]
        for attr in ("class", "id", "alt", "title"):
            if el.get(attr):
                item[attr] = el[attr] if isinstance(el[attr], str) else " ".join(el[attr])
        results.append(item)

    return results


def _extract_with_llm(html_content: str, description: str, cfg: dict) -> str:
    """Use an LLM to extract structured data from HTML."""
    api_key = os.getenv("OPENROUTER_API_KEY") or os.getenv("OPENAI_API_KEY")
    if not api_key:
        _die("LLM extraction requires OPENROUTER_API_KEY or OPENAI_API_KEY")

    base_url = "https://openrouter.ai/api/v1" if os.getenv("OPENROUTER_API_KEY") else "https://api.openai.com/v1"
    model = "google/gemini-2.0-flash-001" if os.getenv("OPENROUTER_API_KEY") else "gpt-4o-mini"

    max_html = 60000
    if len(html_content) > max_html:
        truncated = html_content[:max_html] + "\n<!-- truncated -->"
    else:
        truncated = html_content

    text_content = _html_to_text(truncated)
    if len(text_content) > 30000:
        text_content = text_content[:30000] + "\n[truncated]"

    resp = httpx.post(
        f"{base_url}/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json={
            "model": model,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a data extraction assistant. Extract the requested data "
                        "from the provided page content. Return the data as a JSON array "
                        "of objects. If the data is a single value, return it as "
                        '{"result": "value"}. Be precise and include all matching items.'
                    ),
                },
                {
                    "role": "user",
                    "content": f"Extract: {description}\n\nPage content:\n{text_content}",
                },
            ],
            "temperature": 0,
            "max_tokens": 4000,
        },
        timeout=60,
    )
    resp.raise_for_status()
    content = resp.json()["choices"][0]["message"]["content"]

    cleaned = content.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r"^```\w*\n?", "", cleaned)
        cleaned = re.sub(r"\n?```$", "", cleaned)

    try:
        parsed = json.loads(cleaned)
        return json.dumps(parsed, indent=2, ensure_ascii=False)
    except (json.JSONDecodeError, ValueError):
        return content


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Intel — Domain analysis, sitemaps, detection
# ---------------------------------------------------------------------------

STAGE_NAMES_INTEL = {
    "overview": "Overview",
    "sitemap": "Sitemap",
    "detection": "Detection",
    "strategy": "Strategy",
    "complete": "Done",
}


def _prism_headers(cfg: dict) -> dict:
    """Build auth headers for Prism API calls."""
    token = get_token(cfg)
    return {"Authorization": f"Bearer {token}"}


def cmd_intel(args, cfg: dict):
    """Route to the appropriate intel subcommand."""
    subcmd = getattr(args, "intel_command", None)
    if not subcmd:
        print(f"{RED}Error: specify a subcommand (analyze, sitemap, detect, urls){RESET}", file=sys.stderr)
        sys.exit(1)

    handler = {
        "analyze": cmd_intel_analyze,
        "sitemap": cmd_intel_sitemap,
        "detect": cmd_intel_detect,
        "urls": cmd_intel_urls,
        "status": cmd_intel_status,
        "clear-cache": cmd_intel_clear_cache,
    }
    handler[subcmd](args, cfg)


def cmd_intel_analyze(args, cfg: dict):
    """Run full domain analysis (overview + sitemap + detection + strategy) via SSE."""
    domain = args.domain
    prism_url = cfg["prism_url"]
    headers = _prism_headers(cfg)

    print(f"\n  {BOLD}Analyzing {CYAN}{domain}{RESET}\n")
    pipeline = PipelineDisplay()

    try:
        with httpx.stream(
            "POST",
            f"{prism_url}/api/analyses",
            json={"domain": domain},
            headers=headers,
            timeout=300,
        ) as response:
            if response.status_code not in (200, 202):
                print(f"{RED}Error: {response.status_code} {response.text}{RESET}", file=sys.stderr)
                sys.exit(1)

            result_data = {}
            for line in response.iter_lines():
                if not line.startswith("data: "):
                    continue
                data = json.loads(line[6:])
                step = data.get("step", "")
                status = data.get("status", "")
                step_data = data.get("data", {})

                label = STAGE_NAMES_INTEL.get(step, step)

                if status == "running":
                    pipeline.update(step, "running...")
                elif status == "progress":
                    # Sitemap progress
                    urls = step_data.get("urls_collected", 0)
                    fetched = step_data.get("sitemaps_fetched", 0)
                    pipeline.update(step, f"{urls:,} URLs from {fetched} sitemaps")
                elif status == "complete":
                    result_data[step] = step_data
                    if step == "overview":
                        title = step_data.get("title", "")
                        pipeline.update(step, title[:60] if title else "done", "done")
                    elif step == "sitemap":
                        n = step_data.get("url_count", 0)
                        pipeline.update(step, f"{n:,} URLs", "done")
                    elif step == "detection":
                        antibot = step_data.get("antibot", [])
                        names = [a.get("name", "?") for a in antibot]
                        pipeline.update(step, ", ".join(names) if names else "none detected", "done")
                    elif step == "strategy":
                        diff = step_data.get("difficulty", "?")
                        method = step_data.get("acquisition_method", "?")
                        pipeline.update(step, f"{diff} / {method}", "done")
                    elif step == "complete":
                        pipeline.finish()
                elif status == "error":
                    err = step_data.get("error", "unknown error")
                    pipeline.update(step, f"FAILED: {err}", "fail")

        # Print summary
        if "strategy" in result_data:
            s = result_data["strategy"]
            print(f"\n  {BOLD}Strategy Summary{RESET}")
            print(f"  {s.get('executive_summary', '')}\n")

    except httpx.TimeoutException:
        pipeline.finish("fail")
        print(f"\n{RED}Error: analysis timed out{RESET}", file=sys.stderr)
        sys.exit(1)


def cmd_intel_sitemap(args, cfg: dict):
    """Trigger a sitemap fetch (sampled or full)."""
    domain = args.domain
    prism_url = cfg["prism_url"]
    headers = _prism_headers(cfg)
    scan_type = "full" if args.full else "sampled"
    force = args.force

    print(f"\n  {BOLD}Sitemap fetch{RESET} ({scan_type}) for {CYAN}{domain}{RESET}")

    resp = httpx.post(
        f"{prism_url}/api/sitemaps/fetch",
        params={"domain": domain},
        json={"scan_type": scan_type, "force": force},
        headers=headers,
        timeout=30,
    )

    if resp.status_code == 200:
        data = resp.json()
        if data.get("cached"):
            ttl_h = (data.get("ttl_remaining_seconds", 0) or 0) / 3600
            print(f"  {GREEN}Cached{RESET} ({data['urls_collected']:,} URLs, {ttl_h:.1f}h remaining)")
            print(f"  scan_id: {data['scan_id']}")
        else:
            print(f"  {GREEN}Complete{RESET}: {data['urls_collected']:,} URLs")
            print(f"  scan_id: {data['scan_id']}")
        return

    if resp.status_code == 202:
        data = resp.json()
        scan_id = data["scan_id"]
        print(f"  {YELLOW}Started{RESET} background job: {scan_id}")

        if not args.wait:
            print(f"  Poll status: calyprium intel status {scan_id}")
            return

        # Poll for completion
        print(f"  Waiting for completion...\n")
        while True:
            time.sleep(5)
            status_resp = httpx.get(
                f"{prism_url}/api/sitemaps/scans/{scan_id}",
                headers=headers,
                timeout=10,
            )
            if status_resp.status_code != 200:
                continue
            scan = status_resp.json()
            status = scan["status"]
            fetched = scan.get("sitemaps_fetched", 0)
            urls = scan.get("urls_collected", 0)

            sys.stderr.write(f"\r\033[K  {DIM}sitemaps: {fetched}  urls: {urls:,}  status: {status}{RESET}")
            sys.stderr.flush()

            if status in ("complete", "failed"):
                sys.stderr.write("\n")
                if status == "complete":
                    print(f"\n  {GREEN}Complete{RESET}: {urls:,} URLs")
                else:
                    print(f"\n  {RED}Failed{RESET}: {scan.get('error', 'unknown')}")
                return

    if resp.status_code == 409:
        detail = resp.json().get("detail", {})
        scan = detail.get("scan", {})
        print(f"  {YELLOW}Already in progress{RESET}: {scan.get('scan_id', '?')}")
        print(f"  status: {scan.get('status')}  urls: {scan.get('urls_collected', 0):,}")
        return

    print(f"  {RED}Error{RESET}: {resp.status_code} {resp.text[:200]}", file=sys.stderr)
    sys.exit(1)


def cmd_intel_detect(args, cfg: dict):
    """Run antibot/technology detection on a domain."""
    domain = args.domain
    prism_url = cfg["prism_url"]
    headers = _prism_headers(cfg)

    print(f"\n  {BOLD}Detection scan{RESET} for {CYAN}{domain}{RESET}\n")

    with Spinner("scanning..."):
        resp = httpx.get(
            f"{prism_url}/api/domains/{domain}/detections",
            headers=headers,
            timeout=60,
        )

    if resp.status_code == 404:
        print(f"  No detection data for {domain}. Run: calyprium intel analyze {domain}")
        return

    if resp.status_code != 200:
        print(f"  {RED}Error{RESET}: {resp.status_code}", file=sys.stderr)
        sys.exit(1)

    data = resp.json()
    detail = data.get("detail", data)

    # Antibot systems
    antibot = detail.get("all_antibot_systems", [])
    if antibot:
        print(f"  {BOLD}Antibot systems:{RESET} {', '.join(antibot)}")
    else:
        print(f"  {BOLD}Antibot systems:{RESET} {GREEN}none detected{RESET}")

    # Risk
    risk = detail.get("risk_distribution", {})
    if risk:
        parts = [f"{k}: {v}" for k, v in risk.items()]
        print(f"  {BOLD}Risk:{RESET} {', '.join(parts)}")

    # Detection count
    total = detail.get("total_detections", 0)
    blocked = detail.get("blocked_count", 0)
    print(f"  {BOLD}Scans:{RESET} {total} ({blocked} blocked)")

    # Latest detection details
    detections = data.get("detections", [])
    if detections:
        latest = detections[0]
        print(f"\n  {DIM}Latest scan:{RESET}")
        print(f"    Status: {latest.get('status_code')}  Risk: {latest.get('overall_risk')}")
        print(f"    APIs tracked: {latest.get('total_apis_tracked', 0)}")
        print(f"    Fingerprint score: {latest.get('total_fingerprint_score', 0)}")

        techs = latest.get("other_technologies", [])
        if techs:
            names = [t.get("name", "?") for t in techs]
            print(f"    Technologies: {', '.join(names)}")
    print()


def cmd_intel_urls(args, cfg: dict):
    """Query sitemap URLs for a domain with optional pattern filtering."""
    domain = args.domain
    prism_url = cfg["prism_url"]
    headers = _prism_headers(cfg)

    params = {"limit": args.limit, "offset": args.offset}
    if args.pattern:
        params["pattern"] = args.pattern
    if args.prefix:
        params["path_prefix"] = args.prefix
    if args.source:
        params["source"] = args.source

    # Text format for piping
    if args.text or not sys.stdout.isatty():
        params["format"] = "text"
        resp = httpx.get(
            f"{prism_url}/api/domains/{domain}/urls",
            params=params,
            headers=headers,
            timeout=30,
        )
        if resp.status_code != 200:
            print(f"Error: {resp.status_code} {resp.text[:200]}", file=sys.stderr)
            sys.exit(1)
        sys.stdout.write(resp.text)
        return

    # JSON format with summary
    resp = httpx.get(
        f"{prism_url}/api/domains/{domain}/urls",
        params=params,
        headers=headers,
        timeout=30,
    )

    if resp.status_code == 404:
        print(f"\n  No sitemap data for {domain}. Run: calyprium intel sitemap {domain}")
        sys.exit(1)

    if resp.status_code != 200:
        print(f"{RED}Error{RESET}: {resp.status_code} {resp.text[:200]}", file=sys.stderr)
        sys.exit(1)

    data = resp.json()
    total = data.get("total", 0)
    urls = data.get("urls", [])
    scan_type = data.get("scan_type", "?")

    print(f"\n  {BOLD}{domain}{RESET} - {total:,} URLs (scan: {scan_type})")
    if args.pattern:
        print(f"  pattern: {args.pattern}")
    if args.prefix:
        print(f"  prefix: {args.prefix}")
    print()

    for url in urls:
        print(f"  {url}")

    if total > len(urls):
        remaining = total - len(urls) - args.offset
        if remaining > 0:
            print(f"\n  {DIM}... and {remaining:,} more (use --limit/--offset or --text to get all){RESET}")
    print()


def cmd_intel_status(args, cfg: dict):
    """Check status of a sitemap scan job."""
    scan_id = args.scan_id
    prism_url = cfg["prism_url"]
    headers = _prism_headers(cfg)

    resp = httpx.get(
        f"{prism_url}/api/sitemaps/scans/{scan_id}",
        headers=headers,
        timeout=10,
    )

    if resp.status_code == 404:
        print(f"{RED}Scan {scan_id} not found{RESET}", file=sys.stderr)
        sys.exit(1)

    if resp.status_code != 200:
        print(f"{RED}Error{RESET}: {resp.status_code}", file=sys.stderr)
        sys.exit(1)

    scan = resp.json()
    status = scan["status"]
    color = GREEN if status == "complete" else YELLOW if status == "running" else RED if status == "failed" else ""

    print(f"\n  {BOLD}Scan {scan['scan_id']}{RESET}")
    print(f"  Domain:    {scan['domain']}")
    print(f"  Type:      {scan['scan_type']}")
    print(f"  Status:    {color}{status}{RESET}")
    print(f"  URLs:      {scan.get('urls_collected', 0):,}")
    print(f"  Sitemaps:  {scan.get('sitemaps_fetched', 0)} fetched / {scan.get('sitemaps_discovered', 0)} discovered")
    if scan.get("completed_at"):
        print(f"  Completed: {scan['completed_at']}")
    if scan.get("error"):
        print(f"  Error:     {RED}{scan['error']}{RESET}")
    print()


def cmd_intel_clear_cache(args, cfg: dict):
    """Clear the mimic auto-routing cache for a domain."""
    domain = args.domain
    mimic_url = cfg["mimic_url"]

    if domain == "all":
        resp = httpx.delete(f"{mimic_url}/api/domain-cache", timeout=10)
    else:
        resp = httpx.delete(f"{mimic_url}/api/domain-cache/{domain}", timeout=10)

    if resp.status_code != 200:
        print(f"  {RED}Error{RESET}: {resp.status_code} {resp.text[:100]}", file=sys.stderr)
        sys.exit(1)

    data = resp.json()
    if domain == "all":
        print(f"  Cleared {data.get('cleared', 0)} cached domains")
    else:
        cleared = data.get("cleared", False)
        print(f"  {domain}: {'cleared' if cleared else 'not in cache'}")


# ---------------------------------------------------------------------------
# Spider commands — direct spider management via Forge API
# ---------------------------------------------------------------------------


def _forge_headers(cfg: dict) -> dict:
    """Build auth headers for Forge API calls."""
    token = get_token(cfg)
    return {"Authorization": f"Bearer {token}"}


def cmd_spider(args, cfg: dict):
    """Route to the appropriate spider subcommand."""
    dispatch = {
        "list": cmd_spider_list,
        "deploy": cmd_spider_deploy,
        "run": cmd_spider_run,
        "status": cmd_spider_status,
        "logs": cmd_spider_logs,
        "results": cmd_spider_results,
    }
    sub = getattr(args, "spider_command", None)
    if not sub or sub not in dispatch:
        print("Usage: calyprium spider {list|deploy|run|status|logs}", file=sys.stderr)
        sys.exit(1)
    dispatch[sub](args, cfg)


def cmd_spider_list(args, cfg: dict):
    """List deployed spiders."""
    forge = cfg["forge_url"]
    headers = _forge_headers(cfg)
    resp = httpx.get(f"{forge}/spiders", headers=headers, timeout=15)

    if resp.status_code != 200:
        print(f"  {RED}Error{RESET}: {resp.status_code} {resp.text[:200]}", file=sys.stderr)
        sys.exit(1)

    spiders = resp.json()
    if not spiders:
        print("  No spiders deployed")
        return

    print(f"\n  {'Slug':<25s} {'Name':<30s} {'Updated':<20s}")
    print(f"  {'-'*24:<25s} {'-'*29:<30s} {'-'*19:<20s}")
    for s in spiders:
        slug = s.get("slug", "?")
        name = s.get("name", "?")
        updated = s.get("updated_at", s.get("created_at", "?"))
        if isinstance(updated, str) and len(updated) > 19:
            updated = updated[:19]
        print(f"  {slug:<25s} {name:<30s} {updated:<20s}")
    print()


def cmd_spider_deploy(args, cfg: dict):
    """Deploy a spider from a .py file."""
    forge = cfg["forge_url"]
    headers = _forge_headers(cfg)

    spider_file = Path(args.file)
    if not spider_file.exists():
        print(f"  {RED}Error{RESET}: File not found: {spider_file}", file=sys.stderr)
        sys.exit(1)

    code = spider_file.read_text(encoding="utf-8")
    name = args.name or spider_file.stem.replace("_", " ").title()
    slug = args.slug  # May be None — server auto-generates

    payload = {"name": name, "code": code}
    if slug:
        payload["slug"] = slug

    with Spinner(f"Deploying {spider_file.name}..."):
        resp = httpx.post(
            f"{forge}/spiders",
            headers={**headers, "Content-Type": "application/json"},
            json=payload,
            timeout=60,
        )

    if resp.status_code not in (200, 201):
        print(f"  {RED}Deploy failed{RESET}: {resp.status_code} {resp.text[:300]}", file=sys.stderr)
        sys.exit(1)

    data = resp.json()
    slug = data.get("slug", "?")
    print(f"  {GREEN}Deployed{RESET}: {BOLD}{slug}{RESET} ({name})")
    print(f"  Run with: calyprium spider run {slug}")


def cmd_spider_run(args, cfg: dict):
    """Trigger a spider run."""
    forge = cfg["forge_url"]
    headers = _forge_headers(cfg)
    slug = args.slug

    # Parse --arg KEY=VALUE and --setting KEY=VALUE pairs
    spider_args = {}
    settings = []
    for kv in (args.arg or []):
        if "=" in kv:
            k, _, v = kv.partition("=")
            spider_args[k] = v
        else:
            print(f"  {RED}Error{RESET}: --arg must be KEY=VALUE, got: {kv}", file=sys.stderr)
            sys.exit(1)

    for kv in (args.setting or []):
        if "=" in kv:
            settings.append(kv)
        else:
            print(f"  {RED}Error{RESET}: --setting must be KEY=VALUE, got: {kv}", file=sys.stderr)
            sys.exit(1)

    payload = {"args": spider_args}
    if settings:
        payload["args"]["setting"] = settings

    with Spinner(f"Starting {slug}..."):
        resp = httpx.post(
            f"{forge}/jobs/spiders/{slug}/run",
            headers={**headers, "Content-Type": "application/json"},
            json=payload,
            timeout=30,
        )

    if resp.status_code not in (200, 201):
        print(f"  {RED}Run failed{RESET}: {resp.status_code} {resp.text[:300]}", file=sys.stderr)
        sys.exit(1)

    data = resp.json()
    job_id = data.get("job_id", "?")
    run_number = data.get("run_number", "?")
    print(f"  {GREEN}Started{RESET}: run #{run_number} (job {job_id})")
    print(f"  Status: calyprium spider status {slug}")
    print(f"  Logs:   calyprium spider logs {slug}")


def cmd_spider_status(args, cfg: dict):
    """Show recent runs for a spider."""
    forge = cfg["forge_url"]
    headers = _forge_headers(cfg)
    slug = args.slug

    resp = httpx.get(f"{forge}/jobs/spiders/{slug}/runs", headers=headers, timeout=15)

    if resp.status_code != 200:
        print(f"  {RED}Error{RESET}: {resp.status_code} {resp.text[:200]}", file=sys.stderr)
        sys.exit(1)

    runs = resp.json()
    if not runs:
        print(f"  No runs for {slug}")
        return

    print(f"\n  {'Run':<6s} {'Status':<12s} {'Items':<10s} {'Started':<22s} {'Duration':<10s}")
    print(f"  {'-'*5:<6s} {'-'*11:<12s} {'-'*9:<10s} {'-'*21:<22s} {'-'*9:<10s}")
    for r in runs:
        num = f"#{r.get('run_number', '?')}"
        status = r.get("status", "?")
        items = str(r.get("items_scraped", "-"))
        started = r.get("started_at", "?")
        if isinstance(started, str) and len(started) > 19:
            started = started[:19]
        duration = r.get("duration", "-")
        if isinstance(duration, (int, float)):
            duration = f"{int(duration)}s"

        # Color-code status
        if status == "finished":
            status_str = f"{GREEN}{status}{RESET}"
        elif status == "running":
            status_str = f"{CYAN}{status}{RESET}"
        elif status == "error":
            status_str = f"{RED}{status}{RESET}"
        else:
            status_str = status

        print(f"  {num:<6s} {status_str:<21s} {items:<10s} {started:<22s} {str(duration):<10s}")
    print()


def cmd_spider_logs(args, cfg: dict):
    """Show logs for a spider run."""
    forge = cfg["forge_url"]
    headers = _forge_headers(cfg)
    slug = args.slug
    lines = args.lines

    params = {"max_lines": lines}
    if hasattr(args, "run") and args.run:
        url = f"{forge}/jobs/spiders/{slug}/runs/{args.run}/logs"
    else:
        url = f"{forge}/jobs/spiders/{slug}/logs"

    resp = httpx.get(url, headers=headers, params=params, timeout=30)

    if resp.status_code != 200:
        print(f"  {RED}Error{RESET}: {resp.status_code} {resp.text[:200]}", file=sys.stderr)
        sys.exit(1)

    data = resp.json()
    logs = data.get("logs", [])
    if not logs:
        msg = data.get("message", "No logs available")
        print(f"  {msg}")
        return

    job_id = data.get("job_id", "?")
    total = data.get("total_lines", len(logs))
    print(f"  {DIM}Job: {job_id} | Lines: {total}{RESET}\n")
    for line in logs:
        print(line)


def cmd_spider_results(args, cfg: dict):
    """Download spider results to local machine."""
    import json as json_mod

    forge = cfg["forge_url"]
    headers = _forge_headers(cfg)
    slug = args.slug
    output = args.output
    run_number = getattr(args, "run", None)
    max_items = getattr(args, "max_items", 0)
    preview = getattr(args, "preview", False)

    # If --preview, show a sample of items
    if preview:
        items = _fetch_items(forge, headers, slug, run_number, max_items or 5)
        if not items:
            print("  No items found")
            return
        for item in items:
            if isinstance(item, str):
                try:
                    parsed = json_mod.loads(item)
                    print(json_mod.dumps(parsed, indent=2))
                except json_mod.JSONDecodeError:
                    print(item)
            else:
                print(json_mod.dumps(item, indent=2))
        return

    # Try MinIO data files first
    resp = httpx.get(
        f"{forge}/jobs/spiders/{slug}/data",
        headers=headers,
        timeout=30,
    )

    if resp.status_code == 200:
        files = resp.json()
        if files:
            _download_data_file(forge, headers, slug, files, output)
            return

    # Fallback: get items from Scrapyd via latest-output or run-specific data
    items = _fetch_items(forge, headers, slug, run_number, max_items or 100000)
    if not items:
        print("  No results available")
        return

    out_path = output or f"{slug}_results.jsonl"
    with open(out_path, "w", encoding="utf-8") as f:
        for item in items:
            if isinstance(item, str):
                f.write(item + "\n")
            else:
                f.write(json_mod.dumps(item) + "\n")
    print(f"  {GREEN}Downloaded{RESET}: {len(items)} items -> {out_path}")


def _fetch_items(forge: str, headers: dict, slug: str, run_number, max_lines: int) -> list:
    """Fetch items from Forge API (Scrapyd or MinIO fallback)."""
    params = {"max_lines": max_lines}
    if run_number:
        url = f"{forge}/jobs/spiders/{slug}/runs/{run_number}/data"
    else:
        url = f"{forge}/jobs/spiders/{slug}/latest-output"
    resp = httpx.get(url, headers=headers, params=params, timeout=60)
    if resp.status_code != 200:
        return []
    data = resp.json()
    return data.get("lines", data.get("items", []))


def _download_data_file(forge: str, headers: dict, slug: str, files: list, output: str):
    """Download the latest data file from MinIO via presigned URL."""
    sorted_files = sorted(
        files, key=lambda x: x.get("last_modified", ""), reverse=True
    )

    if len(sorted_files) > 1 and not output:
        print(f"\n  Available data files for {BOLD}{slug}{RESET}:\n")
        for i, f in enumerate(sorted_files):
            size = f.get("size", 0)
            size_str = (
                f"{size / 1048576:.1f} MB" if size > 1048576
                else f"{size / 1024:.1f} KB" if size > 1024
                else f"{size} B"
            )
            mod = f.get("last_modified", "?")
            if isinstance(mod, str) and len(mod) > 19:
                mod = mod[:19]
            print(f"  {i+1}. {f['name']:<40s} {size_str:>10s}  {mod}")
        print()

    target = sorted_files[0]
    filename = target["name"]
    print(f"  Downloading: {filename}")

    url_resp = httpx.get(
        f"{forge}/jobs/spiders/{slug}/data/{filename}/download",
        headers=headers,
        timeout=30,
    )
    if url_resp.status_code != 200:
        print(f"  {RED}Error{RESET}: {url_resp.status_code} {url_resp.text[:200]}", file=sys.stderr)
        sys.exit(1)

    download_url = url_resp.json().get("download_url")
    if not download_url:
        print(f"  {RED}Error{RESET}: No download URL returned", file=sys.stderr)
        sys.exit(1)

    out_path = output or filename
    with Spinner(f"Saving to {out_path}..."):
        dl_resp = httpx.get(download_url, timeout=300, follow_redirects=True)

    if dl_resp.status_code != 200:
        print(f"  {RED}Download failed{RESET}: {dl_resp.status_code}", file=sys.stderr)
        sys.exit(1)

    with open(out_path, "wb") as f:
        f.write(dl_resp.content)

    size = len(dl_resp.content)
    size_str = (
        f"{size / 1048576:.1f} MB" if size > 1048576
        else f"{size / 1024:.1f} KB"
    )
    print(f"  {GREEN}Downloaded{RESET}: {out_path} ({size_str})")


def main():
    parser = argparse.ArgumentParser(
        prog="calyprium",
        description="Calyprium CLI -- autonomous web scraping from the command line",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""\
            commands:
              fetch <url>                Fetch a page and extract content
              scrape <url> <desc>        Autonomous spider pipeline
              spider <subcommand>        Deploy, run, and manage spiders
              intel <subcommand>         Domain analysis, sitemaps, detection
              data                       List recent threads/runs
              chat [message]             Converse with the agent

            spider subcommands:
              spider deploy <file>       Deploy a spider .py file to Forge
              spider run <slug>          Trigger a spider run
              spider status <slug>       Show recent runs for a spider
              spider logs <slug>         Show logs for a spider run
              spider results <slug>      Download scraped data
              spider list                List deployed spiders

            intel subcommands:
              intel analyze <domain>     Full analysis (overview + sitemap + detection + strategy)
              intel sitemap <domain>     Trigger sitemap fetch (--full for background)
              intel detect <domain>      View antibot/technology detection results
              intel urls <domain>        Query sitemap URLs (--pattern for regex filtering)
              intel status <scan_id>     Check sitemap scan job status

            examples:
              calyprium spider deploy forge/spiders/digikey.py --name "DigiKey Products"
              calyprium spider run digikey --arg url_source=prism://www.digikey.com
              calyprium spider status digikey
              calyprium intel urls digikey.com --pattern "/en/products/detail/" -n 50
              calyprium fetch https://example.com -f text --extract "product prices"
              calyprium scrape https://books.toscrape.com "book titles and prices"

            environment:
              --env prod|local           Target environment (default: prod)
              CALYPRIUM_ENV              Same as --env, via env var
        """),
    )

    env_names = ", ".join(ENVIRONMENTS.keys())
    parser.add_argument(
        "--env", default=os.getenv("CALYPRIUM_ENV", DEFAULT_ENV),
        choices=ENVIRONMENTS.keys(), metavar="ENV",
        help=f"Target environment ({env_names}; default: {DEFAULT_ENV})",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- fetch ---
    p_fetch = subparsers.add_parser("fetch", help="Fetch a page and extract content")
    p_fetch.add_argument("url", help="URL to fetch")
    p_fetch.add_argument(
        "--format", "-f", choices=["html", "markdown", "text", "json"],
        default="markdown", help="Output format (default: markdown)",
    )
    p_fetch.add_argument("--selector", "-s", default=None, help="CSS selector to extract elements (outputs JSON)")
    p_fetch.add_argument("--extract", "-e", default=None, help="LLM-based extraction: describe the data to extract")
    p_fetch.add_argument("--raw", action="store_true", help="Skip content extraction, convert full HTML")
    p_fetch.add_argument("--output", "-o", default=None, help="Write output to file instead of stdout")
    p_fetch.add_argument("--engine", default=None, help="Browser engine (camoufox, playwright_chromium, nodriver)")
    p_fetch.add_argument("--stealth", default=None, choices=["basic", "moderate", "maximum"], help="Stealth level")
    p_fetch.add_argument("--no-proxy", action="store_true", help="Disable proxy usage")
    p_fetch.add_argument("--proxy-type", default=None, help="Proxy type (residential, datacenter, residential_rotating)")
    p_fetch.add_argument("--proxy-country", default=None, help="Proxy country code (e.g., US, GB)")
    p_fetch.add_argument("--proxy-profile", default=None, help="Veil proxy profile name")
    p_fetch.add_argument("--proxy-template", default=None, choices=["default", "cheapest", "reliable", "fastest"], help="Veil proxy template preset")
    p_fetch.add_argument("--timeout", type=int, default=None, help="Navigation timeout in ms (1000-120000)")
    p_fetch.add_argument("--wait", type=int, default=None, help="Wait time after load in ms")
    p_fetch.add_argument("--wait-until", default=None, choices=["load", "domcontentloaded", "networkidle"], help="When to consider navigation complete")
    p_fetch.add_argument("--screenshot", nargs="?", const="screenshot.png", default=None, help="Save screenshot (optionally specify filename)")
    p_fetch.add_argument("--network", action="store_true", help="Show captured network requests")
    p_fetch.add_argument("--console", action="store_true", help="Capture browser console messages")
    p_fetch.add_argument("--track-api", action="store_true", help="Track browser API calls (anti-bot analysis)")
    p_fetch.add_argument("--no-spectre", action="store_true", help="Disable Spectre fingerprint generation")
    p_fetch.add_argument("--spectre-profile", default=None, help="Spectre fingerprint profile ID")
    p_fetch.add_argument("--spectre-session", default=None, help="Spectre session ID for sticky fingerprints")

    # --- scrape ---
    p_scrape = subparsers.add_parser("scrape", help="Run autonomous spider pipeline")
    p_scrape.add_argument("url", help="Target URL to scrape")
    p_scrape.add_argument("description", help="Description of data to extract")
    p_scrape.add_argument("--agent", "-a", default=None, help="Agent to use (default: auto_spider)")
    p_scrape.add_argument("--no-stream", action="store_true", help="Run without streaming")
    p_scrape.add_argument("--max-items", type=int, default=None, help="Maximum items to scrape")
    p_scrape.add_argument("--max-pages", type=int, default=None, help="Maximum pages to crawl")

    # --- data ---
    p_data = subparsers.add_parser("data", help="List threads or show thread details")
    p_data.add_argument("thread_id", nargs="?", default=None, help="Thread ID (omit to list all)")
    p_data.add_argument("--limit", "-n", type=int, default=10, help="Number of threads to list")

    # --- chat ---
    p_chat = subparsers.add_parser("chat", help="Converse with the agent")
    p_chat.add_argument("message", nargs="?", default=None, help="Message to send (omit for interactive mode)")
    p_chat.add_argument("--resume", default=None, metavar="THREAD_ID", help="Resume an existing thread")
    p_chat.add_argument("--agent", "-a", default=None, help="Agent to use (default: chat)")

    # --- intel ---
    p_intel = subparsers.add_parser("intel", help="Domain analysis, sitemaps, detection")
    intel_sub = p_intel.add_subparsers(dest="intel_command")

    p_analyze = intel_sub.add_parser("analyze", help="Full domain analysis (overview + sitemap + detection + strategy)")
    p_analyze.add_argument("domain", help="Domain to analyze")

    p_sitemap = intel_sub.add_parser("sitemap", help="Trigger sitemap fetch")
    p_sitemap.add_argument("domain", help="Domain to fetch sitemaps for")
    p_sitemap.add_argument("--full", action="store_true", help="Full fetch (all sitemaps, background job)")
    p_sitemap.add_argument("--force", action="store_true", help="Bypass TTL cache")
    p_sitemap.add_argument("--wait", "-w", action="store_true", help="Wait for completion (poll)")

    p_detect = intel_sub.add_parser("detect", help="View antibot/technology detection results")
    p_detect.add_argument("domain", help="Domain to check")

    p_urls = intel_sub.add_parser("urls", help="Query sitemap URLs")
    p_urls.add_argument("domain", help="Domain to query")
    p_urls.add_argument("--pattern", "-p", default=None, help="Regex pattern to filter URLs")
    p_urls.add_argument("--prefix", default=None, help="URL path prefix filter")
    p_urls.add_argument("--source", default=None, help="Filter by sitemap source category")
    p_urls.add_argument("--limit", "-n", type=int, default=20, help="Max URLs to return (default: 20)")
    p_urls.add_argument("--offset", type=int, default=0, help="Pagination offset")
    p_urls.add_argument("--text", "-t", action="store_true", help="Output one URL per line (for piping)")

    p_status = intel_sub.add_parser("status", help="Check sitemap scan job status")
    p_status.add_argument("scan_id", help="Scan ID to check")

    p_clear = intel_sub.add_parser("clear-cache", help="Clear mimic auto-routing cache for a domain")
    p_clear.add_argument("domain", help="Domain to clear (or 'all' to clear everything)")

    # --- spider ---
    p_spider = subparsers.add_parser("spider", help="Deploy, run, and manage spiders via Forge")
    spider_sub = p_spider.add_subparsers(dest="spider_command")

    p_sp_list = spider_sub.add_parser("list", help="List deployed spiders")

    p_sp_deploy = spider_sub.add_parser("deploy", help="Deploy a spider from a .py file")
    p_sp_deploy.add_argument("file", help="Path to spider .py file")
    p_sp_deploy.add_argument("--name", default=None, help="Spider display name")
    p_sp_deploy.add_argument("--slug", default=None, help="Spider slug (auto-generated if omitted)")

    p_sp_run = spider_sub.add_parser("run", help="Trigger a spider run")
    p_sp_run.add_argument("slug", help="Spider slug")
    p_sp_run.add_argument("--arg", action="append", metavar="KEY=VALUE", help="Spider argument (repeatable)")
    p_sp_run.add_argument("--setting", action="append", metavar="KEY=VALUE", help="Scrapy setting override (repeatable)")

    p_sp_status = spider_sub.add_parser("status", help="Show recent runs for a spider")
    p_sp_status.add_argument("slug", help="Spider slug")

    p_sp_logs = spider_sub.add_parser("logs", help="Show logs for a spider run")
    p_sp_logs.add_argument("slug", help="Spider slug")
    p_sp_logs.add_argument("--run", type=int, default=None, help="Run number (default: most recent)")
    p_sp_logs.add_argument("--lines", "-n", type=int, default=100, help="Number of log lines (default: 100)")

    p_sp_results = spider_sub.add_parser("results", help="Download spider results")
    p_sp_results.add_argument("slug", help="Spider slug")
    p_sp_results.add_argument("-o", "--output", default=None, help="Output file path (default: {slug}_results.jsonl)")
    p_sp_results.add_argument("--run", type=int, default=None, help="Run number (default: most recent)")
    p_sp_results.add_argument("--preview", action="store_true", help="Preview a few items instead of downloading")
    p_sp_results.add_argument("--max-items", type=int, default=0, help="Max items to download (0 = all)")

    # --- login / logout ---
    subparsers.add_parser("login", help="Sign in via browser (Keycloak PKCE)")
    subparsers.add_parser("logout", help="Remove stored credentials")

    args = parser.parse_args()
    cfg = get_config(args.env)
    cfg["_env_name"] = args.env

    commands = {
        "fetch": cmd_fetch,
        "scrape": cmd_scrape,
        "data": cmd_data,
        "chat": cmd_chat,
        "intel": cmd_intel,
        "spider": cmd_spider,
        "login": cmd_login,
        "logout": cmd_logout,
    }

    commands[args.command](args, cfg)


if __name__ == "__main__":
    main()
