#!/usr/bin/env python3
"""Print one authenticated Pocket Option Socket.IO auth object; keep secrets in memory."""
import argparse
import fcntl
import json
import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import urlsplit

MARKET_URL = "https://pocketoption.com/en/cabinet/quick-high-low/"
LOGIN_URL = "https://pocketoption.com/en/login/"
HEADER_RE = re.compile(r'^45(\d+)-\["([^"]+)"')


class Failure(Exception):
    """A fixed, secret-free operator message."""


class Socket:
    def __init__(self, demo):
        self.demo, self.auth, self.success, self.pending = demo, None, False, []

    def sent(self, payload):
        if not isinstance(payload, str) or not payload.startswith('42["auth"'):
            return
        try:
            auth = json.loads(payload[2:])[1]
            mode, token, uid = int(auth["isDemo"]), auth["session"], auth["uid"]
        except (IndexError, KeyError, TypeError, ValueError, OverflowError):
            return
        if isinstance(token, str) and token and isinstance(uid, int) and not isinstance(uid, bool) and uid > 0:
            self.auth, self.success = auth if mode == self.demo else None, False

    def received(self, payload):
        if isinstance(payload, str):
            match = HEADER_RE.match(payload)
            if match:
                self.pending.append([int(match[1]), match[2]])
        elif self.pending:
            self.pending[0][0] -= 1
            if self.pending[0][0] <= 0:
                self.success |= self.pending.pop(0)[1] == "successauth"


def browser(playwright, args, headless):
    return playwright.chromium.launch_persistent_context(
        str(args.profile), executable_path=args.chrome, headless=headless,
        viewport={"width": 1440, "height": 1000}, locale="en-US", accept_downloads=False,
    )


def navigate(page, url, timeout):
    from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
    try:
        page.goto(url, wait_until="commit", timeout=timeout)
    except PlaywrightTimeoutError:
        pass  # The anti-bot front end can leave navigation pending.


def capture(playwright, args):
    sockets = []
    with browser(playwright, args, True) as context:
        for existing in list(context.pages):
            existing.close()
        page = context.new_page()

        def opened(websocket):
            url = urlsplit(websocket.url)
            if url.scheme == "wss" and (url.hostname or "").endswith(".po.market") and url.path == "/socket.io/":
                socket = Socket(args.account_class == "demo")
                sockets.append(socket)
                websocket.on("framesent", socket.sent)
                websocket.on("framereceived", socket.received)

        page.on("websocket", opened)
        navigate(page, MARKET_URL, 60_000)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            url = urlsplit(page.url)
            if url.hostname == "pocketoption.com":
                if url.path.rstrip("/") == "/en/login":
                    return None
                if "/cabinet/" in url.path:
                    for socket in sockets:
                        if socket.auth is not None and socket.success:
                            return socket.auth
            page.wait_for_timeout(250)
    raise Failure("could not confirm market authentication")


def login(playwright, args):
    with browser(playwright, args, False) as context:
        page = context.pages[0] if context.pages else context.new_page()
        deadline = time.monotonic() + args.login_timeout
        print("pocket-auth: solve the captcha and submit the login form in the browser", file=sys.stderr, flush=True)
        navigate(page, LOGIN_URL, min(60_000, args.login_timeout * 1000))
        prepared = False
        while time.monotonic() < deadline:
            url = urlsplit(page.url)
            if url.hostname == "pocketoption.com" and url.path.rstrip("/") != "/en/login":
                return
            page.set_default_timeout(max(1, min(30_000, (deadline - time.monotonic()) * 1000)))
            email = page.locator('input[name="email"]')
            password = page.locator('input[name="password"]')
            submit = page.locator('button[type="submit"]')
            if not prepared and email.count() and password.count() and submit.count() and email.first.is_visible():
                user, secret = os.environ.get("POCKET_LOGIN"), os.environ.get("POCKET_PASSWORD")
                if user and secret:
                    email.first.fill(user)
                    password.first.fill(secret)
                remember = page.locator('input[name="remember"]')
                if remember.count():
                    remember.first.check(force=True)
                prepared = True
            page.wait_for_timeout(500)
    raise Failure("interactive login timed out")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    config = Path(os.environ.get("XDG_CONFIG_HOME") or "~/.config").expanduser()
    parser.add_argument("--profile", type=Path, default=config / "binary-alpha/pocket-profile")
    parser.add_argument("--login-timeout", type=float, default=600, metavar="SECONDS")
    parser.add_argument("--account-class", choices=["real", "demo"], default="real")
    parser.add_argument("--chrome", metavar="PATH", help="browser executable; default: Playwright's Chromium")
    args = parser.parse_args()
    try:
        os.umask(0o077)
        args.profile = args.profile.expanduser().resolve()
        args.profile.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with open(str(args.profile) + ".lock", "a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            args.profile.mkdir(exist_ok=True, mode=0o700)
            args.profile.chmod(0o700)
            for name in ("DEBUG", "DEBUGP", "DEBUG_FILE", "PWDEBUG"):
                os.environ.pop(name, None)  # Playwright's debug logging would echo the form values.
            from playwright.sync_api import sync_playwright
            with sync_playwright() as playwright:
                auth = capture(playwright, args)
                if auth is None:
                    if not (os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY")):
                        print("pocket-auth: interactive login required; run tools/pocket_auth.py from a desktop session", file=sys.stderr)
                        return 3
                    login(playwright, args)
                    auth = capture(playwright, args)
                    if auth is None:
                        raise Failure("authentication failed after interactive login")
            print(json.dumps(auth, separators=(",", ":")), flush=True)
        return 0
    except Failure as error:
        print(f"pocket-auth: {error}", file=sys.stderr)
    except KeyboardInterrupt:
        print("pocket-auth: interrupted", file=sys.stderr)
    except Exception as error:
        # Playwright messages name the failing step; they carry no form value or frame.
        print(f"pocket-auth: {type(error).__name__}: {str(error).splitlines()[0][:200]}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
