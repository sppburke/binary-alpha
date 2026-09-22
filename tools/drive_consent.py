#!/usr/bin/env python3
"""One-time Google Drive consent for Binary Alpha.

Runs the loopback consent flow for a Desktop OAuth client with the drive.file scope, exchanges
the code for a refresh token, creates the private archive root folder with that grant, and writes
BINARY_ALPHA_DRIVE_CREDENTIAL and BINARY_ALPHA_DRIVE_ROOT into the private env file. No secret
is printed. Standard library only.

Usage: drive_consent.py /path/to/client_secret_....json [--folder-name binary-alpha-archive]
"""
import http.server
import json
import os
import secrets
import sys
import urllib.parse
import urllib.request
import webbrowser

ENV = os.path.join(os.path.expanduser(os.environ.get("XDG_CONFIG_HOME") or "~/.config"),
                   "binary-alpha", "binary-alpha.env")
SCOPE = "https://www.googleapis.com/auth/drive.file"


def main() -> int:
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    client = json.load(open(sys.argv[1], encoding="utf-8"))
    client = client.get("installed") or client.get("web") or client
    client_id, client_secret = client["client_id"], client["client_secret"]
    folder_name = sys.argv[sys.argv.index("--folder-name") + 1] if "--folder-name" in sys.argv else "binary-alpha-archive"

    state = secrets.token_urlsafe(16)
    code_holder = {}

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            query = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            if query.get("state", [""])[0] == state and "code" in query:
                code_holder["code"] = query["code"][0]
                body = b"Consent received. You can close this tab."
            else:
                body = b"Unexpected request."
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    redirect = f"http://127.0.0.1:{server.server_port}"
    url = "https://accounts.google.com/o/oauth2/v2/auth?" + urllib.parse.urlencode({
        "client_id": client_id,
        "redirect_uri": redirect,
        "response_type": "code",
        "scope": SCOPE,
        "access_type": "offline",
        "prompt": "consent",
        "state": state,
    })
    print("Open this URL in the browser signed in to the Google account that owns the archive:", file=sys.stderr)
    print(url, file=sys.stderr)
    webbrowser.open(url)
    while "code" not in code_holder:
        server.handle_request()
    server.server_close()

    token = json.load(urllib.request.urlopen(urllib.request.Request(
        "https://oauth2.googleapis.com/token",
        data=urllib.parse.urlencode({
            "code": code_holder["code"],
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": redirect,
            "grant_type": "authorization_code",
        }).encode(),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )))
    refresh = token.get("refresh_token")
    if not refresh:
        raise SystemExit("no refresh token returned; revoke the app's access in the Google account and run again")

    folder = json.load(urllib.request.urlopen(urllib.request.Request(
        "https://www.googleapis.com/drive/v3/files?fields=id,name",
        data=json.dumps({"name": folder_name, "mimeType": "application/vnd.google-apps.folder"}).encode(),
        headers={"Authorization": f"Bearer {token['access_token']}", "Content-Type": "application/json"},
    )))

    credential = json.dumps({"client_id": client_id, "client_secret": client_secret, "refresh_token": refresh}, separators=(",", ":"))
    lines = open(ENV, encoding="utf-8").read().splitlines()
    updated = []
    seen = set()
    for line in lines:
        key = line.split("=", 1)[0] if "=" in line and not line.startswith("#") else None
        if key == "BINARY_ALPHA_DRIVE_CREDENTIAL":
            line = "BINARY_ALPHA_DRIVE_CREDENTIAL='" + credential + "'"
        elif key == "BINARY_ALPHA_DRIVE_ROOT":
            line = "BINARY_ALPHA_DRIVE_ROOT='" + folder["id"] + "'"
        if key:
            seen.add(key)
        updated.append(line)
    if "BINARY_ALPHA_DRIVE_CREDENTIAL" not in seen:
        updated.append("BINARY_ALPHA_DRIVE_CREDENTIAL='" + credential + "'")
    if "BINARY_ALPHA_DRIVE_ROOT" not in seen:
        updated.append("BINARY_ALPHA_DRIVE_ROOT='" + folder["id"] + "'")
    tmp = ENV + ".tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        handle.write("\n".join(updated) + "\n")
    os.chmod(tmp, 0o600)
    os.replace(tmp, ENV)
    print(f"Drive consent stored; archive root folder '{folder['name']}' created (id written to the env file).", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
