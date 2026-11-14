# Hosting the page and the server

The goal is a local server that serves the page and the review API to the
reviewer on this machine. The page calls the API at `/review-api/<ns>` on
its own origin, so serve both from one origin.

## Reuse what is there

Look for a running review server before you start one:

```sh
systemctl --user list-units --all 'review*'
curl -s http://127.0.0.1:8787/health
```

When one answers and follows `references/server-api.md`, add your namespace
to it: add `--ns <NAME>=<TITLE>` to `ExecStart` in its unit file, then run
`systemctl --user daemon-reload && systemctl --user restart <unit>`. The
data is on disk, and open pages retry, so a restart loses nothing. When the
server that is there follows a different contract, ask the user before you
change it.

## The local server

Write the page to `<dir>/<name>/index.html`, and run the review server with
`--static <dir>` on `127.0.0.1:<port>`. It serves the page and the API from
one origin. The link is `http://127.0.0.1:<port>/<name>/`. Give each page
its own directory, such as `<repo>-pr<N>-walkthrough` and
`<repo>-pr<N>-walkthrough-r2`.

## On an exe.dev fleet VM

The VM's agent notes have the full rules. Read them. One option: when
`fleet-artifacts` runs nginx on port 8000 and serves `~/artifacts`, write
the page to `~/artifacts/<name>/index.html`, run the review server without
`--static` on a local port such as 8787, and add one route:

```sh
cat > ~/.config/fleet-artifacts/routes/review-api.conf <<'EOF'
location /review-api/ { proxy_pass http://127.0.0.1:8787/; }
EOF
sudo systemctl reload fleet-artifacts
```

The link is `https://$(hostname).exe.xyz/<name>/`. The exe.dev proxy
controls who can open it. In the test browser, open
`http://127.0.0.1:8000/<name>/`, the same nginx without the exe.dev login.

## Run it durably

The server must survive the end of your session. With systemd, use a user
unit, such as `~/.config/systemd/user/review-api.service`:

```ini
[Unit]
Description=Review comments API for literate diff walkthroughs

[Service]
ExecStart=/usr/bin/python3 -I %h/review-comments/server.py --root %h/review-comments --port 8787 --ns "myrepo-pr42=myrepo PR #42"
Restart=always
RestartSec=2

[Install]
WantedBy=default.target
```

```sh
systemctl --user daemon-reload
systemctl --user enable --now review-api
loginctl show-user "$USER" -p Linger
```

When `Linger=no`, user units stop at logout. Ask the user to run
`sudo loginctl enable-linger "$USER"`, or run it yourself when you have
permission. Without systemd, start the server with `nohup` in the
background, and tell the user that it does not survive a reboot.

Check the server through the same path that the page uses, such as
`curl -s http://127.0.0.1:<port>/review-api/health`.
