# Hosting the page and the server

The page calls the API at `/review-api/<ns>` on its own origin. So serve
the page and the API from one origin. First find out which setup the
machine has.

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

## Fleet setup

This applies only on an exe.dev fleet VM, where `fleet-artifacts.service`
runs nginx on port 8000 and serves `~/artifacts`. Check:
`systemctl is-active fleet-artifacts`, and that
`~/.config/fleet-artifacts/disabled` does not exist. The VM's own agent
notes have the full rules. Read them.

- **The page**: write it to `~/artifacts/<name>/index.html`. The link is
  `https://$(hostname).exe.xyz/<name>/`. Give each page its own directory,
  such as `<repo>-pr<N>-walkthrough` and `<repo>-pr<N>-walkthrough-r2`.
- **The API**: run the server on a free local port, such as 8787. Add one
  nginx route, then reload. A bad route fails the reload and keeps the old
  config.

  ```sh
  cat > ~/.config/fleet-artifacts/routes/review-api.conf <<'EOF'
  location /review-api/ { proxy_pass http://127.0.0.1:8787/; }
  EOF
  sudo systemctl reload fleet-artifacts
  ```

- **Testing**: open `http://127.0.0.1:8000/<name>/` in the test browser.
  This is the same nginx, without the exe.dev login.
- **Access**: only the VM owner can open the link. Do not change the
  sharing (`share set-public`, `share port`, `share add`, `share add-link`).
- **When `disabled` exists**: another server owns port 8000. Leave it
  alone. Run the review server with `--static ~/artifacts` on a port from
  3000 to 9999. The link is `https://$(hostname).exe.xyz:<port>/<name>/`.

## Other machines

Run the review server with `--static <dir>`, where `<dir>/<name>/index.html`
is the page, on `127.0.0.1:<port>`. The server then serves the page and the
API from one origin. The link is `http://127.0.0.1:<port>/<name>/`. A
reviewer on another computer can use an SSH tunnel:
`ssh -L <port>:127.0.0.1:<port> <host>`.

When the machine already has a web server that the user uses, a route from
`/review-api/` to the review server works too, as in "Fleet setup".

Do not bind to a public address, and do not put the server behind a public
URL, unless the user asks. The API has no authentication, and the page
quotes private code.

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

Check the server through the same path that the page uses:
`curl -s http://127.0.0.1:8000/review-api/health` in the fleet setup, or
`curl -s http://127.0.0.1:<port>/review-api/health` with `--static`.
