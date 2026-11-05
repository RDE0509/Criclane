# Deploying Pitch Pulse publicly

Three options, roughly easiest → most control. All work with the files in
this folder as-is.

---

## 1. Streamlit Community Cloud — fastest, free, zero servers

Not self-hosted, but it's the official free way to put a Streamlit app
online with a public URL and automatic HTTPS, and it's worth knowing
about even though it isn't open-source infrastructure itself (Streamlit
the *framework* is Apache-2.0 open source; the Cloud is a free hosted
service on top of it).

1. Push this folder to a **public** GitHub repo (needs `cricket_dashboard.py`,
   `sample_data.json`, `requirements.txt`).
2. Go to **share.streamlit.io** → "New app" → pick the repo, branch, and
   `cricket_dashboard.py` as the entry point.
3. Click Deploy. You'll get a public `https://<name>.streamlit.app` URL
   in a couple of minutes, free, with HTTPS handled for you.

Limitation: free tier apps sleep after inactivity and wake on next visit,
and you don't control the underlying server.

---

## 2. Coolify — open-source, self-hosted, your own server (recommended for "real" open source hosting)

[Coolify](https://coolify.io) is an open-source, self-hosted
platform-as-a-service — you point it at any VPS you own (Hetzner,
DigitalOcean, Vultr, Hostinger, even a Raspberry Pi) over SSH, and it
handles Docker builds, deploys, and **automatic HTTPS** for you through
a web dashboard. You pay only for the VPS (roughly $5–10/month), and the
platform itself is free to self-host.

1. Get a small VPS (2GB RAM is plenty) and point a domain's DNS `A`
   record at its IP.
2. SSH in and install Coolify with their one-line installer
   (see coolify.io/docs for the current install command).
3. In the Coolify dashboard: New Project → connect this GitHub repo →
   build pack: **Dockerfile** (use the `Dockerfile` in this folder) →
   set the domain → Deploy.
4. Coolify provisions a free Let's Encrypt certificate for your domain
   automatically.

This gives you a genuinely self-hosted, open-source deployment stack
with none of the vendor lock-in of a managed PaaS.

---

## 3. Manual Docker + Caddy on any VPS — full control, no extra platform

If you'd rather not install a PaaS at all, the `Dockerfile`,
`docker-compose.yml`, and `Caddyfile` in this folder are a complete,
open-source self-hosting stack on their own (Docker + Caddy, both
open source):

1. Get any VPS, point your domain's DNS at its IP, and make sure ports
   80/443 are open.
2. Edit `Caddyfile` — replace `your-domain.com` with your real domain.
3. Copy this whole folder to the server, then run:
   ```bash
   docker compose up -d --build
   ```
4. Caddy automatically requests and renews a free HTTPS certificate the
   first time it starts, and reverse-proxies your domain to the
   dashboard container.

To update after a code change: `git pull && docker compose up -d --build`.

---

## A note on the live data feed when deployed publicly

However you deploy it, remember the live fetch hits Cricbuzz's
undocumented `/api/home` endpoint (see `README.md`). On a public VPS
this is *more* likely to get rate-limited or blocked than on your laptop,
since it's now a server IP making repeated automated requests every
5–60 seconds. If that happens in production, swap in one of the
open-source alternatives listed in `README.md` (e.g. self-host
`IPL-2026-API-Free` alongside this app, or wire up `pycricbuzz`) rather
than hammering Cricbuzz's internal endpoint continuously.
