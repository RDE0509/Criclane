# Pitch Pulse — Cricket Scores Dashboard

A Streamlit dashboard that turns Cricbuzz-shaped match JSON into a
scoreboard-styled live/upcoming/completed view.

## Run it

```bash
pip install -r requirements.txt
streamlit run cricket_dashboard.py
```

Three data sources are switchable from the sidebar:

1. **Try live Cricbuzz feed** — calls `https://www.cricbuzz.com/api/home`
   directly with browser-like headers.
2. **Upload a JSON snapshot** — drop in any file shaped like the sample
   (or the real Cricbuzz home-page response).
3. **Bundled sample data** — `sample_data.json`, included so the dashboard
   always has something to show.

## Auto-refresh

The sidebar has an **Auto-refresh** toggle (on by default) and an
interval picker (5/10/15/30/60s, default 10s). It uses the
[`streamlit-autorefresh`](https://github.com/kmcgrady/streamlit-autorefresh)
component to silently rerun the app on a timer without a full page
reload, so your filters and scroll position survive each refresh. If
that package isn't installed, the toggle just won't do anything — the
rest of the dashboard still works with a manual "Refresh now" button.

## What's in the UI

- A scrolling live-scores ticker across the top (like a broadcast lower-third).
- Per-match cards with team badges, proportional score bars, and the
  batting team marked with ▶.
- Current run rate (CRR) and required run rate (RRR) for live limited-overs
  matches, computed from the raw ball-by-ball overs figures.
- Live / Upcoming / Completed / All tabs, plus format and free-text search filters.

## Deploying this publicly

See `DEPLOY.md` for three ways to put this online: the free official
Streamlit Community Cloud, a self-hosted open-source PaaS (Coolify), or
a manual Docker + Caddy setup on any VPS.

## Why the live fetch might fail

`cricbuzz.com/api/home` is not a documented, public API — it's the
internal endpoint Cricbuzz's own website calls. It can:

- block requests that don't look like a real browser,
- rate-limit or geo-block server traffic,
- change its JSON shape without notice.

The app already falls back to the bundled sample automatically if the
live call fails, so the dashboard never breaks — but for anything beyond
personal/learning use, treat direct scraping as fragile and check
Cricbuzz's terms of service before relying on it in production.

## Open-source alternatives (no direct scraping required from you)

| Project | What it is | Notes |
|---|---|---|
| [`pycricbuzz`](https://github.com/codophobia/pycricbuzz) (PyPI) | A small Python wrapper that does the Cricbuzz scraping for you and returns clean dicts (`matches()`, `livescore()`, `scorecard()`) | `pip install pycricbuzz`. Last released 2019 — works often, but Cricbuzz layout drift can break it; treat as best-effort. |
| [`IPL-2026-API-Free`](https://github.com/cu-sanjay/IPL-2026-API-Free) | Self-hosted Flask service that scrapes live scores/schedule/points table from multiple sources and serves clean JSON | Free to deploy on Render/Railway/Heroku. IPL-focused. |
| [`Cricket-API`](https://github.com/tarun7r/Cricket-API) (tarun7r) | Flask app + website for live scores, fixtures, tables, and player stats across ODI/T20/Test/IPL | Explicitly marked "educational purposes" — fine for a personal dashboard like this one. |
| [CricketData.org](https://cricketdata.org/) free tier | A hosted, non-scraping JSON API rather than an open-source repo | Good if you'd rather not scrape at all and can live with a free-tier rate limit. Several open-source projects build on top of it. |

### Swapping one in

To point this dashboard at `pycricbuzz` instead of the raw endpoint,
replace `fetch_live()` in `cricket_dashboard.py` with something like:

```python
from pycricbuzz import Cricbuzz

@st.cache_data(ttl=30, show_spinner=False)
def fetch_live(_cache_buster: int):
    c = Cricbuzz()
    return {"payload": c.matches()}  # adapt extract_matches() to match its shape
```

You'll need to adjust `extract_matches()` to match whichever library's
return shape you choose — they're all slightly different.

## Files

- `cricket_dashboard.py` — the app
- `sample_data.json` — bundled fallback / demo data
- `requirements.txt` — Python dependencies
- `Dockerfile`, `docker-compose.yml`, `Caddyfile` — self-hosting stack (see `DEPLOY.md`)
- `DEPLOY.md` — step-by-step public deployment options
