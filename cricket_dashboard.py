"""
Pitch Pulse — a Streamlit cricket scores dashboard.

Reads Cricbuzz-shaped match JSON (either fetched live, uploaded, or the
bundled sample) and renders it as a stadium-scoreboard-style dashboard,
with optional auto-refresh.

Run:
    streamlit run cricket_dashboard.py
"""

import hashlib
import json
import math
import os
import time
from datetime import datetime, timezone

import requests
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False

# --------------------------------------------------------------------------
# Page setup
# --------------------------------------------------------------------------

st.set_page_config(
    page_title="Pitch Pulse — Live Cricket Scores",
    page_icon="🏏",
    layout="wide",
    initial_sidebar_state="expanded",
)

SAMPLE_PATH = os.path.join(os.path.dirname(__file__), "sample_data.json")
LIVE_URL = "https://www.cricbuzz.com/api/home"
OVERS_LIMIT = {"T20": 20, "T10": 10, "ODI": 50}

# Curated jersey-style palette (kept inside the theme's tonal range so
# team badges read as "broadcast graphics", not a rainbow of random hues).
JERSEY_PALETTE = [
    "#3B6FA0", "#A0522D", "#4B7F52", "#8E5A8A", "#B5651D", "#5C6BC0",
    "#6E8B3D", "#A23B3B", "#3E7C7C", "#9C6B30", "#5A4FCF", "#2F8F6A",
]

# --------------------------------------------------------------------------
# Design tokens — stadium-at-dusk scoreboard theme
# --------------------------------------------------------------------------

CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@500;700;800&family=Space+Mono:wght@400;700&family=Inter:wght@400;500;600&display=swap');

:root{
  --bg-deep:#1A3A52;
  --panel:#2C5AA0;
  --panel-edge:#4A7BC4;
  --chalk:#FFFFFF;
  --willow:#FFB800;
  --willow-dim:#E6A500;
  --ball-red:#FF6B35;
  --floodlight:#FFD700;
  --muted:#B8C5D6;
}

html, body, [class*="css"]{ font-family:'Inter', sans-serif; }
.stApp{
  background:
    repeating-linear-gradient(0deg, rgba(255,255,255,0.012) 0px, rgba(255,255,255,0.012) 1px, transparent 1px, transparent 3px),
    radial-gradient(circle at 20% -10%, #2C5AA0 0%, var(--bg-deep) 55%), var(--bg-deep);
}
section[data-testid="stSidebar"]{ background:#1A3A52; border-right:1px solid var(--panel-edge); }

h1,h2,h3{ font-family:'Barlow Condensed', sans-serif; color:var(--chalk); letter-spacing:0.01em; }

/* ---- Hero header band ---- */
.pp-hero{
  display:flex; align-items:flex-end; justify-content:space-between;
  border-bottom:2px solid var(--panel-edge); padding-bottom:14px; margin-bottom:10px;
}
.pp-hero .eyebrow{
  font-family:'Space Mono', monospace; font-size:12px; letter-spacing:0.18em;
  color:var(--floodlight); text-transform:uppercase; margin-bottom:2px;
}
.pp-hero h1{ font-size:42px; font-weight:800; margin:0; line-height:1; }
.pp-hero .sub{ color:var(--muted); font-size:14px; margin-top:4px; }
.pp-updated{
  font-family:'Space Mono', monospace; color:var(--muted); font-size:12px; text-align:right;
}
.pp-updated b{ color:var(--chalk); }
.pp-live-dot{
  display:inline-block; width:7px; height:7px; border-radius:50%; background:var(--ball-red);
  margin-right:6px; animation: pp-pulse 1.4s infinite;
}
@media (prefers-reduced-motion: reduce){ .pp-live-dot{ animation:none; } }

/* ---- Scrolling live ticker ---- */
.pp-ticker-wrap{
  background:#2C5AA0; border:1px solid var(--panel-edge); border-radius:8px;
  overflow:hidden; margin-bottom:18px; position:relative; height:34px;
}
.pp-ticker-wrap::before, .pp-ticker-wrap::after{
  content:""; position:absolute; top:0; bottom:0; width:28px; z-index:2;
}
.pp-ticker-wrap::before{ left:0; background:linear-gradient(90deg, #2C5AA0, transparent); }
.pp-ticker-wrap::after{ right:0; background:linear-gradient(270deg, #2C5AA0, transparent); }
.pp-ticker{
  display:flex; align-items:center; gap:36px; white-space:nowrap;
  font-family:'Space Mono', monospace; font-size:12.5px; color:var(--chalk);
  height:34px; animation: pp-scroll 32s linear infinite; padding-left:100%;
}
.pp-ticker span.lbl{ color:var(--ball-red); font-weight:700; margin-right:8px; }
@keyframes pp-scroll{ 0%{ transform:translateX(0); } 100%{ transform:translateX(-100%); } }
@media (prefers-reduced-motion: reduce){ .pp-ticker{ animation:none; padding-left:14px; } }

/* ---- Match card ---- */
.pp-card{
  background:var(--panel); border:1px solid var(--panel-edge); border-radius:10px;
  padding:16px 18px; margin-bottom:14px; position:relative; overflow:hidden;
  transition:border-color .15s ease, transform .15s ease;
}
.pp-card:hover{ border-color:#33493d; transform:translateY(-1px); }
.pp-card::before{
  content:""; position:absolute; left:0; top:0; bottom:0; width:4px;
}
.pp-card.live::before{ background:var(--ball-red); }
.pp-card.upcoming::before{ background:var(--floodlight); }
.pp-card.completed::before{ background:var(--willow); }

.pp-card-top{ display:flex; justify-content:space-between; align-items:flex-start; gap:10px; margin-bottom:12px; }
.pp-series{ font-size:12px; color:var(--muted); text-transform:uppercase; letter-spacing:0.06em; }
.pp-desc{ font-family:'Barlow Condensed', sans-serif; font-weight:700; font-size:17px; color:var(--chalk); margin-top:1px;}

.pp-badges{ display:flex; gap:6px; flex-shrink:0; }
.pp-chip{
  font-family:'Space Mono', monospace; font-size:11px; padding:3px 9px; border-radius:20px;
  border:1px solid var(--panel-edge); color:var(--muted); white-space:nowrap;
}
.pp-chip.fmt{ color:var(--chalk); border-color:#33493d; background:#102019; }
.pp-chip.live{ color:#1a0c08; background:var(--ball-red); border-color:var(--ball-red); font-weight:700; }
.pp-chip.live .dot{
  display:inline-block; width:6px; height:6px; border-radius:50%; background:#1a0c08;
  margin-right:5px; animation: pp-pulse 1.4s infinite;
}
@keyframes pp-pulse{ 0%,100%{opacity:1;} 50%{opacity:0.25;} }
.pp-chip.upcoming{ color:#3a2c08; background:var(--floodlight); border-color:var(--floodlight); font-weight:700; }
.pp-chip.completed{ color:#0c2417; background:var(--willow); border-color:var(--willow); font-weight:700; }

.pp-team-row{ display:flex; align-items:center; gap:10px; margin:6px 0; }
.pp-badge{
  width:30px; height:30px; border-radius:50%; flex-shrink:0; display:flex; align-items:center;
  justify-content:center; font-family:'Space Mono', monospace; font-weight:700; font-size:11px;
  color:#F8F6EF; box-shadow:inset 0 0 0 1px rgba(255,255,255,0.18);
}
.pp-team-name{ width:175px; flex-shrink:0; font-size:14px; color:var(--chalk); font-weight:600; }
.pp-team-name.bat{ color:var(--floodlight); }
.pp-bat-arrow{ color:var(--floodlight); margin-left:5px; font-size:11px; }
.pp-bar-track{ flex:1; height:9px; background:#1A3A52; border-radius:5px; overflow:hidden; border:1px solid var(--panel-edge); }
.pp-bar-fill{ height:100%; border-radius:5px; background:linear-gradient(90deg, var(--willow-dim), var(--willow)); }
.pp-score{
  font-family:'Space Mono', monospace; font-size:14px; color:var(--chalk); width:150px;
  text-align:right; flex-shrink:0;
}
.pp-score .yet{ color:var(--muted); font-style:italic; }

.pp-rates{ display:flex; gap:14px; margin:8px 0 0 40px; }
.pp-rate{ font-family:'Space Mono', monospace; font-size:11.5px; color:var(--muted); }
.pp-rate b{ color:var(--chalk); }

.pp-status{
  font-size:13px; color:var(--chalk); margin-top:10px; padding-top:10px;
  border-top:1px dashed var(--panel-edge);
}
.pp-status b{ color:var(--floodlight); }
.pp-meta{ font-size:12px; color:var(--muted); margin-top:4px; }

.pp-empty{
  text-align:center; padding:50px 0; color:var(--muted); font-family:'Barlow Condensed', sans-serif;
  font-size:18px; border:1px dashed var(--panel-edge); border-radius:10px;
}

/* Sidebar tidy-up */
.pp-side-h{ font-family:'Barlow Condensed', sans-serif; color:var(--floodlight); letter-spacing:0.08em;
  text-transform:uppercase; font-size:12px; margin:18px 0 6px 0; }
</style>
"""

STATE_TO_CATEGORY = {
    "complete": "completed",
    "abandoned": "completed",
    "cancelled": "completed",
    "no result": "completed",
    "preview": "upcoming",
}


def categorize(state: str) -> str:
    return STATE_TO_CATEGORY.get((state or "").strip().lower(), "live")


def badge_color(short_name: str) -> str:
    h = int(hashlib.md5((short_name or "?").encode()).hexdigest(), 16)
    return JERSEY_PALETTE[h % len(JERSEY_PALETTE)]


# --------------------------------------------------------------------------
# Data loading
# --------------------------------------------------------------------------

@st.cache_data(ttl=8, show_spinner=False)
def fetch_live(_cache_buster: int):
    """Attempt to pull live data straight from Cricbuzz's unofficial endpoint."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        ),
        "Accept": "application/json",
    }
    resp = requests.get(LIVE_URL, headers=headers, timeout=6)
    resp.raise_for_status()
    return resp.json()


@st.cache_data(show_spinner=False)
def load_sample():
    with open(SAMPLE_PATH, "r") as f:
        return json.load(f)


def extract_matches(raw: dict):
    """Normalize either the real Cricbuzz home-page shape (typeMatches ->
    seriesMatches -> seriesAdWrapper -> matches) or the flatter shape used
    in the bundled sample (payload.matches: [{match: {...}}])."""
    payload = raw.get("payload", raw) if isinstance(raw, dict) else {}
    out = []

    if "typeMatches" in payload:
        for tm in payload.get("typeMatches", []):
            for sm in tm.get("seriesMatches", []):
                wrapper = sm.get("seriesAdWrapper", {})
                for m in wrapper.get("matches", []):
                    out.append(m)
    elif "matches" in payload:
        for item in payload.get("matches", []):
            out.append(item.get("match", item))

    last_updated = payload.get("responseLastUpdated")
    return out, last_updated


# --------------------------------------------------------------------------
# Parsing helpers
# --------------------------------------------------------------------------

def latest_innings(team_score: dict):
    """Return the most recent innings dict (has runs/wickets/overs/inningsId)."""
    if not team_score:
        return None
    last_key = sorted(team_score.keys())[-1]
    return team_score.get(last_key)


def innings_str(team_score: dict):
    """Build something like '220/2 (19.6)' or '391 & 222/6 (58.6)' for tests,
    falling back gracefully when fields are missing."""
    if not team_score:
        return None, None
    parts = []
    last_runs = None
    for key in sorted(team_score.keys()):
        inn = team_score[key]
        runs = inn.get("runs")
        if runs is None:
            continue
        wkts = inn.get("wickets")
        last_runs = runs
        chunk = f"{runs}" if (wkts is None or wkts >= 10) else f"{runs}/{wkts}"
        parts.append(chunk)
    if not parts:
        return None, None
    line = " & ".join(parts)
    overs = list(team_score.values())[-1].get("overs")
    if overs is not None:
        line = f"{line} ({overs})"
    return line, last_runs


def to_decimal_overs(overs):
    """Cricket overs are written ball-by-ball (18.3 = 18 overs + 3 balls),
    not true decimals. Convert to a real decimal for run-rate maths."""
    if overs is None:
        return None
    whole = int(overs)
    balls = round((overs - whole) * 10)
    return whole + balls / 6


def run_rate(runs, overs):
    dec = to_decimal_overs(overs)
    if not dec:
        return None
    return round(runs / dec, 2)


def parse_match(m: dict):
    info = m.get("matchInfo", {})
    score = m.get("matchScore", {})

    team1 = info.get("team1", {})
    team2 = info.get("team2", {})
    t1_short = team1.get("teamSName", team1.get("teamName", "T1")[:4].upper())
    t2_short = team2.get("teamSName", team2.get("teamName", "T2")[:4].upper())

    t1_score = score.get("team1Score")
    t2_score = score.get("team2Score")
    t1_line, t1_runs = innings_str(t1_score)
    t2_line, t2_runs = innings_str(t2_score)
    t1_last = latest_innings(t1_score)
    t2_last = latest_innings(t2_score)

    max_runs = max(t1_runs or 0, t2_runs or 0, 1)
    state = info.get("state", "")
    category = categorize(state)
    fmt = (info.get("matchFormat") or "").upper()
    bat_team_id = info.get("currBatTeamId")
    team1_id, team2_id = team1.get("teamId"), team2.get("teamId")

    crr, rrr = None, None
    if category == "live" and fmt in OVERS_LIMIT:
        limit = OVERS_LIMIT[fmt]
        batting_last = t1_last if bat_team_id == team1_id else (t2_last if bat_team_id == team2_id else None)
        chasing_target_innings = t2_last if bat_team_id == team1_id else t1_last
        if batting_last and batting_last.get("overs") is not None and batting_last.get("runs") is not None:
            crr = run_rate(batting_last["runs"], batting_last["overs"])
            if batting_last.get("inningsId") == 2 and chasing_target_innings and chasing_target_innings.get("runs") is not None:
                target = chasing_target_innings["runs"] + 1
                overs_left = max(limit - to_decimal_overs(batting_last["overs"]), 0)
                runs_needed = target - batting_last["runs"]
                if overs_left > 0.05 and runs_needed > 0:
                    rrr = round(runs_needed / overs_left, 2)

    return {
        "id": info.get("matchId"),
        "series": info.get("seriesName", ""),
        "desc": info.get("matchDesc", ""),
        "format": fmt,
        "state": state,
        "category": category,
        "status": info.get("status", "") or info.get("shortStatus", ""),
        "team1_name": team1.get("teamName", "TBD"),
        "team2_name": team2.get("teamName", "TBD"),
        "team1_short": t1_short,
        "team2_short": t2_short,
        "bat_team_id": bat_team_id,
        "team1_id": team1_id,
        "team2_id": team2_id,
        "t1_line": t1_line,
        "t2_line": t2_line,
        "t1_pct": max(8, round((t1_runs or 0) / max_runs * 100)) if t1_runs else 0,
        "t2_pct": max(8, round((t2_runs or 0) / max_runs * 100)) if t2_runs else 0,
        "crr": crr,
        "rrr": rrr,
        "venue": info.get("venueInfo", {}),
        "start_ms": info.get("startDate"),
    }


def fmt_start(ms):
    if not ms:
        return ""
    dt = datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)
    return dt.strftime("%b %d, %Y · %H:%M UTC")


# --------------------------------------------------------------------------
# Rendering
# --------------------------------------------------------------------------

def category_chip(cat: str) -> str:
    if cat == "live":
        return '<span class="pp-chip live"><span class="dot"></span>LIVE</span>'
    if cat == "upcoming":
        return '<span class="pp-chip upcoming">UPCOMING</span>'
    return '<span class="pp-chip completed">COMPLETE</span>'


def team_row(name, short, line, pct, is_batting):
    bat_class = " bat" if is_batting else ""
    score_html = line if line else '<span class="yet">yet to bat</span>'
    arrow = '<span class="pp-bat-arrow">▶</span>' if is_batting else ""
    color = badge_color(short)
    return f"""
    <div class="pp-team-row">
      <div class="pp-badge" style="background:{color};">{short}</div>
      <div class="pp-team-name{bat_class}">{name}{arrow}</div>
      <div class="pp-bar-track"><div class="pp-bar-fill" style="width:{pct}%;"></div></div>
      <div class="pp-score">{score_html}</div>
    </div>"""


def render_card(pm: dict) -> str:
    venue = pm["venue"] or {}
    venue_str = ", ".join(filter(None, [venue.get("ground"), venue.get("city")]))
    rows = team_row(
        pm["team1_name"], pm["team1_short"], pm["t1_line"], pm["t1_pct"], pm["bat_team_id"] == pm["team1_id"]
    ) + team_row(
        pm["team2_name"], pm["team2_short"], pm["t2_line"], pm["t2_pct"], pm["bat_team_id"] == pm["team2_id"]
    )

    rates = ""
    if pm["crr"] is not None:
        bits = [f'<div class="pp-rate">CRR <b>{pm["crr"]}</b></div>']
        if pm["rrr"] is not None:
            bits.append(f'<div class="pp-rate">RRR <b>{pm["rrr"]}</b></div>')
        rates = f'<div class="pp-rates">{"".join(bits)}</div>'

    meta_bits = [venue_str]
    if pm["start_ms"]:
        meta_bits.append(fmt_start(pm["start_ms"]))
    meta = " • ".join(filter(None, meta_bits))

    return f"""
    <div class="pp-card {pm['category']}">
      <div class="pp-card-top">
        <div>
          <div class="pp-series">{pm['series']}</div>
          <div class="pp-desc">{pm['desc']}</div>
        </div>
        <div class="pp-badges">
          <span class="pp-chip fmt">{pm['format']}</span>
          {category_chip(pm['category'])}
        </div>
      </div>
      {rows}
      {rates}
      <div class="pp-status">{pm['status']}</div>
      <div class="pp-meta">{meta}</div>
    </div>"""


def render_ticker(live_matches):
    if not live_matches:
        return ""
    bits = []
    for pm in live_matches:
        t1 = pm["t1_line"] or "yet to bat"
        t2 = pm["t2_line"] or "yet to bat"
        bits.append(
            f'<span class="lbl">●LIVE</span>{pm["team1_short"]} {t1} vs '
            f'{pm["team2_short"]} {t2} — {pm["status"]}'
        )
    content = "<span>&nbsp;&nbsp;&nbsp;&nbsp;</span>".join(bits) + "<span>&nbsp;&nbsp;&nbsp;&nbsp;</span>"
    return f'<div class="pp-ticker-wrap"><div class="pp-ticker">{content}{content}</div></div>'


# --------------------------------------------------------------------------
# Sidebar — data source, auto-refresh & filters
# --------------------------------------------------------------------------

st.markdown(CSS, unsafe_allow_html=True)

with st.sidebar:
    st.markdown("### 🏏 Pitch Pulse")
    st.caption("A scoreboard-styled cricket dashboard.")

    st.markdown('<div class="pp-side-h">Auto-refresh</div>', unsafe_allow_html=True)
    auto_on = st.toggle("Auto-refresh", value=True)
    interval_s = st.select_slider("Every", options=[5, 10, 15, 30, 60], value=10)
    if auto_on and not HAS_AUTOREFRESH:
        st.caption("Install `streamlit-autorefresh` (see requirements.txt) to enable this.")
    elif auto_on:
        st_autorefresh(interval=interval_s * 1000, key="pp_autorefresh")

    st.markdown('<div class="pp-side-h">Data source</div>', unsafe_allow_html=True)
    source = st.radio(
        "Data source",
        ["Try live Cricbuzz feed", "Upload a JSON snapshot", "Bundled sample data"],
        label_visibility="collapsed",
    )

    uploaded = None
    if source == "Upload a JSON snapshot":
        uploaded = st.file_uploader("Cricbuzz-shaped JSON", type=["json"])

    if source == "Try live Cricbuzz feed":
        if st.button("🔄 Refresh now", use_container_width=True):
            fetch_live.clear()
        st.caption(
            "⚠️ Cricbuzz's `/api/home` endpoint is undocumented and not "
            "officially public — it may block server-side requests or "
            "change shape without notice. If it fails, the dashboard "
            "falls back to the bundled sample automatically."
        )

    st.markdown('<div class="pp-side-h">Open-source alternatives</div>', unsafe_allow_html=True)
    with st.expander("Don't want to rely on scraping Cricbuzz directly?"):
        st.markdown(
            "- **pycricbuzz** (PyPI/GitHub `codophobia/pycricbuzz`) — a "
            "small Python wrapper around Cricbuzz's data, easy to swap "
            "in here.\n"
            "- **IPL-2026-API-Free** (GitHub `cu-sanjay`) — self-hosted "
            "Flask service you deploy yourself that scrapes and serves "
            "clean JSON.\n"
            "- **Cricket-API** (GitHub `tarun7r`) — Flask app + website "
            "covering ODI/T20/Test/IPL scores and player stats.\n"
            "- **CricketData.org free tier** — a hosted, non-scraping "
            "JSON API some of the above projects build on.\n\n"
            "See the bundled `README.md` for setup notes, and `DEPLOY.md` "
            "for how to put this dashboard online for free with open-source "
            "tooling."
        )

# --------------------------------------------------------------------------
# Load + parse data
# --------------------------------------------------------------------------

raw = None
load_note = None

if source == "Try live Cricbuzz feed":
    try:
        raw = fetch_live(int(time.time() // max(interval_s, 5)))
        load_note = ("success", "Live data fetched from Cricbuzz.")
    except Exception as exc:
        load_note = ("warning", f"Live fetch failed ({exc}). Showing bundled sample instead.")
        raw = load_sample()
elif source == "Upload a JSON snapshot":
    if uploaded is not None:
        try:
            raw = json.load(uploaded)
            load_note = ("success", "Loaded your uploaded snapshot.")
        except Exception as exc:
            load_note = ("error", f"Couldn't parse that file ({exc}). Showing bundled sample.")
            raw = load_sample()
    else:
        load_note = ("info", "Upload a JSON file in the sidebar — showing bundled sample for now.")
        raw = load_sample()
else:
    raw = load_sample()
    load_note = ("info", "Showing bundled sample data.")

matches_raw, last_updated_ms = extract_matches(raw)
matches = [parse_match(m) for m in matches_raw]

# --------------------------------------------------------------------------
# Hero header + live ticker
# --------------------------------------------------------------------------

updated_str = fmt_start(last_updated_ms) if last_updated_ms else datetime.now(timezone.utc).strftime("%b %d, %Y · %H:%M UTC")
refresh_note = f"auto-refreshing every {interval_s}s" if (auto_on and HAS_AUTOREFRESH) else "auto-refresh off"

st.markdown(
    f"""
    <div class="pp-hero">
      <div>
        <div class="eyebrow"><span class="pp-live-dot"></span>Live · Upcoming · Complete</div>
        <h1>Pitch Pulse</h1>
        <div class="sub">Every match, scoreboard-style — {refresh_note}.</div>
      </div>
      <div class="pp-updated">DATA REFRESHED<br><b>{updated_str}</b></div>
    </div>
    """,
    unsafe_allow_html=True,
)

if load_note:
    level, text = load_note
    getattr(st, level)(text)

all_live_for_ticker = [parse_match(m) for m in matches_raw if categorize(m.get("matchInfo", {}).get("state", "")) == "live"]
ticker_html = render_ticker(all_live_for_ticker)
if ticker_html:
    st.markdown(ticker_html, unsafe_allow_html=True)

# --------------------------------------------------------------------------
# Filters
# --------------------------------------------------------------------------

all_formats = sorted({m["format"] for m in matches if m["format"]})
col_f, col_s = st.columns([2, 3])
with col_f:
    chosen_formats = st.multiselect("Format", all_formats, default=all_formats)
with col_s:
    query = st.text_input("Search team, series, or venue", placeholder="e.g. Australia, Oval, T20 World Cup")


def matches_filters(pm):
    if chosen_formats and pm["format"] not in chosen_formats:
        return False
    if query:
        q = query.lower()
        haystack = " ".join([
            pm["series"], pm["team1_name"], pm["team2_name"],
            (pm["venue"] or {}).get("ground", ""), (pm["venue"] or {}).get("city", ""),
        ]).lower()
        if q not in haystack:
            return False
    return True


filtered = [m for m in matches if matches_filters(m)]

live_matches = [m for m in filtered if m["category"] == "live"]
upcoming_matches = [m for m in filtered if m["category"] == "upcoming"]
completed_matches = [m for m in filtered if m["category"] == "completed"]

# --------------------------------------------------------------------------
# Tabs
# --------------------------------------------------------------------------

tab_live, tab_upcoming, tab_completed, tab_all = st.tabs(
    [
        f"🔴 Live ({len(live_matches)})",
        f"📅 Upcoming ({len(upcoming_matches)})",
        f"✅ Completed ({len(completed_matches)})",
        f"🏏 All ({len(filtered)})",
    ]
)


def render_list(group, empty_text):
    if not group:
        st.markdown(f'<div class="pp-empty">{empty_text}</div>', unsafe_allow_html=True)
        return
    for pm in group:
        st.markdown(render_card(pm), unsafe_allow_html=True)


with tab_live:
    render_list(live_matches, "No live matches right now. Check back soon — or peek at Upcoming →")
with tab_upcoming:
    render_list(upcoming_matches, "Nothing scheduled in this filter.")
with tab_completed:
    render_list(completed_matches, "No completed matches in this filter.")
with tab_all:
    render_list(live_matches + upcoming_matches + completed_matches, "No matches match your filters.")
