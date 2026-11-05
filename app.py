"""
Criclane — live cricket scores & stats, Cricbuzz-style.

A Streamlit app hosted as a Databricks App. It reads the gold consumption
layer (criclane.gold.*) that the Criclane ETL pipeline keeps fresh every few
seconds, so this UI is always showing near-real-time data.

Tabs: Home (live), Recent, Upcoming, Match Center (commentary / scorecard /
info), Points Table, Rankings, Series (schedule) and News.

Databricks Apps inject the SDK auth env vars automatically. The SQL warehouse
is picked up from env var CRICLANE_WAREHOUSE_ID.
"""

import os
from datetime import datetime, timezone

import streamlit as st
from databricks.sdk import WorkspaceClient

try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False

WAREHOUSE_ID = os.environ.get("CRICLANE_WAREHOUSE_ID", "a01e8fceeb00f98a")
CATALOG = "criclane"

st.set_page_config(page_title="Criclane — Live Cricket", page_icon="🏏", layout="wide")

# ---------------------------------------------------------------------------
# Data access (Databricks SQL statement execution, cached 5s)
# ---------------------------------------------------------------------------

@st.cache_resource
def get_client():
    return WorkspaceClient()


@st.cache_data(ttl=5, show_spinner=False)
def q(sql: str):
    w = get_client()
    res = w.statement_execution.execute_statement(
        statement=sql, warehouse_id=WAREHOUSE_ID, wait_timeout="30s"
    )
    state = getattr(res.status.state, "value", res.status.state)
    if state != "SUCCEEDED" or not res.manifest or not res.result:
        return []
    cols = [c.name for c in res.manifest.schema.columns]
    out = []
    for r in res.result.data_array or []:
        vals = []
        for f in r:
            v = getattr(f, "v", f)   # SDK versions differ: Row -> Column .v, or plain str
            if v == "NULL":
                v = None
            vals.append(v)
        out.append(dict(zip(cols, vals)))
    return out


def n(v, cast=float):
    try:
        return cast(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def ts(v):
    if v is None:
        return None
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00").replace(" ", "T"))
    except ValueError:
        try:
            return datetime.fromtimestamp(n(v, int) / 1000.0, tz=timezone.utc)
        except Exception:
            return None


def time_ago(v):
    d = ts(v)
    if not d:
        return ""
    if d.tzinfo is None:
        d = d.replace(tzinfo=timezone.utc)
    s = int((datetime.now(timezone.utc) - d).total_seconds())
    if s < 60:
        return f"{s}s ago"
    if s < 3600:
        return f"{s // 60}m ago"
    if s < 86400:
        return f"{s // 3600}h ago"
    return f"{s // 86400}d ago"


# ---------------------------------------------------------------------------
# Theme — stadium-at-dusk scoreboard
# ---------------------------------------------------------------------------

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Barlow+Condensed:wght@500;700;800&family=Space+Mono:wght@400;700&family=Inter:wght@400;500;600&display=swap');
    :root{ --bg-deep:#1A3A52; --panel:#2C5AA0; --panel-edge:#4A7BC4; --chalk:#FFF;
           --willow:#FFB800; --ball-red:#FF6B35; --floodlight:#FFD700; --muted:#B8C5D6; }
    html, body, [class*="css"]{ font-family:'Inter', sans-serif; }
    .stApp{ background:
        radial-gradient(circle at 20% -10%, #2C5AA0 0%, var(--bg-deep) 55%), var(--bg-deep); }
    section[data-testid="stSidebar"]{ background:#1A3A52; border-right:1px solid var(--panel-edge); }
    h1,h2,h3{ font-family:'Barlow Condensed', sans-serif; color:var(--chalk); }
    .stTabs [data-baseweb="tab-list"]{ gap:4px; border-bottom:2px solid var(--panel-edge); }
    .stTabs [data-baseweb="tab"]{ background:#1A3A52; color:var(--muted); border-radius:8px 8px 0 0;
        padding:6px 16px; font-weight:600; }
    .stTabs [aria-selected="true"]{ background:var(--panel); color:var(--chalk);
        border:1px solid var(--panel-edge); border-bottom:2px solid var(--ball-red); }
    div[data-testid="stDataFrame"]{ background:#14324a; border:1px solid var(--panel-edge); border-radius:10px; }
    .cl-card{ background:#14324a; border:1px solid var(--panel-edge); border-radius:12px;
        padding:14px 18px; margin-bottom:12px; }
    .cl-card:hover{ border-color:var(--floodlight); }
    .cl-eyebrow{ font-family:'Space Mono', monospace; font-size:11px; letter-spacing:.16em;
        color:var(--floodlight); text-transform:uppercase; }
    .cl-series{ color:var(--muted); font-size:13px; margin:2px 0 6px; }
    .cl-teams{ display:flex; justify-content:space-between; align-items:center; gap:10px; }
    .cl-team{ font-size:18px; font-weight:600; color:var(--chalk); }
    .cl-score{ font-family:'Space Mono', monospace; font-size:18px; color:var(--willow); }
    .cl-status{ color:var(--muted); font-size:13px; margin-top:6px; }
    .cl-live{ color:var(--ball-red); font-weight:700; }
    .cl-dot{ display:inline-block; width:7px; height:7px; border-radius:50%;
        background:var(--ball-red); margin-right:6px; animation:cl-pulse 1.4s infinite; }
    @keyframes cl-pulse{ 0%,100%{opacity:1} 50%{opacity:.25} }
    .cl-head{ display:flex; justify-content:space-between; align-items:flex-end;
        border-bottom:2px solid var(--panel-edge); padding-bottom:10px; margin-bottom:14px; }
    .cl-head h1{ font-size:44px; font-weight:800; margin:0; line-height:1; color:var(--chalk); }
    .cl-head .sub{ color:var(--muted); font-size:13px; margin-top:4px; }
    .cl-updated{ font-family:'Space Mono', monospace; color:var(--muted); font-size:12px; text-align:right; }
    .cl-updated b{ color:var(--chalk); }
    .cl-comm{ border-left:3px solid var(--panel-edge); padding:6px 12px; margin-bottom:6px;
        background:#14324a; border-radius:0 8px 8px 0; color:var(--chalk); font-size:14px; }
    .cl-comm .t{ color:var(--muted); font-size:11px; font-family:'Space Mono', monospace; }
    .cl-news{ border-left:4px solid var(--willow); padding:8px 14px; margin-bottom:10px;
        background:#14324a; border-radius:0 10px 10px 0; }
    .cl-news h4{ color:var(--chalk); margin:0 0 4px; font-size:15px; }
    .cl-news p{ color:var(--muted); font-size:13px; margin:0 0 4px; }
    .cl-news .meta{ color:var(--floodlight); font-size:11px; font-family:'Space Mono', monospace; }
    .cl-stat{ display:inline-block; background:#14324a; border:1px solid var(--panel-edge);
        border-radius:8px; padding:6px 12px; margin:2px 6px 2px 0; color:var(--chalk); font-size:13px; }
    .cl-stat b{ color:var(--willow); }
    .cl-badge{ background:var(--ball-red); color:#fff; border-radius:4px;
        padding:1px 7px; font-size:11px; font-weight:700; letter-spacing:.1em; }
    </style>
    """,
    unsafe_allow_html=True,
)


# ---------------------------------------------------------------------------
# Shared queries
# ---------------------------------------------------------------------------

def get_matches():
    rows = q(f"""
        SELECT m.match_id, m.series_id, m.series_name, m.match_desc, m.match_format,
               m.start_ts, m.end_ts, m.state, m.status, m.short_status, m.state_title,
               m.team1_id, m.team1_name, m.team1_sname, m.team1_score,
               m.team2_id, m.team2_name, m.team2_sname, m.team2_score,
               m.venue_ground, m.venue_city, m.venue_country, m.curr_bat_team_id,
               m.last_updated
        FROM {CATALOG}.gold.matches_latest m""")
    for r in rows:
        r["start_dt"] = ts(r.get("start_ts"))
        r["_sort"] = n(r.get("start_ts"), int) or 0
        live = r.get("state") == "In Progress"
        r["_rank"] = 0 if live else (1 if r.get("state") == "Complete" else 2)
    rows.sort(key=lambda r: (r["_rank"], r["_sort"]))
    return rows


def get_miniscores():
    rows = q(f"""
        SELECT s.match_id, s.bat_team_name, s.bat_runs, s.bat_wkts, s.overs,
               s.striker_name, s.striker_runs, s.striker_balls,
               s.nonstriker_name, s.nonstriker_runs, s.nonstriker_balls,
               s.bowler_name, s.bowler_overs, s.bowler_runs, s.bowler_wkts,
               s.crr, s.rrr, s.target, s.last_wicket, s.partnership,
               s.recent_overs, s.last_updated
        FROM {CATALOG}.gold.v_miniscores s""")
    return {r["match_id"]: r for r in rows if r.get("match_id")}


def get_freshness():
    rows = q(f"""
        SELECT source, last_success_ts, records, last_run_ts
        FROM {CATALOG}.gold.pipeline_status WHERE source = 'ingest_cycle'""")
    if not rows:
        return None, 0
    r = rows[0]
    d = ts(r.get("last_success_ts"))
    age = int((datetime.now(timezone.utc) - d).total_seconds()) if d else None
    return age, n(r.get("records"), int) or 0


# ---------------------------------------------------------------------------
# Render helpers
# ---------------------------------------------------------------------------

STATE_COLORS = {"In Progress": "cl-live", "Complete": "", "Preview": ""}


def match_card(m, mini=None, key=""):
    live = m.get("state") == "In Progress"
    badge = f"<span class='cl-badge'>LIVE</span> " if live else ""
    stats = ""
    if live and mini:
        bat = mini.get("bat_team_name") or ""
        parts = []
        if mini.get("crr") is not None:
            parts.append(f"CRR <b>{n(mini['crr']):.2f}</b>")
        if mini.get("rrr") is not None and (n(mini["rrr"], float) or 0) > 0:
            parts.append(f"RRR <b>{n(mini['rrr']):.2f}</b>")
        if mini.get("target"):
            parts.append(f"Target <b>{mini['target']}</b>")
        if parts:
            stats = f"<div class='cl-status'>{bat} &nbsp;·&nbsp; " + " &nbsp;·&nbsp; ".join(parts) + "</div>"
    st.markdown(f"""
        <div class='cl-card' id='mc-{m.get("match_id")}{key}'>
          <div class='cl-eyebrow'>{badge}{m.get("series_name") or ""} · {m.get("match_desc") or ""} · {m.get("match_format") or ""}</div>
          <div class='cl-teams'>
            <div>
              <div class='cl-team'>{'▶ ' if m.get('curr_bat_team_id') == m.get('team1_id') else ''}{m.get("team1_name") or ""}</div>
              <div class='cl-score'>{m.get("team1_score") or "—"}</div>
            </div>
            <div style='text-align:right'>
              <div class='cl-team'>{m.get("team2_name") or ""}{' ◀' if m.get('curr_bat_team_id') == m.get('team2_id') else ''}</div>
              <div class='cl-score'>{m.get("team2_score") or "—"}</div>
            </div>
          </div>
          <div class='cl-status {STATE_COLORS.get(m.get("state"), "")}'>{m.get("status") or m.get("short_status") or ""}</div>
          {stats}
          <div class='cl-status' style='color:var(--floodlight);font-size:11px'>
            {m.get("venue_ground") or ""}, {m.get("venue_city") or ""}</div>
        </div>
    """, unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------

if HAS_AUTOREFRESH:
    st_autorefresh(interval=5000, key="criclane_refresh")

fresh_age, fresh_recs = get_freshness()
fresh_txt = ("data " + (f"{fresh_age}s old" if fresh_age is not None else "fresh")) if fresh_age is not None else "waiting for pipeline"

st.markdown(f"""
    <div class='cl-head'>
      <div>
        <div class='cl-eyebrow'>🏏 LIVE CRICKET · POWERED BY DATABRICKS</div>
        <h1>Criclane</h1>
        <div class='sub'>Live scores · commentary · scorecards · points tables · rankings · news</div>
      </div>
      <div class='cl-updated'>
        <div><span class='cl-dot'></span><b>{fresh_txt}</b></div>
        <div>ingest records: {fresh_recs}</div>
      </div>
    </div>
""", unsafe_allow_html=True)

matches = get_matches()
minis = get_miniscores()
live_matches = [m for m in matches if m.get("state") == "In Progress"]
complete = [m for m in matches if m.get("state") == "Complete"]
upcoming = [m for m in matches if m.get("state") not in ("In Progress", "Complete")]

tab_home, tab_recent, tab_upcoming, tab_center, tab_points, tab_rank, tab_series, tab_news = st.tabs(
    ["🏠 Live", "📋 Recent", "📅 Upcoming", "🎯 Match Center", "🏆 Points Table", "📊 Rankings", "🗂 Series", "📰 News"]
)

with tab_home:
    if not live_matches:
        st.info("No live matches right now — check Upcoming for what's next.")
    for m in live_matches:
        match_card(m, minis.get(m.get("match_id")))
        if st.button("Match Center →", key=f"go_{m.get('match_id')}"):
            st.session_state["sel_match"] = m.get("match_id")
    if upcoming:
        st.markdown("#### Upcoming next")
        for m in upcoming[:4]:
            st.markdown(f"""
                <div class='cl-card'>
                  <div class='cl-eyebrow'>{m.get("series_name")} · {m.get("match_desc")}</div>
                  <div class='cl-teams'><div class='cl-team'>{m.get("team1_name")} vs {m.get("team2_name")}</div>
                  <div class='cl-status'>{m.get("status") or m.get("short_status")}</div></div>
                </div>
            """, unsafe_allow_html=True)

with tab_recent:
    if not complete:
        st.info("No recently completed matches in the current window.")
    for m in complete:
        match_card(m)

with tab_upcoming:
    rows = q(f"""
        SELECT match_id, series_name, match_desc, match_format, start_ts, end_ts,
               team1_name, team1_sname, team2_name, team2_sname,
               venue_ground, venue_city, venue_country, series_category
        FROM {CATALOG}.gold.v_schedule ORDER BY start_ts""")
    if not rows:
        st.info("Schedule is loading — the slow ingestion loop runs every 10 minutes.")
    for r in rows:
        d = ts(r.get("start_ts"))
        when = d.strftime("%a %d %b · %H:%M UTC") if d else (r.get("start_ts") or "")
        st.markdown(f"""
            <div class='cl-card'>
              <div class='cl-eyebrow'>{r.get("series_category") or ""} · {r.get("series_name")}</div>
              <div class='cl-teams'>
                <div class='cl-team'>{r.get("team1_name")} vs {r.get("team2_name")}
                  <span style='color:var(--muted);font-weight:400'> · {r.get("match_desc")} ({r.get("match_format")})</span></div>
                <div class='cl-status'>{when}</div>
              </div>
              <div class='cl-status'>{r.get("venue_ground")}, {r.get("venue_city")}</div>
            </div>
        """, unsafe_allow_html=True)

with tab_center:
    if not matches:
        st.info("No matches loaded yet.")
    else:
        choices = {f"{m.get('team1_sname') or ''} v {m.get('team2_sname') or ''} — {m.get('match_desc') or ''} ({m.get('state')})":
                   m.get("match_id") for m in matches}
        default_lbl = next((k for k, v in choices.items() if v == st.session_state.get("sel_match")), list(choices)[0])
        sel = st.selectbox("Match", list(choices), index=list(choices).index(default_lbl))
        mid = choices[sel]
        st.session_state["sel_match"] = mid
        m = next(x for x in matches if x.get("match_id") == mid)

        sub_live, sub_card, sub_info = st.tabs(["💬 Commentary", "🧾 Scorecard", "ℹ Info"])

        with sub_live:
            mini = minis.get(mid)
            if mini:
                cols = []
                if mini.get("crr") is not None:
                    cols.append(f"CRR <b>{n(mini['crr']):.2f}</b>")
                if mini.get("rrr") is not None and (n(mini.get("rrr"), float) or 0) > 0:
                    cols.append(f"RRR <b>{n(mini['rrr']):.2f}</b>")
                if mini.get("target"):
                    cols.append(f"Target <b>{mini['target']}</b>")
                if mini.get("partnership"):
                    cols.append(f"Partnership <b>{mini['partnership']}</b>")
                if mini.get("last_wicket") and mini["last_wicket"] not in ("None", ""):
                    cols.append(f"Last wkt <b>{mini['last_wicket']}</b>")
                st.markdown("<div>" + " &nbsp;·&nbsp; ".join(
                    f"<span class='cl-stat'>{c}</span>" for c in cols) + "</div>", unsafe_allow_html=True)
                s, ns, b = mini.get("striker_name"), mini.get("nonstriker_name"), mini.get("bowler_name")
                if s:
                    st.markdown(
                        f"<div><span class='cl-stat'>🏏 {s} <b>{mini.get('striker_runs') or 0} ({mini.get('striker_balls') or 0})</b>*</span> "
                        f"<span class='cl-stat'>{ns} <b>{mini.get('nonstriker_runs') or 0} ({mini.get('nonstriker_balls') or 0})</b></span> "
                        f"<span class='cl-stat'>⚡ {b} <b>{mini.get('bowler_overs') or 0} ov</b></span></div>",
                        unsafe_allow_html=True)
                if mini.get("recent_overs"):
                    st.markdown(f"<div class='cl-status' style='color:var(--floodlight)'>Recent: {mini['recent_overs']}</div>",
                                unsafe_allow_html=True)
            comms = q(f"""
                SELECT comm_ts, innings_id, comm_text, event, batsman_name, bowler_name
                FROM {CATALOG}.gold.commentary_latest
                WHERE match_id = {mid} ORDER BY comm_ts DESC""")
            if not comms:
                st.info("No commentary yet for this match.")
            for c in comms:
                d = ts(c.get("comm_ts")) or datetime.now(timezone.utc)
                tag = f"<span class='cl-badge'>{c['event'].upper()}</span> " if c.get("event") and c["event"] not in ("none", "commentary") else ""
                st.markdown(
                    f"<div class='cl-comm'>{tag}{c.get('comm_text') or ''}"
                    f"<div class='t'>innings {c.get('innings_id')} · {time_ago(d)}</div></div>",
                    unsafe_allow_html=True)

        with sub_card:
            innings = q(f"""
                SELECT innings_id, bat_team_name, bat_team_sname, runs, wickets, overs, extras
                FROM {CATALOG}.gold.v_match_innings WHERE match_id = {mid} ORDER BY innings_id""")
            for inn in innings:
                st.markdown(f"#### {inn.get('bat_team_name') or ''} — {inn.get('runs') or 0}/{inn.get('wickets') or 0} ({inn.get('overs') or 0} ov)")
                bat = q(f"""
                    SELECT bat_name, out_desc, runs, balls, fours, sixes, strike_rate
                    FROM {CATALOG}.gold.v_scorecard_batting
                    WHERE match_id = {mid} AND innings_id = {inn['innings_id']}
                    ORDER BY position""")
                if bat:
                    st.dataframe(
                        [{"Batter": b.get("bat_name"), "Out": b.get("out_desc") or "not out",
                          "R": n(b.get("runs"), int), "B": n(b.get("balls"), int),
                          "4s": n(b.get("fours"), int), "6s": n(b.get("sixes"), int),
                          "SR": n(b.get("strike_rate"))} for b in bat],
                        hide_index=True, use_container_width=True)
                bowl = q(f"""
                    SELECT bowler_name, overs, maidens, runs_conceded, wickets, economy
                    FROM {CATALOG}.gold.v_scorecard_bowling
                    WHERE match_id = {mid} AND innings_id = {inn['innings_id']}
                    ORDER BY position""")
                if bowl:
                    st.dataframe(
                        [{"Bowler": b.get("bowler_name"), "O": n(b.get("overs")),
                          "M": n(b.get("maidens"), int), "R": n(b.get("runs_conceded"), int),
                          "W": n(b.get("wickets"), int), "Econ": n(b.get("economy"))} for b in bowl],
                        hide_index=True, use_container_width=True)
            if not innings:
                st.info("Scorecard not loaded yet (refreshed every 60s for live matches).")

        with sub_info:
            st.markdown(f"""
                <div class='cl-card'>
                  <div class='cl-eyebrow'>{m.get('series_name')} · {m.get('match_desc')} · {m.get('match_format')}</div>
                  <div class='cl-teams'><div class='cl-team'>{m.get('team1_name')} vs {m.get('team2_name')}</div></div>
                  <div class='cl-status'>{m.get('venue_ground')}, {m.get('venue_city')}, {m.get('venue_country')}</div>
                  <div class='cl-status'>Start: {ts(m.get('start_ts')).strftime('%d %b %Y %H:%M UTC') if ts(m.get('start_ts')) else ''}</div>
                  <div class='cl-status'>{m.get('status')}</div>
                </div>
            """, unsafe_allow_html=True)

with tab_points:
    pts = q(f"""
        SELECT DISTINCT series_id, series_name FROM {CATALOG}.gold.v_points_tables
        ORDER BY series_name""")
    if not pts:
        st.info("No points tables loaded yet — the slow loop refreshes them every 10 minutes.")
    else:
        sel_series = st.selectbox("Series", [f"{p['series_name']} ({p['series_id']})" for p in pts])
        sid = pts[[f"{p['series_name']} ({p['series_id']})" for p in pts].index(sel_series)]["series_id"]
        rows = q(f"""
            SELECT table_name, position, team_name, team_sname, matches, won, lost,
                   tied, drawn, points, nrr
            FROM {CATALOG}.gold.v_points_tables WHERE series_id = {sid}
            ORDER BY table_name, position""")
        for group in sorted({r.get("table_name") or "" for r in rows}):
            st.markdown(f"#### {group}")
            grp = [r for r in rows if (r.get("table_name") or "") == group]
            st.dataframe(
                [{"#": n(r.get("position"), int), "Team": r.get("team_name"),
                  "P": n(r.get("matches"), int), "W": n(r.get("won"), int),
                  "L": n(r.get("lost"), int), "T": n(r.get("tied"), int),
                  "D": n(r.get("drawn"), int), "Pts": n(r.get("points"), int),
                  "NRR": n(r.get("nrr"))} for r in grp],
                hide_index=True, use_container_width=True)

with tab_rank:
    c1, c2, c3 = st.columns(3)
    gender = c1.selectbox("Gender", ["men", "women"])
    category = c2.selectbox("Category", ["batting", "bowling", "all-rounder", "teams"])
    fmt = c3.selectbox("Format", ["test", "odi", "t20"])
    rows = q(f"""
        SELECT rank, name, country, rating, points, trend
        FROM {CATALOG}.gold.v_rankings
        WHERE gender = '{gender}' AND category = '{category}' AND format = '{fmt}'
        ORDER BY rank""")
    if rows:
        st.dataframe(
            [{"Rank": n(r.get("rank"), int), "Name": r.get("name"), "Country": r.get("country"),
              "Rating": n(r.get("rating"), int), "Points": n(r.get("points"), int),
              "Trend": r.get("trend") or "—"} for r in rows],
            hide_index=True, use_container_width=True)
    else:
        st.info(f"No {gender}/{category}/{fmt} rankings loaded yet.")

with tab_series:
    rows = q(f"""
        SELECT series_id, series_name, series_slug, match_type,
               series_start_ts, series_end_ts
        FROM {CATALOG}.gold.v_series ORDER BY series_name""")
    for r in rows:
        s, e = ts(r.get("series_start_ts")), ts(r.get("series_end_ts"))
        window = ""
        if s and e:
            window = f"{s.strftime('%d %b %Y')} – {e.strftime('%d %b %Y')}"
        st.markdown(f"""
            <div class='cl-card'>
              <div class='cl-eyebrow'>{r.get('match_type') or ''}</div>
              <div class='cl-team'>{r.get('series_name')}</div>
              <div class='cl-status'>{window}</div>
            </div>
        """, unsafe_allow_html=True)

with tab_news:
    rows = q(f"""
        SELECT story_id, headline, intro, pub_ts, source, story_type, context
        FROM {CATALOG}.gold.v_news ORDER BY pub_ts DESC""")
    if not rows:
        st.info("No news loaded yet.")
    for r in rows:
        st.markdown(f"""
            <div class='cl-news'>
              <h4>{r.get('headline')}</h4>
              <p>{r.get('intro') or ''}</p>
              <div class='meta'>{r.get('story_type') or ''} · {r.get('context') or ''} ·
                {time_ago(r.get('pub_ts'))} · {r.get('source') or ''}</div>
            </div>
        """, unsafe_allow_html=True)
