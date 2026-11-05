# Criclane - production ingestion loop (runs as a Spark Python file task)
# Params come from sys.argv as key=value pairs (job task parameters).
#   run_seconds=0            run forever
#   fast_interval=5          fast tick seconds (live scores)
#   scorecard_interval=60    scorecard refresh seconds
#   slow_interval=600        schedule / news / points-table seconds
#   rankings_interval=3600   rankings refresh seconds
#   max_live_pages_per_cycle=4  live match pages fetched per tick
#   series_cap=12            max series for news / points-table fetches

import sys
import time
import json
from collections import deque

LIB_DIR = "/Workspace/Users/ritik69ritik@gmail.com/Cricbuzz ETL Pipeline"
if LIB_DIR not in sys.path:
    sys.path.insert(0, LIB_DIR)
import criclane_lib as cl

try:
    spark
except NameError:
    from databricks.sdk.runtime import spark as spark

DEFAULTS = {
    "run_seconds": 0.0,
    "fast_interval": 5.0,
    "scorecard_interval": 60.0,
    "slow_interval": 600.0,
    "rankings_interval": 3600.0,
    "max_live_pages_per_cycle": 4.0,
    "series_cap": 12.0,
}


def load_params(argv):
    p = dict(DEFAULTS)
    for a in argv:
        if "=" in a:
            k, v = a.split("=", 1)
            if k in p:
                p[k] = float(v)
    return p


def append(bronze, rows):
    cl.append_rows(spark, cl.bronze_table(bronze), rows, cl.BRONZE_SCHEMAS[bronze])


def ingest_home(hashes):
    # Fetch /api/home; write changed match payloads to bronze.
    # Returns (live_ids, recently_finished_ids, series_map).
    matches = cl.fetch_home_matches()
    rows, live, recent_done, series_map = [], [], [], {}
    for x in matches:
        info = (x.get("match") or {}).get("matchInfo") or {}
        mid = cl.to_int(info.get("matchId"))
        if mid is None:
            continue
        if info.get("seriesId"):
            series_map[cl.to_int(info.get("seriesId"))] = (
                info.get("seriesName") or "", cl.slugify(info.get("seriesName")))
        h = cl.content_hash(x)
        if hashes.get(("home", mid)) != h:
            rows.append({"match_id": mid, "series_id": cl.to_int(info.get("seriesId")),
                         "state": info.get("state"), "payload": json.dumps(x),
                         "payload_hash": h, "fetch_ts": cl.now_utc(), "processed": False})
            hashes[("home", mid)] = h
        st = info.get("state")
        if st == "In Progress":
            live.append(mid)
        elif st == "Complete" and cl.epoch_ms_to_ts(info.get("endDate")):
            end_ts = cl.epoch_ms_to_ts(info.get("endDate"))
            if (cl.now_utc() - end_ts).total_seconds() < 12 * 3600:
                recent_done.append(mid)
    if rows:
        append("home_matches", rows)
    return live, recent_done, series_map


def main(argv=None):
    P = load_params(sys.argv[1:] if argv is None else argv)
    print("Criclane ingest params:", P, flush=True)

    end_at = time.time() + P["run_seconds"] if P["run_seconds"] > 0 else None
    next_due = {"scorecard": 0.0, "slow": 0.0, "rankings": 0.0}
    hashes = {}          # (kind, id) -> content hash, skips unchanged payloads
    live_rr = deque()    # round-robin of live match ids
    consecutive_failures = 0
    cycle = 0

    print("Criclane poller starting - fast tick {0:.0f}s".format(P["fast_interval"]), flush=True)

    while True:
        cycle += 1
        t0 = time.time()
        if end_at and t0 >= end_at:
            print("Run window elapsed after {0} cycles. Exiting.".format(cycle - 1), flush=True)
            break

        stats = {}
        try:
            live, recent_done, series_map = ingest_home(hashes)
            consecutive_failures = 0

            # round-robin live match pages (miniscore + commentary)
            for mid in live:
                if mid not in live_rr:
                    live_rr.append(mid)
            for mid in list(live_rr):
                if mid not in live:
                    live_rr.remove(mid)
            for _ in range(min(int(P["max_live_pages_per_cycle"]), len(live_rr))):
                if not live_rr:
                    break
                mid = live_rr.popleft()
                data = cl.fetch_match_live(mid)
                if data and (data.get("miniscore") or data.get("commentary")):
                    h = cl.content_hash(data)
                    if hashes.get(("live", mid)) != h:
                        append("match_miniscore", [{"match_id": mid, "payload": json.dumps(data),
                                                    "payload_hash": h, "fetch_ts": cl.now_utc(),
                                                    "processed": False}])
                        hashes[("live", mid)] = h
                if mid in live:
                    live_rr.append(mid)

            # scorecards: live + recently finished
            if t0 >= next_due["scorecard"]:
                next_due["scorecard"] = t0 + P["scorecard_interval"]
                targets = list(dict.fromkeys(live + recent_done))[:8]
                for mid in targets:
                    sc = cl.fetch_scorecard(mid)
                    if sc:
                        h = cl.content_hash(sc)
                        if hashes.get(("scard", mid)) != h:
                            append("match_scorecard", [{"match_id": mid, "payload": json.dumps(sc),
                                                        "payload_hash": h, "fetch_ts": cl.now_utc(),
                                                        "processed": False}])
                            hashes[("scard", mid)] = h

            # slow loop: schedule, series news, points tables
            if t0 >= next_due["slow"]:
                next_due["slow"] = t0 + P["slow_interval"]
                for cat in cl.SCHEDULE_CATEGORIES:
                    try:
                        sch = cl.fetch_schedule(cat)
                        if sch:
                            h = cl.content_hash(sch)
                            if hashes.get(("sched", cat)) != h:
                                append("schedule_pages", [{"category": cat, "payload": json.dumps(sch),
                                                           "fetch_ts": cl.now_utc(), "processed": False}])
                                hashes[("sched", cat)] = h
                    except Exception as e:
                        cl.log_etl(spark, "schedule:{0}".format(cat), "ERROR", 0, str(e))
                for sid, (sname, slug) in list(series_map.items())[:int(P["series_cap"])]:
                    if not slug:
                        continue
                    try:
                        nw = cl.fetch_series_news(sid, slug)
                        if nw:
                            append("news_pages", [{"series_id": sid, "series_name": sname,
                                                   "payload": json.dumps(nw), "fetch_ts": cl.now_utc(),
                                                   "processed": False}])
                    except Exception:
                        pass
                    try:
                        pt = cl.fetch_points_table(sid, slug)
                        if pt and pt.get("pointsTable"):
                            h = cl.content_hash(pt)
                            if hashes.get(("pt", sid)) != h:
                                append("points_table_pages", [{"series_id": sid, "series_name": sname,
                                                               "payload": json.dumps(pt),
                                                               "fetch_ts": cl.now_utc(),
                                                               "processed": False}])
                                hashes[("pt", sid)] = h
                    except Exception:
                        pass

            # hourly: rankings
            if t0 >= next_due["rankings"]:
                next_due["rankings"] = t0 + P["rankings_interval"]
                for gender in cl.RANKING_GENDERS:
                    for cat in cl.RANKING_CATEGORIES:
                        try:
                            rk = cl.fetch_rankings(gender, cat)
                            if rk:
                                append("rankings_pages", [{"gender": gender, "category": cat,
                                                           "payload": json.dumps(rk),
                                                           "fetch_ts": cl.now_utc(),
                                                           "processed": False}])
                        except Exception:
                            pass

            # transforms: bronze -> silver -> gold, every cycle
            stats = cl.process_all_pending(spark)
            cl.refresh_gold(spark, stats)
            cl.update_pipeline_status(spark, "ingest_cycle",
                                      sum(v for v in stats.values() if v > 0))
            cl.log_etl(spark, "cycle:{0}".format(cycle), "OK",
                      sum(v for v in stats.values() if v > 0),
                      "live={0} pending={1}".format(len(live), stats))

        except Exception as e:
            consecutive_failures += 1
            cl.log_etl(spark, "cycle:{0}".format(cycle), "ERROR", 0, repr(e)[:450])
            print("cycle {0} ERROR: {1!r}".format(cycle, e), flush=True)
            if consecutive_failures >= 5:
                print("5 consecutive failures - backing off 60s", flush=True)
                time.sleep(60)
                consecutive_failures = 0

        elapsed = time.time() - t0
        if cycle % 10 == 1 or elapsed > P["fast_interval"]:
            print("cycle {0}: {1:.1f}s, live_pages={2}, stats={3}".format(
                cycle, elapsed, list(live_rr), stats), flush=True)
        time.sleep(max(0.5, P["fast_interval"] - elapsed))


if __name__ == "__main__":
    main()
