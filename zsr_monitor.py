#!/usr/bin/env python3
"""
ZSR vlaky - monitoring meskani a polohy (neoficialne API mapa.zsr.sk).

Pouzitie:
  python zsr_monitor.py collect               # jeden snimok do SQLite
  python zsr_monitor.py run --interval 60     # slucka: zber + upozornenia (+ volitelne publikovanie)
  python zsr_monitor.py report --hours 24     # markdown report -> docs/report.md
  python zsr_monitor.py map                   # mapa + grafy (zivy snimok) -> docs/index.html
  python zsr_monitor.py publish               # report + mapa + git commit + push
"""
import argparse
import json
import logging
import os
import sqlite3
import statistics
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests

BASE = "https://mapa.zsr.sk"
API = BASE + "/api/action"
TZ = ZoneInfo("Europe/Bratislava")
UTC = timezone.utc
log = logging.getLogger("zsr")


# --------------------------------------------------------------------------
# Konfiguracia (.env alebo premenne prostredia)
# --------------------------------------------------------------------------
def load_env(path=".env"):
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def cfg(name, default=""):
    return os.environ.get(name, default)


# --------------------------------------------------------------------------
# Stahovanie dat
# --------------------------------------------------------------------------
def make_session():
    s = requests.Session()
    s.headers.update({
        "Accept": "*/*",
        "Accept-Language": "sk,en;q=0.8",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": BASE,
        "Referer": BASE + "/index.aspx",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) zsr-monitor/1.0",
        "X-Requested-With": "XMLHttpRequest",
    })
    try:  # ziskanie cerstvych cookies (session) automaticky
        s.get(BASE + "/index.aspx", timeout=20)
    except requests.RequestException as e:
        log.warning("Nacitanie index.aspx zlyhalo: %s", e)
    return s


def fetch_trains(session=None):
    """Vrati (zoznam_vlakov, session). Pri chybe obnovi session a skusi znova."""
    session = session or make_session()
    for _ in range(3):
        try:
            r = session.post(API, data={"action": "gtall"}, timeout=30)
            if r.status_code == 200:
                data = r.json()
                if isinstance(data, list):
                    return data, session
            log.warning("Neocakavana odpoved: HTTP %s", r.status_code)
        except (requests.RequestException, ValueError) as e:
            log.warning("Chyba pri stahovani: %s", e)
        time.sleep(3)
        session = make_session()
    raise RuntimeError("API nedostupne")


# --------------------------------------------------------------------------
# Databaza
# --------------------------------------------------------------------------
COLS = ["train_key", "typ", "cislo", "nazov_vlaku", "nazov", "dopravca", "delay",
        "delay_origin", "delay_origin_station", "info_station", "next_station",
        "lat", "lon", "angle", "disruption", "disruption_code", "nad_od", "nad_do",
        "poznamka", "cas", "cas_plan", "order_in_station", "confirmed_dep"]

SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshots(
  id INTEGER PRIMARY KEY AUTOINCREMENT, ts TEXT NOT NULL, n_trains INTEGER);
CREATE TABLE IF NOT EXISTS observations(
  id INTEGER PRIMARY KEY AUTOINCREMENT, snapshot_id INTEGER NOT NULL, ts TEXT NOT NULL,
  train_key TEXT, typ TEXT, cislo TEXT, nazov_vlaku TEXT, nazov TEXT, dopravca TEXT,
  delay INTEGER, delay_origin INTEGER, delay_origin_station TEXT, info_station TEXT,
  next_station TEXT, lat REAL, lon REAL, angle INTEGER, disruption INTEGER,
  disruption_code INTEGER, nad_od TEXT, nad_do TEXT, poznamka TEXT, cas TEXT,
  cas_plan TEXT, order_in_station INTEGER, confirmed_dep INTEGER);
CREATE INDEX IF NOT EXISTS ix_obs_key_ts ON observations(train_key, ts);
CREATE INDEX IF NOT EXISTS ix_obs_ts ON observations(ts);
"""


def connect(path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA)
    return conn


def to_record(t):
    pos = t.get("Position") or [None, None]
    typ = (t.get("TypVlaku") or "").strip()
    cislo = str(t.get("CisloVlaku") or "").strip()
    return {
        "train_key": f"{typ} {cislo}",
        "typ": typ, "cislo": cislo,
        "nazov_vlaku": (t.get("NazovVlaku") or "").strip(),
        "nazov": (t.get("Nazov") or "").strip(),
        "dopravca": (t.get("Dopravca") or "").strip(),
        "delay": t.get("Meska"),
        "delay_origin": t.get("MeskaSkutocne"),
        "delay_origin_station": t.get("MeskaSkutocneZoStanice"),
        "info_station": t.get("InfoZoStanice"),
        "next_station": t.get("NextStation"),
        "lat": pos[0], "lon": pos[1],
        "angle": t.get("Angle"),
        "disruption": int(bool(t.get("Narusenie"))),
        "disruption_code": t.get("NarusenieKod"),
        "nad_od": t.get("NadOd"), "nad_do": t.get("NadDo"),
        "poznamka": t.get("Poznamka"),
        "cas": t.get("Cas"), "cas_plan": t.get("CasPlan"),
        "order_in_station": t.get("OrderInStation"),
        "confirmed_dep": int(bool(t.get("PotvrdenyOdj"))),
    }


def now_iso():
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S")


def store_snapshot(conn, trains):
    ts = now_iso()
    cur = conn.execute("INSERT INTO snapshots(ts, n_trains) VALUES(?,?)", (ts, len(trains)))
    sid = cur.lastrowid
    sql = (f"INSERT INTO observations(snapshot_id, ts, {','.join(COLS)}) "
           f"VALUES(:snapshot_id, :ts, {','.join(':' + c for c in COLS)})")
    rows = []
    for t in trains:
        r = to_record(t)
        r.update(snapshot_id=sid, ts=ts)
        rows.append(r)
    conn.executemany(sql, rows)
    conn.commit()
    return sid, ts


def prune(conn, keep_days):
    cutoff = (datetime.now(UTC) - timedelta(days=keep_days)).strftime("%Y-%m-%dT%H:%M:%S")
    conn.execute("DELETE FROM observations WHERE ts < ?", (cutoff,))
    conn.execute("DELETE FROM snapshots WHERE ts < ?", (cutoff,))
    conn.commit()


# --------------------------------------------------------------------------
# Upozornenia (Telegram alebo konzola)
# --------------------------------------------------------------------------
_alert_state = {}


def send_alert(text):
    token, chat = cfg("TELEGRAM_TOKEN"), cfg("TELEGRAM_CHAT_ID")
    print("ALERT:", text)
    if token and chat:
        try:
            requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                          data={"chat_id": chat, "text": text}, timeout=15)
        except requests.RequestException as e:
            log.warning("Telegram zlyhal: %s", e)


def check_alerts(trains):
    """WATCH=TATRAN,Ex 603,Os 2009  ALERT_DELAY=10  ALERT_STEP=5"""
    watch = [w.strip().lower() for w in cfg("WATCH").split(",") if w.strip()]
    if not watch:
        return
    threshold = int(cfg("ALERT_DELAY", "10"))
    step = int(cfg("ALERT_STEP", "5"))
    for t in trains:
        r = to_record(t)
        hay = f"{r['train_key']} {r['nazov']} {r['nazov_vlaku']}".lower()
        if not any(w in hay for w in watch):
            continue
        key, d = r["train_key"], r["delay"] or 0
        prev = _alert_state.get(key, {"delay": 0, "disr": 0})
        if d >= threshold and d - prev["delay"] >= step:
            send_alert(f"{r['nazov']}: meskanie {d} min (stanica: {r['info_station']})")
            prev["delay"] = d
        elif d < threshold:
            prev["delay"] = 0
        if r["disruption"] and not prev["disr"]:
            send_alert(f"{r['nazov']}: narusenie prevadzky (kod {r['disruption_code']})")
        prev["disr"] = r["disruption"]
        _alert_state[key] = prev


# --------------------------------------------------------------------------
# Report
# --------------------------------------------------------------------------
def md_table(headers, rows):
    if not rows:
        return "_ziadne data_\n"
    out = ["| " + " | ".join(headers) + " |", "|" + "---|" * len(headers)]
    out += ["| " + " | ".join("" if c is None else str(c) for c in r) + " |" for r in rows]
    return "\n".join(out) + "\n"


def stats(vals):
    vals = [v for v in vals if v is not None]
    if not vals:
        return None
    n = len(vals)
    return {"n": n, "avg": round(sum(vals) / n, 1), "med": statistics.median(vals),
            "p5": round(100 * sum(v > 5 for v in vals) / n), "p10": round(100 * sum(v > 10 for v in vals) / n)}


def get_runs(conn, since):
    """Zluci pozorovania do 'jazd' (rovnaky vlak, medzera medzi pozorovaniami < 2 h)."""
    rows = conn.execute(
        "SELECT train_key, nazov, dopravca, ts, delay, disruption FROM observations "
        "WHERE ts >= ? ORDER BY train_key, ts", (since,)).fetchall()
    runs, cur = [], None
    for r in rows:
        t = datetime.fromisoformat(r["ts"])
        if (cur is None or cur["key"] != r["train_key"] or t - cur["last_t"] > timedelta(hours=2)):
            cur = {"key": r["train_key"], "name": r["nazov"], "carrier": r["dopravca"],
                   "first": r["delay"] or 0, "last": r["delay"] or 0, "max": r["delay"] or 0,
                   "n": 0, "disr": 0, "last_t": t}
            runs.append(cur)
        d = r["delay"] or 0
        cur.update(last=d, last_t=t, max=max(cur["max"], d))
        cur["n"] += 1
        cur["disr"] += r["disruption"] or 0
    return runs


def stationary_trains(conn, latest_ts, min_minutes):
    """Vlaky, ktorych Position sa nemenila >= min_minutes (poloha = posledna stanica, preto len priblizne)."""
    latest = conn.execute("SELECT train_key, nazov, lat, lon, cas, delay FROM observations WHERE ts=?",
                          (latest_ts,)).fetchall()
    t_latest = datetime.fromisoformat(latest_ts)
    out = []
    for r in latest:
        if r["cas"] == "---":
            continue  # caka na odchod
        hist = conn.execute("SELECT ts, lat, lon FROM observations WHERE train_key=? AND ts<=? "
                            "ORDER BY ts DESC LIMIT 120", (r["train_key"], latest_ts)).fetchall()
        since = t_latest
        for h in hist:
            if h["lat"] != r["lat"] or h["lon"] != r["lon"]:
                break
            since = datetime.fromisoformat(h["ts"])
        mins = int((t_latest - since).total_seconds() // 60)
        if mins >= min_minutes:
            out.append((r["nazov"], mins, r["delay"]))
    return sorted(out, key=lambda x: -x[1])


def build_report(conn, hours, stuck_minutes=20):
    snap = conn.execute("SELECT id, ts FROM snapshots ORDER BY id DESC LIMIT 1").fetchone()
    if not snap:
        return "# ZSR report\n\nZatial ziadne data. Spusti `collect` alebo `run`.\n"
    since = (datetime.now(UTC) - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%S")
    latest = conn.execute("SELECT * FROM observations WHERE snapshot_id=?", (snap["id"],)).fetchall()
    local = datetime.fromisoformat(snap["ts"]).replace(tzinfo=UTC).astimezone(TZ)
    n_snaps = conn.execute("SELECT COUNT(*) FROM snapshots WHERE ts>=?", (since,)).fetchone()[0]
    md = [f"# Prevadzka ZSR - report\n",
          f"Posledny snimok: **{local:%d.%m.%Y %H:%M}** (Europe/Bratislava), "
          f"obdobie: poslednych {hours} h, snimkov v obdobi: {n_snaps}\n"]

    # 1. aktualny stav
    s = stats([r["delay"] for r in latest])
    md.append("## Aktualny stav\n")
    md.append(md_table(["Vlakov", "Priemer (min)", "Median", "Meskanie >5 min", "Meskanie >10 min", "Narusenia"],
                       [[s["n"], s["avg"], s["med"], f"{s['p5']} %", f"{s['p10']} %",
                         sum(r["disruption"] for r in latest)]]))
    top = sorted(latest, key=lambda r: -(r["delay"] or 0))[:10]
    md.append("### Najviac meskajuce teraz\n")
    md.append(md_table(["Vlak", "Meskanie", "Stanica", "Dopravca"],
                       [[r["nazov"], f"{r['delay']} min", r["info_station"], r["dopravca"]] for r in top]))

    # 2. podla kategorie a dopravcu (cez cele obdobie)
    for title, col in (("Podla kategorie vlaku", "typ"), ("Podla dopravcu", "dopravca")):
        rows = conn.execute(
            f"SELECT {col} AS k, COUNT(*) AS n, AVG(delay) AS a, "
            f"100.0*SUM(delay>5)/COUNT(*) AS p5, 100.0*SUM(delay>10)/COUNT(*) AS p10 "
            f"FROM observations WHERE ts>=? AND delay IS NOT NULL GROUP BY {col} ORDER BY a DESC",
            (since,)).fetchall()
        md.append(f"## {title}\n")
        md.append(md_table(["", "Pozorovani", "Priem. meskanie", ">5 min", ">10 min"],
                           [[r["k"], r["n"], f"{r['a']:.1f}", f"{r['p5']:.0f} %", f"{r['p10']:.0f} %"] for r in rows]))

    # 3. podla hodiny dna
    byhour = defaultdict(list)
    for r in conn.execute("SELECT ts, delay FROM observations WHERE ts>=? AND delay IS NOT NULL", (since,)):
        h = datetime.fromisoformat(r["ts"]).replace(tzinfo=UTC).astimezone(TZ).hour
        byhour[h].append(r["delay"])
    md.append("## Podla hodiny dna\n")
    md.append(md_table(["Hodina", "Priem. meskanie", ">5 min"],
                       [[f"{h:02d}:00", stats(v)["avg"], f"{stats(v)['p5']} %"] for h, v in sorted(byhour.items())]))

    # 4. jazdy: najhorsie, zhorsujuce sa, narusenia
    runs = get_runs(conn, since)
    md.append("## Najvacsie meskania v obdobi\n")
    md.append(md_table(["Vlak", "Max meskanie", "Pozorovani"],
                       [[r["name"], f"{r['max']} min", r["n"]] for r in sorted(runs, key=lambda r: -r["max"])[:10]]))
    worse = [r for r in runs if r["n"] >= 3]
    worse.sort(key=lambda r: r["last"] - r["first"], reverse=True)
    md.append("## Vlaky, ktorym meskanie najviac narastlo (zaciatok -> koniec)\n")
    md.append(md_table(["Vlak", "Zaciatok", "Koniec", "Zmena"],
                       [[r["name"], r["first"], r["last"], f"{r['last'] - r['first']:+d}"] for r in worse[:10]]))
    better = sorted(worse, key=lambda r: r["last"] - r["first"])[:5]
    md.append("### Vlaky, ktore meskanie najviac dobehli\n")
    md.append(md_table(["Vlak", "Zaciatok", "Koniec", "Zmena"],
                       [[r["name"], r["first"], r["last"], f"{r['last'] - r['first']:+d}"] for r in better]))
    disr = sorted([r for r in runs if r["disr"]], key=lambda r: -r["disr"])
    md.append("## Vlaky s narusenim prevadzky v obdobi\n")
    md.append(md_table(["Vlak", "Pozorovani s narusenim"], [[r["name"], r["disr"]] for r in disr[:15]]))

    # 5. NAD (nahradna autobusova doprava) - aktualne
    nad = [[r["nazov"], r["nad_od"], r["nad_do"]] for r in latest if r["nad_od"] or r["nad_do"]]
    md.append("## Nahradna autobusova doprava (aktualne)\n")
    md.append(md_table(["Vlak", "Od", "Do"], nad))
    whole = [r["nazov"] for r in latest if "n\u00e1hradn\u00fa autobusov\u00fa" in (r["poznamka"] or "")]
    if whole:
        md.append("Vlaky s NAD v celej trase: " + ", ".join(whole) + "\n")

    # 6. stanice
    st = defaultdict(int)
    for r in latest:
        st[r["info_station"]] += 1
    md.append("## Najviac vlakov pri jednej stanici (teraz)\n")
    md.append(md_table(["Stanica", "Vlakov"], sorted(st.items(), key=lambda x: -x[1])[:10]))

    # 7. stojace vlaky
    stuck = stationary_trains(conn, snap["ts"], stuck_minutes)
    md.append(f"## Vlaky bez zmeny polohy >= {stuck_minutes} min\n")
    md.append("_Poloha v API je posledna prejdena stanica, preto ide len o orientacny ukazovatel._\n")
    md.append(md_table(["Vlak", "Minut na mieste", "Meskanie"], stuck[:15]))
    return "\n".join(md)


# --------------------------------------------------------------------------
# Dashboard (data pre grafy)
# --------------------------------------------------------------------------
REPO_URL = cfg("ZSR_REPO_URL", "https://github.com/lalasonycierny/idos-mapazsr")
REPORT_URL = cfg("ZSR_REPORT_URL", REPO_URL + "/blob/main/docs/report.md")


def _local(dt_utc):
    return dt_utc.replace(tzinfo=UTC).astimezone(TZ)


def group_stats(conn, col, since):
    rows = conn.execute(
        f"SELECT {col} AS k, COUNT(*) AS n, AVG(delay) AS a, 100.0*SUM(delay>5)/COUNT(*) AS p5, "
        f"SUM(delay<=0) AS b0, SUM(delay BETWEEN 1 AND 5) AS b1, SUM(delay BETWEEN 6 AND 14) AS b2, "
        f"SUM(delay>=15) AS b3 FROM observations WHERE ts>=? AND delay IS NOT NULL "
        f"GROUP BY {col} ORDER BY a DESC", (since,)).fetchall()
    out = {"labels": [], "avg": [], "p5": [], "n": [], "b": [[], [], [], []]}
    for r in rows:
        if not r["k"]:
            continue
        out["labels"].append(str(r["k"])[:30])
        out["avg"].append(round(r["a"], 1))
        out["p5"].append(round(r["p5"]))
        out["n"].append(r["n"])
        for i in range(4):
            out["b"][i].append(round(100.0 * r[f"b{i}"] / r["n"], 1))
    return out


def dashboard_data(conn, hours, stuck_minutes=20):
    snap = conn.execute("SELECT id, ts FROM snapshots ORDER BY id DESC LIMIT 1").fetchone()
    if not snap:
        return None
    since = (datetime.now(UTC) - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%S")
    latest = conn.execute("SELECT * FROM observations WHERE snapshot_id=?", (snap["id"],)).fetchall()
    delays = [r["delay"] for r in latest if r["delay"] is not None]
    s = stats(delays) or {"n": 0, "avg": 0, "med": 0, "p5": 0, "p10": 0}
    nad_whole = [r["nazov"] for r in latest if "náhradnú autobusovú" in (r["poznamka"] or "")]
    first = conn.execute("SELECT MIN(ts) FROM snapshots WHERE ts>=?", (since,)).fetchone()[0]
    n_snaps = conn.execute("SELECT COUNT(*) FROM snapshots WHERE ts>=?", (since,)).fetchone()[0]
    d = {"hours": hours, "n_snaps": n_snaps,
         "first": _local(datetime.fromisoformat(first)).strftime("%d.%m.%Y %H:%M") if first else "",
         "last": _local(datetime.fromisoformat(snap["ts"])).strftime("%d.%m.%Y %H:%M"),
         "kpi": {"n": s["n"], "avg": s["avg"], "med": s["med"], "p5": s["p5"], "p10": s["p10"],
                 "disr": sum(r["disruption"] or 0 for r in latest),
                 "nad": sum(1 for r in latest if r["nad_od"] or r["nad_do"]) + len(nad_whole)}}

    # casovy priebeh (10-minutove okna)
    ser = {"labels": [], "avg": [], "p5": [], "n": []}
    for r in conn.execute(
            "SELECT substr(ts,1,15) AS b, COUNT(*) AS n, AVG(delay) AS a, 100.0*SUM(delay>5)/COUNT(*) AS p5, "
            "COUNT(DISTINCT snapshot_id) AS s FROM observations WHERE ts>=? AND delay IS NOT NULL "
            "GROUP BY b ORDER BY b", (since,)):
        t = _local(datetime.fromisoformat(r["b"] + "0:00"))
        ser["labels"].append(t.strftime("%d.%m %H:%M"))
        ser["avg"].append(round(r["a"], 1))
        ser["p5"].append(round(r["p5"]))
        ser["n"].append(round(r["n"] / r["s"]))
    d["series"] = ser

    # histogram aktualneho stavu
    edges = [("náskok", lambda x: x < 0), ("0 (včas)", lambda x: x == 0), ("1–2", lambda x: 1 <= x <= 2),
             ("3–5", lambda x: 3 <= x <= 5), ("6–10", lambda x: 6 <= x <= 10),
             ("11–15", lambda x: 11 <= x <= 15), ("16–30", lambda x: 16 <= x <= 30),
             ("30+", lambda x: x > 30)]
    d["hist"] = {"labels": [e[0] for e in edges], "values": [sum(1 for x in delays if e[1](x)) for e in edges]}

    d["typ"] = group_stats(conn, "typ", since)
    d["carrier"] = group_stats(conn, "dopravca", since)

    # hodina dna + heatmapa den x hodina
    hs, hc = [0.0] * 24, [0] * 24
    hp = [0] * 24
    hm_s = [[0.0] * 24 for _ in range(7)]
    hm_c = [[0] * 24 for _ in range(7)]
    for r in conn.execute("SELECT ts, delay FROM observations WHERE ts>=? AND delay IS NOT NULL", (since,)):
        t = _local(datetime.fromisoformat(r["ts"]))
        h, dd = t.hour, r["delay"]
        hs[h] += dd
        hc[h] += 1
        hp[h] += dd > 5
        hm_s[t.weekday()][h] += dd
        hm_c[t.weekday()][h] += 1
    d["hour"] = {"avg": [round(hs[h] / hc[h], 1) if hc[h] else None for h in range(24)],
                 "p5": [round(100 * hp[h] / hc[h]) if hc[h] else None for h in range(24)],
                 "n": hc}
    d["heat"] = [[round(hm_s[w][h] / hm_c[w][h], 1) if hm_c[w][h] else None for h in range(24)]
                 for w in range(7)]

    # vlaky
    top = sorted(latest, key=lambda r: -(r["delay"] or 0))[:12]
    d["top_now"] = [{"name": r["nazov"], "v": r["delay"] or 0, "st": r["info_station"]} for r in top]
    runs = get_runs(conn, since)
    d["top_max"] = [{"name": r["name"], "v": r["max"]} for r in sorted(runs, key=lambda r: -r["max"])[:12]]
    multi = [r for r in runs if r["n"] >= 3]
    ch = sorted(multi, key=lambda r: r["last"] - r["first"])
    d["worse"] = [{"name": r["name"], "v": r["last"] - r["first"]} for r in reversed(ch[-8:])]
    d["better"] = [{"name": r["name"], "v": r["last"] - r["first"]} for r in ch[:8]]
    d["disr"] = [{"name": r["name"], "n": r["disr"]} for r in
                 sorted([r for r in runs if r["disr"]], key=lambda r: -r["disr"])[:15]]
    d["nad"] = [{"name": r["nazov"], "od": r["nad_od"] or "", "do": r["nad_do"] or ""}
                for r in latest if r["nad_od"] or r["nad_do"]]
    d["nad_whole"] = nad_whole

    # stanice
    st = defaultdict(int)
    for r in latest:
        st[r["info_station"]] += 1
    d["st_now"] = [{"name": k, "n": v} for k, v in sorted(st.items(), key=lambda x: -x[1])[:10]]
    d["st_delay"] = [{"name": r["k"], "avg": round(r["a"], 1), "n": r["n"]} for r in conn.execute(
        "SELECT info_station AS k, COUNT(*) AS n, AVG(delay) AS a FROM observations WHERE ts>=? "
        "AND delay IS NOT NULL AND info_station IS NOT NULL GROUP BY info_station HAVING COUNT(*)>=3 "
        "ORDER BY a DESC LIMIT 10", (since,))]
    d["stuck"] = [{"name": n, "min": m, "delay": dl} for n, m, dl in
                  stationary_trains(conn, snap["ts"], stuck_minutes)[:15]]
    return d


# --------------------------------------------------------------------------
# Stranka: mapa + grafy (jeden HTML subor)
# --------------------------------------------------------------------------
PAGE_HTML = r"""<!DOCTYPE html>
<html lang="sk"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Vlaky ŽSR – mapa a štatistiky</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<style>
:root{--bg:#f4f6f8;--card:#fff;--text:#1d2733;--muted:#5f6b7a;--grid:#e1e6ec;--accent:#0b6bcb}
@media (prefers-color-scheme:dark){:root{--bg:#0f141a;--card:#18202a;--text:#e6ebf1;--muted:#97a3b3;--grid:#2a3441;--accent:#5aa9ff}}
*{box-sizing:border-box}
body{margin:0;font-family:system-ui,-apple-system,Segoe UI,sans-serif;background:var(--bg);color:var(--text)}
header{padding:14px 20px;display:flex;flex-wrap:wrap;gap:10px 24px;align-items:center;justify-content:space-between}
header h1{margin:0;font-size:20px}
.links a{color:var(--accent);text-decoration:none;margin-right:16px;font-weight:600}
.links a:hover{text-decoration:underline}
.muted{color:var(--muted);font-size:13px}
main{padding:0 20px 40px;max-width:1400px;margin:0 auto}
#map{height:520px;border-radius:10px;border:1px solid var(--grid)}
.maptools{margin:8px 0 18px;font-size:14px}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin:18px 0}
.kpi{background:var(--card);border:1px solid var(--grid);border-radius:10px;padding:12px 14px}
.kpi b{display:block;font-size:26px;margin-top:2px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(420px,1fr));gap:16px}
.card{background:var(--card);border:1px solid var(--grid);border-radius:10px;padding:14px}
.card.wide{grid-column:1/-1}
.card h2{margin:0 0 4px;font-size:16px}
.card p.muted{margin:0 0 10px}
.chart{position:relative;height:300px}
.chart.tall{height:380px}
table{border-collapse:collapse;width:100%;font-size:13px}
th,td{padding:5px 8px;border-bottom:1px solid var(--grid);text-align:left}
th{color:var(--muted);font-weight:600}
.heat{overflow-x:auto}
.heat table{font-size:11px;min-width:760px}
.heat td,.heat th{text-align:center;padding:4px 2px;border:1px solid var(--bg)}
.legend span{margin-right:12px;white-space:nowrap}
.dot{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:5px}
nav a{color:var(--muted);margin-right:12px;font-size:13px;text-decoration:none}
nav a:hover{color:var(--accent)}
@media (max-width:520px){.grid{grid-template-columns:1fr}}
</style></head><body>
<header>
 <div><h1>🚆 Vlaky ŽSR – mapa a štatistiky</h1>
 <span class="muted">Snímok mapy: __TS__</span></div>
 <div class="links">
  <a href="__REPORT_URL__" target="_blank" rel="noopener">📄 Textový report</a>
  <a href="__REPO_URL__" target="_blank" rel="noopener">⭐ GitHub repozitár</a>
 </div>
</header>
<main>
<div id="map"></div>
<div class="maptools legend">
 <span><i class="dot" style="background:#2e7d32"></i>včas / náskok</span>
 <span><i class="dot" style="background:#f9a825"></i>1–5 min</span>
 <span><i class="dot" style="background:#ef6c00"></i>6–14 min</span>
 <span><i class="dot" style="background:#b71c1c"></i>15+ min</span>
 &nbsp; Min. meškanie:
 <select id="flt"><option value="-99">všetky</option><option value="1">1+</option>
 <option value="5">5+</option><option value="10">10+</option><option value="20">20+</option></select>
 &nbsp; <span id="cnt" class="muted"></span>
</div>

<div id="dash" style="display:none">
<nav>
 <a href="#s-kpi">Prehľad</a><a href="#s-time">Vývoj</a><a href="#s-cat">Kategórie a dopravcovia</a>
 <a href="#s-hour">Hodiny</a><a href="#s-trains">Vlaky</a><a href="#s-st">Stanice</a><a href="#s-prob">Problémy</a>
</nav>
<p class="muted" id="period"></p>
<div class="kpis" id="s-kpi"></div>
<div class="grid">
 <div class="card wide" id="s-time"><h2>Vývoj meškania v čase</h2>
  <p class="muted">Priemerné meškanie a podiel vlakov s meškaním nad 5 min (10-minútové okná)</p>
  <div class="chart tall"><canvas id="c-series"></canvas></div></div>
 <div class="card"><h2>Počet vlakov v sieti</h2>
  <p class="muted">Priemerný počet vlakov v jednom snímku</p>
  <div class="chart"><canvas id="c-count"></canvas></div></div>
 <div class="card"><h2>Rozdelenie meškaní teraz</h2>
  <p class="muted">Počet vlakov podľa veľkosti meškania (posledný snímok)</p>
  <div class="chart"><canvas id="c-hist"></canvas></div></div>

 <div class="card" id="s-cat"><h2>Priemerné meškanie podľa kategórie</h2>
  <p class="muted">Os, REX, R, Ex, EC, RJ, EN …</p>
  <div class="chart"><canvas id="c-typ"></canvas></div></div>
 <div class="card"><h2>Štruktúra meškaní podľa kategórie</h2>
  <p class="muted">Podiel pozorovaní v pásmach meškania (%)</p>
  <div class="chart"><canvas id="c-typb"></canvas></div></div>
 <div class="card"><h2>Priemerné meškanie podľa dopravcu</h2>
  <p class="muted">ZSSK, RegioJet, Leo Express, České dráhy, ÖBB …</p>
  <div class="chart"><canvas id="c-car"></canvas></div></div>
 <div class="card"><h2>Štruktúra meškaní podľa dopravcu</h2>
  <p class="muted">Podiel pozorovaní v pásmach meškania (%)</p>
  <div class="chart"><canvas id="c-carb"></canvas></div></div>

 <div class="card wide" id="s-hour"><h2>Meškanie podľa hodiny dňa</h2>
  <p class="muted">Priemerné meškanie (stĺpce) a podiel meškaní nad 5 min (čiara), miestny čas</p>
  <div class="chart"><canvas id="c-hour"></canvas></div></div>
 <div class="card wide"><h2>Heatmapa: deň v týždni × hodina</h2>
  <p class="muted">Priemerné meškanie v minútach. Sivé bunky = zatiaľ bez dát.</p>
  <div class="heat" id="heat"></div></div>

 <div class="card" id="s-trains"><h2>Najviac meškajúce vlaky teraz</h2>
  <p class="muted">Aktuálne meškanie v minútach</p>
  <div class="chart tall"><canvas id="c-top"></canvas></div></div>
 <div class="card"><h2>Najväčšie meškania v období</h2>
  <p class="muted">Maximálne meškanie jazdy vlaku</p>
  <div class="chart tall"><canvas id="c-max"></canvas></div></div>
 <div class="card wide"><h2>Zmena meškania počas jazdy</h2>
  <p class="muted">Rozdiel medzi koncom a začiatkom sledovania (kladné = meškanie narástlo, záporné = vlak dobehol)</p>
  <div class="chart tall"><canvas id="c-change"></canvas></div></div>

 <div class="card" id="s-st"><h2>Najviac vlakov pri jednej stanici</h2>
  <p class="muted">Počet vlakov podľa poslednej stanice (teraz)</p>
  <div class="chart"><canvas id="c-stn"></canvas></div></div>
 <div class="card"><h2>Stanice s najvyšším meškaním</h2>
  <p class="muted">Priemerné meškanie vlakov hlásených pri stanici (min. 3 pozorovania)</p>
  <div class="chart"><canvas id="c-std"></canvas></div></div>

 <div class="card" id="s-prob"><h2>Narušenia prevádzky</h2>
  <p class="muted">Vlaky s príznakom narušenia v sledovanom období</p><div id="t-disr"></div></div>
 <div class="card"><h2>Náhradná autobusová doprava</h2>
  <p class="muted">Aktuálne úseky a vlaky s NAD</p><div id="t-nad"></div></div>
 <div class="card"><h2>Vlaky bez zmeny polohy</h2>
  <p class="muted">Poloha v API je posledná stanica, preto ide len o orientačný údaj</p><div id="t-stuck"></div></div>
</div>
</div>
<p class="muted" id="nodata" style="display:none">Štatistiky sa zobrazia po prvých uložených snímkoch (spusti <code>collect</code> alebo <code>run</code>).</p>
</main>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<script>
const T = __DATA__;
const D = __DASH__;
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const col = d => d >= 15 ? '#b71c1c' : d >= 6 ? '#ef6c00' : d >= 1 ? '#f9a825' : '#2e7d32';

/* ---------- mapa ---------- */
const map = L.map('map').setView([48.7, 19.5], 8);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
  {maxZoom: 18, attribution: '&copy; OpenStreetMap'}).addTo(map);
const layer = L.layerGroup().addTo(map);
function draw() {
  const min = +document.getElementById('flt').value;
  layer.clearLayers(); let n = 0;
  T.filter(t => t.lat != null && t.delay >= min).forEach(t => {
    n++;
    const m = L.circleMarker([t.lat, t.lon], {radius: 7, color: '#222', weight: 1,
      fillColor: col(t.delay), fillOpacity: .9});
    m.bindPopup('<b>' + esc(t.name) + '</b><br>' + esc(t.carrier) + '<br>Meškanie: ' + t.delay +
      ' min<br>' + esc(t.text) + (t.disr ? '<br><b>Narušenie prevádzky</b>' : ''));
    if (t.route) L.polyline(t.route, {color: col(t.delay), weight: 2, opacity: .5}).addTo(layer);
    m.addTo(layer);
  });
  document.getElementById('cnt').textContent = n + ' vlakov na mape';
}
document.getElementById('flt').onchange = draw; draw();

/* ---------- grafy ---------- */
if (!D) { document.getElementById('nodata').style.display = 'block'; }
else { document.getElementById('dash').style.display = 'block'; dashboard(); }

function dashboard() {
  const css = getComputedStyle(document.documentElement);
  const v = n => css.getPropertyValue(n).trim();
  Chart.defaults.color = v('--muted');
  Chart.defaults.borderColor = v('--grid');
  Chart.defaults.font.family = 'system-ui,-apple-system,Segoe UI,sans-serif';
  Chart.defaults.maintainAspectRatio = false;
  const mk = (id, cfg) => new Chart(document.getElementById(id), cfg);
  const BLUE = '#2f80ed', ORANGE = '#ef6c00';
  const bandNames = ['včas / náskok', '1–5 min', '6–14 min', '15+ min'];
  const bandCols = ['#2e7d32', '#f9a825', '#ef6c00', '#b71c1c'];

  document.getElementById('period').textContent =
    'Obdobie: posledných ' + D.hours + ' h, snímkov: ' + D.n_snaps +
    (D.first ? ' (od ' + D.first + ')' : '') + ', posledný: ' + D.last;

  const k = D.kpi;
  const cards = [['Vlakov teraz', k.n], ['Priemer (min)', k.avg], ['Medián (min)', k.med],
    ['Meškanie > 5 min', k.p5 + ' %'], ['Meškanie > 10 min', k.p10 + ' %'],
    ['Narušenia', k.disr], ['Vlaky s NAD', k.nad]];
  document.getElementById('s-kpi').innerHTML =
    cards.map(c => '<div class="kpi"><span class="muted">' + esc(c[0]) + '</span><b>' + esc(c[1]) + '</b></div>').join('');

  // vývoj v čase
  mk('c-series', {type: 'line', data: {labels: D.series.labels, datasets: [
    {label: 'Priemerné meškanie (min)', data: D.series.avg, borderColor: BLUE, backgroundColor: BLUE,
     pointRadius: 0, tension: .25, yAxisID: 'y'},
    {label: 'Podiel > 5 min (%)', data: D.series.p5, borderColor: ORANGE, backgroundColor: ORANGE,
     pointRadius: 0, tension: .25, yAxisID: 'y1'}]},
   options: {interaction: {mode: 'index', intersect: false}, scales: {
     x: {ticks: {maxTicksLimit: 12}},
     y: {title: {display: true, text: 'min'}, beginAtZero: true},
     y1: {position: 'right', title: {display: true, text: '%'}, beginAtZero: true, max: 100, grid: {drawOnChartArea: false}}}}});
  mk('c-count', {type: 'line', data: {labels: D.series.labels, datasets: [
    {label: 'Vlakov v sieti', data: D.series.n, borderColor: '#2e7d32', backgroundColor: 'rgba(46,125,50,.15)',
     fill: true, pointRadius: 0, tension: .25}]},
   options: {plugins: {legend: {display: false}}, scales: {x: {ticks: {maxTicksLimit: 8}}, y: {beginAtZero: true}}}});
  mk('c-hist', {type: 'bar', data: {labels: D.hist.labels, datasets: [
    {data: D.hist.values, backgroundColor: ['#2e7d32', '#2e7d32', '#f9a825', '#f9a825', '#ef6c00', '#ef6c00', '#b71c1c', '#b71c1c']}]},
   options: {plugins: {legend: {display: false}}, scales: {y: {beginAtZero: true, title: {display: true, text: 'počet vlakov'}}}}});

  // kategórie a dopravcovia
  const avgBar = (id, g, horiz) => mk(id, {type: 'bar', data: {labels: g.labels, datasets: [
    {label: 'Priemerné meškanie (min)', data: g.avg, backgroundColor: g.avg.map(col)}]},
    options: {indexAxis: horiz ? 'y' : 'x', plugins: {legend: {display: false},
      tooltip: {callbacks: {afterLabel: c => 'pozorovaní: ' + g.n[c.dataIndex] + ', > 5 min: ' + g.p5[c.dataIndex] + ' %'}}},
      scales: {[horiz ? 'x' : 'y']: {beginAtZero: true, title: {display: true, text: 'min'}}}}});
  const stackBar = (id, g, horiz) => mk(id, {type: 'bar', data: {labels: g.labels,
    datasets: bandNames.map((n, i) => ({label: n, data: g.b[i], backgroundColor: bandCols[i]}))},
    options: {indexAxis: horiz ? 'y' : 'x', scales: {x: {stacked: true, max: horiz ? 100 : undefined},
      y: {stacked: true, max: horiz ? undefined : 100}}}});
  avgBar('c-typ', D.typ, false); stackBar('c-typb', D.typ, false);
  avgBar('c-car', D.carrier, true); stackBar('c-carb', D.carrier, true);

  // hodina dňa
  const hl = [...Array(24).keys()].map(h => String(h).padStart(2, '0') + ':00');
  mk('c-hour', {type: 'bar', data: {labels: hl, datasets: [
    {type: 'bar', label: 'Priemerné meškanie (min)', data: D.hour.avg, backgroundColor: D.hour.avg.map(x => col(x ?? 0)), yAxisID: 'y'},
    {type: 'line', label: 'Podiel > 5 min (%)', data: D.hour.p5, borderColor: BLUE, backgroundColor: BLUE, tension: .25, yAxisID: 'y1'}]},
   options: {scales: {y: {beginAtZero: true, title: {display: true, text: 'min'}},
     y1: {position: 'right', beginAtZero: true, max: 100, title: {display: true, text: '%'}, grid: {drawOnChartArea: false}}}}});

  // heatmapa
  const days = ['Po', 'Ut', 'St', 'Št', 'Pi', 'So', 'Ne'];
  let h = '<table><thead><tr><th></th>' + hl.map(x => '<th>' + x.slice(0, 2) + '</th>').join('') + '</tr></thead><tbody>';
  D.heat.forEach((row, i) => {
    h += '<tr><th>' + days[i] + '</th>' + row.map(x => {
      if (x === null) return '<td style="background:var(--grid)"></td>';
      const a = Math.min(1, .15 + x / 20);
      return '<td style="background:' + col(x) + ';color:#111;opacity:' + a.toFixed(2) + '" title="' + x + ' min">' + Math.round(x) + '</td>';
    }).join('') + '</tr>';
  });
  document.getElementById('heat').innerHTML = h + '</tbody></table>';

  // vlaky
  const hBar = (id, rows, key, label, colorFn) => mk(id, {type: 'bar', data: {labels: rows.map(r => r.name),
    datasets: [{label: label, data: rows.map(r => r[key]), backgroundColor: rows.map(r => colorFn(r[key]))}]},
    options: {indexAxis: 'y', plugins: {legend: {display: false}}, scales: {x: {beginAtZero: true}}}});
  hBar('c-top', D.top_now, 'v', 'Meškanie (min)', col);
  hBar('c-max', D.top_max, 'v', 'Max meškanie (min)', col);
  const chg = D.worse.concat(D.better.filter(b => !D.worse.some(w => w.name === b.name)));
  hBar('c-change', chg, 'v', 'Zmena (min)', x => x > 0 ? '#b71c1c' : '#2e7d32');

  // stanice
  mk('c-stn', {type: 'bar', data: {labels: D.st_now.map(r => r.name), datasets: [
    {label: 'Vlakov', data: D.st_now.map(r => r.n), backgroundColor: BLUE}]},
    options: {indexAxis: 'y', plugins: {legend: {display: false}}, scales: {x: {beginAtZero: true, ticks: {precision: 0}}}}});
  mk('c-std', {type: 'bar', data: {labels: D.st_delay.map(r => r.name), datasets: [
    {label: 'Priemerné meškanie (min)', data: D.st_delay.map(r => r.avg), backgroundColor: D.st_delay.map(r => col(r.avg))}]},
    options: {indexAxis: 'y', plugins: {legend: {display: false},
      tooltip: {callbacks: {afterLabel: c => 'pozorovaní: ' + D.st_delay[c.dataIndex].n}}}, scales: {x: {beginAtZero: true}}}});

  // tabuľky
  const table = (id, heads, rows) => {
    document.getElementById(id).innerHTML = rows.length
      ? '<table><thead><tr>' + heads.map(x => '<th>' + esc(x) + '</th>').join('') + '</tr></thead><tbody>' +
        rows.map(r => '<tr>' + r.map(c => '<td>' + esc(c) + '</td>').join('') + '</tr>').join('') + '</tbody></table>'
      : '<p class="muted">žiadne dáta</p>';
  };
  table('t-disr', ['Vlak', 'Pozorovaní s narušením'], D.disr.map(r => [r.name, r.n]));
  table('t-nad', ['Vlak', 'Od', 'Do'],
    D.nad.map(r => [r.name, r.od, r.do]).concat(D.nad_whole.map(n => [n, 'celá trasa', ''])));
  table('t-stuck', ['Vlak', 'Minút na mieste', 'Meškanie'], D.stuck.map(r => [r.name, r.min, r.delay + ' min']));
}
</script></body></html>
"""


def build_map(trains, out_path, with_routes=False, conn=None, hours=24):
    items = []
    for t in trains:
        r = to_record(t)
        if r["lat"] is None:
            continue
        item = {"name": r["nazov"], "carrier": r["dopravca"], "delay": r["delay"] or 0,
                "lat": r["lat"], "lon": r["lon"], "text": t.get("MeskaText") or "",
                "disr": r["disruption"]}
        if with_routes:
            pts = []
            for seg in t.get("Trasa") or []:
                pts += seg.get("Value") or []
            item["route"] = pts
        items.append(item)
    dash = dashboard_data(conn, hours) if conn is not None else None
    ts = datetime.now(TZ).strftime("%d.%m.%Y %H:%M")

    def js(obj):  # bezpecne vlozenie JSON do <script>
        return json.dumps(obj, ensure_ascii=False).replace("</", "<\\/")

    html = (PAGE_HTML.replace("__DATA__", js(items)).replace("__DASH__", js(dash))
            .replace("__TS__", ts).replace("__REPORT_URL__", REPORT_URL).replace("__REPO_URL__", REPO_URL))
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    return len(items)


# --------------------------------------------------------------------------
# Publikovanie na GitHub (git add/commit/push)
# --------------------------------------------------------------------------
def git(*args):
    return subprocess.run(["git", *args], capture_output=True, text=True)


def git_publish(paths, message):
    if git("rev-parse", "--is-inside-work-tree").returncode != 0:
        log.error("Nie si v git repozitari (spusti `git init` a pridaj remote).")
        return False
    git("add", *paths)
    if git("diff", "--cached", "--quiet").returncode == 0:
        log.info("Ziadne zmeny na commit.")
        return True
    c = git("commit", "-m", message)
    if c.returncode != 0:
        log.error("commit zlyhal: %s", c.stderr)
        return False
    p = git("push")
    if p.returncode != 0:
        log.error("push zlyhal: %s", p.stderr)
        return False
    log.info("Publikovane na GitHub.")
    return True


def publish(conn, args, trains=None):
    if trains is None:
        trains, _ = fetch_trains()
    os.makedirs("docs", exist_ok=True)
    with open("docs/report.md", "w", encoding="utf-8") as f:
        f.write(build_report(conn, args.hours))
    build_map(trains, "docs/index.html", with_routes=False, conn=conn, hours=args.hours)
    stamp = datetime.now(TZ).strftime("%Y-%m-%d %H:%M")
    return git_publish(["docs"], f"Aktualizacia reportu a mapy {stamp}")


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------
def main():
    load_env()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--db", default=cfg("ZSR_DB", "zsr.db"))
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("collect")
    r = sub.add_parser("run")
    r.add_argument("--interval", type=int, default=60, help="sekundy medzi snimkami (min. 30)")
    r.add_argument("--keep-days", type=int, default=30)
    r.add_argument("--publish-every", type=int, default=0, help="minuty; 0 = nepublikovat")
    r.add_argument("--hours", type=int, default=24)
    rp = sub.add_parser("report")
    rp.add_argument("--hours", type=int, default=24)
    rp.add_argument("--stuck-minutes", type=int, default=20)
    rp.add_argument("--out", default="docs/report.md")
    m = sub.add_parser("map")
    m.add_argument("--routes", action="store_true", help="vykreslit aj trasy (velky subor)")
    m.add_argument("--hours", type=int, default=24)
    m.add_argument("--out", default="docs/index.html")
    pb = sub.add_parser("publish")
    pb.add_argument("--hours", type=int, default=24)
    args = p.parse_args()

    conn = connect(args.db)
    try:
        if args.cmd == "collect":
            trains, _ = fetch_trains()
            sid, ts = store_snapshot(conn, trains)
            log.info("Ulozeny snimok %s (%d vlakov)", sid, len(trains))
        elif args.cmd == "run":
            interval = max(30, args.interval)
            session, last_pub = None, time.time()
            while True:
                try:
                    trains, session = fetch_trains(session)
                    sid, ts = store_snapshot(conn, trains)
                    log.info("Snimok %s: %d vlakov", sid, len(trains))
                    check_alerts(trains)
                    if args.publish_every and time.time() - last_pub >= args.publish_every * 60:
                        publish(conn, args, trains)
                        prune(conn, args.keep_days)
                        last_pub = time.time()
                except RuntimeError as e:
                    log.error("%s", e)
                time.sleep(interval)
        elif args.cmd == "report":
            md = build_report(conn, args.hours, args.stuck_minutes)
            os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
            with open(args.out, "w", encoding="utf-8") as f:
                f.write(md)
            log.info("Report ulozeny: %s", args.out)
        elif args.cmd == "map":
            trains, _ = fetch_trains()
            n = build_map(trains, args.out, args.routes, conn=conn, hours=args.hours)
            log.info("Mapa ulozena: %s (%d vlakov)", args.out, n)
        elif args.cmd == "publish":
            sys.exit(0 if publish(conn, args) else 1)
    except KeyboardInterrupt:
        pass
    finally:
        conn.close()


if __name__ == "__main__":
    main()
