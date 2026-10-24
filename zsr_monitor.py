#!/usr/bin/env python3
"""
ZSR vlaky - monitoring meskani a polohy (neoficialne API mapa.zsr.sk).

Pouzitie:
  python zsr_monitor.py collect               # jeden snimok do SQLite
  python zsr_monitor.py run --interval 60     # slucka: zber + upozornenia (+ volitelne publikovanie)
  python zsr_monitor.py report --hours 24     # markdown report -> docs/report.md
  python zsr_monitor.py map                   # mapa vlakov (zivy snimok) -> docs/index.html
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
# Mapa (Leaflet, jeden HTML subor)
# --------------------------------------------------------------------------
MAP_HTML = """<!DOCTYPE html>
<html lang="sk"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Vlaky ZSR</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
<style>
 html,body{height:100%;margin:0;font-family:system-ui,sans-serif}
 #map{height:100%}
 .box{position:absolute;z-index:1000;background:#fff;color:#222;padding:8px 12px;border-radius:6px;
      box-shadow:0 1px 5px rgba(0,0,0,.4);font-size:13px}
 #info{top:10px;right:10px}
 #legend{bottom:20px;left:10px;line-height:1.6}
 .dot{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:6px}
</style></head><body>
<div id="map"></div>
<div id="info" class="box">
 <b>Vlaky ZSR</b><br><span id="ts"></span><br><span id="cnt"></span><br>
 Min. meskanie: <select id="flt"><option value="-99">vsetky</option><option value="1">1+</option>
 <option value="5">5+</option><option value="10">10+</option><option value="20">20+</option></select>
</div>
<div id="legend" class="box">
 <div><span class="dot" style="background:#2e7d32"></span>vcas / nahodu</div>
 <div><span class="dot" style="background:#f9a825"></span>1-5 min</div>
 <div><span class="dot" style="background:#ef6c00"></span>6-14 min</div>
 <div><span class="dot" style="background:#b71c1c"></span>15+ min</div>
</div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
const DATA = __DATA__;
document.getElementById('ts').textContent = '__TS__';
const map = L.map('map').setView([48.7, 19.5], 8);
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
  {maxZoom: 18, attribution: '&copy; OpenStreetMap'}).addTo(map);
const color = d => d >= 15 ? '#b71c1c' : d >= 6 ? '#ef6c00' : d >= 1 ? '#f9a825' : '#2e7d32';
const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
let layer = L.layerGroup().addTo(map);
function draw() {
  const min = +document.getElementById('flt').value;
  layer.clearLayers(); let n = 0;
  DATA.filter(t => t.lat != null && t.delay >= min).forEach(t => {
    n++;
    const m = L.circleMarker([t.lat, t.lon], {radius: 7, color: '#222', weight: 1,
      fillColor: color(t.delay), fillOpacity: .9});
    m.bindPopup('<b>' + esc(t.name) + '</b><br>' + esc(t.carrier) + '<br>Meskanie: ' + t.delay +
      ' min<br>' + esc(t.text) + (t.disr ? '<br><b>Narusenie prevadzky</b>' : ''));
    if (t.route) L.polyline(t.route, {color: color(t.delay), weight: 2, opacity: .5}).addTo(layer);
    m.addTo(layer);
  });
  document.getElementById('cnt').textContent = n + ' vlakov';
}
document.getElementById('flt').onchange = draw; draw();
</script></body></html>
"""


def build_map(trains, out_path, with_routes=False):
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
    ts = datetime.now(TZ).strftime("Stav: %d.%m.%Y %H:%M")
    payload = json.dumps(items, ensure_ascii=False).replace("</", "<\\/")
    html = MAP_HTML.replace("__DATA__", payload).replace("__TS__", ts)
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
    build_map(trains, "docs/index.html", with_routes=False)
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
            n = build_map(trains, args.out, args.routes)
            log.info("Mapa ulozena: %s (%d vlakov)", args.out, n)
        elif args.cmd == "publish":
            sys.exit(0 if publish(conn, args) else 1)
    except KeyboardInterrupt:
        pass
    finally:
        conn.close()


if __name__ == "__main__":
    main()
