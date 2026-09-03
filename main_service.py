import base64
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict
import json
import os
import threading
import time
from datetime import datetime
from flask import Flask, send_file
from google.transit import gtfs_realtime_pb2
import pandas as pd
import requests

app = Flask(__name__)


def create_empty_pb(filename):
    """Tworzy pusty plik PB na starcie, eliminuje błędy 404."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    feed.header.timestamp = int(datetime.now().timestamp())
    with open(filename, "wb") as f:
        f.write(feed.SerializeToString())


def fetch_stop_departures(args):
    kp_stop_id, headers = args
    url = f"https://pksgostynin.kiedyprzyjedzie.pl/api/departures/{kp_stop_id}"
    try:
        res = requests.get(url, headers=headers, timeout=2.0)
        if res.status_code == 200:
            departures = res.json().get("rows", [])
            for dep in departures:
                dep["kp_stop_id"] = kp_stop_id
            return departures
    except Exception:
        pass
    return []


def fetch_vehicle_positions_kp(headers):
    """Próbuje pobrać pozycje pojazdów z typowych endpointów mapy KP."""
    urls = [
        "https://pksgostynin.kiedyprzyjedzie.pl/api/realtime/map",
        "https://pksgostynin.kiedyprzyjedzie.pl/api/map_items",
        "https://pksgostynin.kiedyprzyjedzie.pl/api/vehicles",
        "https://pksgostynin.kiedyprzyjedzie.pl/api/realtime/vehicles"
    ]
    
    for url in urls:
        try:
            res = requests.get(url, headers=headers, timeout=2.5)
            if res.status_code == 200:
                data = res.json()
                if isinstance(data, dict):
                    # Często obiekty są w kluczach 'vehicles', 'items' lub 'rows'
                    return data.get("vehicles") or data.get("items") or data.get("rows") or []
                elif isinstance(data, list):
                    return data
        except Exception:
            continue
    return []


def update_loop():
    print("Ladowanie mapy slupkow i plikow GTFS Static...", flush=True)

    create_empty_pb("trip_updates.pb")
    create_empty_pb("vehicle_positions.pb")

    try:
        with open("stop_map_auto.json", "r", encoding="utf-8") as f:
            stop_map = json.load(f)
    except Exception as e:
        print(f"Blad ladowania stop_map_auto.json: {e}", flush=True)
        stop_map = {}

    csv_kwargs = {"dtype": str, "on_bad_lines": "skip", "engine": "python"}
    routes = pd.read_csv("routes.txt", **csv_kwargs)
    trips = pd.read_csv("trips.txt", **csv_kwargs)
    stop_times = pd.read_csv("stop_times.txt", **csv_kwargs)

    stop_times["static_time"] = (
        stop_times["departure_time"].str.strip().str.slice(0, 5)
    )

    if "route_id" in trips.columns and "route_id" in stop_times.columns:
        my_gtfs = stop_times.merge(trips, on="route_id", suffixes=("", "_y"))
    else:
        my_gtfs = stop_times.merge(trips, on="trip_id")

    if "route_short_name" not in my_gtfs.columns:
        my_gtfs = my_gtfs.merge(routes, on="route_id")

    gtfs_index = defaultdict(list)
    for _, row in my_gtfs.iterrows():
        try:
            line = str(row["route_short_name"]).strip()
            stop = str(row["stop_id"]).strip()
            time_parts = str(row["static_time"]).split(":")
            total_min = int(time_parts[0]) * 60 + int(time_parts[1])
            
            gtfs_index[(line, stop)].append({
                "trip_id": str(row["trip_id"]),
                "stop_sequence": int(row["stop_sequence"]),
                "total_min": total_min
            })
        except Exception:
            continue

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json",
    }

    print("Serwis wystartowal! Tworzenie trip_updates.pb oraz vehicle_positions.pb...\n", flush=True)

    while True:
        start_time = time.time()
        all_live_departures = []

        stop_keys = list(stop_map.keys())
        tasks = [(k, headers) for k in stop_keys]

        with ThreadPoolExecutor(max_workers=12) as executor:
            results = executor.map(fetch_stop_departures, tasks)
            for res in results:
                all_live_departures.extend(res)

        feed_tu = gtfs_realtime_pb2.FeedMessage()
        feed_tu.header.gtfs_realtime_version = "2.0"
        feed_tu.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
        feed_tu.header.timestamp = int(datetime.now().timestamp())

        feed_vp = gtfs_realtime_pb2.FeedMessage()
        feed_vp.header.gtfs_realtime_version = "2.0"
        feed_vp.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
        feed_vp.header.timestamp = int(datetime.now().timestamp())

        matched_count = 0
        active_trips_by_line = {}

        for dep in all_live_departures:
            kp_stop_id = dep.get("kp_stop_id")
            kp_line = str(dep.get("line_name", "")).strip()
            kp_time = str(dep.get("static_time", "")).strip()
            delay_minutes = dep.get("time_diff", 0)
            delay_seconds = int(delay_minutes * 60)

            if kp_stop_id not in stop_map:
                continue

            my_stop_id = str(stop_map[kp_stop_id]["my_stop_id"])
            candidates = gtfs_index.get((kp_line, my_stop_id), [])
            if not candidates:
                continue

            try:
                kp_parts = kp_time.split(":")
                kp_total_minutes = int(kp_parts[0]) * 60 + int(kp_parts[1])

                best_match = None
                min_diff = 999

                for cand in candidates:
                    diff = abs(cand["total_min"] - kp_total_minutes)
                    if diff <= 30 and diff < min_diff:
                        min_diff = diff
                        best_match = cand

                if not best_match:
                    continue

                my_trip_id = best_match["trip_id"]
                my_stop_sequence = best_match["stop_sequence"]

                entity_tu = feed_tu.entity.add()
                entity_tu.id = f"tu_{my_trip_id}"
                trip_update = entity_tu.trip_update
                trip_update.trip.trip_id = my_trip_id
                trip_update.trip.schedule_relationship = gtfs_realtime_pb2.TripDescriptor.SCHEDULED

                stu = trip_update.stop_time_update.add()
                stu.stop_id = my_stop_id
                stu.stop_sequence = my_stop_sequence
                stu.departure.delay = delay_seconds

                matched_count += 1
                active_trips_by_line[kp_line] = my_trip_id

            except Exception:
                continue

        # Pobieranie pozycji pojazdów z dedykowanego endpointu mapy
        raw_vehicles = fetch_vehicle_positions_kp(headers)
        for idx, veh in enumerate(raw_vehicles):
            lat = veh.get("lat") or veh.get("latitude") or veh.get("y")
            lon = veh.get("lon") or veh.get("longitude") or veh.get("x") or veh.get("lng")
            line_name = str(veh.get("line_name") or veh.get("line") or "").strip()

            if lat and lon:
                try:
                    entity_vp = feed_vp.entity.add()
                    entity_vp.id = f"vp_{veh.get('id', idx)}"
                    vp = entity_vp.vehicle
                    
                    if line_name in active_trips_by_line:
                        vp.trip.trip_id = active_trips_by_line[line_name]
                    
                    vp.vehicle.id = str(veh.get("id") or veh.get("side_number") or f"bus_{idx}")
                    vp.vehicle.label = line_name
                    vp.position.latitude = float(lat)
                    vp.position.longitude = float(lon)
                    vp.timestamp = int(datetime.now().timestamp())
                except Exception:
                    pass

        with open("trip_updates.pb", "wb") as f:
            f.write(feed_tu.SerializeToString())

        with open("vehicle_positions.pb", "wb") as f:
            f.write(feed_vp.SerializeToString())

        exec_time = round(time.time() - start_time, 2)
        current_hour = datetime.now().strftime("%H:%M:%S")
        print(
            f"[{current_hour}] Zaktualizowano GTFS-RT ({matched_count} kursow,"
            f" {len(feed_vp.entity)} pozycji GPS) w {exec_time}s.",
            flush=True,
        )

        time.sleep(3)


@app.route("/trip_updates.pb")
def serve_trip_updates():
    if os.path.exists("trip_updates.pb"):
        return send_file("trip_updates.pb", mimetype="application/octet-stream")
    return "Trwa generowanie pliku...", 404


@app.route("/vehicle_positions.pb")
def serve_vehicle_positions():
    if os.path.exists("vehicle_positions.pb"):
        return send_file(
            "vehicle_positions.pb", mimetype="application/octet-stream"
        )
    return "Trwa generowanie pliku...", 404


@app.route("/")
def index():
    return "Serwis GTFS-RT Żyrardów działa poprawnie!"


if __name__ == "__main__":
    t = threading.Thread(target=update_loop, daemon=True)
    t.start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
