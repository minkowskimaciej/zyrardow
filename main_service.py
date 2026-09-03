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

    # PRE-INDEXING (Szybkie wyszukiwanie w słowniku zamiast wolnego DataFrame)
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

        # OPTYMALIZACJA WĄTKÓW (12 optymalne pod darmowe CPU Render)
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
        processed_executions = set()

        for dep in all_live_departures:
            kp_stop_id = dep.get("kp_stop_id")
            kp_line = str(dep.get("line_name", "")).strip()
            kp_time = str(dep.get("static_time", "")).strip()
            delay_minutes = dep.get("time_diff", 0)
            delay_seconds = int(delay_minutes * 60)

            trip_exec_id = dep.get("trip_execution_id")

            if kp_stop_id not in stop_map:
                continue

            my_stop_id = str(stop_map[kp_stop_id]["my_stop_id"])

            # Szybki lookup w słowniku
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

                # --- TripUpdate ---
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

            except Exception:
                continue

            # --- VehiclePosition (wyciąganie GPS z obiektu dep/veh) ---
            if trip_exec_id and trip_exec_id not in processed_executions:
                processed_executions.add(trip_exec_id)

                lat = dep.get("lat") or dep.get("latitude")
                lon = dep.get("lon") or dep.get("longitude") or dep.get("lng")
                veh_obj = dep.get("vehicle") or dep.get("execution") or {}

                if not lat and isinstance(veh_obj, dict):
                    lat = veh_obj.get("lat") or veh_obj.get("latitude")
                    lon = veh_obj.get("lon") or veh_obj.get("longitude") or veh_obj.get("lng")

                if lat and lon:
                    try:
                        entity_vp = feed_vp.entity.add()
                        entity_vp.id = f"vp_{my_trip_id}"
                        vp = entity_vp.vehicle
                        vp.trip.trip_id = my_trip_id
                        vp.vehicle.id = str(
                            (veh_obj.get("id") if isinstance(veh_obj, dict) else None)
                            or trip_exec_id
                        )
                        vp.vehicle.label = str(kp_line)
                        vp.position.latitude = float(lat)
                        vp.position.longitude = float(lon)
                        vp.timestamp = int(datetime.now().timestamp())
                    except Exception:
                        pass

        # Pobieranie pojazdów bezpośrednio z API mapy (jeśli brakowało pozycji w odjazdach)
        if len(feed_vp.entity) == 0:
            try:
                map_url = "https://pksgostynin.kiedyprzyjedzie.pl/api/vehicles"
                m_res = requests.get(map_url, headers=headers, timeout=2.0)
                if m_res.status_code == 200:
                    v_list = m_res.json()
                    if isinstance(v_list, list):
                        for v in v_list:
                            v_lat = v.get("lat") or v.get("latitude")
                            v_lon = v.get("lon") or v.get("longitude")
                            v_line = str(v.get("line_name", "")).strip()
                            if v_lat and v_lon:
                                entity_vp = feed_vp.entity.add()
                                entity_vp.id = f"vp_map_{v.get('id', time.time())}"
                                vp = entity_vp.vehicle
                                vp.vehicle.id = str(v.get("id", "bus"))
                                vp.vehicle.label = v_line
                                vp.position.latitude = float(v_lat)
                                vp.position.longitude = float(v_lon)
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
