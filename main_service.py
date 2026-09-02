import json
import threading
import time
import os
import pandas as pd
import requests
from flask import Flask, send_file
from google.transit import gtfs_realtime_pb2
from datetime import datetime

app = Flask(__name__)

def update_loop():
    print("Ladowanie mapy slupkow i plikow GTFS Static...")
    with open("stop_map_auto.json", "r", encoding="utf-8") as f:
        stop_map = json.load(f)

    csv_kwargs = {"dtype": str, "on_bad_lines": "skip", "engine": "python"}
    routes = pd.read_csv("routes.txt", **csv_kwargs)
    trips = pd.read_csv("trips.txt", **csv_kwargs)
    stop_times = pd.read_csv("stop_times.txt", **csv_kwargs)

    stop_times["static_time"] = stop_times["departure_time"].str.strip().str.slice(0, 5)
    my_gtfs = stop_times.merge(trips, on="trip_id").merge(routes, on="route_id")

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json"
    }

    print("Serwis wystartowal! Tworzenie pliku trip_updates.pb w petli...\n")

    while True:
        start_time = time.time()
        all_live_departures = []

        for kp_stop_id in stop_map.keys():
            url = f"https://pksgostynin.kiedyprzyjedzie.pl/api/departures/{kp_stop_id}"
            try:
                res = requests.get(url, headers=headers, timeout=3)
                if res.status_code == 200:
                    departures = res.json().get("rows", [])
                    for dep in departures:
                        dep["kp_stop_id"] = kp_stop_id
                        all_live_departures.append(dep)
                time.sleep(0.01)
            except Exception:
                continue

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.header.gtfs_realtime_version = "2.0"
        feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
        feed.header.timestamp = int(datetime.now().timestamp())

        matched_count = 0

        for dep in all_live_departures:
            kp_stop_id = dep.get("kp_stop_id")
            kp_line = str(dep.get("line_name", "")).strip()
            kp_time = str(dep.get("static_time", "")).strip()
            delay_minutes = dep.get("time_diff", 0)
            delay_seconds = int(delay_minutes * 60)

            if kp_stop_id not in stop_map:
                continue

            my_stop_id = stop_map[kp_stop_id]["my_stop_id"]

            matches = my_gtfs[
                (my_gtfs["route_short_name"].str.strip() == kp_line) &
                (my_gtfs["stop_id"] == my_stop_id) &
                (my_gtfs["static_time"] == kp_time)
            ]

            if not matches.empty:
                match = matches.iloc[0]
                my_trip_id = match["trip_id"]
                my_stop_sequence = int(match["stop_sequence"])

                entity = feed.entity.add()
                entity.id = f"tu_{my_trip_id}"

                trip_update = entity.trip_update
                trip_update.trip.trip_id = my_trip_id
                trip_update.trip.schedule_relationship = gtfs_realtime_pb2.TripDescriptor.SCHEDULED

                stu = trip_update.stop_time_update.add()
                stu.stop_id = my_stop_id
                stu.stop_sequence = my_stop_sequence
                stu.departure.delay = delay_seconds

                matched_count += 1

        with open("trip_updates.pb", "wb") as f:
            f.write(feed.SerializeToString())

        exec_time = round(time.time() - start_time, 2)
        current_hour = datetime.now().strftime("%H:%M:%S")
        print(f"[{current_hour}] Zaktualizowano trip_updates.pb ({matched_count} kursow) w {exec_time}s.")

        time.sleep(2)

@app.route("/trip_updates.pb")
def serve_gtfs_rt():
    if os.path.exists("trip_updates.pb"):
        return send_file("trip_updates.pb", mimetype="application/octet-stream")
    return "Trwa generowanie pliku...", 404

@app.route("/")
def index():
    return "Serwis GTFS-RT Żyrardów działa poprawnie!"

if __name__ == "__main__":
    # Uruchomienie pętli pobierającej dane w osobnym wątku
    t = threading.Thread(target=update_loop, daemon=True)
    t.start()
    
    # Uruchomienie serwera Flask na porcie przydzielonym przez Render
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
