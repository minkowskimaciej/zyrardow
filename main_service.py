import base64
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


def update_loop():
  print("Ladowanie mapy slupkow i plikow GTFS Static...", flush=True)
  with open("stop_map_auto.json", "r", encoding="utf-8") as f:
    stop_map = json.load(f)

  csv_kwargs = {"dtype": str, "on_bad_lines": "skip", "engine": "python"}
  routes = pd.read_csv("routes.txt", **csv_kwargs)
  trips = pd.read_csv("trips.txt", **csv_kwargs)
  stop_times = pd.read_csv("stop_times.txt", **csv_kwargs)

  stop_times["static_time"] = (
      stop_times["departure_time"].str.strip().str.slice(0, 5)
  )
  my_gtfs = stop_times.merge(trips, on="route_id" if "route_id" in trips else "trip_id", suffixes=('', '_y'))

  # Próba dopasowania połączonej tabeli GTFS
  if "route_short_name" not in my_gtfs.columns:
    my_gtfs = stop_times.merge(trips, on="trip_id").merge(routes, on="route_id")

  headers = {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      "Accept": "application/json",
  }

  print(
      "Serwis wystartowal! Tworzenie trip_updates.pb oraz"
      " vehicle_positions.pb...\n",
      flush=True,
  )

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

      my_stop_id = stop_map[kp_stop_id]["my_stop_id"]

      candidates = my_gtfs[
          (my_gtfs["route_short_name"].astype(str).str.strip() == kp_line)
          & (my_gtfs["stop_id"].astype(str) == str(my_stop_id))
      ].copy()

      if candidates.empty:
        continue

      try:
        kp_parts = kp_time.split(":")
        kp_total_minutes = int(kp_parts[0]) * 60 + int(kp_parts[1])

        cand_parts = candidates["static_time"].str.split(":", expand=True)
        candidates["cand_total_minutes"] = (
            cand_parts[0].astype(int) * 60 + cand_parts[1].astype(int)
        )

        candidates["time_diff_abs"] = (
            candidates["cand_total_minutes"] - kp_total_minutes
        ).abs()
        valid_candidates = candidates[candidates["time_diff_abs"] <= 30]

        if valid_candidates.empty:
          continue

        best_match = valid_candidates.loc[
            valid_candidates["time_diff_abs"].idxmin()
        ]
        my_trip_id = str(best_match["trip_id"])
        my_stop_sequence = int(best_match["stop_sequence"])

        # --- TripUpdate ---
        entity_tu = feed_tu.entity.add()
        entity_tu.id = f"tu_{my_trip_id}"
        trip_update = entity_tu.trip_update
        trip_update.trip.trip_id = my_trip_id
        trip_update.trip.schedule_relationship = (
            gtfs_realtime_pb2.TripDescriptor.SCHEDULED
        )

        stu = trip_update.stop_time_update.add()
        stu.stop_id = str(my_stop_id)
        stu.stop_sequence = my_stop_sequence
        stu.departure.delay = delay_seconds

        # --- VehiclePosition ---
        if trip_exec_id and trip_exec_id not in processed_executions:
          processed_executions.add(trip_exec_id)

          # Bezpośredni URL pobierania bez kodowania (API KiedyPrzyjedzie obsługuje raw ID)
          exec_url = f"https://pksgostynin.kiedyprzyjedzie.pl/api/trip_execution/{trip_exec_id}"

          try:
            exec_res = requests.get(exec_url, headers=headers, timeout=2)
            if exec_res.status_code == 200:
              exec_data = exec_res.json()
              vehicle_info = exec_data.get("vehicle")

              if (
                  vehicle_info
                  and "lat" in vehicle_info
                  and "lon" in vehicle_info
              ):
                entity_vp = feed_vp.entity.add()
                entity_vp.id = f"vp_{my_trip_id}"

                vp = entity_vp.vehicle
                vp.trip.trip_id = my_trip_id
                vp.position.latitude = float(vehicle_info["lat"])
                vp.position.longitude = float(vehicle_info["lon"])
                vp.timestamp = int(datetime.now().timestamp())
          except Exception:
            pass

        matched_count += 1

      except Exception:
        continue

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

    time.sleep(2)


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
