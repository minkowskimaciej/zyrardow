import os
import threading
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import Flask, send_file, make_response
from google.transit import gtfs_realtime_pb2

from alerts import build_alerts_feed

app = Flask(__name__)


def create_empty_pb(filename):
    """Tworzy pusty plik PB na starcie, eliminuje błędy 404."""
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    feed.header.timestamp = int(datetime.now().timestamp())
    with open(filename, "wb") as f:
        f.write(feed.SerializeToString())


def update_loop():
    create_empty_pb("alerts.pb")
    tz_poland = ZoneInfo("Europe/Warsaw")

    print("Serwis wystartował! Moduł Alerts aktywny...\n", flush=True)

    while True:
        start_time = time.time()

        # Zapis alertów do pliku binarnego PB
        try:
            with open("alerts.pb", "wb") as f:
                f.write(build_alerts_feed())
        except Exception as e:
            print(f"Błąd podczas generowania alerts.pb: {e}", flush=True)

        exec_time = round(time.time() - start_time, 2)
        current_hour = datetime.now(tz_poland).strftime("%H:%M:%S")

        print(
            f"[{current_hour}] GTFS-RT Alerts updated | Duration: {exec_time}s",
            flush=True,
        )

        time.sleep(15)


def send_file_no_cache(filename):
    """Pomocnicza funkcja serwująca pliki .pb z wyłączonym cache'owaniem."""
    response = make_response(send_file(filename, mimetype="application/octet-stream"))
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.route("/alerts.pb")
def serve_alerts():
    if os.path.exists("alerts.pb"):
        return send_file_no_cache("alerts.pb")
    return "Trwa generowanie pliku...", 404


@app.route("/")
def index():
    return "Serwis GTFS-RT Alerts działa poprawnie!"


if __name__ == "__main__":
    t = threading.Thread(target=update_loop, daemon=True)
    t.start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
