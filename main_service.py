        with open("trip_updates.pb", "wb") as f:
            f.write(feed_tu.SerializeToString())
 
        with open("vehicle_positions.pb", "wb") as f:
            f.write(feed_vp.SerializeToString())
 
        match_time = round(time.time() - match_start, 2)
        exec_time = round(time.time() - start_time, 2)
        current_hour = datetime.now(tz_poland).strftime("%H:%M:%S")
 
        print(
            f"[{current_hour}] GTFS-RT updated ({len(updates_by_trip)} trips, {matched_count} stops) "
            f"| Total: {exec_time}s (fetch={fetch_time}s, match={match_time}s)",
            flush=True,
        )
 
        time.sleep(3)
 
 
def send_file_no_cache(filename):
    """Pomocnicza funkcja serwująca pliki .pb z wyłączonym cache'owaniem."""
    response = make_response(send_file(filename, mimetype="application/octet-stream"))
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response
 
 
@app.route("/trip_updates.pb")
def serve_trip_updates():
    if os.path.exists("trip_updates.pb"):
        return send_file_no_cache("trip_updates.pb")
    return "Trwa generowanie pliku...", 404
 
 
@app.route("/vehicle_positions.pb")
def serve_vehicle_positions():
    if os.path.exists("vehicle_positions.pb"):
        return send_file_no_cache("vehicle_positions.pb")
    return "Trwa generowanie pliku...", 404
 
 
@app.route("/")
def index():
    return "Serwis GTFS-RT Żyrardów działa poprawnie!"
 
 
if __name__ == "__main__":
    t = threading.Thread(target=update_loop, daemon=True)
    t.start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
