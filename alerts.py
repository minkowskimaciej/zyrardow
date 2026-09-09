import time
from google.transit import gtfs_realtime_pb2

# ---- Konfiguracja alertów ----
# Dodajesz tu nowe eventy ręcznie. Format czasu: "YYYY-MM-DD HH:MM:SS" (czas lokalny, Europe/Warsaw)
ALERTS = [
    {
        "id": "ztc-bike-race-2026",
        "start": "2026-09-13 06:00:00",
        "end":   "2026-09-13 20:00:00",
        "cause": "MAINTENANCE",
        "effect": "DETOUR",
        "header": "ŻTC Bike Race",
        "description": (
            "Linie 5 i 0 kursują z pominięciem przystanku "
            "\"Zalew Żyrardowski\" w godz. 6:00-20:00 (13.09) "
            "z powodu finału ŻTC Bike Race."
        ),
        "route_ids": ["5", "0"],
        "stop_ids": ["zalew-zyrardowski-01"],  # <-- PODMIEŃ na prawdziwy stop_id ze stops.txt
    },
    # kolejny alert dopisujesz tu jako kolejny dict w liście
]


def _to_timestamp(date_str: str) -> int:
    return int(time.mktime(time.strptime(date_str, "%Y-%m-%d %H:%M:%S")))


def _is_relevant(alert_cfg: dict, now: int) -> bool:
    """Pokazuj alert też z wyprzedzeniem, żeby appki zdążyły go zcache'ować."""
    start = _to_timestamp(alert_cfg["start"])
    end = _to_timestamp(alert_cfg["end"])
    lead_time = 3600 * 24  # pokaż się na 24h przed startem
    return (start - lead_time) <= now <= end


def build_alerts_feed() -> bytes:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    feed.header.timestamp = int(time.time())

    now = int(time.time())

    for cfg in ALERTS:
        if not _is_relevant(cfg, now):
            continue  # feed po prostu nie zawiera nieaktualnych alertów

        entity = feed.entity.add()
        entity.id = cfg["id"]
        alert = entity.alert

        period = alert.active_period.add()
        period.start = _to_timestamp(cfg["start"])
        period.end = _to_timestamp(cfg["end"])

        alert.cause = getattr(gtfs_realtime_pb2.Alert, cfg["cause"])
        alert.effect = getattr(gtfs_realtime_pb2.Alert, cfg["effect"])

        header = alert.header_text.translation.add()
        header.text = cfg["header"]
        header.language = "pl"

        desc = alert.description_text.translation.add()
        desc.text = cfg["description"]
        desc.language = "pl"

        for route_id in cfg.get("route_ids", []):
            ie = alert.informed_entity.add()
            ie.route_id = route_id
            for stop_id in (cfg.get("stop_ids") or [None]):
                if stop_id:
                    ie.stop_id = stop_id

    return feed.SerializeToString()
