import time
from datetime import datetime
from zoneinfo import ZoneInfo
from google.transit import gtfs_realtime_pb2

# ---- Konfiguracja alertów ----
ALERTS = []


def _to_timestamp(date_str: str | None) -> int | None:
    """Konwertuje czas do Epoch Timestamp lub zwraca None."""
    if not date_str:
        return None
    tz = ZoneInfo("Europe/Warsaw")
    dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=tz)
    return int(dt.timestamp())


def _is_relevant(alert_cfg: dict, now: int) -> bool:
    """Sprawdza, czy alert powinien być aktywny w pliku feedu."""
    start = _to_timestamp(alert_cfg.get("start"))
    end = _to_timestamp(alert_cfg.get("end"))
    lead_time = 3600 * 24  # 24h wyprzedzenia

    # Jeśli podano start, ukryj alert zanim minie czas wyprzedzenia
    if start and now < (start - lead_time):
        return False

    # Jeśli podano end i ten czas minął, wygaś alert
    if end and now > end:
        return False

    return True


def build_alerts_feed() -> bytes:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.header.incrementality = gtfs_realtime_pb2.FeedHeader.FULL_DATASET
    feed.header.timestamp = int(time.time())

    now = int(time.time())

    for cfg in ALERTS:
        if not _is_relevant(cfg, now):
            continue

        entity = feed.entity.add()
        entity.id = cfg["id"]
        alert = entity.alert

        period = alert.active_period.add()
        start_ts = _to_timestamp(cfg.get("start"))
        end_ts = _to_timestamp(cfg.get("end"))

        if start_ts:
            period.start = start_ts
        if end_ts:
            period.end = end_ts  # Przypisujemy tylko wtedy, gdy end nie jest None

        alert.cause = getattr(gtfs_realtime_pb2.Alert, cfg["cause"])
        alert.effect = getattr(gtfs_realtime_pb2.Alert, cfg["effect"])

        header = alert.header_text.translation.add()
        header.text = cfg["header"]
        header.language = "pl"

        desc = alert.description_text.translation.add()
        desc.text = cfg["description"]
        desc.language = "pl"

        # Dodanie powiązań z liniami (route_id)
        for route_id in cfg.get("route_ids", []):
            ie = alert.informed_entity.add()
            ie.route_id = route_id

        # Dodanie powiązań z przystankami (stop_id)
        for stop_id in cfg.get("stop_ids", []):
            ie = alert.informed_entity.add()
            ie.stop_id = stop_id

    return feed.SerializeToString()
