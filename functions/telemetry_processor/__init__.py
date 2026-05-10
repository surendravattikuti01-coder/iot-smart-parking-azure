"""
Azure Function - IoT Parking Sensor Telemetry Processor
=========================================================
Triggered by IoT Hub Event Hub endpoint.
Processes sensor occupancy events and updates Cosmos DB state.

Sensor payload format:
{
  "deviceId": "sensor-zone-A-001",
  "zoneId": "zone-A",
  "spaceId": "A-001",
  "occupied": true,
  "confidence": 0.98,
  "timestamp": "2026-05-10T14:32:00Z",
  "battery_pct": 87.3,
  "temperature_c": 24.1,
  "type": "occupancy"
}
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import azure.functions as func
from azure.cosmos import CosmosClient, exceptions as cosmos_exceptions
from azure.monitor.opentelemetry import configure_azure_monitor
from opentelemetry import trace

# ─── Telemetry Setup ──────────────────────────────────────
configure_azure_monitor(
    connection_string=os.environ["APPLICATIONINSIGHTS_CONNECTION_STRING"]
)
tracer = trace.get_tracer(__name__)
logger = logging.getLogger(__name__)

# ─── Cosmos DB Client (singleton) ─────────────────────────
_cosmos_client: CosmosClient | None = None
_container = None

def _get_container():
    global _cosmos_client, _container
    if _container is None:
        _cosmos_client = CosmosClient(
            url=os.environ["COSMOS_ENDPOINT"],
            credential=os.environ["COSMOS_KEY"],
        )
        db = _cosmos_client.get_database_client("ParkingPlatform")
        _container = db.get_container_client("ParkingSpaces")
    return _container


# ─── Helper: Validate Sensor Payload ──────────────────────
def validate_payload(event: dict) -> tuple[bool, str]:
    required = ["deviceId", "zoneId", "spaceId", "occupied", "timestamp"]
    for field in required:
        if field not in event:
            return False, f"Missing required field: {field}"
    if not isinstance(event["occupied"], bool):
        return False, "Field 'occupied' must be boolean"
    confidence = event.get("confidence", 1.0)
    if not 0.0 <= confidence <= 1.0:
        return False, f"Invalid confidence value: {confidence}"
    return True, "ok"


# ─── Core: Update Parking Space State ─────────────────────
def update_parking_space(event: dict, container) -> dict:
    """
    Upsert parking space document in Cosmos DB.
    Uses optimistic concurrency via ETag.
    """
    space_id = event["spaceId"]
    zone_id = event["zoneId"]
    occupied = event["occupied"]
    confidence = event.get("confidence", 1.0)
    ts = event.get("timestamp", datetime.now(timezone.utc).isoformat())

    # Build document
    doc = {
        "id": space_id,
        "zoneId": zone_id,
        "spaceId": space_id,
        "deviceId": event["deviceId"],
        "occupied": occupied,
        "confidence": confidence,
        "lastSeen": ts,
        "batteryPct": event.get("battery_pct"),
        "temperatureC": event.get("temperature_c"),
        "updatedAt": datetime.now(timezone.utc).isoformat(),
    }

    # Try read-modify-write with ETag for concurrency
    try:
        existing = container.read_item(item=space_id, partition_key=zone_id)
        etag = existing.get("_etag")
        doc["_etag"] = etag
        container.replace_item(
            item=space_id,
            body=doc,
            if_match_etag=etag,
        )
        action = "updated"
    except cosmos_exceptions.CosmosResourceNotFoundError:
        container.create_item(body=doc)
        action = "created"
    except cosmos_exceptions.CosmosAccessConditionFailedError:
        # Concurrent write — re-read and retry once
        logger.warning(f"ETag conflict for {space_id}, retrying...")
        container.upsert_item(body=doc)
        action = "upserted_after_conflict"

    logger.info(
        f"Space {space_id} in zone {zone_id}: {action} | "
        f"occupied={occupied} | confidence={confidence:.2f}"
    )
    return {"spaceId": space_id, "zoneId": zone_id, "action": action}


# ─── Zone Summary: Compute Occupancy Rate ─────────────────
def compute_zone_summary(zone_id: str, container) -> dict:
    """
    Query all spaces in a zone and compute occupancy statistics.
    Updates a ZoneSummary document.
    """
    query = """
        SELECT VALUE c FROM c
        WHERE c.zoneId = @zoneId
        AND c.id != @summaryId
    """
    items = list(container.query_items(
        query=query,
        parameters=[
            {"name": "@zoneId", "value": zone_id},
            {"name": "@summaryId", "value": f"summary-{zone_id}"},
        ],
        partition_key=zone_id,
    ))

    total = len(items)
    occupied_count = sum(1 for i in items if i.get("occupied", False))
    available = total - occupied_count
    occupancy_rate = (occupied_count / total * 100) if total > 0 else 0.0

    summary = {
        "id": f"summary-{zone_id}",
        "zoneId": zone_id,
        "totalSpaces": total,
        "occupiedSpaces": occupied_count,
        "availableSpaces": available,
        "occupancyRate": round(occupancy_rate, 2),
        "updatedAt": datetime.now(timezone.utc).isoformat(),
        "type": "zone_summary",
    }

    container.upsert_item(body=summary)
    logger.info(
        f"Zone {zone_id} summary: {occupied_count}/{total} occupied "
        f"({occupancy_rate:.1f}%)"
    )
    return summary


# ─── Azure Function Entry Point ───────────────────────────
app = func.FunctionApp()


@app.event_hub_message_trigger(
    arg_name="events",
    event_hub_name="parking-telemetry",
    connection="EVENTHUB_CONNECTION_STRING",
    cardinality="many",
    consumer_group="telemetry-processor",
)
def process_parking_telemetry(events: list[func.EventHubEvent]) -> None:
    """
    Batch-process IoT sensor events from Event Hub.
    Runs with cardinality='many' for high-throughput batch processing.
    """
    container = _get_container()
    zones_updated: set[str] = set()
    processed = failed = 0

    with tracer.start_as_current_span("process_parking_telemetry") as span:
        span.set_attribute("event_count", len(events))

        for event in events:
            try:
                raw_body = event.get_body().decode("utf-8")
                payload = json.loads(raw_body)

                # Handle both single event and array
                sensor_events = payload if isinstance(payload, list) else [payload]

                for sensor_event in sensor_events:
                    # Validate
                    valid, reason = validate_payload(sensor_event)
                    if not valid:
                        logger.warning(
                            f"Invalid payload from {sensor_event.get('deviceId', 'unknown')}: "
                            f"{reason} | raw={raw_body[:200]}"
                        )
                        failed += 1
                        continue

                    # Skip low-confidence readings
                    if sensor_event.get("confidence", 1.0) < 0.75:
                        logger.info(
                            f"Skipping low-confidence reading from "
                            f"{sensor_event['deviceId']}: {sensor_event['confidence']:.2f}"
                        )
                        continue

                    # Update space state
                    result = update_parking_space(sensor_event, container)
                    zones_updated.add(result["zoneId"])
                    processed += 1

            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error: {e} | body={event.get_body()[:200]}")
                failed += 1
            except Exception as e:
                logger.error(f"Unexpected error processing event: {e}", exc_info=True)
                failed += 1

        # Recompute zone summaries for all affected zones
        for zone_id in zones_updated:
            try:
                summary = compute_zone_summary(zone_id, container)
                span.set_attribute(f"zone.{zone_id}.occupancy_rate", summary["occupancyRate"])
            except Exception as e:
                logger.error(f"Zone summary failed for {zone_id}: {e}", exc_info=True)

        span.set_attribute("processed_count", processed)
        span.set_attribute("failed_count", failed)
        span.set_attribute("zones_updated", len(zones_updated))

        logger.info(
            f"Telemetry batch complete: processed={processed}, "
            f"failed={failed}, zones_updated={len(zones_updated)}"
        )


@app.route(route="zones/{zoneId}/availability", methods=["GET"])
def get_zone_availability(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP endpoint: Get real-time availability for a parking zone.
    GET /api/zones/{zoneId}/availability
    """
    zone_id = req.route_params.get("zoneId")
    if not zone_id:
        return func.HttpResponse(
            json.dumps({"error": "zoneId is required"}),
            status_code=400,
            mimetype="application/json",
        )

    try:
        container = _get_container()
        summary_id = f"summary-{zone_id}"
        summary = container.read_item(item=summary_id, partition_key=zone_id)

        # Remove Cosmos DB internal fields
        response_data = {k: v for k, v in summary.items() if not k.startswith("_")}
        return func.HttpResponse(
            json.dumps(response_data),
            status_code=200,
            mimetype="application/json",
            headers={"Cache-Control": "no-cache, max-age=5"},
        )
    except cosmos_exceptions.CosmosResourceNotFoundError:
        return func.HttpResponse(
            json.dumps({"error": f"Zone '{zone_id}' not found"}),
            status_code=404,
            mimetype="application/json",
        )
    except Exception as e:
        logger.error(f"Error fetching zone {zone_id}: {e}", exc_info=True)
        return func.HttpResponse(
            json.dumps({"error": "Internal server error"}),
            status_code=500,
            mimetype="application/json",
        )


@app.route(route="health", methods=["GET"])
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """Liveness probe for the function app."""
    try:
        container = _get_container()
        # Light connectivity test
        list(container.query_items("SELECT TOP 1 c.id FROM c", enable_cross_partition_query=True))
        return func.HttpResponse(
            json.dumps({"status": "healthy", "timestamp": datetime.now(timezone.utc).isoformat()}),
            status_code=200,
            mimetype="application/json",
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return func.HttpResponse(
            json.dumps({"status": "unhealthy", "error": str(e)}),
            status_code=503,
            mimetype="application/json",
        )
