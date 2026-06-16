"""Rider slots routes.

Admin endpoints live under ``/api/v1/ops/rider-slots`` (gated by the ADMIN_API_KEY
in app.py's auth middleware — designed for Retool). Rider endpoints live under
``/api/v1/riders/<rider_id>/slots`` (JWT-gated by default — same convention as the
other rider order routes).
"""
from aws_lambda_powertools import Logger, Tracer, Metrics

from services.rider_slots_service import RiderSlotsService, SlotError

logger = Logger()
tracer = Tracer()
metrics = Metrics()


def register_rider_slots_routes(app):
    """Register admin (ops) + rider slot routes."""

    # ----------------------------------------------------------------- ADMIN (ops, API key)
    @app.post("/api/v1/ops/rider-slots")
    @tracer.capture_method
    def ops_create_slot():
        try:
            body = app.current_event.json_body or {}
            slot = RiderSlotsService.create_slot(body)
            metrics.add_metric(name="RiderSlotCreated", unit="Count", value=1)
            return slot, 201
        except SlotError as e:
            return {"error": e.code, "message": e.message}, e.http_status
        except Exception as e:
            logger.error("Error creating rider slot", exc_info=True)
            return {"error": "CreateSlotFailed", "message": str(e)}, 500

    @app.put("/api/v1/ops/rider-slots/<slot_id>")
    @tracer.capture_method
    def ops_update_slot(slot_id: str):
        try:
            body = app.current_event.json_body or {}
            slot = RiderSlotsService.update_slot(slot_id, body)
            metrics.add_metric(name="RiderSlotUpdated", unit="Count", value=1)
            return slot, 200
        except SlotError as e:
            return {"error": e.code, "message": e.message}, e.http_status
        except Exception as e:
            logger.error(f"Error updating rider slot {slot_id}", exc_info=True)
            return {"error": "UpdateSlotFailed", "message": str(e)}, 500

    @app.delete("/api/v1/ops/rider-slots/<slot_id>")
    @tracer.capture_method
    def ops_delete_slot(slot_id: str):
        try:
            RiderSlotsService.delete_slot(slot_id)
            metrics.add_metric(name="RiderSlotDeleted", unit="Count", value=1)
            return {"message": "Slot deleted", "slotId": slot_id}, 200
        except SlotError as e:
            return {"error": e.code, "message": e.message}, e.http_status
        except Exception as e:
            logger.error(f"Error deleting rider slot {slot_id}", exc_info=True)
            return {"error": "DeleteSlotFailed", "message": str(e)}, 500

    @app.post("/api/v1/ops/rider-slots/<slot_id>/release")
    @tracer.capture_method
    def ops_release_slot(slot_id: str):
        try:
            body = app.current_event.json_body or {}
            slot = RiderSlotsService.release_slot(slot_id, body.get("releaseAt"))
            metrics.add_metric(name="RiderSlotReleased", unit="Count", value=1)
            return slot, 200
        except SlotError as e:
            return {"error": e.code, "message": e.message}, e.http_status
        except Exception as e:
            logger.error(f"Error releasing rider slot {slot_id}", exc_info=True)
            return {"error": "ReleaseSlotFailed", "message": str(e)}, 500

    @app.get("/api/v1/ops/rider-slots")
    @tracer.capture_method
    def ops_list_slots():
        """Admin view — all slots (incl. drafts) with live booked counts + settings."""
        try:
            slots = RiderSlotsService.list_all_slots()
            enriched = []
            for s in slots:
                booked = RiderSlotsService._booked_count(s.get("slotId"))
                enriched.append({
                    **s,
                    "bookedSeats": booked,
                    "availableSeats": max(0, int(s.get("totalSeats", 0)) - booked),
                })
            return {
                "slots": enriched,
                "total": len(enriched),
                "settings": RiderSlotsService.get_settings(),
            }, 200
        except Exception as e:
            logger.error("Error listing rider slots", exc_info=True)
            return {"error": "ListSlotsFailed", "message": str(e)}, 500

    # ----------------------------------------------------------------- RIDER (JWT)
    @app.get("/api/v1/riders/<rider_id>/slots/available")
    @tracer.capture_method
    def rider_available_slots(rider_id: str):
        try:
            # Defaults to T-7..T+7 in the service when from/to are omitted.
            query = app.current_event.query_string_parameters or {}
            slots = RiderSlotsService.list_available_for_rider(
                rider_id, query.get("from"), query.get("to")
            )
            profile = RiderSlotsService.get_rider_slots_profile(rider_id)
            return {"slots": slots, "total": len(slots), "bookingBanUntil": profile.get("bookingBanUntil")}, 200
        except Exception as e:
            logger.error(f"Error listing available slots for {rider_id}", exc_info=True)
            return {"error": "AvailableSlotsFailed", "message": str(e)}, 500

    @app.get("/api/v1/riders/<rider_id>/slots/my")
    @tracer.capture_method
    def rider_my_slots(rider_id: str):
        try:
            bookings = RiderSlotsService.list_rider_bookings(rider_id)
            profile = RiderSlotsService.get_rider_slots_profile(rider_id)
            return {
                "bookings": bookings,
                "total": len(bookings),
                "bookingBanUntil": profile.get("bookingBanUntil"),
            }, 200
        except Exception as e:
            logger.error(f"Error listing bookings for {rider_id}", exc_info=True)
            return {"error": "MySlotsFailed", "message": str(e)}, 500

    @app.get("/api/v1/riders/<rider_id>/slots/profile")
    @tracer.capture_method
    def rider_slots_profile(rider_id: str):
        try:
            return RiderSlotsService.get_rider_slots_profile(rider_id), 200
        except Exception as e:
            logger.error(f"Error fetching slots profile for {rider_id}", exc_info=True)
            return {"error": "ProfileFailed", "message": str(e)}, 500

    @app.post("/api/v1/riders/<rider_id>/slots/<slot_id>/book")
    @tracer.capture_method
    def rider_book_slot(rider_id: str, slot_id: str):
        try:
            booking = RiderSlotsService.book_slot(rider_id, slot_id)
            metrics.add_metric(name="RiderSlotBooked", unit="Count", value=1)
            return {"message": "Slot booked", "booking": booking}, 201
        except SlotError as e:
            return {"error": e.code, "message": e.message}, e.http_status
        except Exception as e:
            logger.error(f"Error booking slot {slot_id} for {rider_id}", exc_info=True)
            return {"error": "BookFailed", "message": str(e)}, 500

    @app.delete("/api/v1/riders/<rider_id>/slots/<slot_id>/book")
    @tracer.capture_method
    def rider_cancel_slot(rider_id: str, slot_id: str):
        try:
            RiderSlotsService.cancel_booking(rider_id, slot_id)
            metrics.add_metric(name="RiderSlotCancelled", unit="Count", value=1)
            return {"message": "Booking cancelled", "slotId": slot_id}, 200
        except SlotError as e:
            return {"error": e.code, "message": e.message}, e.http_status
        except Exception as e:
            logger.error(f"Error cancelling slot {slot_id} for {rider_id}", exc_info=True)
            return {"error": "CancelFailed", "message": str(e)}, 500
