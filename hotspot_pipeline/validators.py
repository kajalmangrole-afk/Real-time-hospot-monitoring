from jsonschema import validate, ValidationError, FormatChecker
from schemas import EVENT_SCHEMAS


def validate_event(event):
    event_type = event.get("event_type")
    if not event_type:
        raise ValueError("Missing event_type field")

    schema = EVENT_SCHEMAS.get(event_type)
    if schema is None:
        raise ValueError(f"Unknown event_type: {event_type}")

    validate(instance=event, schema=schema, format_checker=FormatChecker())
    return True


def safe_validate(event):
    try:
        validate_event(event)
        return True, None
    except (ValidationError, ValueError) as exc:
        return False, str(exc)
