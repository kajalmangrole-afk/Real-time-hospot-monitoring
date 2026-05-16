# import json
# import os
# from kafka import KafkaProducer, KafkaConsumer

# BASE_DIR = os.path.dirname(__file__)
# DEFAULT_BOOTSTRAP_SERVERS = os.getenv(
#     "KAFKA_BOOTSTRAP_SERVERS",
#     "kafka-d44d5dc-mangrolekaju-90fb.d.aivencloud.com:26653"
# )
# DEFAULT_SECURITY_PROTOCOL = os.getenv("KAFKA_SECURITY_PROTOCOL", "SSL")
# DEFAULT_API_VERSION = tuple(int(part) for part in os.getenv("KAFKA_API_VERSION", "2,8,1").split(","))

# TOPICS = {
#     "connection": os.getenv("KAFKA_TOPIC_CONNECTION", "connection_events"),
#     "bandwidth": os.getenv("KAFKA_TOPIC_BANDWIDTH", "bandwidth_events"),
#     "dns": os.getenv("KAFKA_TOPIC_DNS", "dns_activity_events"),
#     "anomaly": os.getenv("KAFKA_TOPIC_ANOMALY", "anomaly_events")
# }


# def _find_default_cert_path(filename):
#     candidates = [
#         os.path.join(BASE_DIR, filename),
#         os.path.join(BASE_DIR, "producer", filename),
#     ]
#     for path in candidates:
#         if os.path.exists(path):
#             return path
#     return None


# def _security_settings():
#     protocol = DEFAULT_SECURITY_PROTOCOL.upper()
#     settings = {
#         "bootstrap_servers": DEFAULT_BOOTSTRAP_SERVERS,
#         "api_version": DEFAULT_API_VERSION,
#     }

#     if protocol in ("SSL", "SASL_SSL", "SASL_PLAINTEXT"):
#         settings["security_protocol"] = protocol

#         ssl_cafile = os.getenv("KAFKA_SSL_CAFILE") or _find_default_cert_path("ca.pem")
#         ssl_certfile = os.getenv("KAFKA_SSL_CERTFILE") or _find_default_cert_path("service.cert")
#         ssl_keyfile = os.getenv("KAFKA_SSL_KEYFILE") or _find_default_cert_path("service.key")

#         if ssl_cafile:
#             settings["ssl_cafile"] = ssl_cafile
#         if ssl_certfile:
#             settings["ssl_certfile"] = ssl_certfile
#         if ssl_keyfile:
#             settings["ssl_keyfile"] = ssl_keyfile

#     if protocol.startswith("SASL"):
#         settings["sasl_mechanism"] = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
#         settings["sasl_plain_username"] = os.getenv("KAFKA_SASL_USERNAME")
#         settings["sasl_plain_password"] = os.getenv("KAFKA_SASL_PASSWORD")

#     return settings


# def get_producer():
#     settings = _security_settings()
#     settings["value_serializer"] = lambda v: json.dumps(v).encode("utf-8")
#     return KafkaProducer(**settings)


# def get_consumer(
#     topic_or_topics,
#     group_id="hotspot-pipeline-group",
#     auto_offset_reset="latest",
#     enable_auto_commit=True,
#     **kwargs,
# ):
#     settings = _security_settings()
#     settings["value_deserializer"] = lambda v: json.loads(v.decode("utf-8"))
#     settings["auto_offset_reset"] = auto_offset_reset
#     settings["enable_auto_commit"] = enable_auto_commit
#     settings["group_id"] = group_id
#     settings.update(kwargs)

#     if isinstance(topic_or_topics, str):
#         topics = [topic_or_topics]
#     else:
#         topics = list(topic_or_topics)

#     return KafkaConsumer(*topics, **settings)

import os
import json
from kafka import KafkaProducer

# -------------------------------------------------
# BASE DIRECTORY
# -------------------------------------------------
BASE_DIR = os.path.dirname(__file__)

# -------------------------------------------------
# TOPIC NAMES (YOUR AIVEN TOPICS)
# -------------------------------------------------
TOPICS = {
    "connection": "connection_events",
    "bandwidth": "bandwidth_events",
    "dns": "dns_activity_events",
    "anomaly": "anomaly_events"
}

# -------------------------------------------------
# KAFKA PRODUCER
# -------------------------------------------------
def get_producer():

    bootstrap_servers = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "kafka-d44d5dc-mangrolekaju-90fb.d.aivencloud.com:26653",
    )
    security_protocol = os.getenv("KAFKA_SECURITY_PROTOCOL", "SSL").upper()

    producer_settings = {
        "bootstrap_servers": bootstrap_servers,
        "api_version": (2, 8, 1),
        "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
    }

    if security_protocol in ("SSL", "SASL_SSL", "SASL_PLAINTEXT"):
        producer_settings["security_protocol"] = security_protocol
        producer_settings["ssl_cafile"] = os.getenv("KAFKA_SSL_CAFILE", os.path.join(BASE_DIR, "ca.pem"))

        if security_protocol == "SSL":
            producer_settings["ssl_certfile"] = os.getenv(
                "KAFKA_SSL_CERTFILE",
                os.path.join(BASE_DIR, "service.cert"),
            )
            producer_settings["ssl_keyfile"] = os.getenv(
                "KAFKA_SSL_KEYFILE",
                os.path.join(BASE_DIR, "service.key"),
            )

        if security_protocol.startswith("SASL"):
            producer_settings["sasl_mechanism"] = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
            producer_settings["sasl_plain_username"] = os.getenv("KAFKA_SASL_USERNAME")
            producer_settings["sasl_plain_password"] = os.getenv("KAFKA_SASL_PASSWORD")

    producer = KafkaProducer(**producer_settings)
    return producer