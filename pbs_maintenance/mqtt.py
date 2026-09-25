"""MQTT connection, TLS, JSON publishing and acknowledgment handling."""
from __future__ import annotations

import json
import logging
from typing import Optional

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None


def mqtt_publish(
    *,
    host: str,
    port: int,
    topic: str,
    payload: dict,
    username: Optional[str],
    password: Optional[str],
    tls: bool,
    cafile: Optional[str],
    insecure: bool,
    client_id: str,
    retain: bool,
    logger: logging.Logger,
    timeout_sec: int = 15,
) -> None:
    if mqtt is None:
        raise RuntimeError("paho-mqtt not installed. Install with: pip install paho-mqtt")

    # Paho-mqtt 2.x supports callback_api_version; older versions don't.
    try:
        client = mqtt.Client(
            client_id=client_id,
            protocol=mqtt.MQTTv311,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        )
    except Exception:
        client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311)

    if username:
        client.username_pw_set(username, password=password)

    if tls:
        client.tls_set(ca_certs=cafile if cafile else None)
        if insecure:
            client.tls_insecure_set(True)

    published = {"ok": False}

    def on_publish(_client, _userdata, mid, *args, **kwargs):
        published["ok"] = True
        logger.info("MQTT publish acknowledged (mid=%s)", mid)

    def on_disconnect(_client, _userdata, *args, **kwargs):
        # args may contain rc/reason_code, properties, flags, etc depending on paho version
        reason = None
        if args:
            reason = args[0]
        reason = kwargs.get("reason_code", reason)
        logger.info("MQTT disconnected (reason=%s)", reason)

    try:
        client.on_publish = on_publish
        client.on_disconnect = on_disconnect
    except Exception:
        pass

    logger.info("Connecting MQTT %s:%d ...", host, port)
    client.connect(host, port, keepalive=30)

    client.loop_start()
    try:
        msg = json.dumps(payload, ensure_ascii=False)
        logger.info("Publishing MQTT topic=%s retain=%s", topic, retain)

        info = client.publish(topic, msg, qos=1, retain=retain)

        # Wait for publish (works on most paho versions). Guard with our own timeout via poll.
        try:
            info.wait_for_publish(timeout=timeout_sec)  # type: ignore[arg-type]
        except TypeError:
            pass

        import time

        deadline = time.time() + timeout_sec
        while time.time() < deadline and not info.is_published() and not published["ok"]:
            time.sleep(0.1)

        if not info.is_published() and not published["ok"]:
            raise TimeoutError(f"MQTT publish not acknowledged within {timeout_sec}s")

    finally:
        try:
            client.disconnect()
        finally:
            client.loop_stop()

    logger.info("MQTT published OK")

