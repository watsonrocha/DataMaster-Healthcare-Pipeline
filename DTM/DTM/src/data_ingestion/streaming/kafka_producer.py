"""
Produtor Kafka para envio de eventos de saúde.

Gera e publica eventos simulados no tópico Kafka para consumo
pelo pipeline de Structured Streaming.
"""

import json
import logging
import time
from typing import Optional

logger = logging.getLogger(__name__)


class HealthcareKafkaProducer:
    """Produtor de eventos de saúde para Kafka."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "healthcare-events",
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self._producer = None

    def _get_producer(self):
        if self._producer is None:
            from kafka import KafkaProducer

            self._producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                max_in_flight_requests_per_connection=1,
            )
        return self._producer

    def send_event(self, event: dict, key: Optional[str] = None) -> None:
        """Envia um evento para o tópico Kafka."""
        producer = self._get_producer()
        future = producer.send(
            self.topic,
            value=event,
            key=key or event.get("patient_id"),
        )
        future.get(timeout=10)

    def send_batch(
        self,
        events: list,
        delay_ms: int = 100,
    ) -> int:
        """Envia um lote de eventos com delay simulado entre eles."""
        producer = self._get_producer()
        count = 0

        for event in events:
            producer.send(
                self.topic,
                value=event,
                key=event.get("patient_id"),
            )
            count += 1

            if delay_ms > 0 and count % 10 == 0:
                producer.flush()
                time.sleep(delay_ms / 1000.0)

        producer.flush()
        logger.info("Enviados %d eventos para %s", count, self.topic)
        return count

    def send_continuous(
        self,
        generator_fn,
        interval_seconds: float = 1.0,
        max_events: Optional[int] = None,
    ) -> int:
        """Envia eventos continuamente a partir de uma função geradora."""
        count = 0
        try:
            while max_events is None or count < max_events:
                events = generator_fn()
                if not isinstance(events, list):
                    events = [events]

                for event in events:
                    self.send_event(event)
                    count += 1

                time.sleep(interval_seconds)

        except KeyboardInterrupt:
            logger.info("Produção interrompida: %d eventos enviados", count)
        finally:
            self.close()

        return count

    def close(self) -> None:
        if self._producer:
            self._producer.flush()
            self._producer.close()
            self._producer = None
