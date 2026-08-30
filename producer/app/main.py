"""
Entry point: generate synthetic events and publish them to Kafka.
"""

import logging
import signal
import time

from app.config import GeneratorConfig
from app.event_generator import EventGenerator
from app.kafka_producer import EventProducer, KafkaProducerConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

_shutdown = False


def _handle_shutdown(signum, frame):
    global _shutdown
    logger.info("Received signal %s, shutting down after current batch...", signum)
    _shutdown = True


def main():
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)

    gen_config = GeneratorConfig()
    kafka_config = KafkaProducerConfig()

    generator = EventGenerator(gen_config)
    producer = EventProducer(kafka_config)

    logger.info(
        "Producing to topic=%s at bootstrap=%s (%.1f events/sec)",
        kafka_config.topic,
        kafka_config.bootstrap_servers,
        gen_config.events_per_second,
    )

    delay = 1.0 / gen_config.events_per_second if gen_config.events_per_second > 0 else 0
    last_stats_at = time.monotonic()

    while not _shutdown:
        event = generator.next_event()
        producer.send(event)

        now = time.monotonic()
        if now - last_stats_at >= 5.0:
            logger.info(
                "sent=%d delivered=%d failed=%d",
                producer.sent_count,
                producer.delivered_count,
                producer.failed_count,
            )
            last_stats_at = now

        if delay:
            time.sleep(delay)

    logger.info("Flushing remaining messages...")
    producer.flush()
    logger.info(
        "Final counts: sent=%d delivered=%d failed=%d",
        producer.sent_count,
        producer.delivered_count,
        producer.failed_count,
    )


if __name__ == "__main__":
    main()
