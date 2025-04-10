#!/usr/bin/env python3
import os
import signal
import sys
import time
import threading
from config import load_config
from utils.logging import get_logger
from services.gcs_service import GCSClient
from services.kafka_service import KafkaConsumer, KafkaProducer
from processor import SurveyProcessor


def main():
    """Main entry point for the trace processor service"""
    # Initialize logger
    logger = get_logger()
    logger.info("Starting trace processor service...")

    # Load configuration
    try:
        config = load_config()
        logger.info("Configuration loaded successfully")
    except Exception as e:
        logger.fatal("Failed to load configuration", e)

    # Initialize GCS client
    try:
        gcs_client = GCSClient(logger)
        logger.info("GCS client initialized successfully")
    except Exception as e:
        logger.fatal("Failed to initialize GCS client", e)

    # Initialize Kafka producer
    try:
        producer = KafkaProducer(
            config.kafka_brokers,
            config.output_topic,
            config.kafka_username,
            config.kafka_password,
            config.kafka_auth,
            logger,
        )
        logger.info("Kafka producer initialized successfully")
    except Exception as e:
        logger.fatal("Failed to initialize Kafka producer", e)

    # Initialize processor
    try:
        survey_processor = SurveyProcessor(gcs_client, producer, logger)
        logger.info("Survey processor initialized successfully")
    except Exception as e:
        logger.fatal("Failed to initialize survey processor", e)

    # Initialize Kafka consumer
    try:
        consumer = KafkaConsumer(
            config.kafka_brokers,
            config.input_topic,
            config.consumer_group,
            config.kafka_username,
            config.kafka_password,
            config.kafka_auth,
            survey_processor.process_message,
            logger,
            config.max_retries,
            config.retry_backoff_ms,
        )
        logger.info("Kafka consumer initialized successfully")
    except Exception as e:
        logger.fatal("Failed to initialize Kafka consumer", e)

    # Setup signal handling for graceful shutdown
    shutdown_event = threading.Event()

    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}, shutting down...")
        shutdown_event.set()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Start consuming messages in a separate thread
    consumer_thread = threading.Thread(target=consumer.consume)
    consumer_thread.daemon = True
    consumer_thread.start()
    logger.info("Kafka consumer started")

    # Wait for shutdown signal
    shutdown_event.wait()

    # Graceful shutdown
    logger.info("Starting graceful shutdown...")

    try:
        # Close the consumer
        logger.info("Closing Kafka consumer...")
        consumer.close()

        # Wait for consumer thread to finish
        consumer_thread.join(timeout=30)

        # Close the producer
        logger.info("Closing Kafka producer...")
        producer.close()

        logger.info("Shutdown complete")
    except Exception as e:
        logger.error("Error during shutdown", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
